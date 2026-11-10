package cleaner

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const readdirPage = 2048

// GracePeriod is the default Grace: a build whose folder was created within this
// window is filtered out (by btime), and one whose warmest chunk was accessed
// within it is never deleted (by atime) — protecting new and recently-used builds.
const GracePeriod = 48 * time.Hour

// baseNames are the two chunk data dirs every build has, by base name.
var baseNames = []string{storage.MemfileName, storage.RootfsName}

// dataDirCandidates lists the on-disk names a data dir may have, in probe order:
// zstd (the prod default), then uncompressed, then lz4. We open these directly
// instead of readdir'ing the build dir to discover them — at worst two extra
// ENOENT opens (cheap LOOKUPs) instead of a full directory read.
func dataDirCandidates(base string) [3]string {
	return [3]string{
		base + storage.CompressionZstd.Suffix(),
		base,
		base + storage.CompressionLZ4.Suffix(),
	}
}

// openDataDir opens a build's data dir, probing the compression variants in
// order. Returns nil if none exist. Each probe is a real open() syscall (an NFS
// LOOKUP), counted whether or not it succeeds.
func (c *Cleaner) openDataDir(ctx context.Context, buildPath, base string) *os.File {
	for _, name := range dataDirCandidates(base) {
		c.OpenC.Add(1)
		c.metrics.recordOpen(ctx)
		if df, err := os.Open(filepath.Join(buildPath, name)); err == nil {
			return df
		}
	}

	return nil
}

// scanWorker classifies its slice of build IDs, appending each kept record (with
// chunks, or empty) to its shard; a build created within Grace is dropped.
func (c *Cleaner) scanWorker(ctx context.Context, ids []string, out *[]build, done *sync.WaitGroup, reqs chan<- *statReq) {
	defer done.Done()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		c.BuildsScanned.Add(1)
		start := time.Now()
		rec, live := c.scanBuild(ctx, id, reqs)
		if live {
			c.metrics.recordScanBuild(ctx, time.Since(start), rec.size)
			c.CacheBytes.Add(rec.size)
			*out = append(*out, rec)
		}
	}
}

// scanBuild classifies one build. A build whose folder was created within Grace
// (by btime) is filtered out entirely up front — it may be a new, in-progress
// build still writing its first chunks, and skipping it here also avoids a
// data-dir readdir. Otherwise it sizes the build from its chunk filenames and
// samples their warmest atime; a build with no chunks sorts coldest (warmest 0)
// so its leftover dir is reaped first. Returns (record, true), or (_, false)
// when the build is filtered out.
//
// In byte-budget mode a Grace-protected build is still sized (readdir only, no
// atime sampling) and charged to CacheBytes: it cannot be evicted, but it is on
// the bill, and a budget that ignored it would let the cache overshoot by the
// whole fresh set.
func (c *Cleaner) scanBuild(ctx context.Context, buildID string, reqs chan<- *statReq) (build, bool) {
	buildPath := filepath.Join(c.Path, buildID)

	if c.Grace > 0 {
		c.metrics.recordStatx(ctx)
		if c.buildAge(buildPath) < c.Grace {
			c.metrics.recordGraced(ctx)
			if c.MaxCacheBytes > 0 {
				c.CacheBytes.Add(c.sizeChunks(ctx, buildPath))
			}

			return build{}, false
		}
	}

	return c.sampleChunks(ctx, buildID, buildPath, reqs), true
}

// sizeChunks sums a build's on-disk bytes from its chunk filenames without
// sampling atimes — the size half of sampleChunks, for builds that only need to
// be counted, not ranked. Returns 0 for a chunkless build.
func (c *Cleaner) sizeChunks(ctx context.Context, buildPath string) uint64 {
	var size uint64
	chunks := 0
	for _, base := range baseNames {
		df := c.openDataDir(ctx, buildPath, base)
		if df == nil {
			continue
		}
		n, sz := c.readDataDir(ctx, df, nil)
		df.Close()
		size += sz
		chunks += n
	}
	if chunks == 0 {
		return 0
	}

	return size + otherFilesBytesEstimate
}

// sampleChunks sizes a build and finds its warmest (most recent) chunk atime. It
// opens each data dir directly by probing the compression variants (no build-dir
// readdir) and samples each in turn — read, sample, statx, close — so a sampled
// chunk's fd is always the dir currently in hand.
func (c *Cleaner) sampleChunks(ctx context.Context, buildID, buildPath string, reqs chan<- *statReq) build {
	var size uint64
	chunks := 0
	var warmest int64

	for _, base := range baseNames {
		// Realistically, in production we fail at most 1 open for each base
		// path, less and less so as more become zstd compressed (the first
		// choice). The alternative is to readdir the build dir to discover the
		// actual data dir. At the moment, we are trying to save ReadDirs, so
		// choosing the open() probe approach.
		df := c.openDataDir(ctx, buildPath, base)
		if df == nil {
			continue
		}
		w, n, sz := c.sampleDataDir(ctx, df, reqs)
		df.Close()
		size += sz
		chunks += n
		if w > warmest {
			warmest = w
		}
	}

	if chunks == 0 {
		return build{uuid: buildID} // no chunks → sorts coldest, reaped
	}

	size += otherFilesBytesEstimate // flat charge for the build's non-chunk blob dirs
	c.metrics.recordSample(ctx, chunks, warmest)

	c.Debug(ctx, "build scanned",
		zap.String("build", buildID),
		zap.Int("chunks", chunks),
		zap.Uint64("size", size),
		zap.Int64("age_s", ageSeconds(warmest)))

	return build{uuid: buildID, timestamp: warmest, size: size}
}

// readDataDir reads one open data dir page by page: it sums on-disk bytes from
// chunk filenames and, when reservoir is non-nil, reservoir-samples up to
// SampleMax names into it (uniform over the dir, since NFS readdir order is not
// atime order). Returns the chunk count and the summed size.
func (c *Cleaner) readDataDir(ctx context.Context, df *os.File, reservoir *[]string) (chunks int, size uint64) {
	for {
		entries, rerr := df.ReadDir(readdirPage)
		c.ReadDirC.Add(1)
		c.metrics.recordRead(ctx, len(entries))
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			size += chunkOnDiskBytes(e.Name())
			if reservoir != nil {
				if len(*reservoir) < c.SampleMaxFiles {
					*reservoir = append(*reservoir, e.Name())
				} else if j := rand.Intn(chunks + 1); j < c.SampleMaxFiles {
					(*reservoir)[j] = e.Name()
				}
			}
			chunks++
		}
		switch {
		case rerr == io.EOF:
		case rerr != nil:
			c.metrics.recordError(ctx, ValOpBuildReaddir)
			c.Info(ctx, "error reading data dir", zap.String("dir", df.Name()), zap.Error(rerr))
		case len(entries) < readdirPage:
		default:
			continue
		}

		break
	}

	return chunks, size
}

// sampleDataDir reads one open data dir: it sums on-disk bytes from chunk
// filenames, reservoir-samples up to SampleMax names (uniform over the dir, since
// NFS readdir order is not atime order), statx's the sample via the stat pool,
// and returns the warmest sampled atime, the chunk count, and the summed size.
// All statx complete before it returns, so the caller may close df immediately.
func (c *Cleaner) sampleDataDir(ctx context.Context, df *os.File, reqs chan<- *statReq) (warmest int64, chunks int, size uint64) {
	reservoir := make([]string, 0, c.SampleMaxFiles)
	chunks, size = c.readDataDir(ctx, df, &reservoir)
	if chunks == 0 {
		return 0, 0, size
	}

	// Hand the sample to the stat pool (independent statx concurrency); df stays
	// open until every response is drained below.
	k := clampSample(chunks, c.SampleMinFiles, c.SamplePercent, c.SampleMaxFiles)
	// Algorithm R leaves the reservoir uniform but not order-uniform, so shuffle
	// before taking the positional prefix of k to stat — same reasoning as list().
	if k < len(reservoir) {
		rand.Shuffle(len(reservoir), func(i, j int) { reservoir[i], reservoir[j] = reservoir[j], reservoir[i] })
	}
	responseCh := make(chan *statReq, k)
	submitted := 0
submit:
	for i := range k {
		select {
		case <-ctx.Done():
			break submit
		case reqs <- &statReq{dirf: df, name: reservoir[i], response: responseCh}:
			submitted++
		}
	}

	for range submitted {
		resp := <-responseCh
		if resp.err != nil {
			c.metrics.recordError(ctx, ValOpStat)

			continue
		}
		if resp.atime > warmest {
			warmest = resp.atime
		}
	}

	return warmest, chunks, size
}

// verifyBuild checks a build's estimates before a cold delete (FF-gated,
// expensive). Two deltas: (1) size — actual non-chunk bytes (headers/snapfile/
// metadata) vs the flat otherFilesBytes charge (chunk sizes are exact from
// filenames, not statted); (2) coldness — the sampled warmest atime vs the true
// warmest from a full stat of every chunk, to catch a sample that missed a
// warmer chunk (churn risk).
func (c *Cleaner) verifyBuild(ctx context.Context, b build) {
	buildPath := filepath.Join(c.Path, b.uuid)

	bf, err := os.Open(buildPath)
	if err != nil {
		return
	}
	entries, _ := bf.ReadDir(-1)
	bf.Close()

	// Names that are chunk data dirs (any compression) — skipped, sizes trusted.
	isData := make(map[string]bool, len(baseNames)*3)
	for _, base := range baseNames {
		for _, name := range dataDirCandidates(base) {
			isData[name] = true
		}
	}

	var otherBytes uint64
	otherFiles := 0
	var warmest int64 // true warmest chunk atime, from a full stat of every chunk
	chunks := 0
	for _, e := range entries {
		full := filepath.Join(buildPath, e.Name())
		if isData[e.Name()] {
			// Chunk data dir: stat EVERY chunk (no sampling) for the true warmest
			// atime, to check the scan's sampled coldness.
			df, derr := os.Open(full)
			if derr != nil {
				continue
			}
			for {
				chunkEntries, rerr := df.ReadDir(readdirPage)
				for _, ce := range chunkEntries {
					if ce.IsDir() {
						continue
					}
					f, serr := c.statInDir(df, ce.Name())
					if serr != nil {
						continue
					}
					if f.ATimeUnix > warmest {
						warmest = f.ATimeUnix
					}
					chunks++
				}
				if rerr != nil || len(chunkEntries) < readdirPage {
					break
				}
			}
			df.Close()

			continue
		}
		if !e.IsDir() {
			if info, serr := e.Info(); serr == nil {
				otherBytes += uint64(info.Size())
				otherFiles++
			}

			continue
		}
		sub, serr := os.Open(full)
		if serr != nil {
			continue
		}
		subEntries, _ := sub.ReadDir(-1)
		sub.Close()
		for _, f := range subEntries {
			if info, ierr := f.Info(); ierr == nil && !f.IsDir() {
				otherBytes += uint64(info.Size())
				otherFiles++
			}
		}
	}

	otherDelta := int64(otherBytes) - otherFilesBytesEstimate
	ageDelta := ageSeconds(b.timestamp) - ageSeconds(warmest)
	c.metrics.recordVerified(ctx, otherDelta, ageDelta)
	c.Debug(ctx, "delete verification",
		zap.String("build", b.uuid),
		zap.Int("chunks", chunks),
		zap.Int("other_files", otherFiles),
		zap.Uint64("other_bytes_actual", otherBytes),
		zap.Int64("other_delta_bytes", otherDelta),
		zap.Int64("sampled_warmest_age_s", ageSeconds(b.timestamp)),
		zap.Int64("actual_warmest_age_s", ageSeconds(warmest)),
		zap.Int64("age_delta_s", ageDelta))
}

// Statter serves fd-relative atime stats from the scan workers, with its own
// concurrency (statx is the NFS-latency-bound step). Exits on ctx cancellation
// or when reqs is closed.
func (c *Cleaner) Statter(ctx context.Context, done *sync.WaitGroup, reqs <-chan *statReq) {
	defer done.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-reqs:
			if !ok {
				return
			}
			c.metrics.recordStatx(ctx)
			f, err := c.statInDir(req.dirf, req.name)
			if err != nil {
				req.err = err
			} else {
				req.atime = f.ATimeUnix
			}
			req.response <- req
		}
	}
}

// buildAge returns how long ago the build dir was created, for the create-time
// filter. Prefers btime (create time); falls back to mtime, which on a build dir
// is ~creation (its data subdirs are made at build time and not modified after).
// Returns 0 (treated as fresh → filtered out) if neither is available.
func (c *Cleaner) buildAge(buildPath string) time.Duration {
	if cand, err := c.stat(buildPath); err == nil && cand.BTimeUnix > 0 {
		return time.Since(time.Unix(cand.BTimeUnix, 0))
	}
	if info, err := os.Stat(buildPath); err == nil {
		return time.Since(info.ModTime())
	}

	return 0
}

// VerifyChunksCacheRoot fails loud at startup if the given path does not look
// like a chunks-cache root. The cleaner assumes depth 1 holds build dirs; if
// invoked one level up by mistake the deletion phase would RemoveAll the whole
// template cache. Require evidence that at least one direct child is a UUID-named
// dir containing a data dir (memfile/rootfs.ext4, any compression). An empty
// root is allowed.
func VerifyChunksCacheRoot(path string) error {
	df, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open chunks-cache root %s: %w", path, err)
	}
	defer df.Close()

	sawAnyDir := false
	sawAnyUUIDDir := false
	for {
		entries, readErr := df.ReadDir(128)
		for _, e := range entries {
			if !e.IsDir() || e.Name() == "lost+found" {
				continue
			}
			sawAnyDir = true
			if _, parseErr := uuid.Parse(e.Name()); parseErr != nil {
				continue
			}
			sawAnyUUIDDir = true
			if hasDataDir(filepath.Join(path, e.Name())) {
				return nil
			}
		}
		switch {
		case readErr == io.EOF:
		case readErr != nil:
			return fmt.Errorf("read chunks-cache root %s: %w", path, readErr)
		case len(entries) < 128:
		default:
			continue
		}

		break
	}

	// No UUID build dir with a data dir. An empty root is fine, but a root holding
	// other subdirs is almost certainly the wrong path — refuse to reap it.
	if !sawAnyUUIDDir {
		if sawAnyDir {
			return fmt.Errorf("%q contains subdirectories but none are UUID-named; refusing to risk a wrong-path reap", path)
		}

		return nil
	}

	return fmt.Errorf("%q has UUID-named children but none contain a %s/ or %s/ data dir (any compression); refusing to risk a wrong-path reap", path, storage.MemfileName, storage.RootfsName)
}

// hasDataDir reports whether buildPath contains at least one chunk data dir,
// probing the compression variants by name (no readdir).
func hasDataDir(buildPath string) bool {
	for _, base := range baseNames {
		for _, name := range dataDirCandidates(base) {
			if info, err := os.Stat(filepath.Join(buildPath, name)); err == nil && info.IsDir() {
				return true
			}
		}
	}

	return false
}
