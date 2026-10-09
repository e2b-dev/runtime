package cleaner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// otherFilesBytesEstimate is the flat size charged for a build's non-chunk data
// (headers, snapfile, metadata). They are KB–MB next to GB of memfile/rootfs
// chunks, so a flat estimate keeps the byte-target math honest without statting
// them; the FF-gated verify pass measures the real error.
const otherFilesBytesEstimate = 1 << 20 // 1 MiB

type Cleaner struct {
	Options
	Counters
	logger.Logger

	metrics *Metrics

	// cacheBytesEstimate is the whole cache's estimated size after the scan:
	// CacheBytes, scaled up by rootBuilds/scanned when build sampling is on.
	cacheBytesEstimate uint64
}

type statReq struct {
	dirf     *os.File
	name     string
	response chan *statReq
	atime    int64
	err      error
}

type Options struct {
	Path string

	// Deletion stops once builds totalling TargetBytesToDelete have been queued.
	// TargetDiskUsagePercent is resolved to TargetBytesToDelete via df before the
	// run (in main); the cleaner itself only reads TargetBytesToDelete.
	TargetBytesToDelete    uint64
	TargetDiskUsagePercent float64

	// Byte budget for elastic filesystems (Amazon EFS), where df/statfs reports a
	// pseudo capacity (~8 EiB) and a percent target can never fire. When the
	// cache's estimated size (summed from chunk filenames during the scan,
	// Grace-protected builds included) exceeds MaxCacheBytes, builds are evicted
	// coldest-first until the estimate is down to it — the same single-threshold
	// rule as the percent mode. Resolved into TargetBytesToDelete after the scan,
	// the only way to size a cache whose statfs total is meaningless. 0 = off.
	MaxCacheBytes uint64

	SampleMinFiles int
	SamplePercent  int
	SampleMaxFiles int

	// Build-survey cap. When BuildSampleMax > 0, only a uniform random sample of
	// the root's build dirs is scanned each run — clamp(BuildSampleMin,
	// BuildSamplePercent%·rootCount, BuildSampleMax) of them, reservoir-sampled
	// during the (unavoidable) full root readdir. This bounds per-run readdir/stat
	// to O(sample) instead of O(builds), which matters at millions of builds. The
	// sample converges to full coverage over successive runs. 0 = scan every build.
	BuildSampleMin     int
	BuildSamplePercent int
	BuildSampleMax     int

	// Grace is one age threshold used as two clocks against a single value.
	// First, a build whose folder was created less than Grace ago (btime) is
	// filtered out entirely up front — protecting new, in-progress builds. Second,
	// among the builds that remain, one is never deleted if its warmest sampled
	// chunk was accessed less than Grace ago (atime) — the anti-churn floor for an
	// old build that was recently resumed. Tests set it to 0 to disable both.
	Grace time.Duration

	DryRun bool
	// MaxConcurrentScan bounds parallel build readdirs; MaxConcurrentStat bounds
	// in-flight statx (the NFS-latency-bound part); MaxConcurrentDelete bounds
	// parallel RemoveAll. Tuned independently.
	MaxConcurrentScan   int
	MaxConcurrentStat   int
	MaxConcurrentDelete int

	// Verify (FF-gated, off by default): before each cold deletion, full-stat the
	// build and log/record its actual on-disk size and age vs the estimate, to
	// validate the filename-size and atime-sample heuristics.
	Verify bool
}

type Counters struct {
	BuildsScanned atomic.Int64
	Deleted       atomic.Int64 // builds deleted to hit the byte target (coldest first)
	BytesFreed    atomic.Uint64
	// CacheBytes is the summed on-disk size of every build the scan sized this
	// run — eviction candidates plus (in byte-budget mode) Grace-protected builds.
	// With build sampling on it covers the sample only; see cacheBytesEstimate.
	CacheBytes atomic.Uint64

	OpenC    atomic.Int64
	ReadDirC atomic.Int64
	StatxC   atomic.Int64
}

// build is the compact record kept for every build: its coldness (the warmest —
// most recent — sampled chunk atime) and its on-disk size. ~tens of bytes;
// builds are orders of magnitude fewer than chunks, so the whole set fits in
// memory.
type build struct {
	uuid      string
	timestamp int64
	size      uint64
}

// Stat is the metadata from one statx — built, read, and discarded (we no longer
// keep these for the duration of the run). BTimeUnix is 0 for fd-relative chunk
// stats, which don't request btime.
type Stat struct {
	ATimeUnix int64
	BTimeUnix int64
}

var ErrUsage = errors.New("usage: clean-nfs-cache <path> [<options>]")

func NewCleaner(opts Options, log logger.Logger, metrics *Metrics) *Cleaner {
	if metrics == nil {
		metrics = NoopMetrics()
	}

	return &Cleaner{Options: opts, Logger: log, metrics: metrics}
}

// Validate checks the options for internal consistency. Clean runs it, and main
// runs it up front so a bad flag combination fails before the "nothing to do"
// early exit can hide it.
func (o Options) Validate() error {
	if o.Path == "" {
		return ErrUsage
	}
	var errs []error
	if o.TargetBytesToDelete == 0 && o.TargetDiskUsagePercent == 0 && o.MaxCacheBytes == 0 {
		errs = append(errs, errors.New("one of target-bytes-to-delete, disk-usage-target-percent or max-cache-bytes must be set"))
	}
	if o.MaxCacheBytes > 0 && o.TargetBytesToDelete > 0 {
		errs = append(errs, errors.New("max-cache-bytes and target-bytes-to-delete are mutually exclusive"))
	}
	if o.SampleMinFiles <= 0 {
		errs = append(errs, errors.New("sample-min must be > 0"))
	}
	if o.SampleMaxFiles < o.SampleMinFiles {
		errs = append(errs, errors.New("sample-max must be >= sample-min"))
	}
	if o.SamplePercent < 0 || o.SamplePercent > 100 {
		errs = append(errs, errors.New("sample-pct must be in [0, 100]"))
	}
	if o.BuildSamplePercent < 0 || o.BuildSamplePercent > 100 {
		errs = append(errs, errors.New("build-sample-pct must be in [0, 100]"))
	}
	if o.TargetDiskUsagePercent < 0 || o.TargetDiskUsagePercent > 100 {
		errs = append(errs, errors.New("disk-usage-target-percent must be in [0, 100]"))
	}
	if o.MaxConcurrentScan <= 0 {
		errs = append(errs, errors.New("max-concurrent-scan must be > 0"))
	}
	if o.MaxConcurrentStat <= 0 {
		errs = append(errs, errors.New("max-concurrent-stat must be > 0"))
	}
	if o.MaxConcurrentDelete <= 0 {
		errs = append(errs, errors.New("max-concurrent-delete must be > 0"))
	}

	return errors.Join(errs...)
}

func (c *Cleaner) Clean(ctx context.Context) error {
	if err := c.Options.Validate(); err != nil {
		return err
	}

	c.OpenC.Add(1)
	c.metrics.recordOpen(ctx)
	root, err := os.Open(c.Path)
	if err != nil {
		return fmt.Errorf("open cache root %s: %w", c.Path, err)
	}
	defer root.Close()

	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	picks, total, err := c.list(ctx, root)
	if err != nil {
		return err
	}
	builds := c.scan(ctx, picks)
	c.sizeCache(ctx, total, len(picks))
	c.deleteColdest(ctx, builds)

	dur := time.Since(start)
	c.metrics.recordLastRunDuration(ctx, dur)
	c.Info(ctx, "clean complete",
		zap.Int64("deleted_builds", c.Deleted.Load()),
		zap.Uint64("bytes_freed_estimate", c.BytesFreed.Load()),
		zap.Uint64("cache_bytes_estimate", c.cacheBytesEstimate),
		zap.Duration("duration", dur))

	return nil
}

// sizeCache estimates the whole cache's on-disk bytes from what the scan sized.
// With build sampling on, only `scanned` of `rootBuilds` were sized, so the sum
// is scaled up by rootBuilds/scanned (the sample is uniform). In byte-budget mode
// it then resolves the budget into TargetBytesToDelete: nothing when the estimate
// is at or under MaxCacheBytes, otherwise the excess. Deletion can only pick from
// the scanned builds, so a sampled run may under-delete and converge over
// successive runs, like the percent mode.
func (c *Cleaner) sizeCache(ctx context.Context, rootBuilds, scanned int) {
	est := c.CacheBytes.Load()
	if scanned > 0 && scanned < rootBuilds {
		est = uint64(float64(est) * float64(rootBuilds) / float64(scanned))
	}
	c.cacheBytesEstimate = est
	c.metrics.recordLastCacheBytes(ctx, est)

	if c.MaxCacheBytes == 0 {
		return
	}
	if est > c.MaxCacheBytes {
		c.TargetBytesToDelete = est - c.MaxCacheBytes
	}
	c.Info(ctx, "byte budget resolved",
		zap.Uint64("cache_bytes_estimate", est),
		zap.Uint64("cache_bytes_sampled", c.CacheBytes.Load()),
		zap.Uint64("max_cache_bytes", c.MaxCacheBytes),
		zap.Uint64("target_bytes_to_delete", c.TargetBytesToDelete))
}

// list reads the whole cache root — the one unavoidable full readdir — and
// returns the build dirs to scan this run (the list phase). With BuildSampleMax > 0
// it keeps only a uniform random sample, clamp(BuildSampleMin,
// BuildSamplePercent%·rootCount, BuildSampleMax) builds, reservoir-sampled as names
// stream by so memory stays O(sample) not O(rootCount); the per-build work
// downstream is then bounded too. A fixed prefix would starve the tail (NFS readdir
// order is server-hash, not stable), so the sample is random. BuildSampleMax == 0
// returns every build. Also returns the root's total build-dir count, so a sampled
// scan can be scaled back up to the whole cache. A genuine readdir error aborts
// the run (returns the error) rather than cleaning off a truncated view of the root.
func (c *Cleaner) list(ctx context.Context, df *os.File) ([]string, int, error) {
	start := time.Now()
	capN := c.BuildSampleMax
	var reservoir []string
	if capN > 0 {
		reservoir = make([]string, 0, capN)
	}
	total := 0
	for ctx.Err() == nil {
		entries, err := df.ReadDir(readdirPage)
		c.ReadDirC.Add(1)
		c.metrics.recordRead(ctx, len(entries))
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			total++
			if capN <= 0 || len(reservoir) < capN {
				reservoir = append(reservoir, e.Name())
			} else if j := rand.Intn(total); j < capN { // Algorithm R: replace with prob capN/total
				reservoir[j] = e.Name()
			}
		}
		switch {
		case err == io.EOF:
		case err != nil:
			c.metrics.recordError(ctx, ValOpRootReaddir)

			return nil, 0, fmt.Errorf("read cache root %s: %w", c.Path, err)
		case len(entries) < readdirPage:
			// EOF: no more entries, exit the loop
		default:
			continue
		}

		break
	}

	// Reservoir holds a uniform sample of size min(total, capN); narrow it to the
	// banded target. The reservoir is uniform but not order-uniform, so shuffle
	// before taking a positional prefix.
	picks := reservoir
	if capN > 0 {
		x := clampSample(total, c.BuildSampleMin, c.BuildSamplePercent, capN)
		if x < len(picks) {
			rand.Shuffle(len(picks), func(i, j int) { picks[i], picks[j] = picks[j], picks[i] })
			picks = picks[:x]
		}
	}

	dur := time.Since(start)
	c.metrics.recordPhase(ctx, ValPhaseList, dur)
	c.metrics.recordLastListBuilds(ctx, total)
	c.Info(ctx, "root listing complete",
		zap.Int("builds", total), zap.Int("picked", len(picks)), zap.Duration("duration", dur))

	return picks, total, nil
}

// scan classifies the picked build dirs concurrently, returning the records
// eligible for deletion (chunkless builds sort coldest; their leftover dirs are
// reaped first).
func (c *Cleaner) scan(ctx context.Context, picks []string) []build {
	start := time.Now()

	// reqs carries fd-relative stat requests from the scanners to the Statter pool.
	// Unbuffered: a successful send means a Statter has taken it.
	reqs := make(chan *statReq)
	var statters sync.WaitGroup
	for range c.MaxConcurrentStat {
		statters.Add(1)
		go c.Statter(ctx, &statters, reqs)
	}

	// Partition the picks across scanners; each appends to its own shard (no lock).
	shards := make([][]build, c.MaxConcurrentScan)
	var scanners sync.WaitGroup
	for w := range c.MaxConcurrentScan {
		lo := w * len(picks) / c.MaxConcurrentScan
		hi := (w + 1) * len(picks) / c.MaxConcurrentScan
		scanners.Add(1)
		go c.scanWorker(ctx, picks[lo:hi], &shards[w], &scanners, reqs)
	}

	scanners.Wait() // every picked build classified; scanners were the only Statter clients
	close(reqs)     // so the Statters drain and exit
	statters.Wait()

	var builds []build
	for _, s := range shards {
		builds = append(builds, s...)
	}

	dur := time.Since(start)
	c.metrics.recordPhase(ctx, ValPhaseScan, dur)
	c.Info(ctx, "scan complete",
		zap.Int64("builds_scanned", c.BuildsScanned.Load()),
		zap.Int("live_builds", len(builds)),
		zap.Int64("open_ops", c.OpenC.Load()),
		zap.Int64("readdir_ops", c.ReadDirC.Load()),
		zap.Int64("statx_ops", c.StatxC.Load()),
		zap.Duration("duration", dur))

	return builds
}

// deleteColdest runs the delete phase: it spawns the delete pool and feeds it the
// coldest builds, coldest-first, until the byte target is reached or the Grace
// floor is crossed, then waits for the pool to drain.
func (c *Cleaner) deleteColdest(ctx context.Context, builds []build) {
	if c.TargetBytesToDelete == 0 {
		c.Info(ctx, "no byte target, nothing to delete",
			zap.Uint64("cache_bytes_estimate", c.cacheBytesEstimate),
			zap.Uint64("max_cache_bytes", c.MaxCacheBytes))

		return
	}
	start := time.Now()

	deleteCh := make(chan build, 1024)
	var deleters sync.WaitGroup
	for range c.MaxConcurrentDelete {
		deleters.Add(1)
		go c.deleteWorker(ctx, deleteCh, &deleters)
	}

	// coldest first = lowest warmest-atime first.
	slices.SortFunc(builds, func(a, b build) int {
		return cmp.Compare(a.timestamp, b.timestamp)
	})

	var floorSec int64
	if c.Grace > 0 {
		floorSec = time.Now().Add(-c.Grace).Unix()
	}
	var queued uint64
	var queuedBuilds int
queueLoop:
	for _, b := range builds {
		if queued >= c.TargetBytesToDelete {
			break
		}
		if floorSec != 0 && b.timestamp > floorSec {
			// Sorted coldest-first, so everything remaining is warmer than the
			// floor — stop rather than delete an active build.
			c.Info(ctx, "reached age floor before target; under-deleting",
				zap.Uint64("queued_bytes", queued),
				zap.Uint64("target_bytes", c.TargetBytesToDelete))

			break
		}
		select {
		case deleteCh <- b:
			queued += b.size
			queuedBuilds++
		case <-ctx.Done():
			break queueLoop
		}
	}

	close(deleteCh)
	deleters.Wait()

	met := queued >= c.TargetBytesToDelete

	// Emit only when the run came up short: it deleted everything it was allowed to
	// (blocked by the grace floor or simply out of builds — same actionable fact)
	// and still didn't hit the target. Any non-zero value is the signal.
	if !met {
		c.metrics.recordUnderTarget(ctx)
	}
	c.metrics.recordPhase(ctx, ValPhaseDelete, time.Since(start))
	// One line with the whole decision — what was selected and the inputs that
	// drove it — so a dry run is auditable from a single log entry.
	c.Info(ctx, "deletion selected",
		zap.Bool("dry_run", c.DryRun),
		zap.Bool("target_met", met),
		zap.Int("num_builds", queuedBuilds),
		zap.Uint64("queued_bytes", queued),
		zap.Uint64("target_bytes", c.TargetBytesToDelete),
		zap.Uint64("cache_bytes_estimate", c.cacheBytesEstimate),
		zap.Uint64("max_cache_bytes", c.MaxCacheBytes),
		zap.Duration("grace", c.Grace))
}

// clampSample returns the per-build sample size for n chunks.
func clampSample(n, sampleMin, samplePct, sampleMax int) int {
	k := (n*samplePct + 99) / 100 // ceil(n·pct/100)
	k = max(k, sampleMin)
	k = min(k, sampleMax)
	k = min(k, n)

	return k
}

// chunkOnDiskBytes returns a cache chunk's on-disk byte size, parsed from its
// filename's size field. Two formats (see storage_cache_seekable.go and
// storage_cache_seekable_compressed.go):
//
//	uncompressed: "{offset}-{size}.bin"   size is decimal (= on-disk, uncompressed)
//	compressed:   "{offset}-{size}.frm"   size is hex (= on-disk frame length)
//
// The compressed cache asserts the frame file's size equals that length, so for
// both formats the name gives the exact on-disk size. Non-chunk files (size.txt,
// temp files) return 0 — negligible next to GB of chunks.
func chunkOnDiskBytes(name string) uint64 {
	var base string
	radix := 10
	switch {
	case strings.HasSuffix(name, ".bin"):
		base = strings.TrimSuffix(name, ".bin")
	case strings.HasSuffix(name, ".frm"):
		base = strings.TrimSuffix(name, ".frm")
		radix = 16
	default:
		return 0
	}
	i := strings.LastIndexByte(base, '-')
	if i < 0 {
		return 0
	}
	n, err := strconv.ParseUint(base[i+1:], radix, 64)
	if err != nil {
		return 0
	}

	return n
}

// ElasticTotalThreshold is the df total above which a filesystem is treated as
// elastic (no provisioned capacity). Amazon EFS reports 2^63 bytes (8 EiB); no
// real disk under this cache is anywhere near 1 PiB.
const ElasticTotalThreshold uint64 = 1 << 50

type DiskInfo struct {
	Total, Used uint64
}

// Elastic reports whether the df total is a pseudo capacity (EFS and the like),
// on which a disk-usage percent target can never fire.
func (d DiskInfo) Elastic() bool {
	return d.Total > ElasticTotalThreshold
}

func GetDiskInfo(ctx context.Context, path string) (DiskInfo, error) {
	// -P (POSIX) keeps every filesystem on one line. Without it GNU df wraps a
	// mount source longer than 20 characters onto its own line — every EFS DNS
	// name is — and the numbers end up on the next line, shifted by one field.
	// -k pins the unit to 1024-byte blocks: under POSIXLY_CORRECT, -P alone
	// switches to 512-byte blocks and the parser would read half the size.
	cmd := exec.CommandContext(ctx, "df", "-Pk", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return DiskInfo{}, fmt.Errorf("df command failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	return parseDiskInfo(string(out))
}

// parseDiskInfo reads the first data row of `df -Pk` output. Sizes are parsed as
// uint64: an EFS total (2^53 KiB) times 1024 is exactly 2^63, which overflows
// int64 to a negative number and silently zeroes the percent math.
func parseDiskInfo(out string) (DiskInfo, error) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return DiskInfo{}, fmt.Errorf("unexpected df output: %q", strings.TrimSpace(out))
	}

	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		totalSize, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return DiskInfo{}, fmt.Errorf("failed to parse total size: %w", err)
		}

		usedSpace, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return DiskInfo{}, fmt.Errorf("failed to parse available space: %w", err)
		}

		// "df" returns kilobytes, not bytes
		return DiskInfo{Total: totalSize * 1024, Used: usedSpace * 1024}, nil
	}

	return DiskInfo{}, fmt.Errorf("could not parse mount point from df output: %q", strings.TrimSpace(out))
}
