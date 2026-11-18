package cleaner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/cacheindex"
)

// Index modes, set by the indexMode key of the cleaner's flag.
//
//   - off: the sampling cleaner, unchanged.
//   - index: walk the last-use index from the coldest build up to the grace
//     floor. The index only proposes candidates; the same on-disk check the
//     sampling scan uses (folder age, sampled chunk atimes) decides each
//     delete. When the index runs out or Redis fails, sampling frees the rest.
const (
	IndexOff = "off"
	IndexOn  = "index"
)

const indexPage = 1000

// indexBuildEvery paces the index build: every listed build the index lacks
// is added as the coldest candidate, so builds cached before the writers
// started, or whose uses were never recorded, still get deleted.
const indexBuildEvery = 7 * 24 * time.Hour

// SetIndex enables index mode; nil leaves the cleaner sampling only.
func (c *Cleaner) SetIndex(idx *cacheindex.Index) {
	c.index = idx
}

// useIndex reports whether this run starts from the index. A byte budget has
// no byte target (it needs a scan to size the cache), so it always samples.
func (c *Cleaner) useIndex() bool {
	return c.index != nil && c.IndexMode == IndexOn && c.TargetBytesToDelete > 0
}

// indexWrites reports whether this run may change the index. A dry run reads
// it but never writes.
func (c *Cleaner) indexWrites() bool {
	return c.index != nil && !c.DryRun
}

// cleanFromIndex deletes the coldest builds the index proposes until the
// target is freed, the deadline is reached, or the index runs out, and returns
// why it stopped. A run that ran out or lost Redis is finished by sampling,
// which then reports whether the target was met.
func (c *Cleaner) cleanFromIndex(ctx context.Context, deadline time.Time) string {
	start := time.Now()
	need := c.TargetBytesToDelete
	floorSec := time.Now().Unix()
	if c.Grace > 0 {
		floorSec = time.Now().Add(-c.Grace).Unix()
	}

	// A delete starts only once a slot is free, and the target is checked
	// after taking the slot, so the overshoot is the deletes still in flight
	// (none with one slot).
	slots := make(chan struct{}, c.MaxConcurrentDelete)
	var deletes sync.WaitGroup
	var readFailed atomic.Bool

	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		expired = timer.C
	}

	it := c.index.ColdestIter(floorSec, indexPage)
	queued := 0
	stop := stopExhausted
queueLoop:
	for {
		if readFailed.Load() {
			stop = stopIndexError

			break
		}
		e, ok, err := it.Next(ctx)
		if err != nil {
			c.metrics.recordError(ctx, ValOpIndex)
			c.Warn(ctx, "reading the coldest builds from the cache index failed", zap.Error(err))
			stop = stopIndexError

			break
		}
		if !ok {
			break
		}
		select {
		case slots <- struct{}{}:
		case <-expired:
			stop = stopBudget

			break queueLoop
		case <-ctx.Done():
			stop = stopCanceled

			break queueLoop
		}
		if c.BytesFreed.Load() >= need {
			<-slots
			stop = stopTarget

			break
		}
		queued++
		deletes.Add(1)
		go func(e cacheindex.Entry) {
			defer deletes.Done()
			defer func() { <-slots }()
			if !c.indexDelete(ctx, e, floorSec) {
				readFailed.Store(true)
			}
		}(e)
	}
	deletes.Wait()

	freed := c.BytesFreed.Load()
	met := freed >= need
	switch {
	case met:
		stop = stopTarget
	case stop == stopBudget || stop == stopCanceled:
		c.metrics.recordUnderTarget(ctx) // no sampling follows
	case readFailed.Load():
		stop = stopIndexError
	}
	c.metrics.IndexRuns.Add(ctx, 1, oneAttr(AttrStop, stop))
	dur := time.Since(start)
	c.metrics.recordPhase(ctx, ValPhaseDelete, dur)
	c.Info(ctx, "deletion selected",
		zap.String("source", "index"),
		zap.Bool("dry_run", c.DryRun),
		zap.Bool("target_met", met),
		zap.String("stop_reason", stop),
		zap.Int("queued_builds", queued),
		zap.Int64("deleted_builds", c.Deleted.Load()),
		zap.Uint64("freed_bytes", freed),
		zap.Uint64("target_bytes", need),
		zap.Duration("grace", c.Grace),
		zap.Duration("duration", dur))

	return stop
}

// indexDelete runs the on-disk check on one candidate: a gone folder drops its
// entry, a recently used build has its score raised and is kept, and a cold
// build is deleted and dropped. It returns false only when Redis failed.
func (c *Cleaner) indexDelete(ctx context.Context, e cacheindex.Entry, floorSec int64) bool {
	// The name comes from Redis, not from a listing: only a build ID may
	// become a path under the root ("", "..", "a/b" must not).
	if _, err := uuid.Parse(e.BuildID); err != nil || filepath.Base(e.BuildID) != e.BuildID {
		c.metrics.recordError(ctx, ValOpIndex)
		c.Warn(ctx, "cache index holds a name that is not a build ID; skipped", zap.String("name", e.BuildID))

		return true
	}
	buildPath := filepath.Join(c.Path, e.BuildID)
	if _, err := os.Stat(buildPath); errors.Is(err, fs.ErrNotExist) {
		// Removing an entry never deletes anything.
		return c.dropEntry(ctx, e.BuildID, e.Score)
	}
	age := c.buildAge
	if c.folderAge != nil {
		age = c.folderAge
	}
	if a := age(buildPath); c.Grace > 0 && a < c.Grace {
		if a > 0 { // a folder younger than grace was recreated: keep it until it ages
			c.keepWarm(ctx, e.BuildID)
		}

		return true
	}

	size, warmest := c.sizeAndWarmest(ctx, buildPath, floorSec)
	if warmest > floorSec {
		c.keepWarm(ctx, e.BuildID) // a read the index never heard of

		return true
	}
	// Read last, so a use recorded while the folder was inspected still saves it.
	score, indexed, err := c.index.Score(ctx, e.BuildID)
	if err != nil {
		c.metrics.recordError(ctx, ValOpIndex)

		return false
	}
	if !indexed || score > floorSec {
		c.metrics.IndexSkippedWarm.Add(ctx, 1)

		return true
	}
	if c.Verify {
		c.verifyBuild(ctx, build{uuid: e.BuildID, timestamp: warmest, size: size})
	}
	dur, ok := c.deleteBuild(ctx, e.BuildID)
	if !ok {
		c.metrics.recordError(ctx, ValOpDelete)

		return true
	}
	c.handled.Store(e.BuildID, struct{}{}) // a dry run must not count it again in sampling
	c.Deleted.Add(1)
	c.BytesFreed.Add(size)
	c.metrics.recordDelete(ctx, dur, size, warmest)
	if !c.indexWrites() {
		return true
	}
	if _, err := c.index.RemoveIfNotNewer(ctx, e.BuildID, score); err != nil {
		c.metrics.recordError(ctx, ValOpIndex)
	}

	return true
}

// indexHandled reports whether this run's index pass already kept or deleted buildID.
func (c *Cleaner) indexHandled(buildID string) bool {
	_, done := c.handled.Load(buildID)

	return done
}

// keepWarm raises a kept candidate's score to now, so later runs stop proposing
// it, and keeps this run's sampling from deleting it.
func (c *Cleaner) keepWarm(ctx context.Context, buildID string) {
	c.handled.Store(buildID, struct{}{})
	c.metrics.IndexSkippedWarm.Add(ctx, 1)
	if !c.indexWrites() {
		return
	}
	if err := c.index.Seed(ctx, map[string]int64{buildID: time.Now().Unix()}); err != nil {
		c.metrics.recordError(ctx, ValOpIndex)
	}
}

// dropEntry removes the entry of a build whose folder is gone, unless it was
// used again since it was proposed. It returns false only when Redis failed.
func (c *Cleaner) dropEntry(ctx context.Context, buildID string, score int64) bool {
	if !c.indexWrites() {
		return true
	}
	removed, err := c.index.RemoveIfNotNewer(ctx, buildID, score)
	if err != nil {
		c.metrics.recordError(ctx, ValOpIndex)

		return false
	}
	if removed {
		c.metrics.IndexStale.Add(ctx, 1)
	}

	return true
}

// buildIndexIfDue runs the index build when none ran within indexBuildEvery
// and the run budget is not spent. A skipped or failed build stays due.
func (c *Cleaner) buildIndexIfDue(ctx context.Context, deadline time.Time) {
	if !c.indexWrites() || (!deadline.IsZero() && time.Now().After(deadline)) {
		return
	}
	start := time.Now()
	due, err := c.index.BuildDue(ctx)
	if err == nil && due {
		var names []string
		names, err = c.listBuildIDs(ctx)
		if err == nil {
			var added int64
			added, err = c.index.AddMissing(ctx, names)
			c.metrics.IndexAdded.Add(ctx, added)
			if err == nil {
				err = c.index.MarkBuilt(ctx, start, indexBuildEvery)
			}
			c.metrics.recordPhase(ctx, ValPhaseIndexBuild, time.Since(start))
			c.Info(ctx, "cache index build", zap.Int("listed_builds", len(names)),
				zap.Int64("added_builds", added), zap.Duration("duration", time.Since(start)))
		}
	}
	if err != nil {
		c.metrics.recordError(ctx, ValOpIndex)
		c.Warn(ctx, "cache index build failed; retrying next run", zap.Error(err))
	}
}

// listBuildIDs lists every build ID at the cache root, on its own descriptor
// so the sampling listing that may follow starts from the beginning.
func (c *Cleaner) listBuildIDs(ctx context.Context) ([]string, error) {
	c.OpenC.Add(1)
	c.metrics.recordOpen(ctx)
	df, err := os.Open(c.Path)
	if err != nil {
		return nil, fmt.Errorf("open cache root %s: %w", c.Path, err)
	}
	defer df.Close()

	var names []string
	for ctx.Err() == nil {
		entries, err := df.ReadDir(readdirPage)
		c.ReadDirC.Add(1)
		c.metrics.recordRead(ctx, len(entries))
		for _, e := range entries {
			if _, perr := uuid.Parse(e.Name()); e.IsDir() && perr == nil {
				names = append(names, e.Name())
			}
		}
		switch {
		case err == io.EOF, err == nil && len(entries) < readdirPage:
			return names, nil
		case err != nil:
			c.metrics.recordError(ctx, ValOpRootReaddir)

			return nil, fmt.Errorf("read cache root %s: %w", c.Path, err)
		}
	}

	return nil, ctx.Err()
}
