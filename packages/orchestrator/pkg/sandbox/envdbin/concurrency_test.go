package envdbin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The cache's risk is interleaving. A warm goroutine spawned per miss, or a read
// counted twice, changes nothing an observer sees when the callers arrive one at
// a time -- the copy is still correct, the counter is merely wrong -- so neither
// a single-sandbox exercise nor a test that only calls the API in sequence can
// reach it. This file is the one place that can hold a window open, land a
// promotion inside it, and run the whole thing under -race.
//
// What they assert is the cache's contract, not its implementation:
//
//   - a delivered copy's bytes always equal the version recorded with it;
//   - the directory holds exactly the live copies, never more;
//   - entries never exceed the cap;
//   - nothing panics or deadlocks while all of that happens at once.
//
// What they do NOT prove is absence of races in general — only that the
// interleavings they drive are clean.

// payloadBytes drops the ELF header the fixtures carry, so a comparison is
// against what the fixture holds rather than the header it needs to be loadable.
func payloadBytes(b []byte) string {
	if len(b) > elfHeaderLen {
		return string(b[elfHeaderLen:])
	}

	return string(b)
}

// contentProbe reports a binary's version by reading it, so a copy's bytes and
// its recorded version are the same fact and any drift between them is
// detectable. It is also closer to the real probe than a constant: the real one
// execs the file it is given, and this reads it.
func contentProbe(_ context.Context, path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	// The payload follows the ELF header the fixtures carry; the version is that
	// payload, so a copy's probe answers with the generation it holds.
	return strings.TrimSpace(payloadBytes(b)), nil
}

// writeGeneration rewrites src as generation n: content "v<n>", and an mtime that
// advances with n so each generation is a distinct cache key. This is what a
// promotion does — an in-place overwrite at a fixed path.
func writeGeneration(t *testing.T, src string, n int) {
	t.Helper()

	// Staged and renamed, so the source is never briefly visible with a
	// write-time mtime. Writing in place and stamping afterwards leaves a window
	// in which a concurrent warm records "now" as the generation's mtime -- an
	// mtime no later generation can beat, which is a property of the fixture and
	// not of a real promotion (a GCS overwrite publishes one update time).
	stage := src + ".staging"
	require.NoError(t, os.WriteFile(stage, elfWithPayload(fmt.Sprintf("v%d", n)), 0o755))
	stamp := time.Unix(1_700_000_000+int64(n), 0)
	require.NoError(t, os.Chtimes(stage, stamp, stamp))
	require.NoError(t, os.Rename(stage, src))
}

// assertDirectoryHoldsExactlyTheLiveCopies is the sweep's contract: every file in
// the cache directory is either a live entry's copy or an in-progress temporary.
// It is checked with the cache quiesced, so no copy is mid-publication.
func assertDirectoryHoldsExactlyTheLiveCopies(t *testing.T, c *Cache) {
	t.Helper()

	c.mu.Lock()
	live := make(map[string]struct{}, len(c.entries))
	for _, e := range c.entries {
		live[e.LocalPath] = struct{}{}
	}
	entries := len(c.entries)
	inFlight := len(c.inFlight)
	warming := len(c.warming)
	c.mu.Unlock()

	require.LessOrEqual(t, entries, maxEntries, "entries must never exceed the cap")
	require.Zero(t, inFlight, "no copy may still be in flight once quiesced")
	require.Zero(t, warming, "no warm may still be claimed once quiesced")

	names, err := os.ReadDir(c.dir)
	require.NoError(t, err)
	for _, n := range names {
		if n.IsDir() || strings.HasPrefix(n.Name(), tempPrefix) {
			continue
		}
		p := filepath.Join(c.dir, n.Name())
		require.Contains(t, live, p, "the directory holds a copy no entry references")
	}
	for p := range live {
		require.FileExists(t, p, "a live entry's copy is missing")
	}
}

// violations collects contract breaches found inside worker goroutines so they can
// be asserted on the test goroutine.
//
// require.* calls t.FailNow, which the testing package documents as valid only
// from the goroutine running the test. Called from a worker it can abort the
// wrong stack and attribute the failure oddly — which is how one unexplained
// failure in this file became unattributable. Collect, then assert.
//
// The rule covers what a worker observes about the CACHE. writeGeneration still
// uses require from the mutator goroutine: a failed fixture write is a broken
// environment rather than a contract breach, and there is nothing to go on
// asserting afterwards.
type violations struct {
	mu   sync.Mutex
	msgs []string
}

func (v *violations) addf(format string, args ...any) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if len(v.msgs) < 20 {
		v.msgs = append(v.msgs, fmt.Sprintf(format, args...))
	}
}

func (v *violations) assertNone(t *testing.T) {
	t.Helper()

	v.mu.Lock()
	defer v.mu.Unlock()

	require.Empty(t, v.msgs, "contract violations observed in workers")
}

func TestConcurrentLookupsAndWarmsUnderPromotionChurn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 0)

	c := newCacheForTest(t, cacheDir, contentProbe)

	const (
		readers    = 24
		iterations = 150
	)

	// The mutator promotes the binary underneath everyone. Bounded by the readers
	// finishing rather than by a timer, so the test has no wall-clock dependency.
	stop := make(chan struct{})
	var promotions atomic.Int64
	var mutator sync.WaitGroup
	mutator.Go(func() {
		for gen := 1; ; gen++ {
			select {
			case <-stop:
				return
			default:
			}
			writeGeneration(t, src, gen)
			promotions.Add(1)
			// Slower than a warm, so the cache reaches a settled state between
			// promotions and the readers can observe hits.
			time.Sleep(2 * time.Millisecond)
		}
	})

	// Phase 1 -- promotions landing during concurrent reads. Hits are not required
	// here: heavy churn can invalidate every warm before it is observed, and
	// demanding hits from a race is how a test becomes flaky. What phase 1 asserts
	// is that nothing panics, nothing races, and no read ever sees bytes that
	// disagree with its version.
	var readerGroup sync.WaitGroup
	var bad violations
	var hits, misses atomic.Int64
	for range readers {
		readerGroup.Go(func() {
			for range iterations {
				entry, ok := c.Lookup(src)
				if !ok {
					misses.Add(1)
					// Warm synchronously and look again, so the invariant below is
					// actually reached. With WarmAsync and no re-check, every lookup
					// missed under churn and the assertion never executed — which is
					// indistinguishable from a passing test.
					if err := c.Warm(t.Context(), src); err == nil {
						entry, ok = c.Lookup(src)
					}
					if !ok {
						continue
					}
				}
				hits.Add(1)

				// The contract: a copy's bytes are the version recorded with it.
				// Copies are immutable — a new generation gets a new filename — so
				// this holds under churn. A vanished copy is the documented race and
				// is tolerated; wrong bytes never are.
				got, err := os.ReadFile(entry.LocalPath)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					bad.addf("reading %s: %v", entry.LocalPath, err)

					continue
				}
				if v := strings.TrimSpace(payloadBytes(got)); v != entry.Version {
					bad.addf("copy %s holds %q but its entry records %q", entry.LocalPath, v, entry.Version)
				}
			}
		})
	}
	readerGroup.Wait()
	close(stop)
	mutator.Wait()
	bad.assertNone(t)

	// Let the last warms retire, then check the invariants with nothing moving.
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.warming) == 0 && len(c.inFlight) == 0
	}, 30*time.Second, 10*time.Millisecond, "warms must all retire")

	assertDirectoryHoldsExactlyTheLiveCopies(t, c)

	// Deliberately NOT asserted here: that the surviving entry describes the final
	// source. A warm for an older generation may legitimately publish last when no
	// newer entry is resident, and the next lookup simply misses and re-warms. Only
	// overwriting a NEWER entry is the defect, and that is a fact about ordering
	// which no post-hoc snapshot can see -- so it is guarded deterministically by
	// TestStoreKeepsTheNewerGenerationWhenAStaleWarmFinishesLast instead. An
	// assertion here would fail on correct code, which is worse than none.

	require.Positive(t, promotions.Load(), "the mutator must have promoted at least once")
	require.Positive(t, misses.Load(), "churn must have produced misses")

	// Phase 2 -- the same readers against a now-stable source, so hits are
	// guaranteed rather than hoped for and the bytes invariant is genuinely
	// exercised. This is the deterministic half; phase 1 is the racy half.
	var settled sync.WaitGroup
	var settledHits atomic.Int64
	for range readers {
		settled.Go(func() {
			for range iterations {
				entry, ok := c.Lookup(src)
				if !ok {
					if err := c.Warm(t.Context(), src); err != nil {
						continue
					}
					entry, ok = c.Lookup(src)
					if !ok {
						continue
					}
				}
				settledHits.Add(1)

				got, err := os.ReadFile(entry.LocalPath)
				if err != nil {
					bad.addf("stable source, unreadable copy %s: %v", entry.LocalPath, err)

					continue
				}
				if v := strings.TrimSpace(payloadBytes(got)); v != entry.Version {
					bad.addf("stable source, copy %s holds %q but entry records %q", entry.LocalPath, v, entry.Version)
				}
			}
		})
	}
	settled.Wait()
	bad.assertNone(t)

	require.Positive(t, settledHits.Load(), "a stable source must produce hits")
	assertDirectoryHoldsExactlyTheLiveCopies(t, c)

	t.Logf("promotions=%d churn_hits=%d churn_misses=%d settled_hits=%d",
		promotions.Load(), hits.Load(), misses.Load(), settledHits.Load())
}

func TestConcurrentWarmsAcrossManyPathsExerciseEvictionAndSweep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")

	// More distinct sources than the cache holds, so eviction and the sweep run
	// continuously and concurrently with publication.
	const paths = 12
	srcs := make([]string, paths)
	for i := range srcs {
		srcs[i] = filepath.Join(dir, fmt.Sprintf("envd.%02d", i))
		writeGeneration(t, srcs[i], i)
	}

	c := newCacheForTest(t, cacheDir, contentProbe)

	const (
		workers    = 24
		iterations = 40
	)
	var wg sync.WaitGroup
	var warmErrors atomic.Int64
	for w := range workers {
		wg.Go(func() {
			for i := range iterations {
				// Deterministic, and deliberately overlapping between workers so
				// several warms of the same path collide.
				src := srcs[(w+i)%paths]
				if _, ok := c.Lookup(src); !ok {
					if err := c.Warm(t.Context(), src); err != nil {
						warmErrors.Add(1)
					}
				}
			}
		})
	}
	wg.Wait()

	// Every source is readable and the directory is healthy, so a warm has no
	// legitimate reason to fail. This is what catches one warm's sweep deleting
	// another's copy before it is published: the victim's probe would fail on a
	// path that vanished under it.
	require.Zero(t, warmErrors.Load(), "no warm should fail while sources and the directory are healthy")

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.warming) == 0 && len(c.inFlight) == 0
	}, 30*time.Second, 10*time.Millisecond)

	assertDirectoryHoldsExactlyTheLiveCopies(t, c)

	// Whatever survived eviction must still be internally consistent: the entry's
	// key matches its source, and its copy holds that version's bytes.
	c.mu.Lock()
	surviving := make([]*Entry, 0, len(c.entries))
	for _, e := range c.entries {
		surviving = append(surviving, e)
	}
	order := append([]string(nil), c.order...)
	c.mu.Unlock()

	require.Len(t, order, len(surviving), "order and entries must not diverge")
	for _, e := range surviving {
		fi, err := os.Stat(e.SourcePath)
		require.NoError(t, err)
		require.Equal(t, fi.Size(), e.size)
		require.True(t, fi.ModTime().Equal(e.modTime))

		got, err := os.ReadFile(e.LocalPath)
		require.NoError(t, err)
		require.Equal(t, e.Version, strings.TrimSpace(payloadBytes(got)))
	}
}

func TestDeliveryIsRefusedWhenAPromotionSupersedesTheHitCopy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 0)

	c := newCacheForTest(t, cacheDir, contentProbe)
	require.NoError(t, c.Warm(t.Context(), src))

	r := NewResolver(c, OpLive)

	// A resolution decides on generation 0 and records its version.
	version, err := r.Version(t.Context(), src)
	require.NoError(t, err)
	require.Equal(t, "v0", version)
	first := r.EntryFor(src)
	require.NotNil(t, first)

	// Now the exact interleaving the refusal exists for, forced rather than
	// hoped for: the binary is promoted, and a warm publishes the new generation
	// -- which deletes the superseded copy this resolution is holding.
	writeGeneration(t, src, 1)
	require.NoError(t, c.Warm(t.Context(), src))
	require.NoFileExists(t, first.LocalPath, "publishing generation 1 must reclaim generation 0's copy")

	// Neither path can now deliver the bytes whose version was recorded: the copy
	// is gone and the source holds v1. Serving the source here is the
	// version_mismatch this whole design exists to prevent.
	path, ok := r.SourcePath(t.Context(), src)
	require.False(t, ok, "a superseded hit must be refused, not served from the changed source")
	require.Empty(t, path)
}

func TestConcurrentDeliveriesNeverServeBytesFromAnotherVersion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 0)

	c := newCacheForTest(t, cacheDir, contentProbe)
	require.NoError(t, c.Warm(t.Context(), src))

	const (
		callers    = 16
		iterations = 60
	)

	// Promotions land while deliveries are being resolved. Paired with a warm, so
	// superseded copies are actually reclaimed underneath the callers rather than
	// the source merely changing.
	stop := make(chan struct{})
	var mutator sync.WaitGroup
	mutator.Go(func() {
		for gen := 1; ; gen++ {
			select {
			case <-stop:
				return
			default:
			}
			writeGeneration(t, src, gen)
			_ = c.Warm(t.Context(), src)
			time.Sleep(500 * time.Microsecond)
		}
	})

	var fromCopy, fromSource, refused atomic.Int64
	var bad violations
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			for range iterations {
				r := NewResolver(c, OpLive)

				version, err := r.Version(t.Context(), src)
				if err != nil {
					continue
				}

				path, ok := r.SourcePath(t.Context(), src)
				if !ok {
					refused.Add(1)

					continue
				}

				got, err := os.ReadFile(path)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					bad.addf("reading %s: %v", path, err)

					continue
				}

				// Two invariants, and the second is the one the design turns on.
				//
				// A copy is immutable, so its bytes must be the version reported with
				// it — wrong bytes are the failure this cache exists to prevent.
				//
				// And every delivered path must be INSIDE the cache directory. A
				// delivery from the source would be correct in its bytes and still
				// wrong: it is a 13 MB read off the mount on the resume path, which
				// is the cost being removed. Counting it would not catch it, because
				// zero is what a passing run reports either way.
				if !strings.HasPrefix(path, cacheDir) {
					fromSource.Add(1)
					bad.addf("delivered %s from outside the cache directory: the resume path must not read the mount", path)

					continue
				}
				fromCopy.Add(1)
				if v := strings.TrimSpace(payloadBytes(got)); v != version {
					bad.addf("delivered copy %s holds %q but %q was reported", path, v, version)
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	mutator.Wait()

	// The run has to have gone through the copy at least sometimes, or the
	// invariant above never ran.
	require.Positive(t, fromCopy.Load(), "deliveries must have come from a copy, or nothing was asserted")
	require.Zero(t, fromSource.Load(), "no delivery may come off the mount")
	t.Logf("from_copy=%d from_source=%d refused=%d", fromCopy.Load(), fromSource.Load(), refused.Load())
}

func TestBackoffSuppressesCopiesAndIsNotReportedAsAFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 1)

	// A regular file where the cache directory's parent should be, so MkdirAll
	// fails and the failure is cache-side.
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))
	cacheDir := filepath.Join(blocker, "cache")

	var probes atomic.Int64
	c := newCacheForTest(t, cacheDir, func(ctx context.Context, path string) (string, error) {
		probes.Add(1)

		return contentProbe(ctx, path)
	})

	// Many concurrent warms against a broken directory: the first fails for real,
	// the rest must be suppressed rather than each re-attempting a copy. This is
	// the state a node reaches when its cache directory is unusable, where a miss
	// recurs on every resume.
	const callers = 24
	var wg sync.WaitGroup
	var bad violations
	var failed, suppressed atomic.Int64
	for range callers {
		wg.Go(func() {
			for range 10 {
				err := c.Warm(t.Context(), src)
				if err == nil {
					bad.addf("a broken cache directory reported success")

					continue
				}
				if errors.Is(err, errWarmSuppressed) {
					suppressed.Add(1)

					continue
				}
				failed.Add(1)
			}
		})
	}
	wg.Wait()
	bad.assertNone(t)

	require.Positive(t, suppressed.Load(),
		"after the first cache-side failure the rest must be suppressed, not retried")
	require.Zero(t, probes.Load(), "a suppressed warm must not reach the probe")
	t.Logf("failed=%d suppressed=%d", failed.Load(), suppressed.Load())

	// Healing the directory does not lift the backoff: that is the point of it.
	require.NoError(t, os.Remove(blocker))
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))
	require.ErrorIs(t, c.Warm(t.Context(), src), errWarmSuppressed,
		"the backoff must hold even once the directory is usable again")

	// And a success must clear it rather than disable the cache for good.
	c.noteSuccess(src)

	require.NoError(t, c.Warm(t.Context(), src))
	_, ok := c.Lookup(src)
	require.True(t, ok, "the cache must recover once the backoff lapses")
	require.Positive(t, probes.Load(), "the recovered warm must have probed")
}

func TestConcurrentWarmsUnderChurnAcrossManyPathsAndEviction(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")

	// More paths than the cap, each being promoted, so eviction, the supersede
	// delete, the sweep and the publish-time freshness check all interleave. The
	// existing suite churns one path or holds many static; this does both.
	const paths = 8
	srcs := make([]string, paths)
	for i := range srcs {
		srcs[i] = filepath.Join(dir, fmt.Sprintf("envd.%02d", i))
		writeGeneration(t, srcs[i], 1)
	}

	c := newCacheForTest(t, cacheDir, contentProbe)

	stop := make(chan struct{})
	var promotions atomic.Int64
	var mutators sync.WaitGroup
	for i := range paths {
		mutators.Go(func() {
			for gen := 2; ; gen++ {
				select {
				case <-stop:
					return
				default:
				}
				writeGeneration(t, srcs[i], gen)
				promotions.Add(1)
				time.Sleep(time.Millisecond)
			}
		})
	}

	const (
		workers    = 24
		iterations = 60
	)
	var wg sync.WaitGroup
	var bad violations
	var warmErrors atomic.Int64
	for w := range workers {
		wg.Go(func() {
			for i := range iterations {
				src := srcs[(w+i)%paths]
				entry, ok := c.Lookup(src)
				if !ok {
					if err := c.Warm(t.Context(), src); err != nil {
						warmErrors.Add(1)
					}

					continue
				}
				// Whatever a hit hands back must carry its own version, however much
				// churn and eviction is going on around it.
				got, err := os.ReadFile(entry.LocalPath)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					bad.addf("reading %s: %v", entry.LocalPath, err)

					continue
				}
				if v := strings.TrimSpace(payloadBytes(got)); v != entry.Version {
					bad.addf("copy %s holds %q but its entry records %q", entry.LocalPath, v, entry.Version)
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	mutators.Wait()
	bad.assertNone(t)

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.warming) == 0 && len(c.inFlight) == 0
	}, 30*time.Second, 10*time.Millisecond)

	// Sources are readable and the directory is healthy throughout, so no warm has
	// a legitimate reason to fail — a sweep or an eviction deleting another warm's
	// unpublished copy would show up here.
	require.Zero(t, warmErrors.Load(), "no warm should fail while sources and the directory are healthy")
	require.Positive(t, promotions.Load())
	assertDirectoryHoldsExactlyTheLiveCopies(t, c)
}

// A warm can complete having published nothing, because a promotion during it
// leaves its copy describing a superseded generation. That needs no retry — the
// next miss warms the new generation — but it must not be reported as having
// populated the cache, and the claim must be released either way or the path
// would never warm again.
func TestAWarmSupersededMidFlightPublishesNothingAndReleasesItsClaim(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 1)

	// Hold the warm inside its probe, so the promotion lands while it is in flight.
	entered := make(chan struct{})
	release := make(chan struct{})
	var probes atomic.Int64
	c := newCacheForTest(t, cacheDir, func(ctx context.Context, path string) (string, error) {
		if probes.Add(1) == 1 {
			close(entered)
			<-release
		}

		return contentProbe(ctx, path)
	})

	var warmErr error
	var done sync.WaitGroup
	done.Go(func() {
		warmErr = c.Warm(t.Context(), src)
	})

	<-entered
	// A promotion while the warm holds a copy of generation 1.
	writeGeneration(t, src, 2)
	close(release)
	done.Wait()

	// Not an error: nothing went wrong, the work was simply overtaken.
	require.NoError(t, warmErr)

	// But nothing may be published, because the copy describes generation 1 and the
	// source is generation 2. Publishing it would leave an entry matching nothing.
	_, ok := c.Lookup(src)
	require.False(t, ok, "a superseded copy must not be published")

	// The directory must not keep the orphan either.
	names, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	for _, n := range names {
		require.True(t, strings.HasPrefix(n.Name(), tempPrefix),
			"a superseded copy must be reclaimed, found %s", n.Name())
	}

	// And the claim must have been released, or this path could never warm again.
	c.mu.Lock()
	warming := len(c.warming)
	c.mu.Unlock()
	require.Zero(t, warming)

	// Which the next warm proves: it publishes generation 2.
	require.NoError(t, c.Warm(t.Context(), src))
	entry, ok := c.Lookup(src)
	require.True(t, ok, "the next warm must publish the new generation")
	require.Equal(t, "v2", entry.Version)
}

// A warm can stat the source before a promotion and open it after one. Its copy
// is named for the generation it stat-ed, and a same-size promotion passes the
// copy's length check, so the newer bytes would land under the older copy's name
// while a resume holding the older entry may still be reading that file.
func TestAStaleWarmNeverWritesAnotherGenerationUnderAnOlderCopysName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 1)
	gen1 := mustStat(t, src)

	// The resume holding the generation-1 entry reads its copy whenever a copy is
	// probed: the moment a just-renamed copy sits under its final name.
	var held atomic.Pointer[Entry]
	var bad violations
	var probes atomic.Int64
	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(ctx context.Context, path string) (string, error) {
		probes.Add(1)
		if e := held.Load(); e != nil {
			if got, err := os.ReadFile(e.LocalPath); err == nil {
				if v := strings.TrimSpace(payloadBytes(got)); v != e.Version {
					bad.addf("copy %s holds %q but its entry records %q", e.LocalPath, v, e.Version)
				}
			}
		}

		return contentProbe(ctx, path)
	})

	require.NoError(t, c.Warm(t.Context(), src))
	first, ok := c.Lookup(src)
	require.True(t, ok)
	require.Equal(t, "v1", first.Version)
	held.Store(first)

	// A same-size promotion, warmed, retires the generation-1 copy.
	writeGeneration(t, src, 2)
	require.NoError(t, c.Warm(t.Context(), src))
	probed := probes.Load()

	// A warm whose source stat predates that promotion.
	var stale atomic.Bool
	stale.Store(true)
	c.statSrc = func(path string) (os.FileInfo, error) {
		if stale.CompareAndSwap(true, false) {
			return gen1, nil
		}

		return os.Stat(path)
	}
	require.NoError(t, c.Warm(t.Context(), src))
	require.False(t, stale.Load(), "the warm must have taken the stale stat")
	bad.assertNone(t)
	require.Equal(t, probed, probes.Load(), "a copy of another generation must be abandoned before it is probed")

	_, err := os.Stat(first.LocalPath)
	require.ErrorIs(t, err, os.ErrNotExist, "nothing may be left under the retired generation-1 copy's name")
	current, ok := c.Lookup(src)
	require.True(t, ok)
	require.Equal(t, "v2", current.Version)
}

// TestAStalledSourceStatInTheWarmerDoesNotBlockLookup pins the property the whole
// design rests on: a wait the WARMER takes on the network filesystem is not
// reachable from a resume. The warmer runs off the resume path, but it shares a
// mutex with Lookup,
// so a source stat taken while holding that mutex puts the wait straight back --
// invisibly, because every unit test runs against a local filesystem that never
// stalls.
//
// There is no clock in here. The warmer is parked inside its source stat and kept
// there, so if the lock were held across it, Lookup could not return at all and
// the test fails as a timeout naming this function. Nothing is asserted about how
// fast anything is.
func TestAStalledSourceStatInTheWarmerDoesNotBlockLookup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeGeneration(t, src, 1)

	cacheDir := filepath.Join(dir, "cache")
	parked := make(chan struct{})
	release := make(chan struct{})
	var arm atomic.Bool

	c := NewCache(cacheDir, contentProbe)
	// Park the publish-time stat, identified by what has already happened rather
	// than by a call count: it is the source stat a warm takes once the bytes are
	// on local disk. Counting calls would silently move the park somewhere
	// harmless if the warm path ever grew another stat, and the test would keep
	// passing without exercising anything.
	//
	// A CAS, not a sync.Once: Once blocks every later caller until the first
	// returns, so the lookup under test would have waited on the gate instead of
	// on the mutex -- a deadlock the fixed code would fail too.
	c.statSrc = func(path string) (os.FileInfo, error) {
		if names, err := os.ReadDir(cacheDir); err == nil && len(names) > 0 &&
			arm.CompareAndSwap(false, true) {
			close(parked)
			<-release
		}

		return os.Stat(path)
	}

	warmed := make(chan error, 1)
	go func() { warmed <- c.Warm(t.Context(), src) }()

	<-parked

	// The park is only meaningful if the warm really is mid-publication: bytes
	// written, entry not yet visible.
	names, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	require.NotEmpty(t, names, "the warm must be parked after writing its copy")

	// The warmer is now inside the stalled stat. A resume arriving here must be
	// answered -- with a miss, since nothing is published yet, which is exactly
	// what it would get with the cache off.
	_, ok := c.Lookup(src)
	require.False(t, ok, "nothing is published while the warm is still in flight")

	close(release)
	require.NoError(t, <-warmed)

	// And the warm still completes normally once the mount answers.
	entry, ok := c.Lookup(src)
	require.True(t, ok, "the warm must publish once its stalled stat returns")
	require.Equal(t, "v1", entry.Version)
	require.True(t, entry.Cached())
}
