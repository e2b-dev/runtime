package envdbin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// stalledStat models a mount whose metadata lookup does not answer until
// released: every call blocks until release is closed, then stats for real.
type stalledStat struct {
	calls   atomic.Int64
	release chan struct{}
	once    sync.Once
	// failOnRelease makes a release by test cleanup answer with an error, so a
	// stat landing during cleanup cannot start a warm that races the temp dir.
	failOnRelease atomic.Bool
}

func newStalledStat(t *testing.T) *stalledStat {
	t.Helper()

	s := &stalledStat{release: make(chan struct{})}
	// Registered after newCacheForTest's cleanup, so it runs first and the cache's
	// own wait for background work is not held up by a stat still blocked here.
	t.Cleanup(func() {
		s.failOnRelease.Store(true)
		s.unblock()
	})

	return s
}

func (s *stalledStat) stat(path string) (os.FileInfo, error) {
	s.calls.Add(1)
	<-s.release
	if s.failOnRelease.Load() {
		return nil, errors.New("released by test cleanup")
	}

	return os.Stat(path)
}

func (s *stalledStat) unblock() { s.once.Do(func() { close(s.release) }) }

func newStalledCache(t *testing.T, budget time.Duration) (*Cache, *stalledStat, string) {
	t.Helper()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})
	c.lookupBudget = budget
	s := newStalledStat(t)
	c.statSrc = s.stat

	return c, s, src
}

// A stalled source stat must not hold a lookup beyond its budget.
func TestALookupDoesNotWaitOnAStalledSourceStat(t *testing.T) {
	t.Parallel()

	const budget = 100 * time.Millisecond
	c, s, src := newStalledCache(t, budget)

	r := NewResolver(c, OpOffline)
	start := time.Now()
	_, err := r.Version(t.Context(), src)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrNotCached, "a stalled stat is a miss, and the upgrade defers")
	require.GreaterOrEqual(t, elapsed, budget, "the lookup waits out its budget before giving up")
	require.Less(t, elapsed, 5*time.Second, "and not the stall")

	// A slow mount is not a broken target: the miss is a deferral, labeled as
	// one, and it arms no schedule that would hold back the warm once it answers.
	require.Equal(t, OutcomeMiss, r.DeferralOutcome())
	require.Equal(t, ReasonNotCached, GatedReason("getversion_failed", r.DeferralOutcome()))
	require.False(t, c.scheduleSuppressed(src), "a stall must not arm the retry schedule")
	require.Equal(t, int64(1), s.calls.Load())
}

func TestLookupsShareOneStalledStatAndFailFastOnceItOutlivesTheBudget(t *testing.T) {
	t.Parallel()

	const budget = 300 * time.Millisecond
	c, s, src := newStalledCache(t, budget)

	// A burst of resumes arriving inside the budget all join the one stat.
	const resumes = 16
	var wg sync.WaitGroup
	for range resumes {
		wg.Go(func() {
			_, err := NewResolver(c, OpLive).Version(t.Context(), src)
			assert.ErrorIs(t, err, ErrNotCached)
		})
	}
	wg.Wait()
	require.Equal(t, int64(1), s.calls.Load(), "concurrent lookups must share one stat, not stack one goroutine each")

	// A resume arriving after the shared stat has outlived the budget does not
	// wait the budget out again.
	start := time.Now()
	_, err := NewResolver(c, OpLive).Version(t.Context(), src)
	require.ErrorIs(t, err, ErrNotCached)
	require.Less(t, time.Since(start), budget/2, "a stat already past its budget must fail the lookup at once")
	require.Equal(t, int64(1), s.calls.Load())
}

// Every lookup behind a slow stat defers with no identity to warm on, so the stat
// itself has to start the warm when it lands -- otherwise a mount slower than the
// budget would never fill a cold cache.
func TestAStatThatLandsAfterItsLookupsGaveUpWarmsTheCache(t *testing.T) {
	t.Parallel()

	c, s, src := newStalledCache(t, 50*time.Millisecond)

	_, err := NewResolver(c, OpLive).Version(t.Context(), src)
	require.ErrorIs(t, err, ErrNotCached)

	s.unblock()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.statFlights) == 0 && len(c.entries) == 1
	}, 10*time.Second, 5*time.Millisecond, "the late stat must retire its flight and warm the cache")

	// The mount answers now, so the next resume takes its own stat and hits.
	r := NewResolver(c, OpLive)
	version, err := r.Version(t.Context(), src)
	require.NoError(t, err)
	require.Equal(t, "0.7.0", version)
	require.Equal(t, OutcomeHit, r.Outcome())
}

// A stat landing as a lookup's budget runs out must leave exactly one side
// responsible for the warm, whichever way the two interleave.
func TestAStatAndALookupRacingTheBudgetCannotBothSkipTheWarm(t *testing.T) {
	t.Parallel()

	fi, err := os.Stat(t.TempDir())
	require.NoError(t, err)
	c := NewCache("", nil)

	t.Run("stat lands first: the lookup takes the result", func(t *testing.T) {
		t.Parallel()

		f := &statFlight{done: make(chan struct{})}
		require.False(t, c.landFlight("/src", f, fi, nil), "no lookup had given up")

		landed, gotFi, gotErr := c.abandonFlight(f, true)
		require.True(t, landed, "a lookup timing out after the stat landed must use its result")
		require.NoError(t, gotErr)
		require.Equal(t, fi, gotFi)
	})

	t.Run("lookup gives up first: the stat warms", func(t *testing.T) {
		t.Parallel()

		f := &statFlight{done: make(chan struct{})}
		landed, _, _ := c.abandonFlight(f, true)
		require.False(t, landed)

		require.True(t, c.landFlight("/src", f, fi, nil), "the stat must see the abandonment and start the warm")
	})

	t.Run("a caller not using the cache gives up: no warm", func(t *testing.T) {
		t.Parallel()

		f := &statFlight{done: make(chan struct{})}
		landed, _, _ := c.abandonFlight(f, false)
		require.False(t, landed)

		require.False(t, c.landFlight("/src", f, fi, nil), "a bounded stat for a cache-off caller must not warm the cache")
	})
}

// End to end through the upgrade resolver: its candidate checks run before the
// version probe, so they must go through the bounded stat too, and a stall there
// must come back as source_stalled within the budget, for either target shape.
func TestTheUpgradeResolutionDoesNotHangOnAStalledCandidateStat(t *testing.T) {
	t.Parallel()

	for _, target := range []string{"promoted", "v0.9.0"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			c, s, src := newStalledCache(t, 100*time.Millisecond)
			r := NewResolver(c, OpOffline)

			start := time.Now()
			path, _, reason := featureflags.ResolveEnvdUpgrade(t.Context(), target, "0.6.0", src, r.Version, r.Stat)
			require.Less(t, time.Since(start), 5*time.Second, "the resolution must not wait out the stall")
			require.Empty(t, path)
			require.Equal(t, featureflags.ReasonSourceStalled, reason)
			require.Equal(t, int64(1), s.calls.Load(), "one stat reached the mount")
		})
	}
}

// A stall surfaces in the candidate check, before Version runs, so that is where
// it must be counted on the reads counter: exactly one read per resolution, as a
// miss caused by the stall.
func TestAStalledResolutionCountsOneStalledRead(t *testing.T) {
	t.Parallel()

	const op = Op("test-stalled-resolution")
	c, _, src := newStalledCache(t, 50*time.Millisecond)

	// Read as growth: the counter outlives the test, and -count repeats it.
	before := readsByCause(t, op)
	r := NewResolver(c, op)
	_, _, reason := featureflags.ResolveEnvdUpgrade(t.Context(), "promoted", "0.6.0", src, r.Version, r.Stat)
	require.Equal(t, featureflags.ReasonSourceStalled, reason)
	require.Equal(t, OutcomeMiss, r.Outcome())

	after := readsByCause(t, op)
	grew := map[string]int64{}
	for cause, n := range after {
		if d := n - before[cause]; d != 0 {
			grew[cause] = d
		}
	}
	require.Equal(t, map[string]int64{missStalled: 1}, grew,
		"a stalled resolution is one read, a miss caused by the stall")
}

// A candidate check that stalls still leads to a warm, so once the mount answers
// the next resolution hits instead of deferring again.
func TestAStalledCandidateCheckWarmsTheCacheWhenItLands(t *testing.T) {
	t.Parallel()

	c, s, src := newStalledCache(t, 50*time.Millisecond)

	_, _, reason := featureflags.ResolveEnvdUpgrade(t.Context(), "promoted", "0.6.0", src, NewResolver(c, OpLive).Version, NewResolver(c, OpLive).Stat)
	require.Equal(t, featureflags.ReasonSourceStalled, reason)

	s.unblock()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.entries) == 1
	}, 10*time.Second, 5*time.Millisecond, "the late candidate stat must warm the cache")

	r := NewResolver(c, OpLive)
	path, version, reason := featureflags.ResolveEnvdUpgrade(t.Context(), "promoted", "0.6.0", src, r.Version, r.Stat)
	require.Empty(t, reason)
	require.Equal(t, src, path)
	require.Equal(t, "0.7.0", version)
	require.Equal(t, OutcomeHit, r.Outcome())
}

// With the cache off, the candidate checks are still bounded, but a stat landing
// after its caller gave up must not fill a cache nobody is using.
func TestABoundedStatForACacheOffCallerNeverWarms(t *testing.T) {
	t.Parallel()

	c, s, src := newStalledCache(t, 50*time.Millisecond)

	_, err := c.BoundedStat(t.Context(), src)
	require.ErrorIs(t, err, featureflags.ErrEnvdSourceStalled)

	s.unblock()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.statFlights) == 0
	}, 10*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.entries) > 0 || len(c.warming) > 0
	}, 200*time.Millisecond, 10*time.Millisecond, "a cache-off caller's stat must not warm the cache")
}

func TestANilCacheStillBoundsTheStat(t *testing.T) {
	t.Parallel()

	src := filepath.Join(t.TempDir(), "envd")
	require.NoError(t, os.WriteFile(src, []byte("x"), 0o755))

	fi, err := (*Cache)(nil).BoundedStat(t.Context(), src)
	require.NoError(t, err)
	require.Equal(t, int64(1), fi.Size())

	_, err = (*Resolver)(nil).Stat(t.Context(), filepath.Join(filepath.Dir(src), "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A stall warns on its own window, so it cannot demote a genuine failure on the
// same path from Warn to Debug.
func TestAStallWarningDoesNotSpendTheFailureWarningWindow(t *testing.T) {
	t.Parallel()

	c := NewCache("", nil)
	require.True(t, c.shouldWarnStall("/src"))
	require.False(t, c.shouldWarnStall("/src"), "stall warnings are rate-limited")
	require.True(t, c.shouldWarn("/src"), "a failure after a stall must still get its Warn")
}

func TestALookupReturnsWhenItsContextIsCancelled(t *testing.T) {
	t.Parallel()

	c, _, src := newStalledCache(t, time.Minute)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, _, err := c.lookupSource(ctx, src)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 5*time.Second, "a cancelled resume must not wait on the stall")
}

// A resume that is cancelled while it waits has learned nothing about the source,
// so it must not arm the schedule a real source failure arms.
func TestACancelledLookupDoesNotCountAgainstTheSource(t *testing.T) {
	t.Parallel()

	c, _, src := newStalledCache(t, time.Minute)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	r := NewResolver(c, OpLive)
	_, err := r.Version(ctx, src)
	require.ErrorIs(t, err, ErrNotCached)
	require.Equal(t, OutcomeMiss, r.DeferralOutcome(), "a cancelled lookup defers like any other miss")
	require.False(t, c.scheduleSuppressed(src), "a cancelled caller must not arm the source's retry schedule")
}
