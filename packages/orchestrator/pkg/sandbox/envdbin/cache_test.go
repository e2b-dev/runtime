package envdbin

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// countingProbe records every path it is asked about, so tests can assert how
// many times the source was actually consulted without measuring elapsed time.
type countingProbe struct {
	mu      sync.Mutex
	calls   []string
	version string
	err     error
	// errOnce fails the first call only, to model a transient failure.
	errOnce atomic.Bool
	// failPath fails every probe of exactly this path. errOnce is a one-shot and
	// therefore racy whenever a background warm can probe concurrently: the warm
	// probes the COPY while the fallback probes the SOURCE, so whichever ran first
	// consumed the failure. Scoping by path makes the intended call the one that
	// fails, at any level of parallelism.
	failPath atomic.Value
}

func (p *countingProbe) probe(_ context.Context, path string) (string, error) {
	p.mu.Lock()
	p.calls = append(p.calls, path)
	p.mu.Unlock()

	if want, ok := p.failPath.Load().(string); ok && want == path {
		return "", errNotExecutable(path)
	}
	if p.errOnce.CompareAndSwap(true, false) {
		return "", errNotExecutable(path)
	}
	if p.err != nil {
		return "", p.err
	}

	return p.version, nil
}

func (p *countingProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.calls)
}

func (p *countingProbe) paths() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.calls...)
}

// writeSource writes a fake binary and stamps it with an explicit mtime, so
// tests never depend on filesystem timestamp granularity.
func writeSource(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()

	require.NoError(t, os.WriteFile(path, elfWithPayload(content), 0o755))
	require.NoError(t, os.Chtimes(path, mtime, mtime))
}

// payloadOf returns the payload elfWithPayload put after the header, so an
// assertion compares what a fixture carries rather than the header it has to
// carry to be loadable.
func payloadOf(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), elfHeaderLen, "not an image this suite wrote")

	return string(b[elfHeaderLen:])
}

// elfHeaderLen is the length of the header elfWithPayload emits.
const elfHeaderLen = 64

// elfWithPayload is a well-formed ELF image the structural check accepts, carrying
// payload after its header, so a
// fixture varies in size and content while remaining something the cache will
// accept. Real ELF rather than text because the cache validates the structure of
// every copy before executing it: a text fixture would be rejected, and stubbing
// that check out for the fixtures' sake would mean no test exercised the
// production path.
func elfWithPayload(payload string) []byte {
	h := make([]byte, elfHeaderLen)
	copy(h, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1, 0})    // magic, 64-bit, little-endian, v1
	binary.LittleEndian.PutUint16(h[16:], 2)            // e_type  = ET_EXEC
	binary.LittleEndian.PutUint16(h[18:], 0x3e)         // e_machine = x86-64
	binary.LittleEndian.PutUint32(h[20:], 1)            // e_version
	binary.LittleEndian.PutUint16(h[52:], elfHeaderLen) // e_ehsize
	binary.LittleEndian.PutUint16(h[54:], 56)           // e_phentsize
	binary.LittleEndian.PutUint16(h[58:], 64)           // e_shentsize

	img := make([]byte, 0, len(h)+len(payload))
	img = append(img, h...)
	img = append(img, payload...)

	return img
}

// newCacheForTest builds a Cache and drains its background warms before the
// test's temp directory is removed.
//
// WarmAsync's goroutine deliberately outlives the call that started it — nothing
// on the resume path waits for a copy — so a test that triggers one races
// t.TempDir()'s cleanup and fails with "directory not empty". That is a property
// of the design, not a defect, and it only shows up under parallelism: it took a
// soak at varied GOMAXPROCS to surface. Cleanups run last-registered-first and
// t.TempDir() registers its own when it is called, so this one must be
// registered after — which it is, since the directory is made before the cache.
func newCacheForTest(t *testing.T, dir string, probe func(context.Context, string) (string, error)) *Cache {
	t.Helper()

	c := NewCache(dir, probe)
	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()

			return len(c.warming) == 0 && len(c.inFlight) == 0
		}, 30*time.Second, 5*time.Millisecond, "background warms must finish before the temp dir is removed")
	})

	return c
}

func TestLookupMissesUntilWarmed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)

	// The whole point: a lookup never copies and never probes, so the mount work a
	// caller on the resume path can be delayed by is one stat.
	_, ok := c.Lookup(src)
	require.False(t, ok, "nothing is cached before a warm")
	require.Zero(t, p.count(), "a lookup must not probe")

	require.NoError(t, c.Warm(t.Context(), src))

	entry, ok := c.Lookup(src)
	require.True(t, ok)
	require.Equal(t, "0.7.0", entry.Version)
	require.Equal(t, 1, p.count())

	// The copy is a distinct file in the cache directory, with the source's bytes
	// and its own executable mode.
	require.True(t, entry.Cached())
	require.Equal(t, filepath.Join(dir, "cache"), filepath.Dir(entry.LocalPath))
	require.Equal(t, "binary-v1", payloadOf(t, entry.LocalPath))
	fi, err := os.Stat(entry.LocalPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), fi.Mode().Perm())

	// Repeat lookups are free and hand back the same entry.
	again, ok := c.Lookup(src)
	require.True(t, ok)
	require.Same(t, entry, again)
	require.Equal(t, 1, p.count())
}

func TestWarmIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)

	require.NoError(t, c.Warm(t.Context(), src))
	require.NoError(t, c.Warm(t.Context(), src))
	require.NoError(t, c.Warm(t.Context(), src))

	// A miss recurs on every resume until a warm lands, so a warm that re-read an
	// already-cached binary would put the read back on the resume path's heels
	// rather than removing it.
	require.Equal(t, 1, p.count(), "warming an already-cached binary must be free")
}

func TestWarmProbesTheCopyNotTheSource(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)
	require.NoError(t, c.Warm(t.Context(), src))

	entry, ok := c.Lookup(src)
	require.True(t, ok)

	// The version and the bytes must describe the same file: probing the source
	// while delivering the copy is the window this cache exists to close.
	require.Equal(t, []string{entry.LocalPath}, p.paths())
	require.NotContains(t, p.paths(), src)
}

func TestInvalidatesOnMtimeAloneAtEqualSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	// Promotion overwrites the object in place. Two builds can be byte-identical
	// in length, so mtime is the only thing that changes here -- if the key
	// ignored it, this would serve the superseded binary forever.
	writeSource(t, src, "AAAAAAAAA", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)
	require.NoError(t, c.Warm(t.Context(), src))
	first, ok := c.Lookup(src)
	require.True(t, ok)

	writeSource(t, src, "BBBBBBBBB", time.Unix(1_700_000_500, 0))
	p.version = "0.7.1"

	// The promoted binary changed, so the cached copy must stop counting.
	_, ok = c.Lookup(src)
	require.False(t, ok, "an advanced mtime must invalidate the entry")

	require.NoError(t, c.Warm(t.Context(), src))
	second, ok := c.Lookup(src)
	require.True(t, ok)
	require.Equal(t, "0.7.1", second.Version)
	require.Equal(t, 2, p.count())

	require.Equal(t, "BBBBBBBBB", payloadOf(t, second.LocalPath))

	// The superseded copy is gone, and the current one is not.
	require.NoFileExists(t, first.LocalPath)
	require.FileExists(t, second.LocalPath)
}

func TestInvalidatesOnSizeChange(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	mtime := time.Unix(1_700_000_000, 0)
	writeSource(t, src, "short", mtime)

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)
	require.NoError(t, c.Warm(t.Context(), src))

	// Same mtime, different size: the other half of the key on its own.
	writeSource(t, src, "considerably-longer", mtime)

	_, ok := c.Lookup(src)
	require.False(t, ok, "a changed size must invalidate the entry")
}

func TestAFailedProbePublishesNothingAndPinsTheIdentity(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	p.errOnce.Store(true)
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)

	err := c.Warm(t.Context(), src)
	require.Error(t, err, "a failing probe must surface")
	require.ErrorIs(t, err, errBadTarget,
		"a copy that will not run is a property of the target, not of this node: it is the only "+
			"place a corrupt promotion is detectable once the resolver stops probing the source")
	_, ok := c.Lookup(src)
	require.False(t, ok)

	// The verdict is about the BYTES, so no delay makes them runnable and this
	// identity is not retried at all. Re-warming reports the suppression rather
	// than copying 13 MB again, and the probe count proves no second attempt.
	probesAfterFailure := p.count()
	require.ErrorIs(t, c.Warm(t.Context(), src), errBadIdentityKnown,
		"an identity already proved unable to run must not be copied again")
	require.Equal(t, probesAfterFailure, p.count(), "no second probe of the same identity")

	// Recovery needs no window and no intervention: a promotion is a DIFFERENT
	// identity, so it is warmed on the very next miss.
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_100, 0))
	require.NoError(t, c.Warm(t.Context(), src))
	entry, ok := c.Lookup(src)
	require.True(t, ok)
	require.Equal(t, "0.7.0", entry.Version)

	// Nor may the failed attempt leave its copy behind.
	names, err := os.ReadDir(filepath.Join(dir, "cache"))
	require.NoError(t, err)
	require.Len(t, names, 1)
	require.Equal(t, filepath.Base(entry.LocalPath), names[0].Name())
}

func TestWarmMissingSourceIsAnError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)

	require.Error(t, c.Warm(t.Context(), filepath.Join(dir, "absent")))
	require.Zero(t, p.count(), "an absent source must not be probed")
}

func TestWarmFailsWhenTheCacheIsUnusable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	// A regular file where the cache directory should be: MkdirAll cannot make a
	// directory under it, so every copy fails.
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(blocker, "cache"), p.probe)

	// A warm failure is the warmer's problem, not a caller's: it reports to its
	// own metric and the resume path simply keeps missing.
	require.Error(t, c.Warm(t.Context(), src))
	_, ok := c.Lookup(src)
	require.False(t, ok)
	require.Zero(t, p.count(), "an unusable cache must not probe anything")
}

func TestNewCacheTouchesNoFilesystem(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")

	// The zero-value config a unit test builds a Factory with has no cache
	// directory at all; constructing one must not be a filesystem operation.
	_ = newCacheForTest(t, cacheDir, (&countingProbe{}).probe)
	_ = newCacheForTest(t, "", (&countingProbe{}).probe)

	require.NoDirExists(t, cacheDir)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestWarmAsyncWithNoDirectoryConfiguredDoesNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, "", p.probe)

	// No goroutine, no probe, no panic — the miss path just stays a miss.
	c.WarmAsync(t.Context(), src)
	_, ok := c.Lookup(src)
	require.False(t, ok)
}

func TestWarmDeduplicatesConcurrentCallers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	// release gates the probe so every goroutine is inside Warm before any of them
	// completes: the assertion is on the number of probes, never on how long
	// anything took.
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var probes atomic.Int64
	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(_ context.Context, _ string) (string, error) {
		probes.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release

		return "0.7.0", nil
	})

	const callers = 16
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			_ = c.Warm(t.Context(), src)
		})
	}

	// Release the flight once the leader is inside the probe. No timing assumption
	// either way: a caller that joined the flight shares its result, and one that
	// arrives after it completed finds the entry already stored. Both leave the
	// probe count at one, which is the property under test.
	<-entered
	close(release)
	wg.Wait()

	require.EqualValues(t, 1, probes.Load(), "concurrent warms must collapse into one read")
	_, ok := c.Lookup(src)
	require.True(t, ok)
}

func TestStoreEvictsOldestPastTheCapAndDeletesItsCopy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, cacheDir, p.probe)

	// One more distinct source path than the cache holds.
	paths := make([]string, maxEntries+1)
	locals := make([]string, len(paths))
	for i := range paths {
		paths[i] = filepath.Join(dir, "envd."+string(rune('a'+i)))
		writeSource(t, paths[i], "binary-"+string(rune('a'+i)), time.Unix(int64(1_700_000_000+i), 0))

		require.NoError(t, c.Warm(t.Context(), paths[i]))
		entry, ok := c.Lookup(paths[i])
		require.True(t, ok)
		locals[i] = entry.LocalPath
	}

	// The first inserted is evicted, and its copy is not merely forgotten.
	require.NoFileExists(t, locals[0])
	for _, l := range locals[1:] {
		require.FileExists(t, l)
	}

	names, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	require.Len(t, names, maxEntries)

	// The evicted path is a miss again, not a silently-served ghost.
	_, ok := c.Lookup(paths[0])
	require.False(t, ok)
}

func TestSweepRemovesOrphansButSparesLiveAndInProgressFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")

	first := filepath.Join(dir, "envd")
	writeSource(t, first, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, cacheDir, p.probe)

	require.NoError(t, c.Warm(t.Context(), first))
	firstEntry, ok := c.Lookup(first)
	require.True(t, ok)

	// Both appear after the once-per-cache reap, so only the sweep can act on
	// them: a copy no entry references, and a temporary file standing in for a
	// concurrent copy still being written.
	orphan := filepath.Join(cacheDir, "envd.999.999.deadbeef")
	inProgress := filepath.Join(cacheDir, tempPrefix+"concurrent")
	require.NoError(t, os.WriteFile(orphan, []byte("stale"), 0o755))
	require.NoError(t, os.WriteFile(inProgress, []byte("partial"), 0o600))

	// A second source path, to trigger a store and with it a sweep.
	second := filepath.Join(dir, "envd.abc")
	writeSource(t, second, "binary-v2", time.Unix(1_700_000_500, 0))
	require.NoError(t, c.Warm(t.Context(), second))
	secondEntry, ok := c.Lookup(second)
	require.True(t, ok)

	require.NoFileExists(t, orphan, "an unreferenced copy must be reclaimed")
	// Deleting this would make the concurrent copy's rename fail, downgrading it
	// to a miss for no reason.
	require.FileExists(t, inProgress, "an in-progress temporary file must be spared")
	require.FileExists(t, firstEntry.LocalPath, "a live copy must survive a sweep")
	require.FileExists(t, secondEntry.LocalPath)
}

func TestFirstUseReapsTemporaryFilesFromAPreviousProcess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))

	// Orphaned by a process that died mid-copy: nothing else ever removes it.
	abandoned := filepath.Join(cacheDir, tempPrefix+"abandoned")
	require.NoError(t, os.WriteFile(abandoned, []byte("partial"), 0o600))

	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, cacheDir, p.probe)
	require.NoError(t, c.Warm(t.Context(), src))

	entry, ok := c.Lookup(src)
	require.True(t, ok)
	require.NoFileExists(t, abandoned)
	require.FileExists(t, entry.LocalPath)
}

func TestLookupTreatsAVanishedCopyAsAMiss(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), p.probe)
	require.NoError(t, c.Warm(t.Context(), src))

	first, ok := c.Lookup(src)
	require.True(t, ok)
	require.NoError(t, os.Remove(first.LocalPath))

	// Without re-checking the file, the key still matches and this stays a
	// permanent phantom hit that is never re-warmed.
	_, ok = c.Lookup(src)
	require.False(t, ok, "a missing copy must read as a miss, not a hit")

	require.NoError(t, c.Warm(t.Context(), src))
	second, ok := c.Lookup(src)
	require.True(t, ok)
	require.FileExists(t, second.LocalPath)
	require.Equal(t, 2, p.count())
}

func TestCacheSideAndSourceSideFailuresSchedule(t *testing.T) {
	t.Parallel()

	t.Run("cache-side failure suppresses the next copy", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := filepath.Join(dir, "envd")
		writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

		blocker := filepath.Join(dir, "blocker")
		require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))
		cacheDir := filepath.Join(blocker, "cache")

		p := &countingProbe{version: "0.7.0"}
		c := newCacheForTest(t, cacheDir, p.probe)
		require.Error(t, c.Warm(t.Context(), src))

		// Repairing the directory does not immediately re-enable copying: a miss
		// recurs on every resume, so retrying a whole 13 MB read each time is
		// exactly what the backoff exists to prevent.
		require.NoError(t, os.Remove(blocker))
		require.NoError(t, os.MkdirAll(cacheDir, 0o755))

		require.ErrorIs(t, c.Warm(t.Context(), src), errWarmSuppressed,
			"the schedule must suppress the next attempt")

		c.noteSuccess(src)

		require.NoError(t, c.Warm(t.Context(), src), "a success must clear the schedule, not disable the cache")
	})

	t.Run("a source-side failure schedules too, from the base", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		cacheDir := filepath.Join(dir, "cache")

		// A directory as the source: os.Stat and os.Open both succeed and the read
		// fails with EISDIR. A source-side failure, deterministic whether or not the
		// suite runs as root — unlike chmod 000, which root ignores.
		src := filepath.Join(dir, "envd")
		require.NoError(t, os.MkdirAll(src, 0o755))

		p := &countingProbe{version: "0.7.0"}
		c := newCacheForTest(t, cacheDir, p.probe)
		require.Error(t, c.Warm(t.Context(), src))

		// It is scheduled, because retrying a 13 MB read on the very next resume
		// puts the outage's own volume back on the mount the resumes depend on.
		c.mu.Lock()
		b := c.backoff[src]
		c.mu.Unlock()
		require.NotNil(t, b, "a source failure must be scheduled")
		require.Equal(t, 1, b.failures)
		require.False(t, b.hasBad,
			"a source failure says nothing about the bytes and must not pin the identity")

		// The first delay is the BASE, not the ceiling: a transient fault must not
		// cost a full window. Asserted on the scheduled instant rather than by
		// waiting, so the test measures the policy and not the machine.
		require.WithinDuration(t, time.Now().Add(backoffBase), b.until, backoffBase,
			"the first retry waits the base, not the ceiling")

		// Positive twin: once the schedule is clear and the source readable, the
		// very next warm copies.
		c.noteSuccess(src)
		require.NoError(t, os.RemoveAll(src))
		writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))
		require.NoError(t, c.Warm(t.Context(), src))
		_, ok := c.Lookup(src)
		require.True(t, ok)
	})
}

func TestLocalNameEncodesTheWholeKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "AAAA", time.Unix(1_700_000_000, 0))
	first, err := os.Stat(src)
	require.NoError(t, err)

	writeSource(t, src, "AAAA", time.Unix(1_700_000_500, 0))
	second, err := os.Stat(src)
	require.NoError(t, err)

	// Equal size, different mtime: the names must still differ, or one generation
	// would be served under the other's name.
	assert.NotEqual(t, localName(src, first), localName(src, second))
	// Carrying the source's basename is a deliberate requirement, not an accident
	// of the format: it is what makes the cache directory readable to someone
	// looking at a node and asking which binary a copy came from.
	assert.Contains(t, localName(src, first), "envd.",
		"a copy's name must identify the source it came from")
}

func TestLocalNameSeparatesSourcesSharingBasenameSizeAndMtime(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	one := filepath.Join(dir, "a")
	two := filepath.Join(dir, "b")
	require.NoError(t, os.MkdirAll(one, 0o755))
	require.NoError(t, os.MkdirAll(two, 0o755))

	mtime := time.Unix(1_700_000_000, 0)
	first := filepath.Join(one, "envd")
	second := filepath.Join(two, "envd")
	writeSource(t, first, "SAMESIZE", mtime)
	writeSource(t, second, "SAMESIZE", mtime)

	fi1, err := os.Stat(first)
	require.NoError(t, err)
	fi2, err := os.Stat(second)
	require.NoError(t, err)

	// Identical basename, size and mtime: without the path in the name these would
	// share one file and serve each other's bytes.
	require.NotEqual(t, localName(first, fi1), localName(second, fi2))
}

func TestCopyContextStopsOnADeadline(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := copyContext(ctx, io.Discard, strings.NewReader(strings.Repeat("x", copyChunk*3)))
	require.ErrorIs(t, err, context.Canceled)
}

func TestNilCacheIsAlwaysAMiss(t *testing.T) {
	t.Parallel()

	// The source must exist, or Lookup would miss on the stat before it ever
	// dereferences the cache — and the test would pass without the guard.
	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	var c *Cache

	_, ok := c.Lookup(src)
	require.False(t, ok)
	c.WarmAsync(t.Context(), src) // must not panic
}

func TestStoreKeepsTheNewerGenerationWhenAStaleWarmFinishesLast(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	src := filepath.Join(dir, "envd")

	p := &countingProbe{version: "0.7.1"}
	c := newCacheForTest(t, cacheDir, p.probe)

	// The post-promotion generation, published first.
	writeSource(t, src, "BBBBBBBBB", time.Unix(1_700_000_500, 0))
	require.NoError(t, c.Warm(t.Context(), src))
	newer, ok := c.Lookup(src)
	require.True(t, ok)

	// Now a warm that began before the promotion finishes: two warms of the same
	// path carry different keys, so they run concurrently and can land in either
	// order. Its copy exists on disk, as a real one would.
	stalePath := filepath.Join(cacheDir, "envd.9.1700000000000000000.stale")
	require.NoError(t, os.WriteFile(stalePath, []byte("AAAAAAAAA"), 0o755))
	c.store(t.Context(), &Entry{
		SourcePath: src,
		LocalPath:  stalePath,
		Version:    "0.7.0",
		size:       9,
		modTime:    time.Unix(1_700_000_000, 0),
	})

	// The newer entry must survive, and its copy must not have been deleted.
	current, ok := c.Lookup(src)
	require.True(t, ok, "a stale warm must not leave an entry that no longer matches the source")
	require.Equal(t, "0.7.1", current.Version)
	require.Equal(t, newer.LocalPath, current.LocalPath)
	require.FileExists(t, newer.LocalPath)
	// And the loser cleans up after itself rather than leaving an orphan.
	require.NoFileExists(t, stalePath)
}

func TestWarmAsyncRunsOneGoroutinePerBinary(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	// Park the warm so every later call arrives while it is still in flight —
	// which is the situation on a node, where a miss recurs on every resume until
	// the warm lands.
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(_ context.Context, _ string) (string, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release

		return "0.7.0", nil
	})

	for range 32 {
		c.WarmAsync(t.Context(), src)
	}
	<-entered

	// The singleflight inside Warm collapses the READ, but not the goroutines
	// waiting on it — and each of those would report the shared read as its own
	// warm, so the counter would measure callers rather than reads.
	c.mu.Lock()
	inFlight := len(c.warming)
	c.mu.Unlock()
	require.Equal(t, 1, inFlight, "a second WarmAsync for the same binary must not spawn")

	close(release)
	require.Eventually(t, func() bool {
		_, ok := c.Lookup(src)

		return ok
	}, 5*time.Second, 10*time.Millisecond)

	// And once the goroutine is done the claim is released, so a later miss can
	// warm again. Waited for rather than asserted outright: the entry is published
	// inside Warm, while the claim is released by the goroutine's defer just
	// after, so reading it immediately races a window that is not a defect.
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.warming) == 0
	}, 5*time.Second, 10*time.Millisecond, "the claim must be released when the warm finishes")
}

// The guard against a stale warm must not be expressible as "newer than what is
// resident". An entry that once recorded an mtime no later source can beat would
// then reject every publication for that path forever, and the cache would be
// silently and permanently wedged: every resume misses, every warm reports
// success, and nothing says why.
func TestAnEntryWithAFutureMtimeDoesNotWedgeTheCache(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	p := &countingProbe{version: "0.7.0"}
	c := newCacheForTest(t, cacheDir, p.probe)

	// An entry recording an mtime far ahead of anything the source will carry.
	// Reachable in production from clock skew or an out-of-order stat; reachable
	// in a fixture from writing a file and stamping it afterwards.
	require.NoError(t, os.MkdirAll(cacheDir, 0o755))
	poisoned := filepath.Join(cacheDir, "envd.9.9999999999999999999.poisoned")
	require.NoError(t, os.WriteFile(poisoned, []byte("stale"), 0o755))
	c.store(t.Context(), &Entry{
		SourcePath: src,
		LocalPath:  poisoned,
		Version:    "9.9.9",
		size:       9,
		modTime:    time.Unix(4_000_000_000, 0),
	})

	// That entry does not describe the source, so it must never have been
	// published in the first place.
	_, ok := c.Lookup(src)
	require.False(t, ok, "an entry that does not describe the source must not be resident")

	// And the cache must still be usable: a warm publishes the real generation.
	require.NoError(t, c.Warm(t.Context(), src))
	entry, ok := c.Lookup(src)
	require.True(t, ok, "the cache must not be wedged by an out-of-order mtime")
	require.Equal(t, "0.7.0", entry.Version)
}

func TestWarmResultSeparatesSuppressionFromFailure(t *testing.T) {
	t.Parallel()

	// A miss recurs on every resume, so during the backoff every one of them would
	// otherwise report a failure — burying the case where the warmer is genuinely
	// broken under the backoff working exactly as intended.
	require.Equal(t, "suppressed", warmResult(errWarmSuppressed))
	require.Equal(t, "suppressed", warmResult(fmt.Errorf("wrapped: %w", errWarmSuppressed)))
	require.Equal(t, "failed", warmResult(errors.New("no space left on device")))
	require.Equal(t, "failed", warmResult(fmt.Errorf("copy: %w", errCacheUnusable)))
}

func TestAZeroValueCacheStatsThroughOsStat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	// statSrc exists only so a test can model a stalled mount. A Cache that never
	// set it must be indistinguishable from one that could not: the field is a test
	// seam, not a behaviour.
	bare := &Cache{
		dir:      filepath.Join(dir, "cache"),
		probe:    func(context.Context, string) (string, error) { return "0.7.0", nil },
		entries:  make(map[string]*Entry),
		inFlight: make(map[string]int),
		warming:  make(map[string]time.Time),
	}
	require.Nil(t, bare.statSrc)

	_, ok := bare.Lookup(src)
	require.False(t, ok, "a lookup on an unwarmed cache misses rather than panics")
	require.NoError(t, bare.Warm(t.Context(), src))

	entry, ok := bare.Lookup(src)
	require.True(t, ok, "a nil statSrc must key the entry exactly as os.Stat would")
	require.Equal(t, "0.7.0", entry.Version)
}

// TestPublishedDescriptionsNameEveryLabelTheCodeEmits closes a silent gap: the
// label vocabulary lives in one module and the description a dashboard reader
// sees in another, so the two drift with nothing failing -- only a human
// comparing them notices.
//
// The descriptions are read back through a real SDK reader rather than the
// description map, so this asserts what is actually published on the instrument.
func TestPublishedDescriptionsNameEveryLabelTheCodeEmits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		counter telemetry.CounterType
		labels  []string
	}{
		{telemetry.OrchestratorEnvdBinaryCacheReads, outcomeStrings(readOutcomes)},
		{telemetry.OrchestratorEnvdBinaryCacheDeliveries, outcomeStrings(deliveryOutcomes)},
		{telemetry.OrchestratorEnvdBinaryCacheWarms, warmResults},
	} {
		t.Run(string(tc.counter), func(t *testing.T) {
			t.Parallel()

			desc := publishedDescription(t, tc.counter)
			require.NotEmpty(t, desc, "an unlabelled metric ships silently")
			for _, l := range tc.labels {
				require.Containsf(t, desc, l,
					"%s emits %q but its published description does not name it", tc.counter, l)
			}
		})
	}
}

func outcomeStrings(os []Outcome) []string {
	out := make([]string, 0, len(os))
	for _, o := range os {
		out = append(out, string(o))
	}

	return out
}

// publishedDescription returns the description the counter is registered with, by
// recording one point and reading it back off a manual reader.
func publishedDescription(t *testing.T, name telemetry.CounterType) string {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	counter, err := telemetry.GetCounter(provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin"), name)
	require.NoError(t, err)
	counter.Add(t.Context(), 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == string(name) {
				return m.Description
			}
		}
	}
	require.Failf(t, "counter was not collected", "%s", name)

	return ""
}

// A clean EOF is not proof of a complete copy. A source that ends the read early
// while still matching the key yields a truncated copy of an intact binary --
// which validateImage rejects as incomplete, and that verdict is about the bytes,
// so an intact promoted artifact would be pinned. The shortfall must therefore be
// a failure of the copy, not of the target.
func TestAShortReadOfAnUnchangedSourceIsACopyFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "the-whole-binary", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})

	// A source whose stat says one length and whose reads end sooner: exactly what
	// a mount ending a read early looks like from here.
	c.statSrc = func(path string) (os.FileInfo, error) {
		fi, err := os.Stat(path)
		if err != nil {
			return nil, err
		}

		return longerThanItReads{FileInfo: fi}, nil
	}

	err := c.Warm(t.Context(), src)
	require.Error(t, err, "a copy that ended early must not be published")
	require.Contains(t, err.Error(), "short read")

	// Scheduled as the copy failure it is, and NOT pinned -- the source is fine.
	fi, statErr := os.Stat(src)
	require.NoError(t, statErr)
	require.False(t, c.identityKnownBad(src, fi),
		"a short read is the copy's fault, never a verdict about the binary")
	c.mu.Lock()
	b := c.backoff[src]
	c.mu.Unlock()
	require.NotNil(t, b)
	require.Equal(t, 1, b.failures)

	// Positive twin: with the real stat the same warm completes, so the assertion
	// above is about the shortfall and not about a warm that never works.
	c.statSrc = os.Stat
	c.noteSuccess(src)
	require.NoError(t, c.Warm(t.Context(), src))
	_, ok := c.Lookup(src)
	require.True(t, ok)
}

// longerThanItReads reports a size one byte beyond what the file actually holds.
type longerThanItReads struct{ os.FileInfo }

func (f longerThanItReads) Size() int64 { return f.FileInfo.Size() + 1 }
