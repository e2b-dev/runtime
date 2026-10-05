//go:build linux

package template

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// pinTestTemplate is a Template whose Close is destructive in the same way the
// real one is: closeTemplate removes the snapfile with os.RemoveAll and
// storageTemplate.Close then removes the instance's cache directory, so the
// regression these tests guard is "a pinned template's file is still on disk",
// not merely "Close was not called".
//
// Each instance owns its own path, as production does: storage.Paths.Cache
// mints a fresh CacheIdentifier per call, so two instances for one build never
// share a snapfile.
type pinTestTemplate struct {
	key          string
	snapfilePath string
	closes       atomic.Int32
}

func newPinTestTemplate(t *testing.T, key string) *pinTestTemplate {
	t.Helper()

	path := filepath.Join(t.TempDir(), "snapfile")
	require.NoError(t, os.WriteFile(path, []byte("vm state"), 0o600))

	return &pinTestTemplate{key: key, snapfilePath: path}
}

func (f *pinTestTemplate) Files() storage.CachePaths {
	return storage.CachePaths{Paths: storage.Paths{BuildID: f.key}}
}

func (f *pinTestTemplate) Close(_ context.Context) error {
	// Remove before recording: a test that waits on the counter would otherwise
	// observe the close and assert on the file before it is gone.
	err := os.RemoveAll(f.snapfilePath)
	f.closes.Add(1)

	return err
}

func (f *pinTestTemplate) snapfileExists() bool {
	_, err := os.Stat(f.snapfilePath)

	return err == nil
}

func (f *pinTestTemplate) Memfile(context.Context) (block.ReadonlyDevice, error) { return nil, nil }
func (f *pinTestTemplate) Rootfs() (block.ReadonlyDevice, error)                 { return nil, nil }
func (f *pinTestTemplate) Snapfile() (File, error)                               { return nil, nil }
func (f *pinTestTemplate) Metadata() (metadata.Template, error)                  { return metadata.Template{}, nil }
func (f *pinTestTemplate) UpdateMetadata(metadata.Template) error                { return nil }

// pinForTest pins a template that is already the live cache entry, for tests
// where no eviction is in flight.
func pinForTest(t *testing.T, c *Cache, tmpl Template) func() {
	t.Helper()

	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	return c.pinLocked(t.Context(), tmpl)
}

// newPinTestCache builds a Cache wired to the REAL eviction callback, and a
// channel that receives each key once that callback has returned. It must call
// c.onEvicted rather than reimplementing it: a harness carrying its own copy of
// the eviction logic tests the copy, not the code that ships, and will pass
// happily while production deletes a live sandbox's snapfile.
//
// The channel is the synchronization barrier these tests need. ttlcache
// dispatches the callback on its own goroutine, so without a completion signal
// an assertion can run before a buggy callback has closed anything and pass for
// the wrong reason.
//
// Constructing a full Cache needs disk config and a DiffStore, so only the cache
// and pin state are built here.
func newPinTestCache(ttl time.Duration) (*Cache, <-chan string) {
	return newPinTestCacheWithPurge(ttl, func(string) {})
}

// newPinTestCacheWithPurge is newPinTestCache with the peer-purge hook exposed.
// Registering a second OnEviction on an existing cache would *add* a callback
// rather than replace it (both would then run), so the hook has to be supplied
// at construction.
func newPinTestCacheWithPurge(ttl time.Duration, purge func(string)) (*Cache, <-chan string) {
	c := &Cache{
		pinned:  make(map[string]*pinnedEntry),
		retired: make(map[*pinnedEntry]struct{}),
	}

	evicted := make(chan string, 32)

	inner := ttlcache.New(ttlcache.WithTTL[string, Template](ttl))
	inner.OnEviction(func(ctx context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[string, Template]) {
		c.onEvicted(ctx, item, purge)
		evicted <- item.Key()
	})

	c.cache = inner

	return c, evicted
}

// waitEvicted blocks until the eviction callback for key has finished.
func waitEvicted(t *testing.T, evicted <-chan string, key string) {
	t.Helper()

	for {
		select {
		case got := <-evicted:
			if got == key {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("eviction callback for %q did not run", key)
		}
	}
}

// The regression that matters: TTL expiry must not delete a live sandbox's
// snapfile out from under it.
func TestPin_SurvivesTTLExpiryAndKeepsSnapfile(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(40 * time.Millisecond)
	go c.cache.Start()
	defer c.cache.Stop()

	tmpl := newPinTestTemplate(t, "build-pinned")
	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)

	release := pinForTest(t, c, tmpl)
	defer release()

	waitEvicted(t, evicted, tmpl.key)

	assert.Zero(t, tmpl.closes.Load(), "pinned template must never be Closed")
	assert.True(t, tmpl.snapfileExists(), "pinned template's snapfile must still exist on disk")

	// Still reachable, and still the same instance — never a second one.
	got, releaseGot, ok := c.LookupPinned(t.Context(), tmpl.key)
	defer releaseGot()
	require.True(t, ok, "pinned template must remain reachable after eviction")
	assert.Same(t, tmpl, got)
}

func TestPin_UnpinnedEntryStillEvictsAndCloses(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(40 * time.Millisecond)
	go c.cache.Start()
	defer c.cache.Stop()

	tmpl := newPinTestTemplate(t, "build-unpinned")
	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)

	waitEvicted(t, evicted, tmpl.key)

	assert.Equal(t, int32(1), tmpl.closes.Load(), "unpinned template must still be closed on expiry")
	assert.False(t, tmpl.snapfileExists(), "unpinned template's files should be removed")
}

// A template is shared by every sandbox from the same build, so the pin is
// counted. A boolean pin would let the first sandbox to exit expose templates
// its siblings are still running on.
func TestPin_RefcountedAcrossSandboxes(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-shared")
	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)

	releaseA := pinForTest(t, c, tmpl)
	releaseB := pinForTest(t, c, tmpl)

	releaseA()
	assert.True(t, c.isPinned(tmpl.key), "still pinned while a second sandbox holds it")

	releaseB()
	assert.False(t, c.isPinned(tmpl.key), "last release lifts the pin")
}

func TestPin_ReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-idempotent")
	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)

	releaseA := pinForTest(t, c, tmpl)
	releaseB := pinForTest(t, c, tmpl)

	// The rollback path and the lifecycle goroutine may both hold the same
	// release; extra calls must not decrement a sibling's pin.
	releaseA()
	releaseA()
	releaseA()

	assert.True(t, c.isPinned(tmpl.key), "repeated release of one pin must not drop another's")

	releaseB()
	assert.False(t, c.isPinned(tmpl.key))
}

// release on an entry that is already at zero must not underflow the refcount.
func TestPin_ReleaseBeyondZeroIsNoop(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-underflow")

	release := pinForTest(t, c, tmpl)
	release()

	// A second, independently created release for the same entry — what a caller
	// that stashed the closure twice would produce without sync.OnceFunc.
	c.pinMu.Lock()
	entry := &pinnedEntry{tmpl: tmpl, key: tmpl.key}
	c.pinMu.Unlock()
	c.release(t.Context(), entry, 0)

	assert.False(t, c.isPinned(tmpl.key))
	assert.Equal(t, 0, entry.refs(), "refcount must not go negative")
}

// The pin-vs-eviction race build.DiffStore documents in scheduleDelete. Pinning
// a pointer the caller already holds cannot be made safe — the eviction decision
// may already have been taken. Production therefore pins only what the cache
// hands back under the same lock the eviction callback uses, so whatever ends up
// pinned is always a live template.
//
// This drives lookupOrAdmit, the function getTemplateWithFetch itself calls, so
// moving the pin out from under extendMu there fails here. A harness that
// reimplemented the lookup would keep passing through exactly that bug.
func TestPin_RaceWithEviction(t *testing.T) {
	t.Parallel()

	for i := range 300 {
		c, _ := newPinTestCache(time.Hour)
		original := newPinTestTemplate(t, "build-race")
		c.cache.Set(original.key, original, ttlcache.DefaultTTL)

		// Production builds a fresh storageTemplate per lookup; if the resident
		// one was evicted and closed, this is what gets cached and pinned.
		fresh := newPinTestTemplate(t, "build-race")

		var (
			wg      sync.WaitGroup
			pinned  Template
			release func()
		)

		wg.Add(2)

		go func() {
			defer wg.Done()
			c.cache.Delete(original.key)
		}()

		go func() {
			defer wg.Done()
			pinned, _, release = c.lookupOrAdmit(t.Context(), original.key, fresh, time.Hour, true)
		}()

		wg.Wait()

		// ttlcache dispatches OnEviction on its own goroutine; let it land.
		time.Sleep(time.Millisecond)

		pt, ok := pinned.(*pinTestTemplate)
		require.True(t, ok)
		require.True(t, pt.snapfileExists(),
			"iteration %d: pinned template's snapfile was deleted", i)
		require.Zero(t, pt.closes.Load(),
			"iteration %d: pinned template was closed", i)

		release()
	}
}

// filesHookTemplate runs a hook whenever the pin machinery asks for its cache
// key, which is the first thing pinLocked does. It is how the test below
// observes the exact moment the pin is being recorded.
type filesHookTemplate struct {
	*pinTestTemplate

	once sync.Once
	hook func()
}

func (f *filesHookTemplate) Files() storage.CachePaths {
	f.once.Do(f.hook)

	return f.pinTestTemplate.Files()
}

// The load-bearing half of the design: the pin must be recorded while extendMu
// is held. Everything else rests on it — a pin taken after the lock is dropped
// can land after the eviction decision was already taken, on a template whose
// snapfile is being deleted, and no amount of care in the callback can recover
// from that.
//
// Asserted directly rather than raced for, because the window is a few
// instructions wide and a probabilistic test would pass against the bug most of
// the time.
func TestPin_IsRecordedUnderExtendMu(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)

	pinning := make(chan struct{})
	proceed := make(chan struct{})

	tmpl := &filesHookTemplate{pinTestTemplate: newPinTestTemplate(t, "build-lock-check")}
	tmpl.hook = func() {
		close(pinning)
		<-proceed
	}

	c.cache.Set("build-lock-check", tmpl, ttlcache.DefaultTTL)

	released := make(chan func(), 1)
	go func() {
		_, _, release := c.lookupOrAdmit(t.Context(), "build-lock-check", tmpl, time.Hour, true)
		released <- release
	}()

	select {
	case <-pinning:
	case <-time.After(5 * time.Second):
		t.Fatal("the pin was never recorded")
	}

	// The pin is being recorded right now. If extendMu is free at this instant,
	// an eviction callback could be running the guard concurrently.
	free := c.extendMu.TryLock()
	if free {
		c.extendMu.Unlock()
	}

	close(proceed)

	assert.False(t, free, "the pin must be recorded while extendMu is held")

	release := <-released
	release()
}

// Once the last pin drops, the template rejoins the TTL cache so it expires
// normally instead of leaking its files forever.
func TestUnpin_ReadmitsEvictedTemplate(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(40 * time.Millisecond)
	go c.cache.Start()
	defer c.cache.Stop()

	tmpl := newPinTestTemplate(t, "build-readmit")
	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)

	release := pinForTest(t, c, tmpl)

	waitEvicted(t, evicted, tmpl.key)
	release()

	item := c.cache.Get(tmpl.key)
	require.NotNil(t, item, "template must rejoin the cache so it can eventually be closed")
	assert.Same(t, tmpl, item.Value())
}

// Re-admission must never displace a different live entry that took the key.
func TestUnpin_DoesNotDisplaceLiveEntry(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)

	pinnedTmpl := newPinTestTemplate(t, "build-collide")
	release := pinForTest(t, c, pinnedTmpl)

	successor := newPinTestTemplate(t, "build-collide")
	c.cache.Set(successor.key, successor, ttlcache.DefaultTTL)

	release()

	item := c.cache.Get("build-collide")
	require.NotNil(t, item)
	assert.Same(t, successor, item.Value(), "live entry must not be replaced by a released pin")
	assert.True(t, successor.snapfileExists())
}

// An eviction whose callback is still pending must close its own instance — and
// only its own. Instances for a build own disjoint directories, so the
// superseded one has files nothing else will ever reclaim; skipping its Close
// would orphan them, while closing it must leave the newer instance untouched.
//
// extendMu is held across the delete-and-reinsert to pin the interleaving
// deterministically: the callback blocks on it until the new entry is in place,
// reproducing "callback ran after the slot was refilled" every run.
func TestEvict_SupersededInstanceClosesOnlyItsOwnFiles(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	old := newPinTestTemplate(t, "build-superseded")
	fresh := newPinTestTemplate(t, "build-superseded")

	c.cache.Set(old.key, old, ttlcache.DefaultTTL)

	c.extendMu.Lock()
	c.cache.Delete(old.key)                          // callback spawns, blocks on extendMu
	c.cache.Set(old.key, fresh, ttlcache.DefaultTTL) // a newer instance takes the slot
	c.extendMu.Unlock()

	waitEvicted(t, evicted, old.key)

	assert.Equal(t, int32(1), old.closes.Load(), "superseded instance owns its files and must be closed")
	assert.False(t, old.snapfileExists(), "superseded instance's files must not be orphaned")
	assert.True(t, fresh.snapfileExists(), "live instance lost its snapfile to a superseded instance's Close")
}

// Peer chunk routing is keyed by build, not by instance, so a superseded
// instance's eviction must not purge routing the newer instance is still using.
func TestEvict_SupersededInstanceKeepsPeerRouting(t *testing.T) {
	t.Parallel()

	purged := make(chan string, 1)
	c, evicted := newPinTestCacheWithPurge(time.Hour, func(k string) { purged <- k })

	old := newPinTestTemplate(t, "build-routing")
	fresh := newPinTestTemplate(t, "build-routing")

	c.cache.Set(old.key, old, ttlcache.DefaultTTL)

	c.extendMu.Lock()
	c.cache.Delete(old.key)
	c.cache.Set(old.key, fresh, ttlcache.DefaultTTL)
	c.extendMu.Unlock()

	waitEvicted(t, evicted, old.key)

	select {
	case k := <-purged:
		t.Fatalf("peer routing purged for %q while a newer instance is live", k)
	default:
	}
}

// The interleaving that deletes a running sandbox's snapshot: the callback is
// already scheduled when a sandbox acquires and pins. The pin is on the
// instance the cache handed back, so the callback must leave that instance
// alone — and, because the two instances own disjoint files, still clean up its
// own.
func TestEvict_PinTakenAfterCallbackScheduled(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	old := newPinTestTemplate(t, "build-pin-after")
	fresh := newPinTestTemplate(t, "build-pin-after")

	c.cache.Set(old.key, old, ttlcache.DefaultTTL)

	c.extendMu.Lock()
	c.cache.Delete(old.key) // callback spawns, blocks on extendMu

	// A sandbox acquires and pins while the eviction is in flight.
	c.cache.Set(old.key, fresh, ttlcache.DefaultTTL)
	release := c.pinLocked(t.Context(), fresh)
	c.extendMu.Unlock()

	defer release()

	waitEvicted(t, evicted, old.key)

	assert.Zero(t, fresh.closes.Load(), "eviction closed the pinned instance")
	assert.True(t, fresh.snapfileExists(), "pinned sandbox lost its snapfile to an in-flight eviction")

	// The other half of guarding by instance, and the half a key-scoped guard
	// gets wrong: the superseded instance is not the pinned one, so it still owns
	// its files and must still be closed. Without this the guard could be
	// key-scoped and no test would notice.
	assert.Equal(t, int32(1), old.closes.Load(), "superseded instance must still be closed")
}

// The same interleaving with the PINNED instance as the one being evicted: a
// guard that asked "is this key pinned" rather than "is this instance pinned"
// would protect whichever instance happened to be evicted, not the one a
// sandbox is running on.
func TestEvict_PinnedInstanceItselfIsNeverClosed(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	running := newPinTestTemplate(t, "build-running")

	c.cache.Set(running.key, running, ttlcache.DefaultTTL)
	release := pinForTest(t, c, running)
	defer release()

	c.cache.Delete(running.key)
	waitEvicted(t, evicted, running.key)

	assert.Zero(t, running.closes.Load(), "the instance backing a live sandbox must never be closed")
	assert.True(t, running.snapfileExists())
}

// The last release re-admits its instance to the cache. If the eviction callback
// that sent it there is still queued, closing it now would leave the cache
// serving a template whose snapfile is already gone, and every create off that
// build would fail to load its snapshot. The guard is about the instance, so it
// has to catch this route back as well as the pin.
//
// extendMu is held across the delete-and-re-admit to reproduce the state the
// final release leaves — pin dropped, same instance back in the cache — with the
// callback still blocked, every run.
func TestEvict_ReadmittedInstanceIsNotClosed(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-readmitted")

	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)
	release := pinForTest(t, c, tmpl)

	c.extendMu.Lock()
	c.cache.Delete(tmpl.key) // callback spawns, blocks on extendMu

	c.pinMu.Lock()
	delete(c.pinned, tmpl.key) // the last release lifts the pin...
	c.pinMu.Unlock()
	c.cache.Set(tmpl.key, tmpl, unpinnedGraceTTL) // ...and puts it back
	c.extendMu.Unlock()

	waitEvicted(t, evicted, tmpl.key)

	assert.Zero(t, tmpl.closes.Load(), "the re-admitted instance is the live cache entry and must not be closed")
	assert.True(t, tmpl.snapfileExists())

	item := c.cache.Get(tmpl.key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item, "the live entry must survive its own stale eviction callback")
	assert.Same(t, tmpl, item.Value())

	release()
}

// Once the key is genuinely free, the normal path must still close and clean
// up — the guards must not turn eviction into a no-op and leak files.
func TestEvict_ClosesWhenKeyIsFree(t *testing.T) {
	t.Parallel()

	purged := make(chan string, 1)
	c, evicted := newPinTestCacheWithPurge(time.Hour, func(k string) { purged <- k })
	tmpl := newPinTestTemplate(t, "build-free")

	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)
	c.cache.Delete(tmpl.key)

	waitEvicted(t, evicted, tmpl.key)

	assert.Equal(t, int32(1), tmpl.closes.Load())
	assert.False(t, tmpl.snapfileExists(), "unreferenced template's snapfile should be removed")

	select {
	case k := <-purged:
		assert.Equal(t, tmpl.key, k)
	case <-time.After(time.Second):
		t.Fatal("peer routing was not purged for an unreferenced template")
	}
}

// The janitor is not started, so the expired entry stays unswept.
func TestAdmit_ExpiredUnsweptEntryIsClosed(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	expired := newPinTestTemplate(t, "build-expired")
	c.cache.Set(expired.key, expired, time.Millisecond)

	require.Eventually(t, func() bool { return !c.cache.Has(expired.key) },
		5*time.Second, time.Millisecond, "entry never expired")

	fresh := newPinTestTemplate(t, expired.key)
	got, found, release := c.lookupOrAdmit(t.Context(), fresh.key, fresh, time.Hour, false)
	defer release()

	assert.False(t, found, "an expired entry must not be served")
	assert.Same(t, fresh, got)

	waitEvicted(t, evicted, expired.key)

	assert.Equal(t, int32(1), expired.closes.Load(), "the displaced instance must be closed")
	assert.False(t, expired.snapfileExists())
	assert.Zero(t, fresh.closes.Load(), "the admitted instance must survive its predecessor's eviction")
	assert.True(t, fresh.snapfileExists())
}

// The entry must expire after the lookup has started but before it admits,
// which extendMu does not prevent. Holding pinMu parks the lookup inside that
// window, at its pinned-template check, until the entry has expired. It guards
// against checking expiry separately from the admission.
func TestAdmit_EntryExpiringDuringLookupIsClosed(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	expiring := newPinTestTemplate(t, "build-expiring")
	c.cache.Set(expiring.key, expiring, 300*time.Millisecond)

	c.pinMu.Lock()

	type result struct {
		got     Template
		found   bool
		release func()
	}
	done := make(chan result, 1)
	fresh := newPinTestTemplate(t, expiring.key)
	ctx := t.Context()
	go func() {
		got, found, release := c.lookupOrAdmit(ctx, fresh.key, fresh, time.Hour, false)
		done <- result{got, found, release}
	}()

	require.Eventually(t, func() bool {
		if c.extendMu.TryLock() {
			c.extendMu.Unlock()

			return false
		}

		return true
	}, 5*time.Second, time.Millisecond, "the lookup never started")
	require.Eventually(t, func() bool { return !c.cache.Has(expiring.key) },
		5*time.Second, time.Millisecond, "entry never expired")

	c.pinMu.Unlock()

	r := <-done
	defer r.release()

	assert.False(t, r.found, "an expired entry must not be served")
	assert.Same(t, fresh, r.got)

	waitEvicted(t, evicted, expiring.key)

	assert.Equal(t, int32(1), expiring.closes.Load(), "the displaced instance must be closed")
	assert.Zero(t, fresh.closes.Load(), "the admitted instance must survive its predecessor's eviction")
}

// setExpired caches tmpl under key and waits until the entry has expired. The
// janitor is not started, so the entry stays unswept.
func setExpired(t *testing.T, c *Cache, key string, tmpl Template) {
	t.Helper()

	c.cache.Set(key, tmpl, time.Millisecond)
	require.Eventually(t, func() bool { return !c.cache.Has(key) },
		5*time.Second, time.Millisecond, "entry never expired")
}

func TestRelease_ExpiredOtherInstanceIsClosed(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	pinned := newPinTestTemplate(t, "build-release-other")
	release := pinForTest(t, c, pinned)

	stale := newPinTestTemplate(t, pinned.key)
	setExpired(t, c, pinned.key, stale)

	release()
	waitEvicted(t, evicted, pinned.key)

	assert.Equal(t, int32(1), stale.closes.Load(), "the displaced instance must be closed")
	assert.False(t, stale.snapfileExists())

	item := c.cache.Get(pinned.key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item, "the released template must be re-admitted")
	assert.Same(t, pinned, item.Value())
	assert.Zero(t, pinned.closes.Load())
}

// Deleting the expired entry queues onEvicted for the very instance being
// re-admitted, so its snapfile survives only through onEvicted's re-admit guard.
func TestRelease_ExpiredSameInstanceIsReadmitted(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-release-same")
	release := pinForTest(t, c, tmpl)
	setExpired(t, c, tmpl.key, tmpl)

	release()
	waitEvicted(t, evicted, tmpl.key)

	assert.Zero(t, tmpl.closes.Load(), "the re-admitted instance must not be closed")
	assert.True(t, tmpl.snapfileExists())

	item := c.cache.Get(tmpl.key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item, "the released template must be re-admitted")
	assert.Same(t, tmpl, item.Value())
	assert.LessOrEqual(t, item.TTL(), unpinnedGraceTTL)
}

func TestAdmit_PinnedBranchClosesExpiredOtherInstance(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	pinned := newPinTestTemplate(t, "build-pinned-other")
	release := pinForTest(t, c, pinned)
	defer release()

	stale := newPinTestTemplate(t, pinned.key)
	setExpired(t, c, pinned.key, stale)

	candidate := newPinTestTemplate(t, pinned.key)
	got, found, releaseLookup := c.lookupOrAdmit(t.Context(), pinned.key, candidate, time.Hour, false)
	defer releaseLookup()

	assert.True(t, found)
	assert.Same(t, pinned, got, "the pinned template must be served")

	waitEvicted(t, evicted, pinned.key)

	assert.Equal(t, int32(1), stale.closes.Load(), "the displaced instance must be closed")
	assert.False(t, stale.snapfileExists())
	assert.Zero(t, pinned.closes.Load())
	assert.Zero(t, candidate.closes.Load())

	item := c.cache.Get(pinned.key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item)
	assert.Same(t, pinned, item.Value())
}

// A lookup for a build whose pinned template has left the TTL cache must serve
// and pin that template, not admit the candidate, and its release must drop
// only its own pin.
func TestAdmit_PinnedBranchPinsTheServedTemplate(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)
	pinned := newPinTestTemplate(t, "build-pinned-lookup")
	releaseFirst := pinForTest(t, c, pinned)

	candidate := newPinTestTemplate(t, pinned.key)
	got, found, releaseLookup := c.lookupOrAdmit(t.Context(), pinned.key, candidate, time.Hour, true)

	assert.True(t, found)
	assert.Same(t, pinned, got, "the pinned template must be served")
	assert.Equal(t, int64(2), c.footprint().pinnedRefs, "the lookup must take its own pin")

	item := c.cache.Get(pinned.key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item, "the pinned template must be re-admitted")
	assert.Same(t, pinned, item.Value())

	releaseLookup()
	assert.True(t, c.isPinned(pinned.key), "the lookup's release must not drop the first pin")
	assert.Equal(t, int64(1), c.footprint().pinnedRefs)

	releaseFirst()
	assert.False(t, c.isPinned(pinned.key))
	assert.Zero(t, pinned.closes.Load())
	assert.Zero(t, candidate.closes.Load())
}

// The eviction callback must not hold extendMu across Close. extendMu is taken
// on every sandbox create and resume, and closeTemplate waits on the template's
// futures with no deadline, so a Close that blocks under the lock is a
// node-wide create/resume stall rather than one stuck eviction.
func TestEvict_CloseDoesNotHoldExtendMu(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	blocking := &blockingCloseTemplate{
		pinTestTemplate: newPinTestTemplate(t, "build-blocking"),
		entered:         make(chan struct{}),
		unblock:         make(chan struct{}),
	}

	c.cache.Set(blocking.key, blocking, ttlcache.DefaultTTL)
	c.cache.Delete(blocking.key)

	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Close was never reached")
	}

	// A create/resume arriving while that Close is stuck must not block.
	other := newPinTestTemplate(t, "build-other")
	ctx := t.Context()
	acquired := make(chan struct{})
	go func() {
		_, _, release := c.lookupOrAdmit(ctx, other.key, other, time.Hour, true)
		release()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition blocked behind an in-progress template Close")
	}

	close(blocking.unblock)
	waitEvicted(t, evicted, blocking.key)
}

// blockingCloseTemplate parks in Close the way closeTemplate parks on an
// unresolved memfile future.
type blockingCloseTemplate struct {
	*pinTestTemplate

	entered chan struct{}
	unblock chan struct{}
}

func (b *blockingCloseTemplate) Close(ctx context.Context) error {
	close(b.entered)
	<-b.unblock

	return b.pinTestTemplate.Close(ctx)
}

// A template re-admitted after its last pin is released must not receive a
// fresh full-length TTL: it already aged out once, and granting another 25h on
// every release is how retention creeps back.
func TestUnpin_ReadmitsWithShortGraceTTL(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(templateExpiration)
	tmpl := newPinTestTemplate(t, "build-grace")

	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)
	release := pinForTest(t, c, tmpl)

	c.cache.Delete(tmpl.key) // evicted while pinned; callback skips the close
	waitEvicted(t, evicted, tmpl.key)

	release()

	item := c.cache.Get(tmpl.key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item, "template should be re-admitted on final unpin")
	assert.LessOrEqual(t, item.TTL(), unpinnedGraceTTL, "re-admitted template got more than the grace TTL")
	assert.Zero(t, tmpl.closes.Load(), "pinned template was closed during its pinned window")
}

// Invalidate promises a refetch on next access. Because lookups consult the pins
// before the cache, a pinned build would otherwise resolve straight back to the
// instance Invalidate was called to discard — on a fresh 25h TTL.
func TestInvalidate_PinnedBuildResolvesToAFreshInstance(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	stale := newPinTestTemplate(t, "build-invalidated")

	c.cache.Set(stale.key, stale, ttlcache.DefaultTTL)
	release := pinForTest(t, c, stale)
	defer release()

	c.Invalidate(stale.key)
	waitEvicted(t, evicted, stale.key)

	// The holder is still running on the instance it started with.
	assert.Zero(t, stale.closes.Load(), "an invalidated template must not be closed while it is in use")
	assert.True(t, stale.snapfileExists())

	// A new lookup must not see it.
	_, releaseCached, cached := c.LookupPinned(t.Context(), stale.key)
	releaseCached()
	assert.False(t, cached, "invalidated template must not be served from the cache")

	replacement := newPinTestTemplate(t, "build-invalidated")
	got, found, releaseNew := c.lookupOrAdmit(t.Context(), replacement.key, replacement, time.Hour, true)
	defer releaseNew()

	assert.False(t, found, "invalidated build must miss and be refetched")
	assert.Same(t, replacement, got)
}

// The last release of an invalidated template closes it rather than re-admitting
// it: re-admission would hand the discarded instance back to the next lookup,
// and leaving it pinned forever would leak its files.
func TestInvalidate_LastReleaseClosesInsteadOfReadmitting(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	stale := newPinTestTemplate(t, "build-retired")

	c.cache.Set(stale.key, stale, ttlcache.DefaultTTL)
	release := pinForTest(t, c, stale)

	c.Invalidate(stale.key)
	waitEvicted(t, evicted, stale.key)

	release()

	require.Eventually(t, func() bool { return stale.closes.Load() == 1 },
		5*time.Second, 5*time.Millisecond, "retired template was never closed")
	assert.False(t, stale.snapfileExists())
	assert.Nil(t, c.cache.Get(stale.key, ttlcache.WithDisableTouchOnHit[string, Template]()),
		"a retired template must not be re-admitted on release")
}

// An eviction racing the final release of a retired instance must still leave it
// alone: it is invisible to lookups but a sandbox is running on it until the
// release lands.
func TestInvalidate_RetiredInstanceIsStillProtectedFromEviction(t *testing.T) {
	t.Parallel()

	purged := make(chan string, 1)
	c, evicted := newPinTestCacheWithPurge(time.Hour, func(k string) { purged <- k })
	stale := newPinTestTemplate(t, "build-retired-evict")

	c.cache.Set(stale.key, stale, ttlcache.DefaultTTL)
	release := pinForTest(t, c, stale)
	defer release()

	c.Invalidate(stale.key)
	waitEvicted(t, evicted, stale.key)

	// A replacement takes the key and expires; its eviction must not touch the
	// retired instance.
	replacement := newPinTestTemplate(t, "build-retired-evict")
	c.cache.Set(replacement.key, replacement, ttlcache.DefaultTTL)
	c.cache.Delete(replacement.key)
	waitEvicted(t, evicted, replacement.key)

	assert.Zero(t, stale.closes.Load(), "retired template lost its files to a successor's eviction")
	assert.True(t, stale.snapfileExists())

	// Peer chunk routing belongs to the build, and the sandbox on the retired
	// instance still reads through it.
	select {
	case k := <-purged:
		t.Fatalf("peer routing purged for %q while a retired instance is still in use", k)
	default:
	}
}

// Invalidate has to be atomic against acquisition. A lookup that has already
// read the pin and is about to re-admit and re-pin the same instance would
// otherwise end up holding it under a second entry, while the retired entry's
// last release Closes it — deleting the snapfile under a live sandbox.
func TestInvalidate_IsSerializedAgainstAcquisition(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)

	pinning := make(chan struct{})
	proceed := make(chan struct{})

	tmpl := &filesHookTemplate{pinTestTemplate: newPinTestTemplate(t, "build-invalidate-race")}
	tmpl.hook = func() {
		close(pinning)
		<-proceed
	}

	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)

	ctx := t.Context()
	released := make(chan func(), 1)
	go func() {
		_, _, release := c.lookupOrAdmit(ctx, tmpl.key, tmpl, time.Hour, true)
		released <- release
	}()

	select {
	case <-pinning:
	case <-time.After(5 * time.Second):
		t.Fatal("the pin was never recorded")
	}

	invalidated := make(chan struct{})
	go func() {
		c.Invalidate(tmpl.key)
		close(invalidated)
	}()

	select {
	case <-invalidated:
		t.Fatal("Invalidate ran while a pin was being recorded; it must serialize on extendMu")
	case <-time.After(100 * time.Millisecond):
	}

	close(proceed)
	release := <-released
	<-invalidated

	// Let the eviction callback that Invalidate's delete spawned finish first.
	// It must leave the instance alone — a sandbox is still running on it — and
	// ordering it ahead of the release is also what makes the close count below
	// exact: both paths would otherwise close the (retired, unreferenced)
	// instance, harmlessly but twice.
	waitEvicted(t, evicted, tmpl.key)
	require.Zero(t, tmpl.closes.Load(),
		"eviction closed a retired instance while its holder was still running on it")

	// One entry holds this instance, so its release is what closes it.
	release()

	require.Eventually(t, func() bool { return tmpl.closes.Load() == 1 },
		5*time.Second, 5*time.Millisecond, "the retired instance was never closed")
}

// The residency gauges are the reason this work exists, so the numbers they
// report are load-bearing. pinned counts builds; pinnedRefs counts holders, and
// the two differ exactly when several sandboxes share a build — the case where
// reading pinned as "live sandboxes" would show a permanent phantom gap.
func TestFootprint_CountsEntriesPinsAndRefs(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)

	resident := newPinTestTemplate(t, "build-resident")
	shared := newPinTestTemplate(t, "build-shared-by-two")

	c.cache.Set(resident.key, resident, ttlcache.DefaultTTL)
	c.cache.Set(shared.key, shared, ttlcache.DefaultTTL)

	releaseA := pinForTest(t, c, shared)
	releaseB := pinForTest(t, c, shared)
	defer releaseA()
	defer releaseB()

	f := c.footprint()

	assert.Equal(t, int64(2), f.entries, "both cached templates are resident")
	assert.Equal(t, int64(1), f.pinned, "one build has an outstanding pin")
	assert.Equal(t, int64(2), f.pinnedRefs, "two sandboxes hold that one build")
}

// A pinned template that has left the TTL cache is still resident, and so is a
// retired one — an Invalidate hides an instance from lookups but every byte of
// it stays in the heap until its last holder is gone. Counting the cache alone
// would understate exactly the footprint these gauges are meant to size.
func TestFootprint_CountsPinnedAndRetiredStragglers(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)

	pinnedTmpl := newPinTestTemplate(t, "build-straggler")
	c.cache.Set(pinnedTmpl.key, pinnedTmpl, ttlcache.DefaultTTL)
	release := pinForTest(t, c, pinnedTmpl)
	defer release()

	c.cache.Delete(pinnedTmpl.key)
	waitEvicted(t, evicted, pinnedTmpl.key)

	f := c.footprint()
	assert.Equal(t, int64(1), f.entries, "a pinned template outside the cache is still resident")
	assert.Equal(t, int64(1), f.pinned)

	retired := newPinTestTemplate(t, "build-retired-footprint")
	c.cache.Set(retired.key, retired, ttlcache.DefaultTTL)
	releaseRetired := pinForTest(t, c, retired)
	defer releaseRetired()

	c.Invalidate(retired.key)
	waitEvicted(t, evicted, retired.key)

	f = c.footprint()
	assert.Equal(t, int64(2), f.entries, "a retired template still holds its mappings")
	assert.Equal(t, int64(2), f.pinned)
	assert.Equal(t, int64(2), f.pinnedRefs)
}

// A template that is both cached and pinned must be counted once, or the gauge
// double-counts every live sandbox's mappings.
func TestFootprint_DoesNotDoubleCountPinnedCacheEntries(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-both")

	c.cache.Set(tmpl.key, tmpl, ttlcache.DefaultTTL)
	release := pinForTest(t, c, tmpl)
	defer release()

	assert.Equal(t, int64(1), c.footprint().entries)
}

// A pin never ages out on its own, so the oldest pin's age is the signal that
// tells a busy node from one that is leaking releases.
func TestFootprint_ReportsOldestPinAge(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)

	assert.Zero(t, c.footprint().oldestPinAgeSeconds, "no pins, no age")

	tmpl := newPinTestTemplate(t, "build-age")
	release := pinForTest(t, c, tmpl)

	backdatePins(c, tmpl.key, 90*time.Second)

	assert.GreaterOrEqual(t, c.footprint().oldestPinAgeSeconds, int64(90))

	release()
	assert.Zero(t, c.footprint().oldestPinAgeSeconds, "age returns to zero once the pin is released")
}

// backdatePins moves every outstanding acquisition on key back by age.
func backdatePins(c *Cache, key string, age time.Duration) {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	for tok, at := range c.pinned[key].holders {
		c.pinned[key].holders[tok] = at.Add(-age)
	}
}

// The age gauge must report the oldest pin still held, not the age of the entry.
// Sandboxes from one build overlap constantly — that is why the pin is
// refcounted — so an entry-level timestamp survives every holder that set it and
// climbs without bound while releases stay perfectly balanced, which is
// indistinguishable from the leak the gauge exists to catch.
func TestFootprint_OldestPinAgeFollowsTheHolder(t *testing.T) {
	t.Parallel()

	c, _ := newPinTestCache(time.Hour)
	tmpl := newPinTestTemplate(t, "build-overlap")

	// The long-lived holder, backdated so its age is unmistakable.
	first := pinForTest(t, c, tmpl)
	backdatePins(c, tmpl.key, 10*time.Minute)

	// A second sandbox on the same build joins while the first is still running.
	second := pinForTest(t, c, tmpl)
	defer second()

	require.Equal(t, int64(2), c.footprint().pinnedRefs, "both holders are counted")
	assert.GreaterOrEqual(t, c.footprint().oldestPinAgeSeconds, int64(600),
		"while the first holder runs, the age is its own")

	// The first sandbox exits. The entry survives on the second holder's pin, and
	// the reported age must now be that holder's, not the departed one's.
	first()

	require.Equal(t, int64(1), c.footprint().pinnedRefs, "one holder left")
	assert.True(t, c.isPinned(tmpl.key), "the entry outlives the first holder")
	assert.Less(t, c.footprint().oldestPinAgeSeconds, int64(600),
		"the departed holder's timestamp must not outlive it")
}

// storage.Paths.Cache creates the instance's directory eagerly, at
// construction — before the cache is consulted — so every lookup that loses the
// race to an already-resident template leaves one behind. Nothing else reclaims
// those: the startup sweep covers DefaultCacheDir, while these live under
// TemplateCacheDir, and the discarded instance never reaches Close. The leak
// scales with lookups rather than with templates, on the hot path.
func TestGetTemplate_DiscardedCandidateLeavesNoDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	c, _ := newPinTestCache(time.Hour)
	c.config = cfg.Config{
		BuilderConfig: cfg.BuilderConfig{
			StorageConfig: storage.Config{TemplateCacheDir: root},
		},
	}

	const buildID = "11111111-2222-3333-4444-555555555555"

	// Already resident, so the candidate below loses and is dropped without ever
	// being fetched or closed.
	resident := newPinTestTemplate(t, buildID)
	c.cache.Set(buildID, resident, ttlcache.DefaultTTL)

	candidate, err := newTemplateFromStorage(
		c.config.BuilderConfig, buildID,
		resolvedHeader(nil), resolvedHeader(nil),
		nil, blockmetrics.Metrics{}, nil, nil, nil,
	)
	require.NoError(t, err)

	candidateDir := filepath.Dir(candidate.Files().CacheSnapfile())
	require.DirExists(t, candidateDir, "Paths.Cache creates the directory up front")

	got, _ := c.getTemplateWithFetch(t.Context(), candidate, 0, 0, "")

	require.Same(t, resident, got, "the resident template must win the lookup")
	assert.NoDirExists(t, candidateDir, "the discarded candidate's directory must not be orphaned")

	// And nothing else under this build's cache parent either, so the leak does
	// not simply move up a level.
	siblings, err := os.ReadDir(filepath.Join(root, buildID, "cache"))
	require.NoError(t, err)
	assert.Empty(t, siblings, "no per-lookup directories may accumulate under the build")
}
