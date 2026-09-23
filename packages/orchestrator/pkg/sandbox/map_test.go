//go:build linux

package sandbox

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func TestMapMarkRunningTracksLifecycle(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.Len(t, sandboxes.Items(), 1)
	require.Len(t, sandboxes.LifecycleItems(), 1)
}

// insertRecorder counts OnInsert notifications — the routing publish a
// refused registration must not trigger.
type insertRecorder struct {
	inserts []*Sandbox
}

func (r *insertRecorder) OnInsert(_ context.Context, sbx *Sandbox) {
	r.inserts = append(r.inserts, sbx)
}
func (r *insertRecorder) OnStopping(context.Context, *Sandbox)       {}
func (r *insertRecorder) OnNetworkRelease(context.Context, *Sandbox) {}

// A second lifecycle under a live sandbox ID is refused and left out of every
// index: not routable, not tracked for cleanup, not announced. The live one is
// untouched, and the caller learns it owns a VM nothing else will stop.
func TestMapMarkRunningRefusesAnotherLifecycleUnderALiveID(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	recorder := &insertRecorder{}
	sandboxes.Subscribe(recorder)

	live := testMapSandbox(t, "lifecycle-live")
	require.NoError(t, sandboxes.MarkRunning(t.Context(), live))

	dup := testMapSandbox(t, "lifecycle-dup")
	err := sandboxes.MarkRunning(t.Context(), dup)
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning)

	got, ok := sandboxes.Get(live.Runtime.SandboxID)
	require.True(t, ok)
	require.Same(t, live, got, "the live lifecycle keeps the ID")
	require.Len(t, sandboxes.LifecycleItems(), 1, "a refused lifecycle is not tracked for cleanup")
	require.Len(t, recorder.inserts, 1, "a refused lifecycle is not announced")
	require.Same(t, live, recorder.inserts[0])

	// The refused lifecycle cannot evict the live one on its way out.
	require.False(t, sandboxes.MarkStopping(t.Context(), dup.Runtime.SandboxID, dup.LifecycleID))
	_, ok = sandboxes.Get(live.Runtime.SandboxID)
	require.True(t, ok)
}

// A reservation holds the ID from before any VM work until the create either
// registers the sandbox or gives up, so a second create is refused without
// building anything.
func TestMapReserveRefusesLiveAndReservedIDs(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()

	r, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)

	_, err = sandboxes.Reserve("sandbox-1")
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "a second create for a reserved ID is refused")

	// The reserving create gives up: the ID is free again.
	r.Release()
	r.Release() // idempotent
	r, err = sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)

	sbx := testMapSandbox(t, "lifecycle-1")
	require.NoError(t, r.MarkRunning(t.Context(), sbx))
	r.Release()
	_, err = sandboxes.Reserve("sandbox-1")
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "a live ID cannot be reserved")
	_, ok := sandboxes.Get("sandbox-1")
	require.True(t, ok)

	require.True(t, sandboxes.MarkStopping(t.Context(), "sandbox-1", "lifecycle-1"))
	r, err = sandboxes.Reserve("sandbox-1")
	require.NoError(t, err, "retiring lifecycle cleanup must not block a new reservation")
	next := testMapSandbox(t, "lifecycle-next")
	require.NoError(t, r.MarkRunning(t.Context(), next))
	sandboxes.MarkStopped(t.Context(), sbx)
	live, ok := sandboxes.Get("sandbox-1")
	require.True(t, ok)
	require.Same(t, next, live)
	require.Len(t, sandboxes.LifecycleItems(), 1)
	r.Release()
}

// A create that reserved the ID registers under it even if a stale reservation
// object from an earlier, released create is still around.
func TestMapReleaseOnlyFreesItsOwnReservation(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()

	first, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	first.Release()

	second, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	first.Release()

	_, err = sandboxes.Reserve("sandbox-1")
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "the second create's reservation must survive the first's late Release")
	second.Release()
}

func TestMapMarkStoppingReservedHandsTheIDToTheSuccessor(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	old := testMapSandbox(t, "lifecycle-old")
	require.NoError(t, sandboxes.MarkRunning(t.Context(), old))

	reservation, err := sandboxes.MarkStoppingReserved(t.Context(), old.Runtime.SandboxID, old.LifecycleID)
	require.NoError(t, err)
	require.NotNil(t, reservation)
	_, live := sandboxes.Get(old.Runtime.SandboxID)
	require.False(t, live, "the old lifecycle has left live queries")

	_, err = sandboxes.Reserve(old.Runtime.SandboxID)
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "a create cannot take the ID mid hand-off")

	successor := testMapSandbox(t, "lifecycle-new")
	require.NoError(t, reservation.MarkRunning(t.Context(), successor))
	reservation.Release()
	got, live := sandboxes.Get(old.Runtime.SandboxID)
	require.True(t, live)
	require.Same(t, successor, got, "a late Release must not touch the successor")

	// A stale lifecycle cannot take a reservation on its way out.
	r, err := sandboxes.MarkStoppingReserved(t.Context(), old.Runtime.SandboxID, old.LifecycleID)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSandboxOperationInProgress)
	require.Nil(t, r)
	_, live = sandboxes.Get(old.Runtime.SandboxID)
	require.True(t, live)
}

func TestMapReserveConcurrentSameIDAdmitsExactlyOne(t *testing.T) {
	t.Parallel()

	for range 500 {
		sandboxes := NewSandboxesMap()
		start := make(chan struct{})
		results := make(chan error, 2)
		for range 2 {
			go func() {
				<-start
				_, err := sandboxes.Reserve("sandbox-1")
				results <- err
			}()
		}
		close(start)

		var refused int
		for range 2 {
			if err := <-results; err != nil {
				require.ErrorIs(t, err, ErrSandboxAlreadyRunning)
				refused++
			}
		}
		require.Equal(t, 1, refused)
	}
}

func TestMapMarkRunningIsIdempotentForTheSameLifecycle(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	recorder := &insertRecorder{}
	sandboxes.Subscribe(recorder)
	sbx := testMapSandbox(t, "lifecycle-1")

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.Len(t, sandboxes.Items(), 1)
	require.Len(t, sandboxes.LifecycleItems(), 1)
	require.Len(t, recorder.inserts, 1, "announced once")
}

func TestReservationOnlyOwnerCanPublish(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	r, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	t.Cleanup(r.Release)
	sbx := testMapSandbox(t, "lifecycle-owner")
	recorder := &insertRecorder{}
	sandboxes.Subscribe(recorder)

	require.ErrorIs(t, sandboxes.MarkRunning(t.Context(), sbx), ErrSandboxAlreadyRunning)
	copied := *r
	require.ErrorIs(t, copied.MarkRunning(t.Context(), sbx), ErrSandboxAlreadyRunning)
	copied.Release()
	other := testMapSandbox(t, "lifecycle-other")
	other.Runtime.SandboxID = "sandbox-other"
	require.Error(t, r.MarkRunning(t.Context(), other))
	require.Empty(t, sandboxes.Items())
	require.Empty(t, recorder.inserts)

	require.NoError(t, r.MarkRunning(t.Context(), sbx))
	require.NoError(t, r.MarkRunning(t.Context(), sbx))
	require.Len(t, recorder.inserts, 1)
	r.Release()
	require.ErrorIs(t, r.MarkRunning(t.Context(), sbx), ErrSandboxAlreadyRunning)
	got, ok := sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, ok)
	require.Same(t, sbx, got)
}

func TestReservationCannotPublishAfterReplacement(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	stale, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	stale.Release()
	owner, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	t.Cleanup(owner.Release)

	require.ErrorIs(t, stale.MarkRunning(t.Context(), testMapSandbox(t, "stale")), ErrSandboxAlreadyRunning)
	stale.Release()
	require.NoError(t, owner.MarkRunning(t.Context(), testMapSandbox(t, "owner")))
}

func TestReservationRemainsHeldAfterPublication(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	r, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	sbx := testMapSandbox(t, "lifecycle-1")
	require.NoError(t, r.MarkRunning(t.Context(), sbx))

	other, err := sandboxes.MarkStoppingReserved(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID)
	require.ErrorIs(t, err, ErrSandboxOperationInProgress, "a second operation cannot replace the publisher's reservation")
	require.Nil(t, other)
	_, err = sandboxes.MarkStoppingReserved(t.Context(), sbx.Runtime.SandboxID, "stale-lifecycle")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSandboxOperationInProgress, "busy only describes the matching live lifecycle")
	require.True(t, sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))
	sandboxes.MarkStopped(t.Context(), sbx)
	_, err = sandboxes.Reserve(sbx.Runtime.SandboxID)
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "the operation still owns the ID after its VM exits")
	r.Release()
	r, err = sandboxes.Reserve(sbx.Runtime.SandboxID)
	require.NoError(t, err)
	r.Release()
}

type handoffSubscriber struct {
	insertRecorder

	onStopping func()
}

func (s *handoffSubscriber) OnStopping(context.Context, *Sandbox) { s.onStopping() }

func TestCheckpointHandoffReservesBeforeSubscribers(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	old := testMapSandbox(t, "old")
	require.NoError(t, sandboxes.MarkRunning(t.Context(), old))
	called := false
	sandboxes.Subscribe(&handoffSubscriber{onStopping: func() {
		called = true
		_, err := sandboxes.Reserve(old.Runtime.SandboxID)
		require.ErrorIs(t, err, ErrSandboxAlreadyRunning)
		_, live := sandboxes.Get(old.Runtime.SandboxID)
		require.False(t, live)
	}})
	r, err := sandboxes.MarkStoppingReserved(t.Context(), old.Runtime.SandboxID, old.LifecycleID)
	require.NoError(t, err)
	t.Cleanup(r.Release)
	require.True(t, called)

	sandboxes.MarkStopped(t.Context(), old)
	_, err = sandboxes.Reserve(old.Runtime.SandboxID)
	require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "old cleanup cannot release the checkpoint's hold")
	require.NoError(t, r.MarkRunning(t.Context(), testMapSandbox(t, "successor")))
}

func TestUnpublishedLifecycleRemainsTrackedDuringClose(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		sandboxes := NewSandboxesMap()
		sbx := testMapSandbox(t, "unpublished")
		sbx.sandboxes = sandboxes
		sbx.cleanup = NewCleanup()
		sandboxes.TrackLifecycle(t.Context(), sbx)
		drained := make(chan error, 1)
		go func() { drained <- sandboxes.WaitLifecycles(t.Context()) }()
		finishCleanup := make(chan struct{})
		sbx.cleanup.Add(t.Context(), func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			<-finishCleanup

			return nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		closed := make(chan error, 1)
		go func() { closed <- sbx.Close(ctx) }()
		synctest.Wait()
		require.Empty(t, drained)
		r, err := sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err, "cleanup tracking does not change admission")
		defer r.Release()
		require.Empty(t, sandboxes.Items(), "cleanup tracking must not expose routing")
		require.Len(t, sandboxes.LifecycleItems(), 1)

		close(finishCleanup)
		require.NoError(t, <-closed)
		require.NoError(t, <-drained)
		require.Error(t, r.MarkRunning(t.Context(), sbx), "a closed lifecycle cannot be resurrected")
		require.NoError(t, r.MarkRunning(t.Context(), testMapSandbox(t, "successor")))
	})
}

// Two lifecycles racing for the same ID: exactly one wins, the other is
// refused, and the registry never holds both.
func TestMapMarkRunningConcurrentSameIDAdmitsExactlyOne(t *testing.T) {
	t.Parallel()

	for range 500 {
		sandboxes := NewSandboxesMap()
		a := testMapSandbox(t, "lifecycle-a")
		b := testMapSandbox(t, "lifecycle-b")

		start := make(chan struct{})
		results := make(chan error, 2)
		for _, sbx := range []*Sandbox{a, b} {
			go func() {
				<-start
				results <- sandboxes.MarkRunning(t.Context(), sbx)
			}()
		}
		close(start)

		var refused int
		for range 2 {
			if err := <-results; err != nil {
				require.ErrorIs(t, err, ErrSandboxAlreadyRunning)
				refused++
			}
		}
		require.Equal(t, 1, refused)
		require.Len(t, sandboxes.Items(), 1)
		require.Len(t, sandboxes.LifecycleItems(), 1)
	}
}

func TestMapLifecycleItemsRemainAfterMarkStopping(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")

	sandboxes.MarkRunning(t.Context(), sbx)
	require.Len(t, sandboxes.Items(), 1)
	require.Len(t, sandboxes.LifecycleItems(), 1)

	marked := sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID)
	require.True(t, marked)
	require.Empty(t, sandboxes.Items())
	require.Len(t, sandboxes.LifecycleItems(), 1)

	sandboxes.MarkStopped(t.Context(), sbx)
	require.Empty(t, sandboxes.LifecycleItems())
}

func TestSandboxCloseMarksLifecycleStopped(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	sbx.cleanup = NewCleanup()
	sbx.sandboxes = sandboxes

	sandboxes.MarkRunning(t.Context(), sbx)
	require.Len(t, sandboxes.LifecycleItems(), 1)

	require.NoError(t, sbx.Close(t.Context()))
	require.Empty(t, sandboxes.LifecycleItems())
}

func TestMapLifecycleItemsAllowDuplicateSandboxIDs(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	oldSbx := testMapSandbox(t, "lifecycle-old")
	newSbx := testMapSandbox(t, "lifecycle-new")

	require.NoError(t, sandboxes.MarkRunning(t.Context(), oldSbx))
	require.True(t, sandboxes.MarkStopping(t.Context(), oldSbx.Runtime.SandboxID, oldSbx.LifecycleID))
	require.NoError(t, sandboxes.MarkRunning(t.Context(), newSbx))

	require.Len(t, sandboxes.LifecycleItems(), 2)
}

func TestMapWaitLifecyclesReturnsWhenEmpty(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()

	require.NoError(t, sandboxes.WaitLifecycles(t.Context()))
}

func TestMapWaitLifecyclesWaitsUntilStopped(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	sandboxes.MarkRunning(t.Context(), sbx)

	waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sandboxes.WaitLifecycles(waitCtx)
	}()

	select {
	case err := <-done:
		require.Failf(t, "WaitLifecycles returned before lifecycle stopped", "err: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	sandboxes.MarkStopped(t.Context(), sbx)
	require.NoError(t, <-done)
}

func TestMapWaitLifecyclesReturnsContextError(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	sandboxes.MarkRunning(t.Context(), sbx)

	waitCtx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, sandboxes.WaitLifecycles(waitCtx), context.Canceled)
}

func TestMapConcurrentMarkStoppingAndStoppedDoesNotResurrectLifecycle(t *testing.T) {
	t.Parallel()

	for range 1000 {
		sandboxes := NewSandboxesMap()
		sbx := testMapSandbox(t, "lifecycle-1")
		sandboxes.MarkRunning(t.Context(), sbx)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start
			sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID)
		}()

		go func() {
			defer wg.Done()
			<-start
			sandboxes.MarkStopped(t.Context(), sbx)
		}()

		close(start)
		wg.Wait()

		require.Empty(t, sandboxes.LifecycleItems())
	}
}

func TestMapMarkRunningRefusesCollidingLifecycle(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	oldSbx := testMapSandbox(t, "lifecycle-old")
	newSbx := testMapSandbox(t, "lifecycle-new")

	require.NoError(t, sandboxes.MarkRunning(t.Context(), oldSbx))

	// The old lifecycle is still live (e.g. it crashed and nothing removed it
	// yet): the new lifecycle must be refused, not silently dropped — a silent
	// drop leaves its FC process running with no id-based way to reach it.
	require.ErrorIs(t, sandboxes.MarkRunning(t.Context(), newSbx), ErrSandboxAlreadyRunning)

	live, ok := sandboxes.Get(oldSbx.Runtime.SandboxID)
	require.True(t, ok)
	require.Same(t, oldSbx, live)
	require.Len(t, sandboxes.LifecycleItems(), 1)
}

func TestMapMarkRunningIdempotentForSameLifecycle(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.Len(t, sandboxes.Items(), 1)
	require.Len(t, sandboxes.LifecycleItems(), 1)
}

func TestSandboxCloseDoesNotRemoveNewerLiveLifecycle(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	oldSbx := testMapSandbox(t, "lifecycle-old")
	newSbx := testMapSandbox(t, "lifecycle-new")
	// The registrar captures the old lifecycle's ID when it is called, where the
	// callback it replaced read it when the chain ran. This test is what pins the
	// two to the same value: it fails if the owner reclaims by anything else.
	attachOwnedCleanup(t, sandboxes, oldSbx)

	sandboxes.MarkRunning(t.Context(), oldSbx)
	require.True(t, sandboxes.MarkStopping(t.Context(), oldSbx.Runtime.SandboxID, oldSbx.LifecycleID))
	require.NoError(t, sandboxes.MarkRunning(t.Context(), newSbx))

	// The old lifecycle's Close must not evict the newer live lifecycle.
	require.NoError(t, oldSbx.Close(t.Context()))

	live, ok := sandboxes.Get(newSbx.Runtime.SandboxID)
	require.True(t, ok)
	require.Same(t, newSbx, live)
}

// stoppingRecorder appends to a shared event log on every OnStopping, so a test
// can attribute a removal to the mechanism that made it rather than only observe
// that the entry is gone.
type stoppingRecorder struct {
	insertRecorder

	mu     *sync.Mutex
	events *[]string
}

func (r *stoppingRecorder) OnStopping(context.Context, *Sandbox) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*r.events = append(*r.events, "reclaim")
}

// attachOwnedCleanup gives sbx the cleanup chain a factory would build: the
// registrar's callback, and nothing else. Both factories register it at one
// fixed point, so a fixture without it asserts a behaviour no real sandbox has.
func attachOwnedCleanup(t *testing.T, sandboxes *Map, sbx *Sandbox) {
	t.Helper()

	sbx.cleanup = NewCleanup()
	sbx.sandboxes = sandboxes
	sandboxes.reclaimLiveEntryOnCleanup(t.Context(), sbx.cleanup, sbx.Runtime.SandboxID, sbx.LifecycleID)
}

// The cleanup chain owns the reclamation, and owns it at one point: after the
// steps registered before the registrar and before those registered after it.
// An end-state assertion cannot see this — the entry is gone either way — so the
// markers and the event log are the test.
func TestSandboxCloseReclaimsLiveEntryInTheCleanupChain(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")

	var (
		mu     sync.Mutex
		events []string
	)
	mark := func(name string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, name)

			return nil
		}
	}
	sandboxes.Subscribe(&stoppingRecorder{mu: &mu, events: &events})

	// The chain runs backward, so a callback registered later runs earlier. The
	// names say when each one RUNS, which is the order being asserted.
	sbx.cleanup = NewCleanup()
	sbx.sandboxes = sandboxes
	sbx.cleanup.Add(t.Context(), mark("late"))
	sandboxes.reclaimLiveEntryOnCleanup(t.Context(), sbx.cleanup, sbx.Runtime.SandboxID, sbx.LifecycleID)
	sbx.cleanup.Add(t.Context(), mark("early"))

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sbx.Close(t.Context()))

	require.Empty(t, sandboxes.Items())
	require.Equal(t, []string{"early", "reclaim", "late"}, events,
		"the chain must reclaim the entry exactly once, between the steps either side of the registrar")
}

// An operation-initiated stop — delete, pause, checkpoint — reclaims the entry
// before the chain runs. The chain's callback must then do nothing at all: no
// second OnStopping for subscribers to act on twice.
func TestSandboxCloseDoesNotReclaimAnEntryAnOperationAlreadyTook(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	attachOwnedCleanup(t, sandboxes, sbx)

	var (
		mu     sync.Mutex
		events []string
	)
	sandboxes.Subscribe(&stoppingRecorder{mu: &mu, events: &events})

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.True(t, sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID),
		"the operation reclaims the entry first, as Delete and Pause require")
	require.NoError(t, sbx.Close(t.Context()))

	require.Equal(t, []string{"reclaim"}, events,
		"the chain's callback must not reclaim an entry an operation already took")
}

// Two drivers can call Close for one lifecycle: the lifecycle goroutine and the
// start rollback. Cleanup.Run is once-guarded, so they reclaim once between them
// and neither returns before the reclamation has completed.
func TestSandboxCloseConcurrentClosesReclaimOnce(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	attachOwnedCleanup(t, sandboxes, sbx)

	var (
		mu     sync.Mutex
		events []string
	)
	sandboxes.Subscribe(&stoppingRecorder{mu: &mu, events: &events})

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	for range 2 {
		go func() {
			defer wg.Done()
			<-start
			assert.NoError(t, sbx.Close(t.Context()))
		}()
	}

	close(start)
	wg.Wait()

	require.Equal(t, []string{"reclaim"}, events)
	require.Empty(t, sandboxes.Items())
	require.Empty(t, sandboxes.LifecycleItems())
}

func testMapSandbox(t *testing.T, lifecycleID string) *Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	return &Sandbox{
		LifecycleID: lifecycleID,
		Metadata: &Metadata{
			Config:  NewConfig(Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "sandbox-1"},
		},
		Resources: &Resources{Slot: slot},
	}
}
