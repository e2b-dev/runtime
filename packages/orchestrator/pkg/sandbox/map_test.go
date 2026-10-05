//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
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
func (r *insertRecorder) OnStopping(context.Context, *Sandbox) {}
func (r *insertRecorder) OnNetworkRelease(context.Context, *Sandbox) error {
	return nil
}

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
	// The owner returns false here, so nothing is counted — but the rule is about
	// registering it, not about which arm the registration happens to take, so that
	// this stays true however the fixture is edited later.
	serializeUnstoppedCounter(t)

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

// unstoppedCounterMu serializes every test that moves lifecycleUnstoppedCounter.
//
// The counter carries one attribute, sandbox_type, and deliberately nothing that
// identifies a run — that is what keeps its series count independent of traffic. So
// a test has nothing to filter its own increments by, and the reader installed in
// TestMain is process-wide and cumulative across the whole binary. The only sound
// reading is a delta, and a delta is sound only while every test that moves the
// instrument holds this lock. Four tests here predate the counter and register the
// owner: two move it, and two sit on a branch that does not currently fire — which
// is a property of their fixtures, not of their subjects, so it can change without
// anyone noticing. TestEveryOwnedCleanupTestSerializesTheCounter keeps all four in.
var unstoppedCounterMu sync.Mutex

// serializeUnstoppedCounter claims the counter for this test until it returns.
func serializeUnstoppedCounter(t *testing.T) {
	t.Helper()

	unstoppedCounterMu.Lock()
	t.Cleanup(unstoppedCounterMu.Unlock)
}

// unstoppedCounts is the counter's current value per sandbox_type. Absent types
// read as zero, so a caller can subtract two of these without checking presence.
func unstoppedCounts(t *testing.T) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.SandboxLifecycleUnstoppedCounterName) {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "%s is not an int64 sum", m.Name)

			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("sandbox_type"))
				require.True(t, ok, "unstopped datapoint without a sandbox_type")
				out[v.AsString()] += dp.Value
			}
		}
	}

	return out
}

// unstoppedDelta is what the counter moved by, per sandbox_type, since before.
func unstoppedDelta(t *testing.T, before map[string]int64) map[string]int64 {
	t.Helper()

	after := unstoppedCounts(t)
	delta := map[string]int64{}
	for _, sbxType := range []string{
		string(sandboxtypes.SandboxTypeSandbox),
		string(sandboxtypes.SandboxTypeBuild),
	} {
		delta[sbxType] = after[sbxType] - before[sbxType]
	}

	require.Subsetf(t, []string{
		string(sandboxtypes.SandboxTypeSandbox),
		string(sandboxtypes.SandboxTypeBuild),
	}, keysOf(after), "the counter grew a sandbox_type value outside the closed set")

	return delta
}

func keysOf(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

// testTypedMapSandbox is testMapSandbox with an explicit sandbox type, which the
// counter reports.
func testTypedMapSandbox(t *testing.T, lifecycleID string, sandboxType sandboxtypes.SandboxType) *Sandbox {
	t.Helper()

	sbx := testMapSandbox(t, lifecycleID)
	sbx.Runtime.SandboxType = sandboxType

	return sbx
}

// attachOwnedCleanup gives sbx the cleanup chain a factory would build: the
// registrar's callback, and nothing else. Both factories register it at one
// fixed point, so a fixture without it asserts a behaviour no real sandbox has.
func attachOwnedCleanup(t *testing.T, sandboxes *Map, sbx *Sandbox) {
	t.Helper()

	sbx.cleanup = NewCleanup()
	sbx.sandboxes = sandboxes
	sandboxes.reclaimLiveEntryOnCleanup(t.Context(), sbx.cleanup, sbx.Runtime.SandboxID, sbx.LifecycleID, sbx.Runtime.SandboxType)
}

// The cleanup chain owns the reclamation, and owns it at one point. The chain runs
// backward, so in RUN order the reclaim falls after the steps registered after the
// registrar and before those registered before it. An end-state assertion cannot
// see this — the entry is gone either way — so the markers and the event log are
// the test.
func TestSandboxCloseReclaimsLiveEntryInTheCleanupChain(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeSandbox)
	before := unstoppedCounts(t)

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
	sandboxes.reclaimLiveEntryOnCleanup(t.Context(), sbx.cleanup, sbx.Runtime.SandboxID, sbx.LifecycleID, sbx.Runtime.SandboxType)
	sbx.cleanup.Add(t.Context(), mark("early"))

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sbx.Close(t.Context()))

	require.Empty(t, sandboxes.Items())
	require.Equal(t, []string{"early", "reclaim", "late"}, events,
		"the chain must reclaim the entry exactly once, between the steps either side of the registrar")

	// The reclamation and its report are the same branch, so the counter moving by
	// one is the same claim as the "reclaim" marker above — read from the metric
	// rather than from the callback, which is where a dashboard reads it.
	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 1,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))
}

// An operation-initiated stop — delete, pause, a checkpoint that resumes fresh —
// reclaims the entry before the chain runs. The chain's callback must then do
// nothing at all: no second OnStopping for subscribers to act on twice.
func TestSandboxCloseDoesNotReclaimAnEntryAnOperationAlreadyTook(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeSandbox)
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

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

	// The load-bearing case for the whole metric: this is what makes the series mean
	// "nobody stopped this lifecycle" rather than "a sandbox ended". Delete, pause
	// and a checkpoint that resumes fresh all mark the entry stopping before the
	// chain runs and must land here, uncounted. An in-place checkpoint is the one
	// that does not, and it is counted — see the test below.
	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 0,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))
}

// Several drivers can call Close for one lifecycle — the lifecycle goroutine, the
// start rollback, the build tree's deferred close alongside Shutdown, and Pause's
// own in-place teardown. Cleanup.Run is once-guarded, so they reclaim once between
// them and none returns before the reclamation has completed.
func TestSandboxCloseConcurrentClosesReclaimOnce(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeSandbox)
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

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

	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 1,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))
}

// Two sequential closes happen for real on the build provisioning path, which
// defers a Close and then calls Shutdown, whose own Close runs first. Cleanup.Run
// is once-guarded, so the second Close never re-enters the chain at all and the
// counter cannot double — this pins that guard, not anything about the counter's
// own arithmetic. The concurrent case above is the one with an interleaving.
func TestSandboxCloseTwiceCountsOnce(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeSandbox)
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sbx.Close(t.Context()))
	require.NoError(t, sbx.Close(t.Context()), "a second Close must stay successful")

	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 1,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))
}

// A build layer boot ends exactly like a crash does — nothing in the build tree
// marks a sandbox stopping, and every successful layer reaches the branch. The
// population has to be visible, or a build tree that stopped reaching it would
// read the same as a healthy one.
func TestSandboxCloseCountsABuildLifecycle(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeBuild)
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sbx.Close(t.Context()))

	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 0,
		string(sandboxtypes.SandboxTypeBuild):   1,
	}, unstoppedDelta(t, before))
}

// SandboxType's zero value is reachable: the resume-build and benchmark harnesses
// build RuntimeMetadata without it. String() maps it to "sandbox", so an unset
// type joins the customer series rather than opening a third one.
func TestSandboxCloseCountsAnUnsetTypeAsACustomerSandbox(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", "")
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	require.NoError(t, sbx.Close(t.Context()))

	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 1,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))
}

// An in-place checkpoint never marks the entry stopping — that is the point of it
// — so when its resume fails, Sandbox.Pause tears the sandbox down with the entry
// still live, having already recorded why. That teardown is orchestrator-chosen
// and still lands in the customer series, because the branch is about which
// mechanism reclaimed the entry and not about intent. Pinning it here is what
// stops the series being read as "guest deaths": a recorded stop reason does not
// take a lifecycle out of it.
func TestSandboxCloseCountsAnOrchestratorTeardownThatSkippedTheMark(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeSandbox)
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	sbx.SetStopReason(StopReasonKilled)
	require.NoError(t, sbx.Close(t.Context()))

	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 1,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))
}

// A lifecycle that never became live has no entry to reclaim: the reboot path
// defers its mark, and any construction failing before MarkRunning never made
// one. Nothing is counted, correctly — and Close returns exactly what it returns
// with an entry present, so the counter cannot turn a successful teardown into a
// failed one.
func TestSandboxCloseWithoutALiveEntryCountsNothing(t *testing.T) {
	t.Parallel()
	serializeUnstoppedCounter(t)

	sandboxes := NewSandboxesMap()
	sbx := testTypedMapSandbox(t, "lifecycle-1", sandboxtypes.SandboxTypeSandbox)
	attachOwnedCleanup(t, sandboxes, sbx)
	before := unstoppedCounts(t)

	// No MarkRunning.
	require.NoError(t, sbx.Close(t.Context()), "Close must succeed with no live entry")

	assert.Equal(t, map[string]int64{
		string(sandboxtypes.SandboxTypeSandbox): 0,
		string(sandboxtypes.SandboxTypeBuild):   0,
	}, unstoppedDelta(t, before))

	// The same Close against a live entry: the counted branch returns the same
	// value, so no error was introduced on either side of the boolean.
	live := testTypedMapSandbox(t, "lifecycle-2", sandboxtypes.SandboxTypeSandbox)
	attachOwnedCleanup(t, sandboxes, live)
	require.NoError(t, sandboxes.MarkRunning(t.Context(), live))
	require.NoError(t, live.Close(t.Context()), "Close must succeed with a live entry too")
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

type releaseSubscriber struct {
	insertRecorder

	onRelease func(*Sandbox) error
}

func (s *releaseSubscriber) OnNetworkRelease(_ context.Context, sbx *Sandbox) error {
	return s.onRelease(sbx)
}

// A failed release is reported after the source lookup is gone, and the other
// subscribers still receive the release.
func TestMapNetworkReleasedJoinsSubscriberErrors(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	ip := sbx.Slot.HostIPString()
	boom := errors.New("boom")
	var released []*Sandbox
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(*Sandbox) error {
		_, err := sandboxes.GetByHostPort(net.JoinHostPort(ip, "1234"))
		assert.Error(t, err, "subscribers run after the source lookup is removed")

		return boom
	}})
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(sbx *Sandbox) error {
		released = append(released, sbx)

		return nil
	}})
	sandboxes.AssignNetwork(t.Context(), sbx)

	require.ErrorIs(t, sandboxes.NetworkReleased(t.Context(), ip), boom)
	assert.Equal(t, []*Sandbox{sbx}, released)
	require.NoError(t, sandboxes.NetworkReleased(t.Context(), ip), "a repeated release notifies nobody")
	assert.Len(t, released, 1)
}
