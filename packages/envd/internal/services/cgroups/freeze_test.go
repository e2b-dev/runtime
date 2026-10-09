package cgroups

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkloadFreezer_FreezeHoldBlocksUnfreeze verifies FreezeHold keeps the
// shared lock held so a concurrent Unfreeze cannot thaw the workload until
// release is called — the serialization the live-upgrade handover relies on.
func TestWorkloadFreezer_FreezeHoldBlocksUnfreeze(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(NewNoopManager())

	release, _, err := f.FreezeHold(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	unfrozen := make(chan struct{})
	go func() {
		_ = f.Unfreeze(t.Context())
		close(unfrozen)
	}()

	select {
	case <-unfrozen:
		t.Fatal("Unfreeze thawed the workload while the freeze hold was active")
	case <-time.After(100 * time.Millisecond):
		// expected: blocked on the held lock
	}

	release()

	select {
	case <-unfrozen:
		// expected: proceeds once the hold is released
	case <-time.After(2 * time.Second):
		t.Fatal("Unfreeze did not proceed after the hold was released")
	}

	assert.NotPanics(t, release, "release must be idempotent")
}

// fakeFreezeManager drives the state-reading path: writes succeed, and whether a
// cgroup ever reports frozen is controlled per process type.
type fakeFreezeManager struct {
	mu        sync.Mutex
	frozen    map[ProcessType]bool
	frozenAt  map[ProcessType]int // reads before this one report frozen
	reads     map[ProcessType]int
	freezeErr map[ProcessType]error
	frozenErr map[ProcessType]error
	readOrder []ProcessType
	// unfreezeErr fails the thaw of a cgroup, leaving it frozen: a dirty thaw.
	unfreezeErr map[ProcessType]error
}

func newFakeFreezeManager() *fakeFreezeManager {
	return &fakeFreezeManager{
		frozen:    map[ProcessType]bool{},
		frozenAt:  map[ProcessType]int{},
		reads:     map[ProcessType]int{},
		freezeErr: map[ProcessType]error{},
		frozenErr: map[ProcessType]error{},
	}
}

func (m *fakeFreezeManager) GetFileDescriptor(ProcessType) (int, bool) { return 0, false }
func (m *fakeFreezeManager) Close() error                              { return nil }

func (m *fakeFreezeManager) Freeze(pt ProcessType) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.freezeErr[pt]; err != nil {
		return err
	}
	m.frozen[pt] = true

	return nil
}

func (m *fakeFreezeManager) Unfreeze(pt ProcessType) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.unfreezeErr[pt]; err != nil {
		return err
	}
	m.frozen[pt] = false

	return nil
}

func (m *fakeFreezeManager) Frozen(pt ProcessType) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.frozenErr[pt]; err != nil {
		return false, err
	}
	m.reads[pt]++
	// The ORDER matters, not only the count: polling every cgroup once per round and polling
	// one cgroup to completion before starting the next produce identical counts, and differ
	// only in how the reads interleave.
	m.readOrder = append(m.readOrder, pt)
	at, ok := m.frozenAt[pt]
	if !ok {
		return m.frozen[pt], nil
	}

	return m.frozen[pt] && m.reads[pt] >= at, nil
}

func TestFreeze_ReadsBackEveryCgroupFrozen(t *testing.T) {
	t.Parallel()
	f := NewWorkloadFreezer(newFakeFreezeManager())

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: time.Second})
	require.NoError(t, err)
	assert.Equal(t, len(WorkloadProcessTypes), res.Requested)
	assert.Equal(t, len(WorkloadProcessTypes), res.Frozen)
	assert.Zero(t, res.NotFrozen)
	assert.Zero(t, res.Failed)
	assert.True(t, res.AllFrozen())
}

// TestFreeze_UnobservableIsNeitherAwaitedNorCountedNotFrozen pins the noop/stub
// managers' contract. Reporting (false, nil) instead of the sentinel would make every
// freeze on a cgroup-less guest look like a workload refusing to stop: each pause would
// spin out the entire wait budget, and the strict live-upgrade handover would
// refuse every swap, on guests that never had the guarantee in the first place.
func TestFreeze_UnobservableIsNeitherAwaitedNorCountedNotFrozen(t *testing.T) {
	t.Parallel()
	f := NewWorkloadFreezer(NewNoopManager())

	// A budget far larger than the test's patience: if the wait were entered at all, the
	// elapsed assertion below would catch it.
	start := time.Now()
	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 10 * time.Second})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, len(WorkloadProcessTypes), res.Requested)
	assert.Equal(t, len(WorkloadProcessTypes), res.Unobservable)
	assert.Zero(t, res.Frozen)
	assert.Zero(t, res.NotFrozen, "unobservable state is not a workload that refused to stop")
	assert.Zero(t, res.Failed, "nor is it a failure")
	assert.True(t, res.AllFrozen(),
		"AllFrozen must hold, or the handover would refuse every swap on such a guest")
	assert.Less(t, elapsed, time.Second, "must not poll for a state that can never appear")
}

func TestFreeze_ReportsNotFrozenRatherThanFailing(t *testing.T) {
	t.Parallel()
	mgr := newFakeFreezeManager()
	// This cgroup's tasks never reach a signal-delivery point, so cgroup.events never
	// reports frozen. The pause must still succeed: an unfreezable customer task must
	// never fail their pause.
	mgr.frozenAt[WorkloadProcessTypes[0]] = 1 << 30
	f := NewWorkloadFreezer(mgr)

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 20 * time.Millisecond})
	require.NoError(t, err, "a workload that will not stop is reported, not an error")
	assert.Equal(t, 1, res.NotFrozen)
	assert.Equal(t, len(WorkloadProcessTypes)-1, res.Frozen)
	assert.False(t, res.AllFrozen())
}

func TestFreeze_WaitIsBoundedBySlowestNotSum(t *testing.T) {
	t.Parallel()
	mgr := newFakeFreezeManager()
	// Every cgroup needs several reads before it reads frozen. Polled together the wait is
	// one slow cgroup's worth; polled one after another it would be the sum, which is
	// what would blow the pause budget on a guest with many busy cgroups.
	const readsBeforeFrozen = 5
	for _, pt := range WorkloadProcessTypes {
		mgr.frozenAt[pt] = readsBeforeFrozen
	}
	f := NewWorkloadFreezer(mgr)

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 5 * time.Second})

	require.NoError(t, err)
	require.True(t, res.AllFrozen())

	// EVERY round must name every cgroup, not just the first: a sweep of everything followed by
	// draining each remaining cgroup in turn passes a first-round check and still costs the sum.
	// Read ORDER is what distinguishes them -- both orders read each cgroup the same number of
	// times, so an assertion on counts would pass either.
	order := mgr.readOrder

	// Every cgroup needs the same number of reads here, so none drops out early and the order
	// must be exactly `reads` whole rounds. That exactness is what lets each round be checked.
	require.Len(t, order, len(WorkloadProcessTypes)*readsBeforeFrozen,
		"expected %d whole rounds over %d cgroups, got %v",
		readsBeforeFrozen, len(WorkloadProcessTypes), order)

	for i := 0; i < len(order); i += len(WorkloadProcessTypes) {
		round := order[i : i+len(WorkloadProcessTypes)]
		for _, pt := range WorkloadProcessTypes {
			assert.Contains(t, round, pt,
				"cgroups must be polled together in every round: %s missing from round %d (%v); full order %v",
				pt, i/len(WorkloadProcessTypes)+1, round, order)
		}
	}
}

// TestFreeze_UnreadableStateIsCountedFailedNotAwaited covers the read-error arm of the wait
// for a cgroup that is STILL THERE and merely unreadable -- deliberately not one that went
// away, which is counted Vanished and covered separately. The distinction is why the fixture
// injects an opaque error rather than an errno: a real ENOENT or ENODEV here would be a
// vanish, and this test is about the other class.
//
// Expected rather than exceptional: the error is surfaced, but the result survives alongside
// it, the cgroup is counted failed rather than notFrozen -- the two say different things to
// the pause path -- and it is dropped from the poll set instead of waiting out a budget on a
// state that can never be read. The remaining cgroup is still awaited normally.
func TestFreeze_UnreadableStateIsCountedFailedNotAwaited(t *testing.T) {
	t.Parallel()

	mgr := newFakeFreezeManager()
	mgr.frozenErr[WorkloadProcessTypes[0]] = errors.New("read cgroup.events: no such file or directory")
	f := NewWorkloadFreezer(mgr)

	start := time.Now()
	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 10 * time.Second})
	elapsed := time.Since(start)

	require.Error(t, err, "the failure is surfaced, as it is for a rejected write")
	assert.Equal(t, len(WorkloadProcessTypes), res.Requested, "every write still landed")
	assert.Equal(t, 1, res.Failed)
	assert.Equal(t, len(WorkloadProcessTypes)-1, res.Frozen)
	assert.Zero(t, res.NotFrozen, "an unreadable state is not a workload refusing to stop")
	assert.False(t, res.AllFrozen())
	assert.Less(t, elapsed, time.Second,
		"a cgroup whose state cannot be read must be dropped from the poll, not waited out")
}

func TestFreeze_CountsWriteFailuresWithoutAborting(t *testing.T) {
	t.Parallel()
	mgr := newFakeFreezeManager()
	// A write the kernel refuses is expected, not exceptional, and must not stop the sweep
	// reaching the remaining cgroups. An opaque error on purpose: a vanish errno would be
	// tolerated rather than counted, which is a different arm.
	mgr.freezeErr[WorkloadProcessTypes[0]] = errors.New("invalid argument")
	f := NewWorkloadFreezer(mgr)

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: time.Second})
	require.Error(t, err, "the failure is surfaced")
	assert.Equal(t, 1, res.Failed)
	assert.Equal(t, len(WorkloadProcessTypes)-1, res.Requested)
	assert.Equal(t, len(WorkloadProcessTypes)-1, res.Frozen,
		"the cgroups that did freeze are still read back frozen")
}

func TestFreeze_ZeroBudgetSkipsTheWait(t *testing.T) {
	t.Parallel()
	mgr := newFakeFreezeManager()
	f := NewWorkloadFreezer(mgr)

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)
	assert.Equal(t, len(WorkloadProcessTypes), res.Requested)
	assert.Zero(t, res.Frozen, "no budget means the state was never read")
	assert.Equal(t, len(WorkloadProcessTypes), res.NotFrozen)
}

// TestFreeze_CancellationStopsThePollAndReleasesTheLock pins that the caller's ctx bounds
// the state read, not just the lock acquire. The poll holds the freeze lock, so a caller
// that has gone away must not leave us spending the rest of its budget in there: the
// rollback /unfreeze and the resume thaw both queue behind that lock.
//
// The window is widest when the budget outlives the caller's real deadline -- the shared
// client caps requests at 10s, so a budget above that is abandoned by the client while
// envd is still legitimately waiting.
func TestFreeze_CancellationStopsThePollAndReleasesTheLock(t *testing.T) {
	t.Parallel()
	mgr := newFakeFreezeManager()
	// Never reads frozen, so only the budget or cancellation can end the wait.
	for _, pt := range WorkloadProcessTypes {
		mgr.frozenAt[pt] = 1 << 30
	}
	f := NewWorkloadFreezer(mgr)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := f.Freeze(ctx, FreezeOptions{MaxWait: 10 * time.Second})
	elapsed := time.Since(start)

	require.NoError(t, err, "cancellation is not a freeze failure: the writes still landed")
	assert.Less(t, elapsed, 2*time.Second,
		"the poll must stop when the caller goes away, not run out the 10s budget")

	// The lock must be free immediately, or the rollback thaw queues behind a caller
	// that is no longer there.
	unfrozen := make(chan struct{})
	go func() {
		_ = f.Unfreeze(t.Context())
		close(unfrozen)
	}()
	select {
	case <-unfrozen:
	case <-time.After(2 * time.Second):
		t.Fatal("Unfreeze blocked: the freeze lock was still held after cancellation")
	}
}

// TestFreezeOptions_MaxCgroupsIsClampedAtTheThawCap: the sweep must never be allowed to cover more
// cgroups than a thaw can reach. That is the drift DefaultThawMaxCgroups forbids, reached by raising
// this knob rather than lowering that constant -- and this one is an int flag, tunable without a
// deploy, so the mistake is a typo rather than a code change. Its consequence is the worst one this
// design has: a workload frozen for the life of the sandbox, with a backstop that re-walks the same
// truncated prefix forever and can never reach what was frozen beyond it.
func TestFreezeOptions_MaxCgroupsIsClampedAtTheThawCap(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		set  int
		want int
	}{
		{"zero means the default", 0, DefaultFreezeMaxCgroups},
		{"negative means the default", -1, DefaultFreezeMaxCgroups},
		{"an ordinary raise is honoured", 4096, 4096},
		{"the thaw cap itself is allowed", DefaultThawMaxCgroups, DefaultThawMaxCgroups},
		{"past the thaw cap is clamped", DefaultThawMaxCgroups + 1, DefaultThawMaxCgroups},
		{"a fat-fingered flag is clamped", 163840, DefaultThawMaxCgroups},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, FreezeOptions{MaxCgroups: tc.set}.maxCgroups())
		})
	}
}

// The freeze generation moves on every freeze -- Freeze, and FreezeHold, which the live
// upgrade's handover takes -- and on nothing else: no thaw, no read and no ResumeFrozen.
func TestWorkloadFreezer_FreezeGenerationAdvancesOnFreezesOnly(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())
	ctx := t.Context()
	gen := f.FreezeGeneration(ctx)

	_, err := f.Freeze(ctx, FreezeOptions{MaxWait: time.Second})
	require.NoError(t, err)
	assert.Equal(t, gen+1, f.FreezeGeneration(ctx), "Freeze advances it")

	release, _, err := f.FreezeHold(ctx, FreezeOptions{MaxWait: time.Second})
	require.NoError(t, err)
	release()
	assert.Equal(t, gen+2, f.FreezeGeneration(ctx), "FreezeHold advances it")

	require.NoError(t, f.Unfreeze(ctx))
	_, err = f.UnfreezeReporting(ctx, DefaultThawMaxCgroups)
	require.NoError(t, err)
	_, matched, _, err := f.ThawIfGeneration(ctx, gen+2, nil)
	require.NoError(t, err)
	assert.True(t, matched)
	_, matched, _, err = f.ThawIfGeneration(ctx, gen, nil)
	require.NoError(t, err)
	assert.False(t, matched)
	f.ResumeFrozen(ctx)
	assert.Equal(t, gen+2, f.FreezeGeneration(ctx), "thaws, reads and ResumeFrozen leave it alone")
}

// ThawIfGeneration thaws only for the current generation; a stale one runs nothing, not even
// afterThaw, and reports the generation it found.
func TestWorkloadFreezer_ThawIfGenerationThawsOnlyTheCurrentGeneration(t *testing.T) {
	t.Parallel()

	mgr := newFakeFreezeManager()
	f := NewWorkloadFreezer(mgr)
	ctx := t.Context()
	entered := f.FreezeGeneration(ctx)
	_, err := f.Freeze(ctx, FreezeOptions{MaxWait: time.Second})
	require.NoError(t, err)

	ran := false
	current, matched, _, err := f.ThawIfGeneration(ctx, entered, func() { ran = true })
	require.NoError(t, err)
	assert.False(t, matched, "a generation a freeze overtook must not thaw")
	assert.Equal(t, entered+1, current)
	assert.False(t, ran, "nothing runs on a mismatch")
	assert.True(t, mgr.frozen[ProcessTypeUser], "the freeze stands")

	_, matched, _, err = f.ThawIfGeneration(ctx, current, func() { ran = true })
	require.NoError(t, err)
	assert.True(t, matched)
	assert.True(t, ran)
	assert.False(t, mgr.frozen[ProcessTypeUser], "the current generation thaws")
}

// afterThaw runs after the thaw and before the freeze lock is released, so whatever it records
// cannot be zeroed by a freeze landing between the thaw and the record.
func TestWorkloadFreezer_ThawIfGenerationRunsAfterThawUnderTheLock(t *testing.T) {
	t.Parallel()

	mgr := newFakeFreezeManager()
	f := NewWorkloadFreezer(mgr)
	_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: time.Second})
	require.NoError(t, err)

	var ran, held, frozen bool
	_, matched, _, err := f.ThawIfGeneration(t.Context(), f.FreezeGeneration(t.Context()), func() {
		ran = true
		frozen = mgr.frozen[ProcessTypeUser]
		held = !f.lock.TryAcquire(1)
		if !held {
			f.lock.Release(1)
		}
	})
	require.NoError(t, err)
	require.True(t, matched)
	require.True(t, ran)
	assert.False(t, frozen, "afterThaw runs after the thaw")
	assert.True(t, held, "afterThaw runs before the lock is released")
}

// The conditional thaw keeps the watchdog's rules: a clean thaw disarms it, a dirty one leaves
// it armed to retry, and a withheld one leaves it armed for the freeze that voided the thaw.
func TestWorkloadFreezer_ThawIfGenerationKeepsTheWatchdogRules(t *testing.T) {
	t.Parallel()

	armed := func(f *WorkloadFreezer) bool {
		f.watchdogMu.Lock()
		defer f.watchdogMu.Unlock()

		return f.watchdog != nil
	}
	freeze := func(t *testing.T, mgr *fakeFreezeManager) *WorkloadFreezer {
		t.Helper()

		f := NewWorkloadFreezer(mgr)
		f.SetThawWatchdog(time.Hour, func(ThawResult, error) {})
		_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: time.Second})
		require.NoError(t, err)
		require.True(t, armed(f), "the freeze arms the watchdog")

		return f
	}

	t.Run("clean", func(t *testing.T) {
		t.Parallel()

		f := freeze(t, newFakeFreezeManager())
		_, matched, _, err := f.ThawIfGeneration(t.Context(), f.FreezeGeneration(t.Context()), nil)
		require.NoError(t, err)
		require.True(t, matched)
		assert.False(t, armed(f))
	})

	t.Run("dirty", func(t *testing.T) {
		t.Parallel()

		mgr := newFakeFreezeManager()
		mgr.unfreezeErr = map[ProcessType]error{ProcessTypeUser: errors.New("EBUSY")}
		f := freeze(t, mgr)
		_, matched, _, err := f.ThawIfGeneration(t.Context(), f.FreezeGeneration(t.Context()), nil)
		require.Error(t, err)
		require.True(t, matched)
		assert.True(t, armed(f))
	})

	t.Run("withheld", func(t *testing.T) {
		t.Parallel()

		f := freeze(t, newFakeFreezeManager())
		_, matched, _, err := f.ThawIfGeneration(t.Context(), f.FreezeGeneration(t.Context())-1, nil)
		require.NoError(t, err)
		require.False(t, matched)
		assert.True(t, armed(f))
	})
}
