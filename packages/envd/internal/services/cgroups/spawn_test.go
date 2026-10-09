//go:build linux

package cgroups

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBeginSpawn_RefusedForTheWholeFrozenWindow is the core contract. The window is what
// matters, not the sweep: a spawn admitted after the sweep returns is placed into a cgroup
// that is by then definitely frozen, which is worse than one that merely races the write.
func TestBeginSpawn_RefusedForTheWholeFrozenWindow(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err, "no freeze in effect: the spawn must be admitted")
	release()

	_, err = f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	_, err = f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen,
		"the sweep has returned but the workload is frozen until the resume thaw")

	require.NoError(t, f.Unfreeze(t.Context()))

	release, err = f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err, "the thaw must re-admit spawns")
	release()
}

// TestBeginSpawn_RefusalIsPromptRatherThanATimeout pins the wait policy. A frozen window
// spans a pause, a snapshot and a resume, so a spawn that waited it out would answer a
// client timeout instead of a retryable error -- and the caller most likely to hit this is
// one polling every few hundred milliseconds.
//
// The admit wait is set to an hour, so the refusal can only come from the held barrier: a
// spawn that queued on the semaphore instead would not return within the test's guard.
func TestBeginSpawn_RefusalIsPromptRatherThanATimeout(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())
	f.spawns.admitWait = time.Hour
	_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	refused := make(chan error, 1)
	go func() {
		_, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
		refused <- err
	}()

	select {
	case err := <-refused:
		require.ErrorIs(t, err, ErrWorkloadFrozen)
	case <-time.After(5 * time.Second):
		t.Fatal("the spawn waited on the semaphore instead of being refused by the held barrier")
	}
}

// TestFreeze_DrainsInFlightSpawnBeforeWriting is the other half of the exclusion: the
// write must not land while a clone is in flight, because that child is placed into the
// cgroup and then frozen before it can exec.
//
// The assertion is on the TREE, not on the timing: cgroup.freeze must still read 0 while
// the spawn is in flight. A test that only checked that Freeze returned late would pass
// against a sweep that wrote first and blocked afterwards.
func TestFreeze_DrainsInFlightSpawnBeforeWriting(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service")
	// The drain must outlast the runner's scheduling, so it ends only on the release below.
	f.spawns.drainWait = time.Hour

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err)

	frozen := make(chan error, 1)
	go func() {
		_, freezeErr := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
		frozen <- freezeErr
	}()

	// Once the drain is queued the sweep is committed to its order: a sweep that wrote
	// before draining has written by now.
	waitDrainQueued(t, f.spawns)
	assert.False(t, frozenOnDisk(t, root, string(ProcessTypeUser)),
		"the sweep wrote cgroup.freeze while a spawn was still in flight")

	select {
	case <-frozen:
		t.Fatal("the freeze completed without waiting for the in-flight spawn")
	default:
	}

	release()

	select {
	case err := <-frozen:
		require.NoError(t, err)
		assert.True(t, frozenOnDisk(t, root, string(ProcessTypeUser)),
			"the freeze must proceed once the spawn has returned")
	case <-time.After(2 * time.Second):
		t.Fatal("the freeze did not proceed after the spawn returned")
	}
}

// TestFreeze_AbortsRatherThanFreezeOverAWedgedSpawn covers a spawn that never returns --
// the state this mechanism exists to prevent, reachable through a freeze envd cannot see.
// Freezing on top of it is what converts a wedged thread into an unresumable snapshot, so
// the freeze must decline. A pause that captures a running workload is the same outcome
// every other freeze failure already produces.
func TestFreeze_AbortsRatherThanFreezeOverAWedgedSpawn(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service")

	// Never released: this is a spawn stuck in the kernel.
	_, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err)

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})

	require.ErrorIs(t, err, ErrSpawnDrainTimeout,
		"the freeze must fail rather than write over a wedged spawn")
	assert.Zero(t, res.Requested, "nothing may be requested when the drain failed")
	assert.False(t, res.AllFrozen(),
		"a freeze that froze nothing must not read as a workload that stopped")
	assert.False(t, frozenOnDisk(t, root, string(ProcessTypeUser)))
	assert.False(t, frozenOnDisk(t, root, string(ProcessTypePTY)))

	// The drain budget is envd's own, not the caller's, and /freeze reads a context error as
	// a caller that went away -- answering 503 and skipping the pre-pause log flush.
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, context.Canceled)
}

// TestFreeze_DrainReportsTheCallersCancellationAsItsOwn is the counterpart: when the
// caller's context is what expired, the error must stay a context error, because that is
// how the /freeze handler tells contention apart from a sweep that ran.
func TestFreeze_DrainReportsTheCallersCancellationAsItsOwn(t *testing.T) {
	t.Parallel()

	f, _ := newTreeFixture(t, "/envd.service", "envd.service")
	// Only the caller's deadline can end this drain.
	f.spawns.drainWait = time.Hour

	_, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err)

	// A live context with a short budget, so the freeze lock is still acquired normally --
	// an already-cancelled context would fail at that lock instead and never reach the drain.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err = f.Freeze(ctx, FreezeOptions{MaxWait: 0})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrSpawnDrainTimeout)
}

// TestBeginSpawn_ProbeRefusesAGuestFreeze covers the freeze no lock can see: the guest
// writes cgroup.freeze itself (a `docker pause`, or a deliberate write from inside the
// sandbox). The barrier knows nothing about it, so the state has to be read.
func TestBeginSpawn_ProbeRefusesAGuestFreeze(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service")

	freezeOnDisk(t, root, string(ProcessTypeUser))

	_, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen)

	release, err := f.BeginSpawn(t.Context(), ProcessTypePTY)
	require.NoError(t, err, "a cgroup nobody froze is unaffected")
	release()
}

// TestBeginSpawn_ProbeRefusesAnUnsettledAncestorFreeze covers the arm cgroup.events cannot
// report: a freeze written to an ancestor that has not settled yet. The target's
// cgroup.events still reads 0, and a child cloned in is stopped all the same, so the
// requested state of every ancestor has to be read too.
func TestBeginSpawn_ProbeRefusesAnUnsettledAncestorFreeze(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service", "customer/nested")

	// Re-home the user cgroup under the ancestor, so there is an intermediate cgroup to
	// freeze: the fixture's default layout puts it directly under the root, whose
	// cgroup.freeze does not exist on cgroup v2.
	mgr := f.Manager().(*Cgroup2Manager) //nolint:errcheck // the fixture always builds this manager
	mgr.cgroupPaths[ProcessTypeUser] = filepath.Join(root, "customer", "nested")

	// Only the REQUEST, deliberately: cgroup.events is left reading "frozen 0" the way the
	// kernel leaves it while tasks are still stopping.
	require.NoError(t, os.WriteFile(filepath.Join(root, "customer", "cgroup.freeze"), []byte("1\n"), 0o644))

	settled, err := mgr.FrozenAt(filepath.Join(root, "customer", "nested"))
	require.NoError(t, err)
	require.False(t, settled, "the fixture must model a freeze that has not settled")

	_, err = f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen)
}

// TestBeginSpawn_ProbeToleratesUnreadableState keeps the guard from becoming an outage. A
// cgroupfs that cannot be read says nothing about whether a freeze is in effect, and
// refusing every process start on a guest whose tree we cannot read would be a worse
// failure than the one this protects against.
func TestBeginSpawn_ProbeToleratesUnreadableState(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service")

	require.NoError(t, os.Remove(filepath.Join(root, string(ProcessTypeUser), "cgroup.events")))
	require.NoError(t, os.Remove(filepath.Join(root, string(ProcessTypeUser), "cgroup.freeze")))

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err)
	release()
}

// TestBeginSpawn_NonWorkloadCgroupsSkipTheBarrier keeps the pause from stalling work it
// has no reason to stall. The socat cgroup is on the liveness allowlist and is never
// frozen by a sweep, so barriering it would stop port forwarding for the length of every
// pause and resume for nothing.
func TestBeginSpawn_NonWorkloadCgroupsSkipTheBarrier(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())
	_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	for _, pt := range []ProcessType{ProcessTypeSocat, ProcessTypeSystem} {
		release, err := f.BeginSpawn(t.Context(), pt)
		require.NoError(t, err, "%s is not a cgroup a sweep freezes", pt)
		release()
	}
}

// TestBeginSpawn_ProbeStillCoversNonWorkloadCgroups is the counterpart: skipping the
// barrier is a statement about OUR freezes only. The guest can freeze the socat cgroup,
// and a socat cloned into it wedges envd exactly as a user process would.
func TestBeginSpawn_ProbeStillCoversNonWorkloadCgroups(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service", "socats")

	mgr := f.Manager().(*Cgroup2Manager) //nolint:errcheck // the fixture always builds this manager
	mgr.cgroupPaths[ProcessTypeSocat] = filepath.Join(root, "socats")
	freezeOnDisk(t, root, "socats")

	_, err := f.BeginSpawn(t.Context(), ProcessTypeSocat)
	require.ErrorIs(t, err, ErrWorkloadFrozen)
}

// TestBeginSpawn_AdmitsConcurrentSpawns pins that the barrier is not a mutual-exclusion
// lock between spawns. Serializing process starts against each other would make the fix a
// throughput regression on every guest that starts processes in parallel.
func TestBeginSpawn_AdmitsConcurrentSpawns(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())

	const spawns = 32
	releases := make([]func(), 0, spawns)
	for range spawns {
		release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
		require.NoError(t, err)
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
		assert.NotPanics(t, release, "release must be idempotent")
	}

	// A freeze drains for the full weight, so it succeeds only if every unit came back.
	_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err, "a spawn's weight was not returned, so the drain could not complete")
}

// TestFreeze_SweepThatFrozeNothingAdmitsSpawns keeps a failed freeze from becoming a
// process-start outage. A sweep whose writes all fail opened no window, and on a guest
// where they always fail there is no thaw coming to lower the barrier.
func TestFreeze_SweepThatFrozeNothingAdmitsSpawns(t *testing.T) {
	t.Parallel()

	mgr := newFakeFreezeManager()
	for _, pt := range WorkloadProcessTypes {
		mgr.freezeErr[pt] = errors.New("refused")
	}
	f := NewWorkloadFreezer(mgr)

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.Error(t, err)
	require.Zero(t, res.Requested)

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err)
	release()
}

// unthawable is a manager whose thaw always fails, i.e. a guest left frozen by a thaw that
// did not work.
type unthawable struct {
	*fakeFreezeManager
}

func (unthawable) Unfreeze(ProcessType) error { return errors.New("thaw refused") }

// TestUnfreeze_DirtyThawKeepsRefusingSpawns follows the static cgroups' thaw: when one of
// them refuses its write it may still be frozen, so admitting a spawn could still wedge
// envd. Refusing is the truthful answer -- anything started there would be frozen on
// arrival.
func TestUnfreeze_DirtyThawKeepsRefusingSpawns(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(unthawable{newFakeFreezeManager()})

	_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)
	require.Error(t, f.Unfreeze(t.Context()))

	_, err = f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen)
}

// TestResumeFrozen_RaisesTheBarrier covers the live-upgrade handover, which deliberately
// leaves the workload frozen for the post-upgrade /init to thaw. The semaphore did not
// survive the execve, so without this the new image's starts into the frozen cgroups would
// rest on the pre-spawn probe alone. The fake manager has no paths, so here there is no
// probe and a missing raise admits the spawn outright.
func TestResumeFrozen_RaisesTheBarrier(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())

	require.NoError(t, f.ResumeFrozen(t.Context()))

	_, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen)

	require.NoError(t, f.Unfreeze(t.Context()))

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err, "the post-upgrade /init thaw must re-admit spawns")
	release()
}

// TestFreeze_SecondFreezeInsideTheSameWindow covers the handover taking the freeze while
// the pause's freeze still stands. raise must be idempotent: a second full-weight acquire
// of the same semaphore would wait out its drain behind the first and refuse the freeze,
// and the barrier must survive the FreezeHold release that follows.
func TestFreeze_SecondFreezeInsideTheSameWindow(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(newFakeFreezeManager())

	_, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	release, _, err := f.FreezeHold(t.Context(), FreezeOptions{MaxWait: 0})
	require.NoError(t, err, "a second freeze inside the same window drained against the first")
	release()

	_, err = f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen,
		"releasing the second freeze's hold must not lower the barrier")
}

// TestSpawnBarrier_RaiseIsSerialized guards the raise path against two freezes deciding to
// acquire at once, which would block the second inside the semaphore rather than returning
// the "already up" answer.
func TestSpawnBarrier_RaiseIsSerialized(t *testing.T) {
	t.Parallel()

	b := newSpawnBarrier()

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() { errs[i] = b.raise(t.Context()) })
	}
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "raise %d", i)
	}

	b.lower()
	assert.NotPanics(t, b.lower, "lower must be idempotent")

	release, err := b.enter(t.Context())
	require.NoError(t, err, "the barrier must be fully released after one lower")
	release()
}

// ptysRemoved models a guest that removed the (empty) pty cgroup: every write to it fails as
// the cgroupfs reports a vanished cgroup.
type ptysRemoved struct {
	*fakeFreezeManager
}

func (m ptysRemoved) Freeze(pt ProcessType) error {
	if pt == ProcessTypePTY {
		return fs.ErrNotExist
	}

	return m.fakeFreezeManager.Freeze(pt)
}

func (m ptysRemoved) Unfreeze(pt ProcessType) error {
	if pt == ProcessTypePTY {
		return fs.ErrNotExist
	}

	return m.fakeFreezeManager.Unfreeze(pt)
}

// TestUnfreeze_VanishedStaticCgroupDoesNotHoldTheBarrier pins that a static cgroup that no
// longer exists does not keep spawns refused. Every later thaw fails on it the same way, so
// counting it as still frozen would refuse every start into the cgroup that did thaw for the
// life of the sandbox. A refused write of any other kind still does, which
// TestUnfreeze_DirtyThawKeepsRefusingSpawns covers.
func TestUnfreeze_VanishedStaticCgroupDoesNotHoldTheBarrier(t *testing.T) {
	t.Parallel()

	f := NewWorkloadFreezer(ptysRemoved{newFakeFreezeManager()})

	res, err := f.Freeze(t.Context(), FreezeOptions{MaxWait: 0})
	require.Error(t, err, "the fixture must fail the pty freeze")
	require.Positive(t, res.Requested, "the user freeze must still open a window")

	_, err = f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.ErrorIs(t, err, ErrWorkloadFrozen, "the window is open until the thaw")

	require.Error(t, f.Unfreeze(t.Context()), "the pty thaw fails as well")

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err, "user was thawed and pty no longer exists, so a start must be admitted")
	release()
}

// TestUnfreeze_PartialThawOfTheGuestReadmitsSpawns pins that the barrier follows the
// cgroups spawns land in, not the whole tree. A discovered walk that truncates -- a guest
// with more cgroups than the bound, which a watchdog retry re-walks identically -- or one
// guest cgroup that keeps refusing its write leaves the thaw dirty for good. Refusing
// process starts on that account would last the life of the sandbox, while user and pty
// are running normally.
func TestUnfreeze_PartialThawOfTheGuestReadmitsSpawns(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/envd.service", "envd.service", "customer")
	f.SetThawWatchdog(time.Hour, nil)

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy, MaxWait: 0})
	require.NoError(t, err)
	require.True(t, frozenOnDisk(t, root, "customer"), "the fixture must model a guest cgroup we froze")

	// A bound of one visits the root and stops: the static cgroups are thawed, the walk is
	// truncated, and "customer" is still frozen.
	thaw, err := f.UnfreezeReporting(t.Context(), 1)
	require.Error(t, err)
	require.True(t, thaw.Truncated)
	require.True(t, frozenOnDisk(t, root, "customer"))
	require.Positive(t, res.Requested)

	release, err := f.BeginSpawn(t.Context(), ProcessTypeUser)
	require.NoError(t, err, "user was thawed, so a start into it must be admitted")
	release()

	// The rest of the dirty-thaw handling is unchanged: the window stays open for the next
	// thaw, and the backstop stays armed to retry it.
	f.sweepMu.Lock()
	active := f.freezeActive
	f.sweepMu.Unlock()
	assert.True(t, active, "a dirty thaw keeps the freeze counted as in effect")

	f.watchdogMu.Lock()
	armed := f.watchdog != nil
	f.watchdogMu.Unlock()
	assert.True(t, armed, "a dirty thaw keeps the watchdog armed")
}

// queuedDrain puts b into the state a spawn can meet mid-pause: a spawn in flight that never
// returns, and a raise queued behind it for the full weight. The drain is effectively
// unbounded and ends only when the returned cancel is called, so nothing a test asserts
// depends on how quickly the runner schedules it. raised yields the raise's result.
func queuedDrain(t *testing.T, b *spawnBarrier) (raised <-chan error, cancel context.CancelFunc) {
	t.Helper()

	b.drainWait = time.Hour

	_, err := b.enter(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	out := make(chan error, 1)
	go func() { out <- b.raise(ctx) }()

	waitDrainQueued(t, b)

	return out, cancel
}

// waitDrainQueued returns once a raise is queued on b for the full weight. The semaphore is
// FIFO, so a one-unit TryAcquire fails exactly once that acquire is queued ahead of it -- a
// direct signal, where a sleep would only be a guess. The window is a hang guard, not a bound.
func waitDrainQueued(t *testing.T, b *spawnBarrier) {
	t.Helper()

	require.Eventually(t, func() bool {
		if b.sem.TryAcquire(1) {
			b.sem.Release(1)

			return false
		}

		return true
	}, 5*time.Second, time.Millisecond, "the raise never queued its drain")
}

// TestSpawnBarrier_EnterRespectsItsOwnBound pins that a spawn meeting a drain in progress
// gives up at its own admit bound rather than waiting for the drain. A lock held across the
// drain would park it until the drain ended instead -- ignoring its own bound on the path
// it was written for.
func TestSpawnBarrier_EnterRespectsItsOwnBound(t *testing.T) {
	t.Parallel()

	b := newSpawnBarrier()
	b.admitWait = 20 * time.Millisecond
	raised, cancelDrain := queuedDrain(t, b)

	_, err := b.enter(t.Context())
	require.ErrorIs(t, err, ErrWorkloadFrozen)

	select {
	case err := <-raised:
		t.Fatalf("the spawn waited for the drain to end (raise returned %v)", err)
	default:
	}

	cancelDrain()
	require.ErrorIs(t, <-raised, context.Canceled)
}

// TestSpawnBarrier_EnterReportsTheCallersCancellation keeps a caller's own deadline from
// being reported as a freeze. A start whose request timed out while it waited behind a
// drain did not meet a frozen workload, and telling the client "unavailable, retry" is the
// wrong answer to a request it has already given up on.
func TestSpawnBarrier_EnterReportsTheCallersCancellation(t *testing.T) {
	t.Parallel()

	b := newSpawnBarrier()
	b.admitWait = time.Hour
	_, cancelDrain := queuedDrain(t, b)
	defer cancelDrain()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err := b.enter(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrWorkloadFrozen)
}
