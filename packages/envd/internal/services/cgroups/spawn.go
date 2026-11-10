package cgroups

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// ErrWorkloadFrozen is returned to a caller that wanted to place a process into a cgroup
// that is frozen, or that is about to be. Either way it is a state that ends without the
// caller changing its request -- a freeze of ours is undone on the resume, and one the guest
// wrote itself lasts until the guest thaws it -- so it maps to a RETRYABLE status at the RPC
// boundary rather than an invalid-argument one.
var ErrWorkloadFrozen = errors.New("the target cgroup is frozen")

// ErrSpawnDrainTimeout means a freeze gave up waiting for an in-flight spawn, so it froze
// nothing.
//
// Deliberately NOT a wrapped context error, even though a context deadline is what detects
// it. /freeze tells a caller that went away (a ctx error) apart from a sweep that ran and
// collected per-cgroup errors, and the drain budget is ours rather than the caller's: a
// wrapped DeadlineExceeded would be read as contention, answer 503 and skip the pre-pause
// log flush on its way out.
var ErrSpawnDrainTimeout = errors.New("timed out waiting for an in-flight process spawn")

// maxSpawnWeight is the semaphore's capacity, not a concurrency limit: a spawn takes 1 and
// the barrier takes all of it, so the number only has to exceed the spawns that can ever be
// in flight at once. It is deliberately far above that -- the semaphore exists to express
// "many readers, one writer", and a capacity a busy guest could exhaust would turn it into
// an accidental throttle on process starts.
const maxSpawnWeight = 1 << 20

// spawnAdmitWait bounds how long a spawn waits when the barrier is being raised rather
// than held: a freeze is draining the spawns already in flight and has not yet written
// cgroup.freeze. The wait lets a spawn that arrives late in a drain succeed when the drain
// gives up, instead of failing for no reason. It is shorter than SpawnDrainWait, so a spawn
// that arrives early in a drain gives up first and is refused.
//
// It is deliberately NOT how long a spawn waits during an established frozen window. That
// window lasts for the pause, the snapshot and the resume -- seconds at best, unbounded at
// worst -- so waiting in it would turn every call into a timeout. See spawnBarrier.enter.
const spawnAdmitWait = 100 * time.Millisecond

// SpawnDrainWait bounds how long a freeze waits for in-flight spawns before giving up.
//
// A spawn holds its weight from the pre-spawn probe until the fork returns: opening a PTY
// where there is one, the rest of exec.Cmd.Start's setup (the /dev/null open for a disabled
// stdin, the exec status pipe), and the clone itself. That is normally well under a
// millisecond. What can exceed this is a guest starved of CPU or memory badly enough that
// the forking thread is not scheduled -- or a spawn that is
// already wedged in the kernel, which is the state this whole mechanism exists to prevent
// and which no freeze can clear.
//
// Timing out therefore aborts the freeze rather than proceeding without the drain. That is
// the conservative direction: a pause that captures a running workload is today's
// behaviour whenever a freeze fails, while a pause that captures a wedged spawn produces a
// snapshot whose envd is stopped.
const SpawnDrainWait = 250 * time.Millisecond

// spawnBarrier makes spawns into the workload cgroups and the freeze sweep mutually
// exclusive. The socat's cgroup is never swept, so its spawns take only the probe.
//
// The hazard it closes is specific and fatal. A process placed with
// clone3(CLONE_INTO_CGROUP) into a cgroup that is already frozen is stopped by the freezer
// on its first return from the fork, before it reaches execve -- and Go's fork/exec path
// carries CLONE_VM|CLONE_VFORK, so the spawning thread blocks in the kernel until the child
// execs. It never does. The wait is a raw syscall that keeps the thread's P with signals
// blocked, so the runtime cannot preempt it, and the next garbage-collection
// stop-the-world waits for that P forever: every goroutine in envd stops, /init's thaw
// and the thaw watchdog included, and nothing is left that could unfreeze
// the child. If the snapshot captures that state, a resume succeeds only when /init
// happens to thaw the cgroup before the next stop-the-world begins.
//
// So the barrier is held from before the sweep writes cgroup.freeze until the thaw of the
// cgroups spawns land in, not merely across the sweep. A spawn admitted in between would
// land in a cgroup that is frozen exactly as if it had raced the write.
//
// It is a weighted semaphore rather than an RWMutex for two reasons: the freeze side needs
// a BOUNDED wait (an RWMutex cannot time out, so one wedged spawn would hold the pause
// forever), and the two sides are acquired and released by different goroutines at
// different times, which a mutex's ownership semantics do not express.
type spawnBarrier struct {
	sem *semaphore.Weighted

	// drainWait and admitWait are SpawnDrainWait and spawnAdmitWait, held per barrier so a
	// test can pull them apart far enough that its outcome does not depend on scheduling.
	drainWait time.Duration
	admitWait time.Duration

	// raiseMu serializes raise against itself, so two freezes cannot both find the barrier
	// down and both queue for the full weight -- the second would then wait out its whole
	// drain budget behind the first. It is held across the drain, which is why it is not
	// the lock that guards held: a spawn must be able to read held without waiting for a
	// drain to finish.
	raiseMu sync.Mutex

	// mu guards held, and only held. Never held across a semaphore operation that can
	// block.
	mu sync.Mutex
	// held says the barrier is up: a freeze of ours is in effect and spawns are refused
	// outright instead of waiting. It also makes raise and lower idempotent, which they
	// must be -- a second freeze inside the same frozen window is normal (the live-upgrade
	// handover freezes while the pause's freeze still stands), and several thaw paths can
	// run for one freeze.
	held bool
}

func newSpawnBarrier() *spawnBarrier {
	return &spawnBarrier{
		sem:       semaphore.NewWeighted(maxSpawnWeight),
		drainWait: SpawnDrainWait,
		admitWait: spawnAdmitWait,
	}
}

func (b *spawnBarrier) isHeld() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.held
}

// raise blocks spawns and waits for the in-flight ones to finish. Idempotent: if the
// barrier is already up, the window it guards is already established and there is nothing
// to drain. Bounded by both ctx and SpawnDrainWait, and on timeout it acquires nothing --
// the caller must not freeze.
func (b *spawnBarrier) raise(ctx context.Context) error {
	b.raiseMu.Lock()
	defer b.raiseMu.Unlock()

	if b.isHeld() {
		return nil
	}

	drainCtx, cancel := context.WithTimeout(ctx, b.drainWait)
	defer cancel()

	if err := b.sem.Acquire(drainCtx, maxSpawnWeight); err != nil {
		// The caller's own context takes precedence: it going away is contention, which its
		// handler already classifies, and only the remaining case is a spawn that will not
		// come back.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return ErrSpawnDrainTimeout
	}

	b.mu.Lock()
	b.held = true
	b.mu.Unlock()

	return nil
}

// lower re-admits spawns. Idempotent, because one freeze can be followed by several thaws --
// the resume /init, /unfreeze, the watchdog, a handover that failed or aborted -- and the
// first one to clear the static cgroups lowers it.
func (b *spawnBarrier) lower() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.held {
		return
	}
	b.sem.Release(maxSpawnWeight)
	b.held = false
}

// enter admits one spawn, returning the release it must call once the fork has returned.
//
// A spawn that finds the barrier UP fails immediately: the window lasts for a pause and a
// resume, so waiting in it would replace a prompt retryable error with a client timeout.
// A spawn that finds a raise queued ahead of it -- the semaphore is FIFO, so TryAcquire
// fails behind a pending full-weight acquire -- waits up to spawnAdmitWait on the
// semaphore itself: admitted if that drain gives up within the wait, refused when the wait
// runs out.
func (b *spawnBarrier) enter(ctx context.Context) (func(), error) {
	if b.sem.TryAcquire(1) {
		return b.releaseOnce(), nil
	}

	if b.isHeld() {
		return nil, ErrWorkloadFrozen
	}

	admitCtx, cancel := context.WithTimeout(ctx, b.admitWait)
	defer cancel()

	if err := b.sem.Acquire(admitCtx, 1); err != nil {
		// The caller's own context ending is its own outcome, reported as such -- the same
		// distinction raise makes. Only our bound running out means "frozen".
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		return nil, ErrWorkloadFrozen
	}

	return b.releaseOnce(), nil
}

// releaseOnce guards against a double release, which would hand a spawn's weight back
// twice and let the barrier be raised while a clone is in flight. A caller that releases
// on more than one exit path -- a deferred release plus an explicit one -- stays correct.
func (b *spawnBarrier) releaseOnce() func() {
	var once sync.Once

	return func() { once.Do(func() { b.sem.Release(1) }) }
}

// BeginSpawn admits a process spawn that will be placed into procType's cgroup, and returns
// the release the caller must invoke once the fork has returned -- not once the process
// exits. Take it immediately around the fork and nothing more: every freeze waits for the
// spawns it has admitted, so whatever a caller does while holding it is charged to the
// pause's drain budget.
//
// Two checks, for two different sources of a frozen cgroup:
//
//   - the barrier, for freezes WE issue. It is exact: a spawn and a sweep cannot overlap.
//     Taken only for the cgroups a sweep actually freezes, so the port forwarder -- whose
//     cgroup is on the liveness allowlist and is never ours to freeze -- is not stalled for
//     the length of every pause.
//   - the probe, for a freeze the GUEST issued. Nothing in this process sees that write, so
//     there is no lock to take; reading the state immediately before the clone is the best
//     available, and it is not a race-free check. It closes the case that matters in
//     practice -- a guest that froze the cgroup and then asked us to spawn into it -- and
//     leaves a window between the read and the clone that it cannot close. A workload spawn
//     wedged in that window keeps its admission, so the next freeze's drain times out and
//     refuses to freeze over it; a socat takes no admission, so nothing here catches it.
func (f *WorkloadFreezer) BeginSpawn(ctx context.Context, procType ProcessType) (func(), error) {
	release := func() {}

	if slices.Contains(WorkloadProcessTypes, procType) {
		var err error
		if release, err = f.spawns.enter(ctx); err != nil {
			return nil, err
		}
	}

	if err := f.probeThawed(procType); err != nil {
		release()

		return nil, err
	}

	return release, nil
}

// probeThawed refuses a spawn into a cgroup that reads frozen, or whose ancestor does.
//
// It reads cgroup.events for the target, which reports the SETTLED state and reports it for
// an inherited freeze too -- a cgroup frozen only because an ancestor is frozen reads frozen
// here, and a child cloned into it would be stopped just the same. The requested state of
// each ancestor is then read as well, because a freeze that has been written but has not
// settled yet still stops a task that arrives afterwards, and cgroup.events would say 0.
//
// A read that FAILS does not refuse the spawn. The probe is a best-effort guard against a
// freeze this process cannot see; turning an unreadable cgroupfs into a process-start
// outage would be a worse failure than the one it protects against, and a cgroup that
// vanished mid-probe has nothing left to freeze anything.
func (f *WorkloadFreezer) probeThawed(procType ProcessType) error {
	pm, ok := f.mgr.(PathManager)
	if !ok {
		// No paths to read. Such a manager also hands out no cgroup fd, so the spawn is
		// not placed into a cgroup at all and cannot be caught by a freezer.
		return nil
	}

	path, ok := pm.PathOf(procType)
	if !ok {
		// A ProcessType with no cgroup of its own -- ProcessTypeSystem, deliberately left
		// unregistered so it stays in envd's own cgroup and is immune to the freeze.
		return nil
	}

	if frozen, err := pm.FrozenAt(path); err == nil && frozen {
		return fmt.Errorf("%w: %s reads frozen", ErrWorkloadFrozen, path)
	}

	for _, ancestor := range AncestorChain(pm.Root(), path) {
		requested, err := pm.FreezeRequestedAt(ancestor)
		if err == nil && requested {
			return fmt.Errorf("%w: %s has a freeze requested", ErrWorkloadFrozen, ancestor)
		}
	}

	return nil
}
