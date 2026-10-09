package cgroups

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// WorkloadProcessTypes are the cgroups holding the customer workload: the
// processes/shells envd spawns (user) and PTY sessions (ptys). These are frozen
// before a pause and thawed on resume; envd's own system processes are excluded.
var WorkloadProcessTypes = []ProcessType{ProcessTypeUser, ProcessTypePTY}

// FreezeMode selects which cgroups a sweep covers.
type FreezeMode string

const (
	// ModeLegacy freezes the static WorkloadProcessTypes list: envd's own user and pty
	// cgroups, and nothing the customer created.
	ModeLegacy FreezeMode = "legacy"
	// ModeHierarchy freezes the complement of envd's ancestor chain, so a cgroup the
	// customer made anywhere in the tree is covered.
	ModeHierarchy FreezeMode = "hierarchy"
)

// DefaultThawWatchdogWindow bounds how long a freeze may stand without a thaw before the
// backstop undoes it.
//
// Ten minutes looks absurd next to the orchestrator's resume-side envd wait, which
// defaults to ten SECONDS (envd-timeout-milliseconds). The mismatch is deliberate, because
// the watchdog is not racing that wait:
//
//   - If /init never arrives, the resume FAILS on that timeout and the sandbox is torn
//     down. There is no surviving frozen guest for a backstop to rescue, so firing inside
//     that window would only ever hit a sandbox that was about to be destroyed anyway.
//   - What the watchdog actually covers is a sandbox that is ALIVE with its workload still
//     frozen: /init arrived but its thaw failed, or the live-upgrade handover left
//     the workload frozen on purpose and the post-upgrade /init never came. Nothing else
//     notices those, because a resume does not restart envd -- it restores this same
//     process mid-execution, so there is no startup path to hook.
//
// So the window is sized against the longest LEGITIMATE gap between a freeze and its thaw,
// not against the common one. The longest measured is a cold resume taking ~292 s to reach
// /init (envd-init bound, dominated by demand faults); ten minutes leaves roughly 2x
// headroom over that.
//
// Erring long is the cheaper mistake here, but only just, and not for the obvious reason:
// firing early does not merely thaw something harmlessly, it releases the customer's tasks
// while envd is still faulting its own pages in, so they compete for the resume -- which is
// precisely what the freeze exists to prevent. Firing late leaves a live sandbox with a
// stopped workload. The first degrades the resume the freeze was meant to speed up; the
// second is a visible outage. Both are bad, which is why this wants a flag and a measured
// distribution rather than a constant, once there is fleet data on how long the gap
// actually is.
//
// It cannot fire during the pause itself: the guest's clock barely advances while the VM is
// paused, so the timer effectively starts counting at the resume.
const DefaultThawWatchdogWindow = 10 * time.Minute

// DefaultThawMaxCgroups bounds the thaw's recursive walk.
//
// Deliberately far above DefaultFreezeMaxCgroups, and sized from a different quantity.
// The freeze is breadth-bounded along a 2-deep chain, so its visited count is tens; the
// thaw recurses the entire tree, and a guest running a container runtime with a cgroup
// per pod and per container legitimately reaches hundreds to low thousands. One shared
// cap would be either uselessly loose for the freeze or dangerously tight for the thaw.
//
// The two also differ in what truncation MEANS, which is the sharper reason to split
// them. Truncating a freeze is a degradation: some cgroups keep running that we would
// rather have stopped, which is merely today's behaviour. Truncating a thaw strands a
// guest frozen, which is a hung sandbox. So this bound exists only as a runaway backstop
// and must never be tuned downward: a thaw cap below what some earlier freeze covered is
// precisely the drift the discover-don't-derive design exists to prevent.
const DefaultThawMaxCgroups = 8192

// DefaultFreezeMaxCgroups bounds how many cgroups one hierarchy sweep may visit.
//
// This is a safety guard, not a performance one: cgroupfs is in-kernel with no I/O, and
// the walk is breadth-bounded rather than recursive, so visited is the sum of child
// counts along envd's 2-deep chain -- tens, in any plausible guest. 512 sits roughly an
// order of magnitude above that. If it ever bites, that is itself the finding, which is
// why truncation is reported rather than swallowed.
//
// The guest is the threat model, so the bound exists for a hostile or pathological
// hierarchy rather than for a normal one.
const DefaultFreezeMaxCgroups = 512

// FreezeOptions is what a caller decides about a sweep. It is a struct rather than
// positional arguments because the two knobs beyond the budget -- which cgroups, and how
// many -- are set by the orchestrator per call and will grow.
type FreezeOptions struct {
	// MaxWait bounds the wait for the written cgroups to read back frozen.
	MaxWait time.Duration
	// Mode selects the static list or the hierarchy walk. Empty means ModeLegacy, so a
	// caller that has not been taught about modes keeps today's behaviour.
	Mode FreezeMode
	// MaxCgroups bounds a hierarchy sweep. Zero or negative means
	// DefaultFreezeMaxCgroups -- a caller cannot accidentally disable the guard.
	MaxCgroups int
}

func (o FreezeOptions) mode() FreezeMode {
	if o.Mode == ModeHierarchy {
		return ModeHierarchy
	}

	return ModeLegacy
}

func (o FreezeOptions) maxCgroups() int {
	if o.MaxCgroups <= 0 {
		return DefaultFreezeMaxCgroups
	}

	// Clamped at the THAW cap, not just floored at the default. A sweep allowed to cover more
	// cgroups than a thaw can reach is the drift DefaultThawMaxCgroups's own comment forbids --
	// "a thaw cap below what some earlier freeze covered" -- arrived at from the other side, by
	// raising this knob instead of lowering that constant. This one is an int flag, tunable
	// without a deploy, so the mistake is a plausible typo rather than a code change, and its
	// consequence is a workload frozen for the life of the sandbox that no backstop can rescue.
	return min(o.MaxCgroups, DefaultThawMaxCgroups)
}

// freezePollInterval is how often cgroup.events is re-read while waiting for a
// freeze to land. Short enough that a fast freeze is not rounded up to something
// visible on the pause path, long enough not to spin.
const freezePollInterval = 2 * time.Millisecond

// HandoverMaxWait bounds the live-upgrade wait, where the policy is strict: the swap is
// refused rather than execve over a running workload. It stays modest because the
// fallback is cheap (keep the current envd) and a slow handover holds the freeze lock,
// blocking the resume thaw.
//
// The pre-pause wait has no constant here on purpose: that budget belongs entirely to
// the orchestrator, which sends it as maxWaitMs. It owns the call timeout, so only it
// knows what wait is observable, and a default of ours would silently compete with it.
const HandoverMaxWait = 2 * time.Second

// WorkloadFreezer serializes freeze/unfreeze of the workload cgroups across
// every caller — the pre-pause /freeze, the pause-rollback /unfreeze, the resume
// thaw /init runs, and the live-upgrade handover — through a single lock,
// so their per-cgroup sweeps can never interleave and strand the workload
// frozen. Freeze and Unfreeze are best-effort: each attempts every cgroup even
// if one fails and returns the joined error.
//
// A single WorkloadFreezer instance must be shared by all of those callers for
// the serialization to hold; construct one and pass it to each.
type WorkloadFreezer struct {
	mgr  Manager
	lock *semaphore.Weighted

	// spawns keeps cgroup-placed process spawns out of the frozen window. It is raised
	// before a sweep writes cgroup.freeze and lowered once the thaw has cleared the static
	// cgroups spawns land in, so it is held for the whole window rather than for the sweep --
	// see spawnBarrier for what a spawn admitted into a frozen cgroup does to this process.
	// It is deliberately NOT f.lock: that lock serializes freeze against thaw and is released
	// when a Freeze finishes, or when a FreezeHold's caller lets go, while the workload
	// stays frozen until the thaw.
	spawns *spawnBarrier

	// thawMu guards thawedCh, the channel closed on the next Unfreeze. It lets
	// callers block until the workload is next thawed (see Thawed).
	thawMu   sync.Mutex
	thawedCh chan struct{}

	// procSelfCgroup is where this freezer looks up envd's own cgroup. A field rather
	// than a package variable so parallel tests can each point at their own fixture.
	procSelfCgroup string

	// sweepMu guards lastSweepMode: which sweep the most recent freeze performed. The
	// resume audit needs it, because "every non-allowlisted cgroup should be frozen" is
	// only true of a hierarchy sweep -- a legacy sweep touches user and pty and nothing
	// else, so scoring a legacy resume against the walk's expectation would report the
	// whole guest as having escaped.
	//
	// This is remembered state, which the THAW deliberately avoids -- but the trade is
	// different for an observer. A thaw that misremembers strands a guest; an audit that
	// has forgotten simply declines to report, which is the correct behaviour after a live
	// upgrade replaced the process image anyway.
	sweepMu       sync.Mutex
	lastSweepMode FreezeMode
	// sweepGen advances on every freeze and every thaw, so a change is detectable even when the
	// value it replaces is identical. See AuditFrozenSet for why equality on the mode is not
	// enough.
	sweepGen uint64
	// guestFrozen are cgroups the guest had frozen before our last sweep, as absolute
	// paths. The thaw skips them. Never explicitly cleared: the next sweep's scan
	// overwrites it, and a stale entry is harmless -- a path the guest has since
	// unfrozen reads 0, so the thaw would not touch it anyway.
	guestFrozen map[string]struct{}
	// freezeActive is true from a sweep until the thaw that clears it. It exists to stop a
	// SECOND freeze inside the same frozen window -- the live-upgrade handover takes the
	// freeze again while the pause's freeze is still in effect -- from re-running the scan,
	// which would see OUR cgroups still frozen, record them as the guest's, and leave the
	// thaw preserving them. A guest stranded frozen is the one outcome this design refuses.
	freezeActive bool

	// watchdogMu guards the fields below.
	watchdogMu     sync.Mutex
	watchdogWindow time.Duration
	watchdogOnFire func(ThawResult, error)
	watchdog       *time.Timer
	// watchdogGen identifies the currently-armed timer. time.Timer.Stop cannot recall a
	// callback that has already been dispatched, so a fired-but-not-yet-run watchdog from
	// one freeze could otherwise thaw the NEXT freeze's cgroups and, if that thaw came back
	// clean, disarm the timer belonging to it -- undoing a freeze and removing its backstop
	// in one go. The generation is what makes a superseded fire a no-op.
	watchdogGen uint64

	// freezeGen advances on every freeze attempt that takes the lock, including one the spawn
	// drain then refuses before freezing anything, and on nothing else. An /init handler
	// reads it when it enters and thaws only while it is unchanged (ThawIfGeneration), so a
	// handler that a freeze overtook -- delivered before a pause and completing after its
	// freeze -- does not undo that freeze and let the snapshot catch the workload running.
	// The check sits under lock for the reason watchdogGen's does. Seeded at random below
	// 2^52, so the values the refusal logs stay below 2^53, and decode exactly in a reader
	// that parses JSON numbers as doubles, for 2^52 freezes.
	freezeGen uint64

	// unobservedInits counts /init handlers whose thaw ran and finished with their request
	// context already done: no client was left to read the response that said so. Added
	// after the thaw, inside its critical section (ThawIfGeneration's afterThaw), and zeroed
	// by every freeze inside its own, so a count from the life a freeze ends is not carried
	// into the snapshot and reported by the first /init of the life resumed from it.
	unobservedInits atomic.Int64
}

// SetThawWatchdog arms a backstop: if a freeze is not followed by a thaw within window,
// thaw anyway and report it through onFire. Zero window disables it.
//
// The window is measured in GUEST monotonic time, which is what makes this work across a
// pause. The timer is armed by the freeze, and the guest's clock barely advances while
// the VM is paused, so it effectively starts counting when the sandbox resumes. Arming it
// at envd startup instead would never fire on the path that matters: a memory-snapshot
// resume restores this same process mid-execution, so there is no startup to hook.
//
// The thaw it performs is the discovering one, so it recovers from a frozen state
// whichever envd version and mode produced it.
func (f *WorkloadFreezer) SetThawWatchdog(window time.Duration, onFire func(ThawResult, error)) {
	f.watchdogMu.Lock()
	defer f.watchdogMu.Unlock()
	f.watchdogWindow = window
	f.watchdogOnFire = onFire
}

// armWatchdog (re)starts the backstop timer. Called after a freeze actually wrote to at
// least one cgroup — arming it for a freeze that froze nothing would fire a thaw with
// nothing to undo and report a false rescue.
func (f *WorkloadFreezer) armWatchdog(ctx context.Context) {
	f.watchdogMu.Lock()
	defer f.watchdogMu.Unlock()

	if f.watchdogWindow <= 0 {
		return
	}
	if f.watchdog != nil {
		f.watchdog.Stop()
	}
	f.watchdogGen++
	gen := f.watchdogGen
	onFire := f.watchdogOnFire
	// WithoutCancel rather than the caller's ctx or a bare Background: the freeze
	// request is long gone by the time this fires, so its cancellation must not reach
	// here -- but any trace or logging values on it are still the right ones to keep.
	thawCtx := context.WithoutCancel(ctx)
	f.watchdog = time.AfterFunc(f.watchdogWindow, func() {
		res, fired, err := f.thawForWatchdog(thawCtx, gen)
		if fired && onFire != nil {
			onFire(res, err)
		}
	})
}

// disarmWatchdog cancels the backstop. The thaw arrived, so there is nothing to rescue.
func (f *WorkloadFreezer) disarmWatchdog() {
	f.watchdogMu.Lock()
	defer f.watchdogMu.Unlock()

	if f.watchdog != nil {
		f.watchdog.Stop()
		f.watchdog = nil
	}
	// Bumped as well as stopped, so a callback already dispatched by this timer becomes a
	// no-op instead of thawing whatever is frozen when it finally runs.
	f.watchdogGen++
}

// thawForWatchdog runs the backstop thaw only if this timer is still the armed one. The
// staleness check happens with the freeze lock HELD, which is what makes it sound: a later
// freeze arms its own timer while holding that lock, so a check made before blocking on it
// could still be overtaken. fired reports whether the thaw actually ran.
func (f *WorkloadFreezer) thawForWatchdog(ctx context.Context, gen uint64) (ThawResult, bool, error) {
	if err := f.lock.Acquire(context.WithoutCancel(ctx), 1); err != nil {
		return ThawResult{}, false, err
	}

	f.watchdogMu.Lock()
	stale := gen != f.watchdogGen
	f.watchdogMu.Unlock()

	if stale {
		f.lock.Release(1)

		return ThawResult{}, false, nil
	}

	res, err := f.unfreezeLocked(DefaultThawMaxCgroups)

	// A rescue that did not fully succeed must arm the backstop again. Nothing else will:
	// armWatchdog is called only by the freeze paths, and a failed thaw does not disarm what it
	// failed to undo. Without this the backstop is single-shot, so one transient write error
	// during the rescue leaves a live sandbox with a stopped workload and no further retry.
	//
	// That argument covers err and Failed, which can succeed on a second attempt. It does NOT
	// cover Truncated: the rescue re-enters with the same bound and thawDiscovered restarts its
	// walk from the root over a sorted readdir, so every retry re-walks the identical prefix and
	// thaws nothing new. It stays in the condition anyway, and deliberately: a truncated thaw can
	// leave cgroups frozen -- whether it does depends on the tree's shape, since the sweep walks
	// the complement of envd's chain while this walks the whole tree breadth-first, so neither
	// prefix contains the other in general -- and a bounded readdir per window that keeps saying
	// so is worth more than silence. What it must not become is the only thing standing between a
	// raised freeze cap and a permanently frozen workload; maxCgroups is clamped for that.
	//
	// Armed with the freeze lock still HELD, which is the invariant the freeze paths follow:
	// FreezeHold arms while holding it, so re-arming after releasing would let a freeze that
	// acquired the lock in between arm its own timer only for this one to stop it and take over
	// the generation. The two timers have the same duration and run the same thaw, so the
	// observable difference is a deadline moved by microseconds -- but "the armed timer belongs
	// to the freeze that is in effect" is cheap to keep and confusing to reason about once lost.
	// Lock order is f.lock then watchdogMu everywhere, including the staleness check above.
	if err != nil || res.Truncated || res.Failed > 0 {
		f.armWatchdog(ctx)
	}
	f.lock.Release(1)

	return res, true, err
}

// NewWorkloadFreezer wraps a cgroup manager with the shared freeze lock.
func NewWorkloadFreezer(mgr Manager) *WorkloadFreezer {
	return &WorkloadFreezer{
		mgr:            mgr,
		lock:           semaphore.NewWeighted(1),
		spawns:         newSpawnBarrier(),
		procSelfCgroup: ProcSelfCgroup,
		freezeGen:      rand.Uint64N(maxFreezeGenSeed),
	}
}

// maxFreezeGenSeed bounds the random seed below 2^52, leaving 2^52 freezes before the
// generation reaches 2^53, above which a double no longer holds every integer exactly.
const maxFreezeGenSeed = 1 << 52

// FreezeGeneration returns the current freeze generation, read with the freeze lock held so
// it is ordered against every freeze: a freeze in progress finishes first, and so does a
// live-upgrade hold, which keeps the lock until its execve replaces the process (a waiting
// /init then never returns) or the handover fails and releases it. The wait is not bounded
// by ctx, so a caller whose context is already done still reads it.
func (f *WorkloadFreezer) FreezeGeneration(ctx context.Context) uint64 {
	_ = f.lock.Acquire(context.WithoutCancel(ctx), 1) // cannot fail: the context is never done
	defer f.lock.Release(1)

	return f.freezeGen
}

// ThawIfGeneration runs the thaw UnfreezeReporting runs, but only if want is still the
// current freeze generation, checked with the freeze lock held so no freeze can come between
// the check and the thaw. On a match, afterThaw (if non-nil) runs after the thaw, still
// inside the critical section, so whatever it records cannot be separated from the thaw by
// a freeze. current is the generation at the check, which a caller logs when matched is
// false and nothing ran. Like UnfreezeReporting, the wait for the lock is not bounded by ctx.
func (f *WorkloadFreezer) ThawIfGeneration(ctx context.Context, want uint64, afterThaw func()) (current uint64, matched bool, res ThawResult, err error) {
	_ = f.lock.Acquire(context.WithoutCancel(ctx), 1) // cannot fail: the context is never done
	defer f.lock.Release(1)

	current = f.freezeGen
	if want != current {
		return current, false, ThawResult{}, nil
	}

	res, err = f.unfreezeLocked(DefaultThawMaxCgroups)

	if afterThaw != nil {
		afterThaw()
	}

	return current, true, res, err
}

// AddUnobservedInit counts one /init handler that thawed with no client left (see
// unobservedInits). Call it from ThawIfGeneration's afterThaw, under the freeze lock.
func (f *WorkloadFreezer) AddUnobservedInit() { f.unobservedInits.Add(1) }

// TakeUnobservedInits returns the unobserved-/init count and resets it to zero.
func (f *WorkloadFreezer) TakeUnobservedInits() int64 { return f.unobservedInits.Swap(0) }

// Thawed returns a channel that is closed the next time the workload is
// unfrozen (a fresh one is installed after each Unfreeze). A re-adopted
// process's carried kill-timer selects on it so the timeout only starts once the
// workload actually runs — the new envd runs while the re-adopted workload is
// still frozen (until the post-upgrade /init), and the timeout must measure
// running time, not frozen time. Call it while the workload is frozen (before
// the thaw you want to observe) so the close isn't missed.
func (f *WorkloadFreezer) Thawed() <-chan struct{} {
	f.thawMu.Lock()
	defer f.thawMu.Unlock()
	if f.thawedCh == nil {
		f.thawedCh = make(chan struct{})
	}

	return f.thawedCh
}

// signalThawed closes the pending Thawed channel (if any) to wake waiters.
func (f *WorkloadFreezer) signalThawed() {
	f.thawMu.Lock()
	defer f.thawMu.Unlock()
	if f.thawedCh != nil {
		close(f.thawedCh)
		f.thawedCh = nil
	}
}

// Manager returns the underlying cgroup manager, for callers that also need it
// for non-freeze work such as process placement.
func (f *WorkloadFreezer) Manager() Manager { return f.mgr }

// FreezeResult reports what a freeze achieved, separating what we DID from what we
// OBSERVED.
//
// Frozen+NotFrozen+Unobservable accounts for every cgroup whose state we managed to read,
// and those are a subset of Requested. Failed does NOT reconcile against Requested,
// because it spans both phases: a write that never landed (so the cgroup was never
// Requested at all) and a state read that errored (so it was). The totals therefore are
//
//	Requested + <writes that failed> + <writes that vanished> == cgroups attempted
//	Frozen + NotFrozen + Unobservable + <polls that vanished> + <reads that failed> == Requested
//
// with Failed being the sum of the two failure terms. Splitting Failed in two would make
// both lines add up on their own; it is kept as one count because a caller acting on it
// does the same thing either way -- tolerate and carry on.
//
// Vanished spans the two phases exactly as Failed does, and is kept as one count for the
// same reason: a caller acting on it does the same thing either way. It is a term in the
// second line rather than a silent subtraction because NotFrozen is derived by
// difference, so a cgroup dropped from the poll set without being counted somewhere
// lands in NotFrozen -- the loudest outcome of the set, and the one that tells the caller
// a snapshot may have caught a live workload.
//
// Note that Frozen is a state read back from cgroup.events, not an acknowledgement of
// our write -- a cgroup the guest froze itself reads frozen too. We never establish
// that our write caused it, only that the workload is stopped, which is the property
// the snapshot actually depends on.
type FreezeResult struct {
	// Requested is the number of cgroups this call wrote cgroup.freeze to.
	Requested int
	// Frozen read back "frozen 1" from cgroup.events within the budget: their tasks
	// have stopped, so a snapshot taken now captures them stopped.
	Frozen int
	// NotFrozen still read "frozen 0" when the budget expired. Their tasks may still
	// be running, so a snapshot taken now can capture a live workload.
	NotFrozen int
	// ScanFailed counts cgroups the guest-freeze scan could not classify because they were
	// unreadable while it walked. A cgroup that had simply gone away is not counted here at
	// all: the scan tolerates a vanish outright, since a cgroup that no longer exists froze
	// nothing the thaw has to preserve. Deliberately a COUNT and not an error: an unclassified
	// cgroup is simply absent from the record, so the thaw clears it, which is the behaviour
	// that predates the record. Returning it as an error instead would be fatal in the one
	// place that treats any freeze error as fatal -- the live-upgrade handover refuses to swap
	// on err != nil -- and would abort upgrades over a race the sweep itself tolerates.
	ScanFailed int
	// PreFrozen is the number of cgroups the GUEST had already frozen when the sweep
	// ran. They are not written to (they are already stopped, which is all the pause
	// needs) and the resume thaw leaves them alone, so a guest's own `docker pause`
	// survives the snapshot.
	PreFrozen int
	// Failed is the number of cgroups that were still there and still errored: the write
	// was refused, or the state could not be read. Expected and tolerated, but a real
	// property of the tree about to be snapshotted. A cgroup that merely went away is
	// counted Vanished instead -- that is a race, and it resolves itself.
	Failed int
	// Vanished is the number of cgroups the hierarchy walk enumerated that the guest then
	// removed before the sweep could finish with them. Held apart from Failed because the
	// two call for opposite responses: Failed describes the tree we are about to snapshot,
	// while Vanished is a claim about the CGROUP only -- tasks migrated out of it before it
	// was removed can still be running, which is what the settle poll's vanish arm records
	// as accepted residual risk.
	//
	// So AllFrozen deliberately ignores Vanished, and NOT because that case cannot arise:
	// holding a pause for the guest's own churn would make every busy guest look like a
	// failed freeze, and the residual risk is judged small inside the freeze budget. Read
	// the vanish arm in awaitFrozen before changing that predicate.
	//
	// Scoped to the walk's own targets on purpose. A static cgroup of envd's own that
	// disappears is reported Failed instead, because that is not guest churn -- see
	// freezeTarget.tolerateVanish.
	//
	// Counted rather than dropped so that a settle-poll vanish cannot reach NotFrozen by
	// subtraction -- but like Failed it spans both phases and so does not reconcile
	// against Requested on its own; see the accounting note above.
	Vanished int
	// VanishedPaths samples the cgroups counted Vanished, bounded like the audit's offender
	// list. A vanish deliberately raises no error, so without this there is no record of
	// WHICH cgroup went away -- the counter says one did and nothing can name it, which is
	// the wrong trade for a guest tearing its workload down mid-pause.
	VanishedPaths []string
	// Unobservable is the number of cgroups whose freeze state cannot be read at all,
	// because this guest has no cgroup manager. Neither a success nor a failure: the
	// write was accepted and there is simply nothing to read back, so these are held
	// apart from NotFrozen rather than reported as a workload that refused to stop.
	Unobservable int
	// SweepDuration is the time spent issuing the writes -- our own cost, scaling
	// with the number of cgroups.
	SweepDuration time.Duration
	// Mode is the sweep that actually ran. Echoed back rather than inferred from the
	// flag, so the caller can tell that envd honoured what it asked for -- an envd too
	// old to know about modes reports legacy while the flag reads on.
	Mode FreezeMode
	// Visited is how many cgroups the walk examined, whether or not it froze them. It is
	// the input for sizing the bound, and it is meaningless in legacy mode.
	Visited int
	// Allowlisted is how many cgroups the walk skipped because the resume depends on
	// them. Reported because the allowlist is expected to grow: a distro that routes
	// journald differently changes this count, and that is worth seeing rather than
	// discovering through a wedged resume.
	Allowlisted int
	// Truncated is true when a walk stopped because it hit its bound rather than because
	// it ran out of tree -- either the freeze sweep, or the guest-freeze scan that runs
	// before it. Coverage is then incomplete, which is a degradation rather than a failure
	// -- but a silent one would be worse than today's behaviour.
	Truncated bool
	// WaitDuration is the time spent polling cgroup.events -- the guest's cost,
	// scaling with how deep in I/O its tasks were. Kept separate from SweepDuration
	// because the two have different causes and different fixes. It is outcome
	// neutral: the wait ends either because everything stopped or because the budget
	// ran out, and this records how long it took either way.
	WaitDuration time.Duration
}

// AllFrozen reports whether every cgroup this call wrote to was then read back frozen.
// Unobservable is not counted against it: a guest with no cgroup manager never had this
// guarantee, and withholding it would newly block the live-upgrade handover there rather
// than protect anything.
func (r FreezeResult) AllFrozen() bool { return r.NotFrozen == 0 && r.Failed == 0 }

// Freeze freezes the workload cgroups and waits for them to read back frozen,
// serialized against all other callers. The ctx bounds both the wait for the lock and
// the wait for the state: a caller that has gone away must not leave us polling, because
// the poll holds the freeze lock and would delay the rollback thaw behind it.
func (f *WorkloadFreezer) Freeze(ctx context.Context, opts FreezeOptions) (FreezeResult, error) {
	release, res, err := f.FreezeHold(ctx, opts)
	release()

	return res, err
}

// FreezeHold freezes the workload cgroups and KEEPS the lock held, returning a
// release func. Unlike Freeze (which releases as soon as the sweep is done), this
// lets a caller keep the freeze uninterruptible across a critical section — the
// live-upgrade handover — so a concurrent thaw (/init's, /unfreeze's, the watchdog's or
// the post-upgrade fallback's) blocks on the lock until release is called and cannot
// thaw the workload mid-handover, and a /init entering meanwhile waits there for its
// freeze-generation read. The frozen cgroup state persists after release; release only
// drops the lock and is idempotent. On a lock-acquire failure it
// returns a no-op release and the error.
func (f *WorkloadFreezer) FreezeHold(ctx context.Context, opts FreezeOptions) (release func(), res FreezeResult, err error) {
	if err := f.lock.Acquire(ctx, 1); err != nil {
		return func() {}, FreezeResult{}, err
	}

	// First, so a sweep that fails or panics part-way has still voided the thaw of every
	// /init handler that entered before it.
	f.freezeGen++
	// A count from before this freeze belongs to the life it ends.
	f.unobservedInits.Store(0)

	var once sync.Once
	release = func() { once.Do(func() { f.lock.Release(1) }) }

	// Before ANY cgroup is written: block new spawns and wait for the in-flight ones to
	// finish, so no clone3(CLONE_INTO_CGROUP) can be in flight against a cgroup this sweep
	// is about to freeze. The barrier stays up past this call -- the thaw lowers it -- because
	// a spawn admitted after the sweep lands in a cgroup that is already frozen.
	//
	// A drain that times out means a spawn is wedged, or not being scheduled at all; either
	// way it may still be headed for a cgroup this sweep would freeze, and freezing on top of
	// a wedge is what turns it into an unresumable snapshot. So the freeze is refused instead: the
	// caller sees an error and pauses an unfrozen workload, which is what it already does
	// whenever a freeze fails.
	if err := f.spawns.raise(ctx); err != nil {
		release()

		// Counted as Failed, on the reading that every workload cgroup is one this sweep
		// could not freeze. The count is not strictly a write that was refused -- no write
		// was attempted -- but it is what makes AllFrozen false, and every consumer of that
		// does the same thing here as for a refused write: report that the pause may capture
		// a running workload. Left as a bare count rather than a new outcome because a result
		// that reads clean while nothing was frozen is the one shape nobody would notice. Mode
		// echoes the sweep that was asked for; none ran.
		return func() {}, FreezeResult{Mode: opts.mode(), Failed: len(WorkloadProcessTypes)}, err
	}

	var errs []error
	sweepStart := time.Now()

	var pending []freezeTarget
	// A hierarchy sweep needs a manager that can address cgroups by path. When it
	// cannot, fall back to the static list rather than freezing nothing: the caller
	// asked for a freeze, and the mode it gets back says which one it actually got.
	pm, canWalk := f.mgr.(PathManager)

	// Rescan only when no freeze of ours is already in place. freezeInEffect, not freezeActive
	// alone: an envd that froze and then crashed has lost the flag but not the freeze, and
	// rescanning there records our own frozen cgroups as the guest's.
	rescan := canWalk && !f.freezeInEffect()

	if rescan {
		// Before writing anything, record what the guest had already frozen so the resume
		// thaw can leave it alone. Run in BOTH modes on purpose: the thaw walks the whole
		// hierarchy whichever sweep ran, so a record scoped to the hierarchy sweep would
		// leave the default configuration clearing the guest's own freezes.
		// Bounded by the THAW cap, not the freeze cap: this walk has the thaw's scope (the
		// whole tree, not the sweep's shallow complement), so the freeze bound would cut it
		// short long before the tree ran out and silently unclassify the remainder.
		guestFrozen, truncated, scanErrs := ScanGuestFrozen(pm, f.procSelfCgroup, DefaultThawMaxCgroups)
		// Counted, NOT returned as an error. The only consumer that treats a freeze error as
		// fatal is the live-upgrade handover, and a cgroup that vanished mid-scan is a race the
		// sweep itself tolerates -- failing the swap over it would abort upgrades for nothing.
		res.ScanFailed = len(scanErrs)
		if truncated {
			res.Truncated = true
		}

		f.sweepMu.Lock()
		f.guestFrozen = guestFrozen
		f.sweepMu.Unlock()
	}

	// Reported from the record rather than from this call's scan, so a second freeze in the
	// same window still says how many cgroups it is leaving to the guest.
	f.sweepMu.Lock()
	res.PreFrozen = len(f.guestFrozen)
	f.sweepMu.Unlock()

	if opts.mode() == ModeHierarchy && canWalk {
		res.Mode = ModeHierarchy
		// rescan is passed on so the walk can adopt a cgroup the guest froze between the scan
		// and the write. Only meaningful when the scan is this call's: without a fresh scan,
		// "frozen and not in the record" describes OUR own freeze just as well as the guest's.
		pending, errs = f.sweepHierarchy(pm, &res, opts.maxCgroups(), rescan)
	} else {
		res.Mode = ModeLegacy
		pending, errs = f.sweepLegacy(&res)
	}
	res.SweepDuration = time.Since(sweepStart)

	poll := f.awaitFrozen(ctx, pending, opts.MaxWait)
	res.Frozen = poll.frozen
	res.Unobservable = poll.unobservable
	res.Vanished += poll.vanished
	for _, p := range poll.vanishedPaths {
		res.VanishedPaths = samplePath(res.VanishedPaths, p)
	}
	res.NotFrozen = res.Requested - poll.frozen - poll.failed - poll.unobservable - poll.vanished
	res.Failed += poll.failed
	res.WaitDuration = time.Since(sweepStart) - res.SweepDuration
	errs = append(errs, poll.errs...)

	f.sweepMu.Lock()
	f.lastSweepMode = res.Mode
	f.sweepGen++
	// Requested, on the same condition and for the same reason the watchdog is armed below: a
	// sweep that wrote nothing has left nothing OF OURS frozen, and a later thaw that read this
	// as a freeze of ours would go discovering and clear cgroups this process never wrote.
	// ORed rather than assigned, because a sweep that writes nothing does not CLOSE a window an
	// earlier one opened -- those cgroups are still frozen, and the flag is what stops the next
	// sweep from rescanning and adopting them as the guest's.
	f.freezeActive = f.freezeActive || res.Requested > 0
	active := f.freezeActive
	f.sweepMu.Unlock()

	// A sweep that had no freeze write accepted, inside no window an earlier freeze opened,
	// opened no window, so holding spawns out of it would refuse process starts until the
	// next thaw for no reason -- and on a guest where every write fails there may never be
	// one. A sweep whose writes were accepted keeps the barrier up even if the freeze never
	// settled: the cgroup may still finish freezing. Read from freezeActive rather than from
	// Requested so a sweep inside a window an EARLIER freeze of this process opened keeps the
	// barrier up. A window left by a previous envd process that is not in freezeActive is
	// not seen here; the pre-spawn probe still refuses a spawn into a cgroup it froze.
	if !active {
		f.spawns.lower()
	}

	// The backstop exists to undo what this sweep did, and a sweep that wrote nothing has left
	// nothing for it to undo. The case that looks like a gap -- a retried freeze against a tree
	// this envd already froze and then lost its flag for -- is not one: FreezeAt is idempotent,
	// so the sweep re-requests every cgroup and Requested is non-zero again. A sweep whose whole
	// candidate set turns out to be the GUEST's writes nothing and needs nothing, because the
	// thaw preserves that set either way.
	if res.Requested > 0 {
		f.armWatchdog(ctx)
	}

	return release, res, errors.Join(errs...)
}

// freezeTarget is one cgroup a sweep wrote to, paired with how to read its settled
// state back. It exists so confirmation is one code path for both modes: the legacy
// sweep names cgroups by ProcessType and the walk names them by path, but what happens
// afterwards -- poll cgroup.events until it says frozen, or give up -- is identical.
type freezeTarget struct {
	// label identifies the cgroup in errors: a ProcessType, or an absolute path.
	label  string
	frozen func() (bool, error)
	// tolerateVanish says a removal of THIS cgroup between the write and the read-back is
	// the guest's own churn rather than a fault. True only for cgroups the hierarchy walk
	// discovered: they belong to the guest, which creates and retires them constantly.
	//
	// False for the static list, and that is the whole reason this field exists. Those
	// cgroups are envd's own, so one disappearing does not mean a transient unit exited --
	// it means something moved our tasks elsewhere and removed the cgroup underneath us
	// (rmdir needs it task-free, which a migration achieves while leaving those tasks
	// running). The freeze we wrote does not follow them, so they are running and the
	// snapshot would catch them running. That has to stay loud.
	//
	// The poll cannot infer this: both kinds arrive in one slice, and in legacy mode the
	// slice is ONLY the static list -- the default configuration, where a silently
	// tolerated vanish would be the single way this counter could ever move.
	tolerateVanish bool
}

// sweepLegacy freezes the static WorkloadProcessTypes list -- today's behaviour, and
// what a guest gets when the walk is off or unavailable.
func (f *WorkloadFreezer) sweepLegacy(res *FreezeResult) ([]freezeTarget, []error) {
	var errs []error
	pending := make([]freezeTarget, 0, len(WorkloadProcessTypes))

	for _, pt := range WorkloadProcessTypes {
		if e := f.mgr.Freeze(pt); e != nil {
			errs = append(errs, fmt.Errorf("freeze %s cgroup: %w", pt, e))
			res.Failed++

			continue
		}
		res.Requested++
		pending = append(pending, freezeTarget{
			label:  string(pt),
			frozen: func() (bool, error) { return f.mgr.Frozen(pt) },
		})
	}

	return pending, errs
}

// sweepHierarchy freezes the complement of envd's own ancestor chain: for each cgroup
// between the root and envd inclusive, every child that is neither on that chain nor on
// the liveness allowlist.
//
// It does NOT recurse below those children, and that is the point rather than an
// omission: cgroup freezing is hierarchical, so one write to a child stops its whole
// subtree. A customer's 5-level container tree is covered by a single write to whichever
// top-level cgroup roots it. The cost is therefore O(depth x branching) with envd's depth
// fixed at 2, not O(total cgroups) -- and it needs no PID enumeration, so there is no
// race between reading cgroup.procs and issuing the write, and no dependence on which
// tasks happen to exist at freeze time.
func (f *WorkloadFreezer) sweepHierarchy(pm PathManager, res *FreezeResult, maxCgroups int, adoptLateFreezes bool) ([]freezeTarget, []error) {
	f.sweepMu.Lock()
	guestFrozen := f.guestFrozen
	f.sweepMu.Unlock()

	root := pm.Root()
	self, err := SelfCgroupPath(f.procSelfCgroup, root)
	if err != nil {
		// Without our own path there is no complement to compute, and freezing a guess
		// could stop envd itself. Fall back rather than risk it.
		res.Mode = ModeLegacy

		pending, legacyErrs := f.sweepLegacy(res)

		return pending, append([]error{fmt.Errorf("locate envd cgroup, falling back to %s mode: %w", ModeLegacy, err)}, legacyErrs...)
	}

	// The static list is frozen FIRST and unconditionally, exactly as the legacy sweep does, so
	// it is a guaranteed subset of every sweep in either mode rather than something this walk
	// happens to reach. The thaw is already symmetric in this respect -- it always clears the
	// static list and only then discovers -- and two things depend on the freeze matching it:
	//
	//   - those cgroups hold what envd itself spawned, so missing them is a coverage regression
	//     against the behaviour that predates this walk;
	//   - their settled state is the thaw's evidence that a freeze of ours is in effect, so a
	//     sweep that skipped them leaves a restarted envd unable to tell that it ever froze.
	//
	// Leaving it to the walk made both contingent on the BOUND, and children are enumerated in
	// name order (readdir sorts), so a guest that creates enough cgroups sorting before "ptys"
	// and "user" pushes them past maxCgroups and out of the sweep. The bound exists for a
	// hostile hierarchy; it must not be a thing a hostile hierarchy can aim.
	pending, errs := f.sweepLegacy(res)

	// Descend into envd's chain AND into the ancestors of every allowlist entry. Without
	// the latter, freezing an intervening cgroup would stop an allowlisted one through the
	// hierarchy while still passing the exact-path check -- see DescendSet.
	descend := DescendSet(root, self)
	inside := make(map[string]struct{}, len(descend))
	for _, c := range descend {
		inside[c] = struct{}{}
	}
	// Already requested above. Skipped rather than re-written so one cgroup is counted once:
	// a second FreezeAt would succeed (the write is idempotent) and inflate Requested and the
	// confirmation set, which is the arithmetic every freeze metric is read against.
	for _, pt := range WorkloadProcessTypes {
		if p, ok := pm.PathOf(pt); ok {
			inside[p] = struct{}{}
		}
	}

	pending = slices.Grow(pending, len(descend)*4)
	// Cgroups the walk found already frozen, merged into the record rather than under a lock taken
	// per child. Deferred, not called at the end of the function: the bound returns early, and a
	// cgroup skipped as the guest's but never recorded is one the thaw then clears -- which is the
	// exact failure this adoption exists to prevent. A defer also covers any early return added
	// later, which a call at the bottom would silently not.
	var lateGuest []string
	defer func() { f.adoptLateGuestFreezes(res, lateGuest) }()
	for _, ancestor := range descend {
		children, e := pm.ChildrenOf(ancestor)
		switch {
		case vanished(e):
			// Nothing to descend into. Expected rather than exceptional: the descend set
			// includes the ancestors of every allowlist entry, and the allowlist is a
			// superset of what any one guest has -- a distro without rpcbind simply has no
			// such cgroup. Counting that as a failure would make the common case look
			// broken.
			continue
		case e != nil:
			// Unreadable for a reason other than being gone, which the branch above
			// handles. Counted, and the walk carries on with the other ancestors.
			errs = append(errs, fmt.Errorf("list children of %s: %w", ancestor, e))
			res.Failed++

			continue
		}

		for _, child := range children {
			if res.Visited >= maxCgroups {
				res.Truncated = true

				return pending, errs
			}
			res.Visited++

			// A cgroup we descend into is never frozen: it either holds envd, or holds
			// something the resume depends on.
			if _, ok := inside[child]; ok {
				continue
			}
			if allowlisted(root, child) {
				res.Allowlisted++

				continue
			}
			if _, guests := guestFrozen[child]; guests {
				// Already stopped, and stopped by the guest. Writing our own request here
				// would make it ours to clear on resume, which is exactly what must not
				// happen -- the thaw keys off who froze it.
				continue
			}
			// The scan is a snapshot and the guest keeps running until the pause completes, so a
			// cgroup can be frozen BY THE GUEST between that snapshot and this write. Reading its
			// request once more, immediately before writing, closes the half of that window this
			// code can see: whatever is already frozen here was frozen by someone else, since a
			// sweep never reads back its own writes -- the static list is handled separately and
			// the walk does not descend below what it freezes.
			//
			// Gated on a fresh scan. Without one, "frozen and not in the record" is equally the
			// description of OUR freeze from earlier in the same window, and adopting that would
			// hand the whole workload to the guest and strand it frozen on resume.
			if adoptLateFreezes {
				requested, e := pm.FreezeRequestedAt(child)
				switch {
				case vanished(e):
					// The probe already learned this path is gone, so the write below
					// would be a second open against it for the same answer. Skipping it
					// matters only because this runs inside the freeze budget with the
					// lock held; the classification that makes it possible is new here.
					recordVanished(res, child)

					continue
				case e == nil && requested:
					lateGuest = append(lateGuest, child)

					continue
				}
			}

			switch e := pm.FreezeAt(child); {
			case vanished(e):
				// Gone between the parent's readdir and this write. Not Requested: the
				// write never landed, so counting it would put a cgroup into the
				// confirmation set that the settle poll can never read back.
				recordVanished(res, child)

				continue
			case e != nil:
				errs = append(errs, fmt.Errorf("freeze %s: %w", child, e))
				res.Failed++

				continue
			}
			res.Requested++
			pending = append(pending, freezeTarget{
				label:          child,
				frozen:         func() (bool, error) { return pm.FrozenAt(child) },
				tolerateVanish: true,
			})
		}
	}

	return pending, errs
}

// adoptLateGuestFreezes folds cgroups the walk found already frozen into the guest-freeze record,
// so the resume thaw leaves them alone. Counted in PreFrozen for the same reason the scan's own
// finds are: it is the number of cgroups this sweep is deliberately leaving stopped.
func (f *WorkloadFreezer) adoptLateGuestFreezes(res *FreezeResult, paths []string) {
	if len(paths) == 0 {
		return
	}

	f.sweepMu.Lock()
	defer f.sweepMu.Unlock()

	if f.guestFrozen == nil {
		f.guestFrozen = make(map[string]struct{}, len(paths))
	}
	for _, p := range paths {
		if _, known := f.guestFrozen[p]; known {
			continue
		}
		f.guestFrozen[p] = struct{}{}
		res.PreFrozen++
	}
}

// sweepState reads the sweep mode and the sweep generation together, so a caller cannot observe
// one from before a change and the other from after it.
func (f *WorkloadFreezer) sweepState() (FreezeMode, uint64) {
	f.sweepMu.Lock()
	defer f.sweepMu.Unlock()

	return f.lastSweepMode, f.sweepGen
}

// freezeInEffect reports whether a freeze of OURS is currently in place, judged from the tree
// rather than from this process's memory. It is the answer to the one question both the thaw and
// the rescan turn on, and memory alone answers it wrongly in two directions:
//
//   - a process that never froze (the master flag off, or /freeze never reaching us) has no record,
//     and a thaw that discovers anyway CLEARS the guest's own freezes -- cgroups the pre-walk code
//     never touched. Failing open is safe against stranding, not against regressing what we never
//     owned;
//   - a process that froze and then crashed comes back with no record while its freeze is still in
//     place, and a rescan there adopts OUR cgroups as the guest's, which the thaw then preserves
//     for the life of the sandbox.
//
// The static list is the evidence because every sweep writes it FIRST, in both modes, before the
// walk and independently of its bound: if we froze, it is frozen. That ordering is what makes this
// sound -- while the hierarchy sweep merely reached those cgroups as ordinary children, a guest
// could push them past the bound (readdir sorts by name) and decide for us whether any evidence
// existed. A guest can still freeze `user` itself and make this read true; the cost of that is a
// discovering thaw, which is what it would have got before any of this existed.
func (f *WorkloadFreezer) freezeInEffect() bool {
	f.sweepMu.Lock()
	active := f.freezeActive
	f.sweepMu.Unlock()

	if active {
		return true
	}

	for _, pt := range WorkloadProcessTypes {
		frozen, err := f.mgr.Frozen(pt)
		if err == nil && frozen {
			return true
		}
	}

	return false
}

// GuestFrozenPaths returns the recorded guest-frozen cgroups as paths relative to the
// cgroup root, sorted. Relative because the value crosses a live upgrade in the handover
// blob, where an absolute path from another process image means nothing.
func (f *WorkloadFreezer) GuestFrozenPaths() []string {
	pm, ok := f.mgr.(PathManager)
	if !ok {
		return nil
	}
	root := pm.Root()

	f.sweepMu.Lock()
	defer f.sweepMu.Unlock()

	out := make([]string, 0, len(f.guestFrozen))
	for p := range f.guestFrozen {
		rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(p))
		if err != nil {
			continue
		}
		out = append(out, rel)
	}
	slices.Sort(out)

	return out
}

// SetGuestFrozenPaths restores the record from relative paths, for an envd taking over
// through a live upgrade. An empty list means "no record", and the thaw then clears
// everything it finds frozen -- the behaviour that predates the record.
func (f *WorkloadFreezer) SetGuestFrozenPaths(rel []string) {
	pm, ok := f.mgr.(PathManager)
	if !ok || len(rel) == 0 {
		return
	}
	root := pm.Root()

	set := make(map[string]struct{}, len(rel))
	for _, r := range rel {
		set[filepath.Join(root, r)] = struct{}{}
	}

	f.sweepMu.Lock()
	f.guestFrozen = set
	f.sweepMu.Unlock()
}

// ResumeFrozen tells the freezer it has inherited a workload that is already frozen BY US --
// the state a live-upgrade handover leaves behind, where the outgoing image froze the guest and
// the incoming one is expected to thaw it at /init. Three pieces of state do not survive an
// execve, and all of them matter while that thaw is still pending:
//
//   - the freeze counts as ACTIVE again, so a freeze taken before the thaw does not re-run the
//     guest scan and adopt our own frozen cgroups as the guest's. That mistake is permanent:
//     every later thaw would preserve them, and the guest stays frozen.
//   - the WATCHDOG is armed, because the timer belonged to the previous process image. Without
//     it an /init that never arrives leaves a frozen guest with no backstop -- precisely the
//     case the watchdog exists for, and the one a live upgrade would otherwise open.
//   - the SPAWN BARRIER is raised, because the semaphore belonged to the previous image too.
//     The workload is frozen and stays frozen until the post-upgrade /init. The pre-spawn
//     probe would refuse a start into it too, but only as far as its read is right and
//     lands before the clone; the barrier makes the refusal exact, as it is after a freeze
//     of this process. It cannot block: a freshly execve'd image has no spawns in flight
//     to drain.
//
// The sweep MODE is deliberately not restored: this process did not perform the freeze and
// cannot know what its predecessor covered, so the resume audit declines rather than guessing.
func (f *WorkloadFreezer) ResumeFrozen(ctx context.Context) error {
	f.sweepMu.Lock()
	f.freezeActive = true
	f.sweepMu.Unlock()

	// The watchdog first, so a barrier that fails to rise cannot leave the inherited freeze
	// without its backstop as well.
	f.armWatchdog(ctx)

	if err := f.spawns.raise(ctx); err != nil {
		return fmt.Errorf("raise the spawn barrier for an inherited freeze: %w", err)
	}

	return nil
}

// UnthawedSweepMode reports which sweep produced the freeze state that is STILL IN PLACE, or
// the empty mode if there is none. Cleared by the first thaw attempt, because after that the
// state a sweep produced no longer exists and anything scoring a tree against it is scoring a
// tree that has already been thawed -- every cgroup reads unfrozen, which an audit would call
// an escape.
//
// That is not hypothetical: /init is retried, and the in-place checkpoint path thaws through
// POST /unfreeze itself and then re-inits, so a post-thaw look at the tree is an ordinary
// occurrence. The resume audit is unaffected because it runs inside /init before its thaw.
func (f *WorkloadFreezer) UnthawedSweepMode() FreezeMode {
	f.sweepMu.Lock()
	defer f.sweepMu.Unlock()

	return f.lastSweepMode
}

// pollOutcome is what one settle poll observed. A struct rather than a growing list of
// positional returns: five counts already outran what a reader can keep straight at a call
// site, and the sampled paths only mean anything beside the count they explain.
type pollOutcome struct {
	frozen        int
	failed        int
	unobservable  int
	vanished      int
	vanishedPaths []string
	errs          []error
}

// recordVanished counts a cgroup that went away and samples its path. One function because
// two places can now discover a vanish -- the late-freeze probe and the write itself -- and
// a second copy of "what recording one means" is exactly the kind of duplication that drifts.
func recordVanished(res *FreezeResult, path string) {
	res.Vanished++
	res.VanishedPaths = samplePath(res.VanishedPaths, path)
}

// awaitFrozen polls the given cgroups until each reports frozen, the budget expires, or
// ctx is cancelled. All cgroups are polled together rather than one after another: the wait
// is then bounded by the slowest cgroup instead of by the sum, which matters because
// a single busy cgroup has been measured taking seconds to stop while the whole
// pre-pause freeze budget is of that order.
func (f *WorkloadFreezer) awaitFrozen(ctx context.Context, targets []freezeTarget, budget time.Duration) pollOutcome {
	var out pollOutcome
	if len(targets) == 0 || budget <= 0 {
		return out
	}

	remaining := slices.Clone(targets)
	deadline := time.Now().Add(budget)

	for {
		next := remaining[:0]
		for _, t := range remaining {
			isFrozen, err := t.frozen()
			switch {
			case errors.Is(err, ErrFrozenUnobservable):
				// Nothing to read and nothing to wait for. Drop it from the poll set so
				// the loop exits immediately instead of spinning out the whole budget.
				out.unobservable++
			case vanished(err) && t.tolerateVanish:
				// A cgroup the WALK found went away while we waited for it to settle -- a
				// transient unit that simply finished. Dropped from the poll set like the
				// unobservable case, because there is nothing left to read: keeping it
				// would spin the full budget re-reading a path that cannot come back.
				//
				// Counted, not discarded: NotFrozen is derived by subtraction, so an
				// uncounted drop would silently become the loudest outcome of the set.
				//
				// The cgroup is certainly gone; its former TASKS are not certainly stopped.
				// rmdir needs the cgroup task-free, which a migration by an unfrozen actor
				// achieves -- and moving into an unfrozen cgroup thaws them. So this arm can
				// cover a workload that is still running, exactly as the static-list case
				// described on tolerateVanish. Accepted as residual risk rather than fixed:
				// inside the freeze budget it is unlikely, and a cgroup created after the
				// parent's readdir was never in the sweep's coverage to begin with. The
				// sampled paths below are what makes it reconstructable after the fact.
				out.vanished++
				out.vanishedPaths = samplePath(out.vanishedPaths, t.label)
			case err != nil:
				out.failed++
				out.errs = append(out.errs, fmt.Errorf("read %s cgroup.events: %w", t.label, err))
			case isFrozen:
				out.frozen++
			default:
				next = append(next, t)
			}
		}
		remaining = next

		// Cancellation is checked alongside the deadline: the caller owns the budget, so
		// once it stops listening the rest of that budget is ours to give back rather
		// than spend holding the lock.
		if len(remaining) == 0 || !time.Now().Before(deadline) || ctx.Err() != nil {
			return out
		}

		sleep := min(freezePollInterval, time.Until(deadline))
		if sleep > 0 {
			select {
			case <-ctx.Done():
				return out
			case <-time.After(sleep):
			}
		}
	}
}

// ThawResult reports what a thaw undid. Reported separately from FreezeResult because
// the thaw is not the freeze's mirror image: it may legitimately thaw more.
type ThawResult struct {
	// Visited is how many cgroups the walk examined.
	Visited int
	// Thawed is how many were found frozen and written back to 0.
	Thawed int
	// Failed is how many could not be read or written. Tolerated individually; a cgroup
	// that vanished mid-walk is a race, not a bug.
	Failed int
	// Truncated is true if the bound stopped the walk. Unlike the freeze's truncation
	// this is an alarm: cgroups past the cap may still be frozen.
	Truncated bool
	// Preserved counts cgroups left frozen on purpose because the guest, not us, had
	// frozen them before the pause.
	Preserved int
	// Discovered is false when the manager cannot walk paths, so only the static list
	// was thawed. Distinguishes "nothing was frozen" from "we could not look".
	Discovered bool
}

// Unfreeze thaws whatever is frozen, serialized against all other callers. It detaches
// the lock wait from ctx cancellation so the thaw always lands even if the caller's
// request context is cancelled — a dropped unfreeze would strand the workload frozen.
// Thawing a non-frozen cgroup is a no-op, so it stays safe to call unconditionally on
// every upgrade and resume outcome.
//
// It DISCOVERS rather than derives, and that asymmetry with the freeze is the whole
// design. The binary that freezes is frequently not the binary that thaws: the
// live-upgrade handover deliberately leaves the workload frozen and lets the new image
// thaw it, and the mode can differ between pause and resume because the orchestrator
// evaluates the flag. A thaw that recomputed what should have been frozen would only be
// correct while both versions computed the same set, which is not a property we control:
//
//   - the allowlist grows, and a new entry is skipped by the walk, so a newer envd would
//     never thaw what an older one froze there;
//   - the mode flag can be on at pause and off at resume, and a legacy-mode thaw would
//     strand the entire customer hierarchy;
//   - the bound can be lowered, truncating the thaw earlier than the freeze.
//
// So the kernel is the source of truth: walk the tree and write 0 to every cgroup outside
// envd's chain whose own cgroup.freeze reads 1. That set is exactly what some freeze
// wrote, whichever version wrote it, in whatever mode, under whatever bound. The
// invariant weakens from "thaw set == freeze set" to "thaw set is a superset of freeze
// set", which holds across any pair of versions. The allowlist is ignored here for the
// same reason: thawing a cgroup that was never frozen is a no-op, so it protects nothing.
func (f *WorkloadFreezer) Unfreeze(ctx context.Context) error {
	_, err := f.UnfreezeReporting(ctx, DefaultThawMaxCgroups)

	return err
}

// UnfreezeReporting is Unfreeze with the counts, for callers that record them.
func (f *WorkloadFreezer) UnfreezeReporting(ctx context.Context, maxCgroups int) (ThawResult, error) {
	if err := f.lock.Acquire(context.WithoutCancel(ctx), 1); err != nil {
		return ThawResult{}, err
	}
	defer f.lock.Release(1)

	return f.unfreezeLocked(maxCgroups)
}

// unfreezeLocked is UnfreezeReporting's body, split out so the watchdog can make its
// staleness decision inside the same critical section as the thaw itself. Callers must hold
// f.lock.
func (f *WorkloadFreezer) unfreezeLocked(maxCgroups int) (ThawResult, error) {
	// Cleared up front, before any cgroup is touched: from here on the tree no longer holds
	// the state the sweep produced, so nothing may audit against it. Unconditional on the
	// outcome, unlike freezeActive below -- a partial thaw has still destroyed the pre-thaw
	// state, and auditing what is left would report the cgroups this thaw already cleared as
	// having escaped.
	f.sweepMu.Lock()
	f.lastSweepMode = ""
	f.sweepGen++
	f.sweepMu.Unlock()

	var res ThawResult
	var errs []error

	// Decided BEFORE anything is thawed, because the static thaw below destroys the evidence
	// this reads. freezeInEffect falls back to the settled state of the static list when the
	// in-memory flag is gone -- which is exactly the crash-restart case -- and by the time the
	// static cgroups have been cleared, that answer is always "no freeze of ours". Discovery
	// would then be skipped on the one path that most needs it, leaving the rest of the
	// hierarchy frozen in a process image with no watchdog to retry it.
	pm, canWalk := f.mgr.(PathManager)
	discover := canWalk && f.freezeInEffect()

	// Always thaw the static list: it is cheap, idempotent, and it is the only thing a
	// manager without path support can do.
	staticThawed := true
	for _, pt := range WorkloadProcessTypes {
		if err := f.mgr.Unfreeze(pt); err != nil {
			errs = append(errs, fmt.Errorf("unfreeze %s cgroup: %w", pt, err))
			res.Failed++
			// A static cgroup that no longer exists holds no frozen task and takes no
			// spawn, so it does not keep the barrier up. Counting it would refuse every
			// start into the cgroup that did thaw for as long as the guest leaves the
			// other one removed, since every later thaw fails the same way.
			if !vanished(err) {
				staticThawed = false
			}
		}
	}

	// The barrier guards spawns into the static cgroups only, so it follows THEIR thaw rather
	// than the whole tree's. Tying it to a clean whole-tree thaw instead would let one guest
	// cgroup that refuses its write -- or a walk truncated at the bound, which a watchdog
	// retry re-walks identically -- refuse every process start for the life of the sandbox,
	// while the cgroups those starts land in are running normally.
	//
	// Lowering it here is safe against the cases that remain. A later freeze of ours raises
	// it again under f.lock before writing anything. A cgroup still frozen for any other
	// reason -- the guest's own, or an ancestor this thaw could not clear -- is refused by the
	// pre-spawn probe, which reads the target's settled state and the request of every
	// ancestor.
	if staticThawed {
		f.spawns.lower()
	}

	// Discover ONLY when a freeze of ours is in effect. Walking the tree unconditionally makes
	// the thaw clear cgroups the guest froze itself on every path where we never swept -- the
	// master flag off, /freeze never reaching us, a transport failure -- none of which the
	// pre-walk code touched, because it only ever thawed the static list. The static thaw above
	// already covers everything a non-sweeping resume owes the guest.
	if discover {
		res.Discovered = true
		errs = append(errs, f.thawDiscovered(pm, &res, maxCgroups)...)
	}

	// Disarm only on a CLEAN thaw. Disarming up front would drop the backstop for exactly
	// the case it exists to cover: a thaw that truncated or failed leaves cgroups frozen,
	// and without the timer nothing ever retries them -- a hung guest. Racing the timer
	// into a second thaw is harmless by comparison, because it blocks on this lock and then
	// finds nothing frozen.
	joined := errors.Join(errs...)
	if joined == nil && !res.Truncated && res.Failed == 0 {
		f.disarmWatchdog()

		// The frozen window is over, so the next freeze scans afresh. Kept set on a dirty
		// thaw on purpose: cgroups are still frozen, the watchdog will retry, and that retry
		// needs the same record this thaw was using.
		f.sweepMu.Lock()
		f.freezeActive = false
		f.sweepMu.Unlock()
	}

	// Wake anyone waiting for the workload to run again (carried kill-timers).
	f.signalThawed()

	return res, joined
}

// thawDiscovered walks the whole tree and thaws every cgroup outside envd's chain that
// reports its own cgroup.freeze as 1.
//
// Unlike the freeze this recurses: a frozen cgroup can be anywhere, including below one
// that is not frozen itself, and the point is to find them all rather than to be cheap.
// It reads cgroup.freeze rather than cgroup.events because only the former distinguishes
// "something froze this cgroup" from "an ancestor is frozen, so this one reads frozen
// too" — writing 0 to the latter would not help, since the ancestor still holds it.
func (f *WorkloadFreezer) thawDiscovered(pm PathManager, res *ThawResult, maxCgroups int) []error {
	var errs []error

	f.sweepMu.Lock()
	guestFrozen := f.guestFrozen
	f.sweepMu.Unlock()

	root := pm.Root()
	self, err := SelfCgroupPath(f.procSelfCgroup, root)
	if err != nil {
		// Without our own path we cannot exclude ourselves, and thawing envd's own
		// cgroup is harmless (it is not frozen) — but neither can we trust the walk, so
		// report and leave the static thaw above as the outcome.
		return append(errs, fmt.Errorf("locate envd cgroup for the thaw: %w", err))
	}

	onChain := make(map[string]struct{})
	for _, c := range AncestorChain(root, self) {
		onChain[c] = struct{}{}
	}

	queue := []string{root}
	for len(queue) > 0 {
		if res.Visited >= maxCgroups {
			res.Truncated = true

			return append(errs, fmt.Errorf("thaw walk truncated at %d cgroups: some may still be frozen", maxCgroups))
		}

		cur := queue[0]
		queue = queue[1:]
		res.Visited++

		_, skip := onChain[cur]
		if _, guests := guestFrozen[cur]; guests && !skip {
			// The guest froze this before we ever swept. Clearing it would restart
			// processes it deliberately suspended, so leave it and say so.
			res.Preserved++
			skip = true
		}
		if !skip {
			frozen, e := pm.FreezeRequestedAt(cur)
			switch {
			case vanished(e):
				// Vanished between readdir and read: a race, not a bug, and the scan and the
				// audit both already tolerate it. Counting it Failed made routine container
				// churn produce a "dirty" thaw, which answered 500 on a thaw that worked, left
				// the watchdog armed to fire mid-operation, and kept freezeActive set so the
				// next pause skipped its rescan.
			case e != nil:
				errs = append(errs, fmt.Errorf("read freeze state of %s: %w", cur, e))
				res.Failed++
			case frozen:
				switch e := pm.UnfreezeAt(cur); {
				case vanished(e):
					// The same race one step later: gone between the read and the write.
					// Nothing left to thaw, so nothing failed.
				case e != nil:
					errs = append(errs, fmt.Errorf("thaw %s: %w", cur, e))
					res.Failed++
				default:
					res.Thawed++
				}
			}
		}

		children, e := pm.ChildrenOf(cur)
		switch {
		case vanished(e):
			// The same race, on the third of the three syscalls this loop makes against a
			// cgroup: it was listed by its parent and removed before it could be read.
			// A cgroup that no longer exists has no children and nothing left to thaw.
			continue
		case e != nil:
			errs = append(errs, fmt.Errorf("list children of %s: %w", cur, e))
			res.Failed++

			continue
		}
		queue = append(queue, children...)
	}

	return errs
}
