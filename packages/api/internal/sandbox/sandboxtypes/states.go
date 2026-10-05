package sandboxtypes

import (
	"time"
)

// StaleCutoff is how long a team entry must be expired before it can be pruned
// This prevents races where TTL is extended, but the sandbox would still be removed
const StaleCutoff = 10 * time.Minute

// TransitionEffect describes what happens to the sandbox when a state action completes.
type TransitionEffect int

const (
	// TransitionExpires marks the sandbox as expired (terminal removal).
	TransitionExpires TransitionEffect = iota
	// TransitionTransient is temporary — the sandbox is restored to its
	// previous state once the transition completes successfully.
	TransitionTransient
)

type StateAction struct {
	// Name is the human-readable identifier for this action (e.g. "pause", "kill").
	Name string
	// TargetState is the sandbox state this action transitions to.
	TargetState State
	// Effect describes whether the transition is terminal or transient.
	Effect TransitionEffect
}

var (
	StateActionPause = StateAction{
		Name:        "pause",
		TargetState: StatePausing,
		Effect:      TransitionExpires,
	}
	StateActionKill = StateAction{
		Name:        "kill",
		TargetState: StateKilling,
		Effect:      TransitionExpires,
	}
	StateActionSnapshot = StateAction{
		Name:        "snapshot",
		TargetState: StateSnapshotting,
		Effect:      TransitionTransient,
	}
)

type KillReason string

const (
	KillReasonUnknown             KillReason = "unknown"
	KillReasonRequest             KillReason = "request"
	KillReasonTimeout             KillReason = "timeout"
	KillReasonAdmin               KillReason = "admin"
	KillReasonOrphaned            KillReason = "orphaned"
	KillReasonBaseTemplateMissing KillReason = "base_template_missing"
)

// String returns the reason as a string, normalizing the empty value to
// "unknown". Keeps API-side log/metric fields consistent with the
// orchestrator-side normalization in pkg/server/sandboxes.go.
func (r KillReason) String() string {
	if r == "" {
		return string(KillReasonUnknown)
	}

	return string(r)
}

// RemoveOpts bundles the parameters that control sandbox removal.
type RemoveOpts struct {
	Action   StateAction
	Eviction bool
	Reason   KillReason

	// FilesystemOnly requests a filesystem-only snapshot on pause (no memory
	// snapshot); resuming it cold-boots from disk. Ignored unless Action is a
	// pause. Default false = full memory snapshot.
	FilesystemOnly bool

	// ExpectExecutionID pins the removal to one incarnation of the sandbox.
	// When set, StartRemoving fails with ErrExecutionMismatch if the stored
	// record carries a different execution ID.
	//
	// For callers that decide from a snapshot taken earlier — a background
	// scan, a queued batch — the sandbox ID alone is not a stable identity:
	// the record can be removed and the ID reused by a resume before the
	// removal runs, and the transition would then land on a live sandbox that
	// was never in scope. Empty means "remove whatever is stored", which is
	// correct for callers acting on a fresh read or on user intent.
	ExpectExecutionID string
}

var AllowedTransitions = map[State]map[State]bool{
	StateRunning: {StatePausing: true, StateKilling: true, StateSnapshotting: true},
	// Pausing→Running is legal only as the restore of a retryably refused
	// pause (the node declined the snapshot; the sandbox never stopped).
	StatePausing:      {StateKilling: true, StateRunning: true},
	StateSnapshotting: {StateRunning: true, StateKilling: true, StatePausing: true},
}

const (
	SandboxTimeoutDefault = time.Second * 15
	// Timeout applied by the v2 create and connect endpoints when the request omits one
	SandboxTimeoutDefaultV2 = time.Minute * 5
	// Should we auto pause the instance by default instead of killing it
	AutoPauseDefault = false
)

type State string

const (
	StateRunning      State = "running"
	StatePausing      State = "pausing"
	StateKilling      State = "killing"
	StateSnapshotting State = "snapshotting"
)
