package sandbox

import "github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"

// This file re-exports the sandbox domain types defined in the sandboxtypes
// leaf package. The types live in sandboxtypes (rather than directly in
// package sandbox) so storage backends like sandbox/storage/memory and
// sandbox/storage/redis can implement the Storage interface without creating
// an import cycle back into package sandbox. External callers can continue to
// reference these symbols through sandbox.Sandbox, sandbox.State, etc.

// Domain types.
type (
	Sandbox     = sandboxtypes.Sandbox
	NodeSandbox = sandboxtypes.NodeSandbox
	State       = sandboxtypes.State
	StateAction = sandboxtypes.StateAction
	KillReason  = sandboxtypes.KillReason
	RemoveOpts  = sandboxtypes.RemoveOpts

	TransitionEffect = sandboxtypes.TransitionEffect
	StateTransition  = sandboxtypes.StateTransition

	InvalidStateTransitionError = sandboxtypes.InvalidStateTransitionError
	LimitExceededError          = sandboxtypes.LimitExceededError
	NotRunningError             = sandboxtypes.NotRunningError
	PauseQueueExhaustedError    = sandboxtypes.PauseQueueExhaustedError
)

// State constants.
const (
	StateRunning      = sandboxtypes.StateRunning
	StatePausing      = sandboxtypes.StatePausing
	StateKilling      = sandboxtypes.StateKilling
	StateSnapshotting = sandboxtypes.StateSnapshotting

	TransitionExpires   = sandboxtypes.TransitionExpires
	TransitionTransient = sandboxtypes.TransitionTransient

	StaleCutoff             = sandboxtypes.StaleCutoff
	SandboxTimeoutDefault   = sandboxtypes.SandboxTimeoutDefault
	SandboxTimeoutDefaultV2 = sandboxtypes.SandboxTimeoutDefaultV2
	AutoPauseDefault        = sandboxtypes.AutoPauseDefault

	KillReasonUnknown             = sandboxtypes.KillReasonUnknown
	KillReasonRequest             = sandboxtypes.KillReasonRequest
	KillReasonTimeout             = sandboxtypes.KillReasonTimeout
	KillReasonAdmin               = sandboxtypes.KillReasonAdmin
	KillReasonOrphaned            = sandboxtypes.KillReasonOrphaned
	KillReasonBaseTemplateMissing = sandboxtypes.KillReasonBaseTemplateMissing
)

// Errors and pre-defined state actions / transition tables.
var (
	ErrAlreadyExists      = sandboxtypes.ErrAlreadyExists
	ErrNotFound           = sandboxtypes.ErrNotFound
	ErrEvictionInProgress = sandboxtypes.ErrEvictionInProgress
	ErrEvictionNotNeeded  = sandboxtypes.ErrEvictionNotNeeded
	ErrExecutionMismatch  = sandboxtypes.ErrExecutionMismatch
	ErrRestoreConflict    = sandboxtypes.ErrRestoreConflict
	ErrTransitionRestored = sandboxtypes.ErrTransitionRestored
	ErrDraining           = sandboxtypes.ErrDraining

	AllowedTransitions = sandboxtypes.AllowedTransitions

	StateActionPause    = sandboxtypes.StateActionPause
	StateActionKill     = sandboxtypes.StateActionKill
	StateActionSnapshot = sandboxtypes.StateActionSnapshot
)

// NewSandbox constructs a Sandbox. Re-exported from sandboxtypes.
var NewSandbox = sandboxtypes.NewSandbox
