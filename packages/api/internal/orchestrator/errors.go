package orchestrator

import "errors"

var (
	ErrSandboxNotFound        = errors.New("sandbox not found")
	ErrAccessForbidden        = errors.New("access forbidden")
	ErrSandboxOperationFailed = errors.New("sandbox operation failed")
	// ErrExecutionChanged: a pause pinned with RemoveOpts.ExpectExecutionID
	// found a different execution stored; nothing was changed.
	ErrExecutionChanged = errors.New("sandbox execution changed")
	// ErrSandboxLifecycleHeld: an operation outside the API's ordinary
	// lifecycle holds the sandbox. Nothing was changed.
	ErrSandboxLifecycleHeld = errors.New("sandbox lifecycle is held by another operation")
)
