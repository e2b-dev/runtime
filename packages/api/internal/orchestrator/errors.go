package orchestrator

import "errors"

var (
	ErrSandboxNotFound        = errors.New("sandbox not found")
	ErrAccessForbidden        = errors.New("access forbidden")
	ErrSandboxOperationFailed = errors.New("sandbox operation failed")
)
