//go:build linux

package builderrors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
)

func TestWrapContextAsUserError_NilError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	assert.NoError(t, WrapContextAsUserError(ctx, nil))
}

func TestWrapContextAsUserError_AlreadyUserError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	userErr := phases.NewPhaseBuildError(phases.PhaseMeta{}, errors.New("user mistake"))
	got := WrapContextAsUserError(ctx, userErr)
	assert.ErrorIs(t, got, userErr)
}

func TestWrapContextAsUserError_InternalTimeout_NotMisclassified(t *testing.T) {
	t.Parallel()

	// Simulate: build context is still active, but error contains context.Canceled
	// from an internal child-context timeout (e.g., WaitForEnvd).
	buildCtx := t.Context() // not canceled

	// This is what doRequestWithInfiniteRetries returns when a child context is canceled
	internalErr := fmt.Errorf("%w with cause: %w", context.Canceled, errors.New("syncing took too long"))

	got := WrapContextAsUserError(buildCtx, internalErr)

	// Should NOT be classified as user error — the build context is still alive
	assert.False(t, IsUserError(got), "internal timeout should not be classified as user error, got: %v", got)
	// Should preserve the original error
	assert.ErrorIs(t, got, internalErr)
}

func TestWrapContextAsUserError_UserCancellation(t *testing.T) {
	t.Parallel()

	// Simulate: user cancels the build → build context is canceled
	buildCtx, cancel := context.WithCancel(t.Context())
	cancel() // user canceled

	// Error must contain context.Canceled (as it would in real code via errors.Join or wrapping)
	someErr := fmt.Errorf("some operation failed: %w", buildCtx.Err())
	got := WrapContextAsUserError(buildCtx, someErr)

	assert.True(t, IsUserError(got), "expected user error when build context is canceled, got: %v", got)
	assert.ErrorIs(t, got, ErrCanceled)
}

func TestWrapContextAsUserError_BuildContextCanceled_ErrorUnrelated(t *testing.T) {
	t.Parallel()

	// Build context is canceled, but the error is unrelated (e.g., disk error).
	// Should NOT be wrapped as user error.
	buildCtx, cancel := context.WithCancel(t.Context())
	cancel()

	diskErr := errors.New("disk I/O error")
	got := WrapContextAsUserError(buildCtx, diskErr)

	assert.False(t, IsUserError(got), "unrelated error should not be classified as user error when build context is canceled, got: %v", got)
	assert.ErrorIs(t, got, diskErr)
}

func TestWrapContextAsUserError_BuildDeadlineExceeded(t *testing.T) {
	t.Parallel()

	// Simulate: build context deadline exceeded
	buildCtx, cancel := context.WithTimeout(t.Context(), 0)
	defer cancel()
	<-buildCtx.Done() // ensure it's expired

	// Error must contain context.DeadlineExceeded (as it would in real code)
	someErr := fmt.Errorf("some operation failed: %w", buildCtx.Err())
	got := WrapContextAsUserError(buildCtx, someErr)

	assert.True(t, IsUserError(got), "expected user error when build context timed out, got: %v", got)
	assert.ErrorIs(t, got, ErrTimeout)
}

func TestWrapContextAsUserError_InternalCanceledError_BuildContextAlive(t *testing.T) {
	t.Parallel()

	// The key regression test: error chain contains context.Canceled from a child
	// context, but the build context is NOT canceled. Must NOT be treated as user error.
	buildCtx := t.Context()

	// Simulate WaitForEnvd timeout chain:
	// child context canceled → doRequestWithInfiniteRetries wraps ctx.Err()
	childCtx, childCancel := context.WithCancelCause(buildCtx)
	childCancel(errors.New("syncing took too long"))

	err := fmt.Errorf("failed to init new envd: %w",
		fmt.Errorf("%w with cause: %w", childCtx.Err(), context.Cause(childCtx)))

	got := WrapContextAsUserError(buildCtx, err)

	assert.False(t, IsUserError(got), "internal child-context cancellation must not be classified as user error, got: %v", got)
	// The original error should be preserved
	assert.ErrorIs(t, got, context.Canceled)
}
