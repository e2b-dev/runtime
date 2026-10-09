package handler

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// eagainStartError mirrors a real spawn failure: os/exec returns
// *fs.PathError{Op: "fork/exec"} carrying the errno, wrapped with %w by Handler.Start.
func eagainStartError() error {
	return fmt.Errorf("error starting process '%s': %w", "sleep 1", &fs.PathError{Op: "fork/exec", Path: "/bin/sh", Err: syscall.EAGAIN})
}

func TestStartErrorCode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		err  error
		name string
		want connect.Code
	}{
		{
			// Both spawn sites wrap it: Handler.Start and the PTY start inside New.
			name: "a frozen workload is retryable",
			err:  fmt.Errorf("error starting process '%s': %w", "sleep 1", cgroups.ErrWorkloadFrozen),
			want: connect.CodeUnavailable,
		},
		{
			name: "fork/exec EAGAIN is resource exhaustion",
			err:  eagainStartError(),
			want: connect.CodeResourceExhausted,
		},
		{
			name: "pty spawn EAGAIN is resource exhaustion",
			err:  fmt.Errorf("error starting pty with command '%s' in dir '%s' with '%d' cols and '%d' rows: %w", "sleep 1", "/home/user", 80, 24, &fs.PathError{Op: "fork/exec", Path: "/bin/sh", Err: syscall.EAGAIN}),
			want: connect.CodeResourceExhausted,
		},
		{
			name: "bare EAGAIN is resource exhaustion",
			err:  syscall.EAGAIN,
			want: connect.CodeResourceExhausted,
		},
		{
			name: "ENOMEM is resource exhaustion",
			err:  fmt.Errorf("wrapped: %w", syscall.ENOMEM),
			want: connect.CodeResourceExhausted,
		},
		{
			name: "EMFILE is resource exhaustion",
			err:  fmt.Errorf("wrapped: %w", syscall.EMFILE),
			want: connect.CodeResourceExhausted,
		},
		{
			name: "ENFILE is resource exhaustion",
			err:  fmt.Errorf("wrapped: %w", syscall.ENFILE),
			want: connect.CodeResourceExhausted,
		},
		{
			name: "out of ptys is resource exhaustion",
			err:  fmt.Errorf("error starting pty: %w", &fs.PathError{Op: "open", Path: "/dev/ptmx", Err: syscall.ENOSPC}),
			want: connect.CodeResourceExhausted,
		},
		{
			name: "expired request timeout is a deadline",
			err:  fmt.Errorf("error starting process '%s': %w", "sleep 1", context.DeadlineExceeded),
			want: connect.CodeDeadlineExceeded,
		},
		{
			name: "cancelled request is a cancellation",
			err:  fmt.Errorf("error starting process '%s': %w", "sleep 1", context.Canceled),
			want: connect.CodeCanceled,
		},
		{
			name: "missing binary stays invalid argument",
			err:  fmt.Errorf("error starting process '%s': %w", "nope", &exec.Error{Name: "nope", Err: exec.ErrNotFound}),
			want: connect.CodeInvalidArgument,
		},
		{
			name: "missing cwd stays invalid argument",
			err:  fmt.Errorf("error starting process '%s': %w", "sleep 1", &fs.PathError{Op: "chdir", Path: "/nope", Err: syscall.ENOENT}),
			want: connect.CodeInvalidArgument,
		},
		{
			name: "permission denied stays invalid argument",
			err:  fmt.Errorf("error starting process '%s': %w", "sleep 1", &fs.PathError{Op: "fork/exec", Path: "/bin/sh", Err: syscall.EACCES}),
			want: connect.CodeInvalidArgument,
		},
		{
			name: "opaque error stays invalid argument",
			err:  errors.New("something went wrong"),
			want: connect.CodeInvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := StartErrorCode(tc.err)
			assert.Equalf(t, tc.want, got, "got %s, want %s", got, tc.want)
		})
	}
}

// Asserts on the code carried by *connect.Error — what the client observes on the wire.
func TestStartErrorClientObservedCode(t *testing.T) {
	t.Parallel()

	exhausted := eagainStartError()
	assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(connect.NewError(StartErrorCode(exhausted), exhausted)))

	badRequest := fmt.Errorf("error starting process '%s': %w", "nope", &exec.Error{Name: "nope", Err: exec.ErrNotFound})
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(connect.NewError(StartErrorCode(badRequest), badRequest)))
}

// Handler.Start must wrap the spawn failure with %w — %v would silently turn
