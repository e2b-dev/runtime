package handler

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputErrorCode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		err  error
		name string
		want connect.Code
	}{
		{
			name: "stdin disabled or closed is a precondition failure",
			err:  ErrStdinUnavailable,
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "stdin on a pty process is a precondition failure",
			err:  ErrStdinOnPty,
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "pty write with no tty is a precondition failure",
			err:  ErrTtyUnavailable,
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "close stdin on a pty process is a precondition failure",
			err:  ErrCloseStdinOnPty,
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "wrapped precondition sentinel is still a precondition failure",
			err:  fmt.Errorf("error writing to stdin of process '%d': %w", 42, ErrStdinUnavailable),
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "EPIPE after exit is a precondition failure",
			err:  fmt.Errorf("error writing to stdin of process '%d': %w", 42, syscall.EPIPE),
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "file already closed after exit is a precondition failure",
			err:  fmt.Errorf("error writing to stdin of process '%d': %w", 42, os.ErrClosed),
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "closed pipe after exit is a precondition failure",
			err:  fmt.Errorf("wrapped: %w", io.ErrClosedPipe),
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "fs closed after exit is a precondition failure",
			err:  &fs.PathError{Op: "write", Path: "|1", Err: fs.ErrClosed},
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "unexpected I/O failure stays internal",
			err:  fmt.Errorf("error writing to stdin of process '%d': %w", 42, syscall.EIO),
			want: connect.CodeInternal,
		},
		{
			name: "opaque error stays internal",
			err:  errors.New("something went wrong"),
			want: connect.CodeInternal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := InputErrorCode(tc.err)
			assert.Equalf(t, tc.want, got, "got %s, want %s", got, tc.want)
		})
	}
}

// Asserts on the code carried by *connect.Error — what the client observes on the wire.
func TestInputErrorClientObservedCode(t *testing.T) {
	t.Parallel()

	precond := fmt.Errorf("error writing to stdin: %w", ErrStdinUnavailable)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(connect.NewError(InputErrorCode(precond), precond)))

	internal := fmt.Errorf("error writing to stdin: %w", syscall.EIO)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(connect.NewError(InputErrorCode(internal), internal)))
}

// WriteStdin returns the typed sentinels for the two synchronous precondition
// states so the service layer can map them without string matching.
func TestWriteStdinPreconditionSentinels(t *testing.T) {
	t.Parallel()

	t.Run("stdin disabled", func(t *testing.T) {
		t.Parallel()
		h := &Handler{} // stdin == nil, no tty
		err := h.WriteStdin([]byte("x"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrStdinUnavailable)
		assert.Equal(t, connect.CodeFailedPrecondition, InputErrorCode(err))
	})

	t.Run("pty process rejects stdin", func(t *testing.T) {
		t.Parallel()
		// A non-nil tty routes stdin writes to ErrStdinOnPty before any pipe use.
		h := &Handler{tty: os.NewFile(0, "fake-tty")}
		err := h.WriteStdin([]byte("x"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrStdinOnPty)
		assert.Equal(t, connect.CodeFailedPrecondition, InputErrorCode(err))
	})
}

// Reproduces issue #3622: a short-lived non-PTY process whose stdin read end is
// gone after exit. WriteStdin must surface an expected-lifecycle error that
// maps to CodeFailedPrecondition, not CodeInternal.
func TestWriteStdinAfterExitIsPrecondition(t *testing.T) {
	t.Parallel()

	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	h := &Handler{stdin: stdin}

	_ = cmd.Wait()
	time.Sleep(50 * time.Millisecond)

	var writeErr error
	for i := 0; i < 100; i++ {
		if writeErr = h.WriteStdin([]byte("hello\n")); writeErr != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Error(t, writeErr, "expected WriteStdin to fail after process exit")

	code := InputErrorCode(writeErr)
	assert.Equalf(t, connect.CodeFailedPrecondition, code,
		"post-exit stdin write should be CodeFailedPrecondition, got %s (raw: %v)", code, writeErr)
}
