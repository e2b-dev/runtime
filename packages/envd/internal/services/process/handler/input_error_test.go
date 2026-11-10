package handler

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
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

// WriteStdin returns the typed sentinels for the two synchronous precondition
// states so the service layer can map them without string matching.
func TestWriteStdinPreconditionSentinels(t *testing.T) {
	t.Parallel()

	t.Run("stdin disabled", func(t *testing.T) {
		t.Parallel()
		h := &Handler{}
		err := h.WriteStdin([]byte("x"))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrStdinUnavailable)
		assert.Equal(t, connect.CodeFailedPrecondition, InputErrorCode(err))
	})

	t.Run("pty process rejects stdin", func(t *testing.T) {
		t.Parallel()
		// A non-nil tty routes stdin writes to ErrStdinOnPty before any pipe use.
		// A zero os.File owns no descriptor, so no finalizer can close a real fd.
		h := &Handler{tty: &os.File{}}
		err := h.WriteStdin([]byte("x"))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrStdinOnPty)
		assert.Equal(t, connect.CodeFailedPrecondition, InputErrorCode(err))
	})
}

// Reproduces issue #3622: a short-lived non-PTY process whose stdin read end is
// gone after exit. WriteStdin must surface an expected-lifecycle error that
// maps to CodeFailedPrecondition, not CodeInternal.
func TestWriteStdinAfterExitIsPrecondition(t *testing.T) {
	t.Parallel()

	cmd := exitZeroCommand(t.Context())
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	h := &Handler{stdin: stdin}

	_ = cmd.Wait()
	time.Sleep(50 * time.Millisecond)

	var writeErr error
	for range 100 {
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
