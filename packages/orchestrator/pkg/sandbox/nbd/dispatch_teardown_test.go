//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
)

// Regression for #3274: in-flight NBD reads/writes and reply-socket writes that
// fail because the sandbox is exiting must be classified as expected teardown
// noise (logged at Warn), while genuine backend failures must not be — they stay
// at Error. isExpectedTeardownError is the single predicate both levels hang off,
// so it is the thing worth locking down.
func TestIsExpectedTeardownError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "context canceled (dispatch ctx cancelled on VM exit)",
			err:  context.Canceled,
			want: true,
		},
		{
			name: "context canceled wrapped",
			err:  fmt.Errorf("read backend: %w", context.Canceled),
			want: true,
		},
		{
			name: "deadline exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "closed network connection (reply socket closed under us)",
			err:  net.ErrClosed,
			want: true,
		},
		{
			name: "closed network connection wrapped in OpError",
			err:  &net.OpError{Op: "write", Net: "unix", Err: net.ErrClosed},
			want: true,
		},
		{
			name: "broken pipe (EPIPE) wrapped in OpError",
			err:  &net.OpError{Op: "write", Net: "unix", Err: syscall.EPIPE},
			want: true,
		},
		{
			name: "bare EPIPE",
			err:  syscall.EPIPE,
			want: true,
		},
		{
			name: "real backend error must stay Error",
			err:  errors.New("backing store returned corrupt block"),
			want: false,
		},
		{
			name: "unexpected EOF is a real failure",
			err:  io.ErrUnexpectedEOF,
			want: false,
		},
		{
			name: "ECONNRESET is not treated as expected teardown",
			err:  syscall.ECONNRESET,
			want: false,
		},
		{
			name: "nil is not an error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isExpectedTeardownError(tt.err); got != tt.want {
				t.Errorf("isExpectedTeardownError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
