//go:build linux

package nbd

import (
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
)

func TestIsSocketClosed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"epipe", syscall.EPIPE, true},
		{"epipe wrapped", fmt.Errorf("write unix @-: %w", syscall.EPIPE), true},
		{"closed pipe", io.ErrClosedPipe, true},
		{"closed pipe wrapped", fmt.Errorf("writeResponse: %w", io.ErrClosedPipe), true},
		{"other", errors.New("some backend error"), false},
		{"econnreset", syscall.ECONNRESET, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isSocketClosed(c.err); got != c.want {
				t.Fatalf("isSocketClosed(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
