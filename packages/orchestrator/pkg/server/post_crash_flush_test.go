package server

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

// wrapLikeCleanup reproduces the real wrapping depth a flush error reaches the
// cleanup call site through: path flush -> NBDProvider.Close -> cleanup.Run's
// errors.Join -> Sandbox.Close.
func wrapLikeCleanup(inner error) error {
	flush := fmt.Errorf("sync NBD device: %w", inner)
	cow := fmt.Errorf("error flushing cow device: %w", flush)
	joined := errors.Join(cow)
	return fmt.Errorf("failed to cleanup sandbox: %w", joined)
}

func TestIsExpectedPostCrashFlushError(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"EIO deep-wrapped":   {wrapLikeCleanup(syscall.EIO), true},
		"ENXIO deep-wrapped": {wrapLikeCleanup(syscall.ENXIO), true},
		"EIO bare":           {syscall.EIO, true},
		"disk full (ENOSPC)": {wrapLikeCleanup(syscall.ENOSPC), false},
		"generic error":      {wrapLikeCleanup(errors.New("boom")), false},
		"nil":                {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isExpectedPostCrashFlushError(tc.err))
		})
	}
}
