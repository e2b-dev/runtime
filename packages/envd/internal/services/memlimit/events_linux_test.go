//go:build linux

package memlimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The socket only listens, so this runs unprivileged; it checks that ending ctx
// unblocks the read rather than leaving the watcher parked in it.
func TestWatchMemoryEvents_StopsWithContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- watchMemoryEvents(ctx, func() {}) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "watcher did not return after its context ended")
	}
	require.Error(t, ctx.Err())
}
