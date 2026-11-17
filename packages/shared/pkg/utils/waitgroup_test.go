package utils

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitGroupWaitAlreadyDone(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	require.NoError(t, WaitGroupWait(t.Context(), &wg))
}

func TestWaitGroupWaitCompletesLater(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(1)

	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-release
		wg.Done()
	}()
	go func() {
		done <- WaitGroupWait(t.Context(), &wg)
	}()

	select {
	case err := <-done:
		require.Failf(t, "WaitGroupWait returned before wait group completed", "err: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "WaitGroupWait did not return after wait group completed")
	}
}

func TestWaitGroupWaitReturnsContextErrorWhileWaiting(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, WaitGroupWait(ctx, &wg), context.Canceled)
}

func TestWaitDoneOrContextPrefersDoneWhenBothReady(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan struct{})
	close(done)

	// Both done and ctx are ready; select would pick a branch pseudo-randomly.
	// A closed done channel means the wait group finished, which must win over
	// the context error, so loop enough times to surface a regression.
	for range 1000 {
		require.NoError(t, waitDoneOrContext(ctx, done))
	}
}

func TestWaitDoneOrContextReturnsContextErrorWhenNotDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, waitDoneOrContext(ctx, make(chan struct{})), context.Canceled)
}
