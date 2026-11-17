package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// The loops use Redis, and shutdown closes it after the drain, so the drain
// must stop them and must not return while one is still running.
func TestDrainStopsTheBackgroundLoopsAndWaitsForThem(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		t.Helper()

		loopCtx, stop := context.WithCancel(t.Context())
		o := &Orchestrator{stopBackground: stop}
		finish := make(chan struct{})
		o.background.Go(func() {
			<-loopCtx.Done()
			<-finish
		})

		var drained atomic.Bool
		var drainErr error
		go func() {
			drainErr = o.Drain(t.Context())
			drained.Store(true)
		}()

		synctest.Wait()
		require.Error(t, loopCtx.Err(), "the loop was not told to stop")
		require.False(t, drained.Load(), "drained while a loop was still running")

		close(finish)
		synctest.Wait()
		require.True(t, drained.Load())
		require.NoError(t, drainErr)
	})
}
