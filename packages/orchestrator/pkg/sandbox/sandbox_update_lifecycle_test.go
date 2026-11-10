package sandbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newUpdateLifecycleTestSandbox() *Sandbox {
	sbx := new(Sandbox)
	sbx.cleanup = NewCleanup()
	sbx.updateAdmission.initialize()

	return sbx
}

func TestSandboxRunUpdateAfterCloseDoesNotInvokeCallback(t *testing.T) {
	t.Parallel()

	sbx := newUpdateLifecycleTestSandbox()
	require.NoError(t, sbx.Close(t.Context()))

	var calls atomic.Int32
	err := sbx.RunUpdate(func() error {
		calls.Add(1)

		return nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSandboxUpdateClosed)
	assert.Zero(t, calls.Load())
}

func TestSandboxCloseWaitsForAdmittedUpdateBeforeCleanup(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		sbx := newUpdateLifecycleTestSandbox()
		updateEntered := make(chan struct{})
		releaseUpdate := make(chan struct{})
		updateDone := make(chan error, 1)
		var updateReturned atomic.Bool

		go func() {
			updateDone <- sbx.RunUpdate(func() error {
				close(updateEntered)
				<-releaseUpdate
				updateReturned.Store(true)

				return nil
			})
		}()
		<-updateEntered

		cleanupEntered := make(chan struct{})
		cleanupRelease := make(chan struct{})
		sbx.cleanup.Add(t.Context(), func(context.Context) error {
			assert.True(t, updateReturned.Load(), "cleanup entered before the admitted update returned")
			close(cleanupEntered)
			<-cleanupRelease

			return nil
		})

		closeDone := make(chan error, 1)
		go func() { closeDone <- sbx.Close(t.Context()) }()
		<-sbx.updateAdmission.closedSignal()

		// The update waits on releaseUpdate and Close waits on its activeDone;
		// both are channel-durable synctest waits. Cleanup must not have begun.
		synctest.Wait()
		select {
		case <-cleanupEntered:
			t.Fatal("cleanup entered before the admitted update was released")
		default:
		}

		close(releaseUpdate)
		require.NoError(t, <-updateDone)
		<-cleanupEntered
		close(cleanupRelease)
		require.NoError(t, <-closeDone)

		var lateCalls atomic.Int32
		err := sbx.RunUpdate(func() error {
			lateCalls.Add(1)

			return nil
		})
		require.ErrorIs(t, err, ErrSandboxUpdateClosed)
		assert.Zero(t, lateCalls.Load())
	})
}

func TestSandboxConcurrentCloseIsIdempotentAndPreservesCleanupError(t *testing.T) {
	t.Parallel()

	sbx := newUpdateLifecycleTestSandbox()
	cleanupErr := errors.New("cleanup failed")
	var cleanupCalls atomic.Int32
	sbx.cleanup.Add(t.Context(), func(context.Context) error {
		cleanupCalls.Add(1)

		return cleanupErr
	})

	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { results <- sbx.Close(t.Context()) })
	}
	wg.Wait()
	close(results)

	for err := range results {
		require.ErrorIs(t, err, cleanupErr)
	}
	assert.Equal(t, int32(1), cleanupCalls.Load())

	var updateCalls atomic.Int32
	err := sbx.RunUpdate(func() error {
		updateCalls.Add(1)

		return nil
	})
	require.ErrorIs(t, err, ErrSandboxUpdateClosed)
	assert.Zero(t, updateCalls.Load())
}
