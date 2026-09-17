//go:build linux

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunDeleteStop_WaitReturnsExactStopError(t *testing.T) {
	t.Parallel()

	want := errors.New("firecracker stop failed")
	err := runDeleteStop(t.Context(), true, func(context.Context) error { return want })
	require.ErrorIs(t, err, want)
}

func TestRunDeleteStop_LegacyReturnsBeforeStopCompletes(t *testing.T) {
	t.Parallel()

	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, runDeleteStop(ctx, false, func(stopCtx context.Context) error {
		entered <- stopCtx
		<-release
		close(done)

		return nil
	}))

	stopCtx := <-entered
	require.NoError(t, stopCtx.Err(), "legacy stop must outlive caller cancellation")
	close(release)
	<-done
}
