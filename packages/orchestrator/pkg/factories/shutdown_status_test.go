//go:build linux

package factories

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// A graceful stop is not a failure: every service ends with
// serviceDoneError, however the group reports it.
func TestShutdownFailed(t *testing.T) {
	t.Parallel()

	require.False(t, shutdownFailed(nil))
	require.False(t, shutdownFailed(serviceDoneError{name: "cmux server"}))
	require.False(t, shutdownFailed(fmt.Errorf("group: %w", serviceDoneError{name: "grpc"})))
	require.True(t, shutdownFailed(errors.New("listen tcp :5008: address already in use")))
}

// Sync on a pipe, as a container's stdout is, fails with EINVAL, and that is
// not a failed shutdown; on a file it succeeds.
func TestSyncFailedOnAPipe(t *testing.T) {
	t.Parallel()

	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	pipeErr := w.Sync()
	require.Error(t, pipeErr, "fsync of a pipe fails")
	require.False(t, syncFailed(pipeErr))

	f, err := os.Create(filepath.Join(t.TempDir(), "log"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	require.False(t, syncFailed(f.Sync()))

	require.True(t, syncFailed(os.ErrClosed))
}

// A service that fails is recorded whatever else is pending: after the
// shutdown signal nothing reads the channel, and when the signal won the race
// the channel is already full and drops the end.
func TestRunServiceRecordsAFailure(t *testing.T) {
	t.Parallel()

	var failed atomic.Bool
	ch := newServiceErrorChannel()
	ch <- errors.New("an earlier end")
	err := runService(t.Context(), logger.NewNopLogger(), "sandbox proxy", func() error {
		return errors.New("listen tcp :5007: bind: address already in use")
	}, &failed, ch)
	require.ErrorAs(t, err, new(serviceDoneError))
	require.True(t, failed.Load())
	require.False(t, shutdownFailed(err), "the group's error alone says nothing")

	var clean atomic.Bool
	err = runService(t.Context(), logger.NewNopLogger(), "grpc server", func() error { return nil }, &clean, newServiceErrorChannel())
	require.ErrorAs(t, err, new(serviceDoneError))
	require.False(t, clean.Load())
}
