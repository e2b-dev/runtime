package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// harvestFlagClient enables the harvest with a one-second budget, consume off.
func harvestFlagClient(t *testing.T) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PauseResumePrefetchHarvestFlag.Key()).VariationForAll(true))
	td.Update(td.Flag(featureflags.PauseResumePrefetchHarvestTimeoutMsFlag.Key()).ValueForAll(ldvalue.Int(1000)))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// A filesystem-only checkpoint has no memfile to resume, so no harvest is
// scheduled even with the flag on.
func TestHarvestCheckpointPrefetchAsync_SkipsFilesystemOnly(t *testing.T) {
	t.Parallel()

	s := &Server{info: &service.ServiceInfo{}, featureFlags: harvestFlagClient(t)}
	res := &snapshotResult{rootfsDiff: &build.NoDiff{}}
	s.harvestCheckpointPrefetchAsync(t.Context(), testHarvestSandbox(), res, &orchestrator.SandboxCheckpointRequest{BuildId: "build-1", FilesystemOnly: true})
	require.Zero(t, s.info.OutstandingWork())
}

// A memory checkpoint taken in place schedules the harvest as tracked work,
// which outlives the request and ends when the snapshot's seal settles.
func TestHarvestCheckpointPrefetchAsync_SchedulesAndTracksWork(t *testing.T) {
	t.Parallel()

	ff := harvestFlagClient(t)
	synctest.Test(t, func(t *testing.T) {
		s := &Server{info: &service.ServiceInfo{}, featureFlags: ff}
		seal := utils.NewSetOnce[build.Diff]()
		res := &snapshotResult{rootfsDiff: build.NewDeferredDiff("rootfs", 4096, seal)}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		s.harvestCheckpointPrefetchAsync(ctx, testHarvestSandbox(), res, &orchestrator.SandboxCheckpointRequest{BuildId: "build-1"})
		require.Equal(t, int64(1), s.info.OutstandingWork())
		cancel()
		synctest.Wait()
		require.Equal(t, int64(1), s.info.OutstandingWork(), "request cancellation must not release harvest work")
		require.NoError(t, seal.SetError(build.ErrDeferredSealFailed))
		synctest.Wait()
		require.Zero(t, s.info.OutstandingWork())
	})
}

// An in-place checkpoint through the CoW window hands the harvest a memfile
// that is still being swept: the harvest must not resume until that seal
// settles, and a failed seal skips it without touching the rootfs.
func TestHarvestResumePrefetchAsync_WaitsForDeferredMemorySeal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		memorySealErr error
	}{
		{name: "seal fails", memorySealErr: errors.New("window cancelled")},
		{name: "seal settles"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ff := harvestFlagClient(t)
			synctest.Test(t, func(t *testing.T) {
				s := &Server{info: &service.ServiceInfo{}, featureFlags: ff}
				memorySealed := make(chan error, 1)
				rootfsSeal := utils.NewSetOnce[build.Diff]()
				var rootfsWaited atomic.Bool
				res := &snapshotResult{
					rootfsDiff:           build.NewDeferredDiff("rootfs", 4096, rootfsSeal),
					memoryExportDeferred: true,
					waitMemorySealed: func(ctx context.Context) error {
						select {
						case err := <-memorySealed:
							return err
						case <-ctx.Done():
							return ctx.Err()
						}
					},
				}
				// The rootfs promise is only consulted once memory has sealed; a
				// failed rootfs seal then ends the harvest without a resume.
				go func() {
					<-time.After(10 * time.Millisecond)
					rootfsWaited.Store(true)
					_ = rootfsSeal.SetError(build.ErrDeferredSealFailed)
				}()

				s.harvestResumePrefetchAsync(t.Context(), testHarvestSandbox(), res, "build-1", nil, harvestSourceCheckpoint)
				require.Equal(t, int64(1), s.info.OutstandingWork())
				<-time.After(500 * time.Millisecond)
				synctest.Wait()
				require.Equal(t, int64(1), s.info.OutstandingWork(), "the harvest must hold until the memory seal settles")

				memorySealed <- tc.memorySealErr
				synctest.Wait()
				require.Zero(t, s.info.OutstandingWork())
				require.True(t, rootfsWaited.Load())
			})
		})
	}
}

// waitSnapshotSealed orders the waits memfile first, names which seal failed,
// and is a no-op when nothing was deferred.
func TestWaitSnapshotSealed(t *testing.T) {
	t.Parallel()

	require.NoError(t, waitSnapshotSealed(t.Context(), &snapshotResult{rootfsDiff: &build.NoDiff{}}))

	memErr := errors.New("sweep aborted")
	err := waitSnapshotSealed(t.Context(), &snapshotResult{
		rootfsDiff:           &build.NoDiff{},
		memoryExportDeferred: true,
		waitMemorySealed:     func(context.Context) error { return memErr },
	})
	require.ErrorIs(t, err, memErr)
	require.ErrorContains(t, err, "memory seal")

	rootfsSeal := utils.NewSetOnce[build.Diff]()
	require.NoError(t, rootfsSeal.SetError(build.ErrDeferredSealFailed))
	memoryWaited := false
	err = waitSnapshotSealed(t.Context(), &snapshotResult{
		rootfsDiff:           build.NewDeferredDiff("rootfs", 4096, rootfsSeal),
		memoryExportDeferred: true,
		waitMemorySealed: func(context.Context) error {
			memoryWaited = true

			return nil
		},
	})
	require.ErrorIs(t, err, build.ErrDeferredSealFailed)
	require.ErrorContains(t, err, "rootfs seal")
	require.True(t, memoryWaited, "memory seal is awaited before the rootfs seal")

	// A deferred flag with no waiter (older snapshot shape) is treated as sealed.
	require.NoError(t, waitSnapshotSealed(t.Context(), &snapshotResult{rootfsDiff: &build.NoDiff{}, memoryExportDeferred: true}))
}
