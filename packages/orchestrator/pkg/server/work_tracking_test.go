//go:build linux

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestCreateTracksWorkUntilCanceled(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, nil)
	s.info.MaxSandboxes.Store(int64(featureflags.MaxSandboxesPerNode.Fallback()))
	s.sandboxCreateDuration = noop.Int64Histogram{}

	synctest.Test(t, func(t *testing.T) {
		s.startingSandboxes = utils.Must(utils.NewAdjustableSemaphore(1))
		require.True(t, s.startingSandboxes.TryAcquire(1))
		defer s.startingSandboxes.Release(1)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := s.Create(ctx, &orchestrator.SandboxCreateRequest{
				Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-create", Snapshot: true},
			})
			done <- err
		}()

		synctest.Wait()
		require.Equal(t, int64(1), s.info.OutstandingWork())
		require.Zero(t, s.sandboxFactory.Sandboxes.Count())

		cancel()
		require.Equal(t, codes.ResourceExhausted, status.Code(<-done))
		require.Zero(t, s.info.OutstandingWork())
		require.Zero(t, s.sandboxFactory.Sandboxes.Count())
	})
}

func TestUploadSnapshotAsyncTracksWorkThroughCompletion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		firstErr error
		retry    bool
	}{
		{name: "success"},
		{name: "permanent failure", firstErr: storage.ErrObjectNotExist},
		{name: "retry then success", firstErr: errors.New("storage unavailable"), retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metaPath := filepath.Join(t.TempDir(), "metadata.json")
			require.NoError(t, os.WriteFile(metaPath, []byte("{}"), 0o600))

			synctest.Test(t, func(t *testing.T) {
				s := &Server{info: &service.ServiceInfo{}, uploadFailedCounter: noop.Int64Counter{}}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				attemptStarted := make(chan context.Context, 1)
				finishAttempt := make(chan struct{})
				var attempts atomic.Int64
				store := storage.NewMockStorageProvider(t)
				blob := storage.NewMockBlob(t)
				buildID := uuid.New()
				store.EXPECT().OpenBlob(mock.Anything, storage.Paths{BuildID: buildID.String()}.Metadata()).Return(blob, nil)
				blob.EXPECT().Put(mock.Anything, []byte("{}"), mock.Anything).
					RunAndReturn(func(ctx context.Context, _ []byte, _ ...storage.PutOption) error {
						attempt := attempts.Add(1)
						attemptStarted <- ctx
						<-finishAttempt
						if attempt == 1 {
							return tc.firstErr
						}

						return nil
					})

				uploads := sandbox.NewUploads(nil, store, nil, nil, nil)
				defer uploads.Stop()
				upload, err := sandbox.NewUpload(ctx, uploads, &sandbox.Snapshot{
					BuildID:            buildID,
					FilesystemSnapshot: true,
					MemorySnapshot: sandbox.MemorySnapshot{
						Diff:       &build.NoDiff{},
						DiffHeader: sandbox.NewResolvedDiffHeader(nil),
					},
					RootfsDiff:       &build.NoDiff{},
					RootfsDiffHeader: sandbox.NewResolvedDiffHeader(nil),
					Metafile:         template.NewLocalFileLink(metaPath),
				}, store, storage.CompressConfig{}, nil, storage.UseCasePause, nil, nil)
				require.NoError(t, err)

				completeStarted := make(chan struct{})
				finishComplete := make(chan struct{})
				completeCalls := 0
				res := &snapshotResult{
					upload: upload,
					completeUpload: func(ctx context.Context, err error) {
						completeCalls++
						upload.Finish(ctx, err)
						close(completeStarted)
						<-finishComplete
					},
				}

				releaseParent := s.info.TrackWork()
				s.uploadSnapshotAsync(ctx, testHarvestSandbox(), res)
				require.Equal(t, int64(2), s.info.OutstandingWork(), "child must overlap parent before launch returns")
				releaseParent()
				attemptCtx := <-attemptStarted
				synctest.Wait()
				require.Equal(t, int64(1), s.info.OutstandingWork())
				require.Equal(t, int64(1), s.uploadsInFlight.Load())

				cancel()
				require.NoError(t, attemptCtx.Err(), "request cancellation must not cancel detached upload")
				finishAttempt <- struct{}{}
				if tc.retry {
					synctest.Wait()
					require.Equal(t, int64(1), s.info.OutstandingWork(), "retry backoff still owns work")
					<-attemptStarted
					require.Equal(t, int64(2), attempts.Load())
					finishAttempt <- struct{}{}
				}

				<-completeStarted
				synctest.Wait()
				if tc.firstErr != nil && !tc.retry {
					require.ErrorIs(t, upload.Wait(t.Context()), tc.firstErr)
				} else {
					require.NoError(t, upload.Wait(t.Context()))
				}
				require.Equal(t, int64(1), s.info.OutstandingWork(), "finished upload must retain work through completion cleanup")
				require.Equal(t, int64(1), s.uploadsInFlight.Load())

				waitReturned := false
				go func() {
					s.uploadsWG.Wait()
					waitReturned = true
				}()
				synctest.Wait()
				require.False(t, waitReturned)
				close(finishComplete)
				synctest.Wait()
				require.True(t, waitReturned)
				require.Equal(t, 1, completeCalls)
				require.Zero(t, s.info.OutstandingWork())
				require.Zero(t, s.uploadsInFlight.Load())
			})
		})
	}
}

func TestHarvestResumePrefetchAsyncTracksWorkThroughSealWait(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		consume bool
		timeout bool
	}{
		{name: "harvest only"},
		{name: "consume", consume: true},
		{name: "deadline", consume: true, timeout: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(featureflags.PauseResumePrefetchHarvestFlag.Key()).VariationForAll(true))
			td.Update(td.Flag(featureflags.PauseResumePrefetchConsumeFlag.Key()).VariationForAll(tc.consume))
			td.Update(td.Flag(featureflags.PauseResumePrefetchHarvestTimeoutMsFlag.Key()).ValueForAll(ldvalue.Int(1000)))
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			synctest.Test(t, func(t *testing.T) {
				s := &Server{info: &service.ServiceInfo{}, featureFlags: ff}
				seal := utils.NewSetOnce[build.Diff]()
				res := &snapshotResult{rootfsDiff: build.NewDeferredDiff("rootfs", 4096, seal)}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				releaseParent := s.info.TrackWork()
				s.harvestResumePrefetchAsync(ctx, testHarvestSandbox(), res, "build-1", nil, harvestSourcePause)
				require.Equal(t, int64(2), s.info.OutstandingWork())
				releaseParent()
				cancel()
				synctest.Wait()
				require.Equal(t, int64(1), s.info.OutstandingWork(), "request cancellation must not release harvest work")
				if tc.timeout {
					<-time.After(time.Second)
				} else {
					require.NoError(t, seal.SetError(build.ErrDeferredSealFailed))
				}
				synctest.Wait()
				require.Zero(t, s.info.OutstandingWork())
			})
		})
	}
}

func TestHarvestResumePrefetchAsyncDisabledTracksNoWork(t *testing.T) {
	t.Parallel()

	s := &Server{info: &service.ServiceInfo{}, featureFlags: admissionFlagClient(t, nil)}
	s.harvestResumePrefetchAsync(t.Context(), nil, nil, "build-1", nil, harvestSourcePause)
	require.Zero(t, s.info.OutstandingWork())
}
