//go:build linux

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// uploadFinish stands in for the template cache's upload finisher: it counts
// the pin and records the outcome it was returned with.
type uploadFinish struct {
	pins    atomic.Int64
	outcome atomic.Uint32
}

func (f *uploadFinish) finish(o template.UploadOutcome) {
	f.pins.Add(-1)
	f.outcome.Store(uint32(o))
}

func (f *uploadFinish) returned(t *testing.T, want template.UploadOutcome) {
	t.Helper()

	assert.Zero(t, f.pins.Load(), "the pin is returned")
	assert.Equal(t, want, template.UploadOutcome(f.outcome.Load()))
}

// checkpointUpload is a filesystem-only snapshot upload whose one storage
// write returns putErr, holding a counted template cache pin, wired into a
// snapshotResult the way snapshotAndCacheSandbox wires it.
func checkpointUpload(t *testing.T, putErr error) (*snapshotResult, *uploadFinish) {
	t.Helper()

	metaPath := filepath.Join(t.TempDir(), "metadata.json")
	require.NoError(t, os.WriteFile(metaPath, []byte("{}"), 0o600))

	buildID := uuid.New()
	store := storage.NewMockStorageProvider(t)
	blob := storage.NewMockBlob(t)
	store.EXPECT().OpenBlob(mock.Anything, storage.Paths{BuildID: buildID.String()}.Metadata()).Return(blob, nil).Maybe()
	blob.EXPECT().Put(mock.Anything, []byte("{}"), mock.Anything).Return(putErr).Maybe()

	uploads := sandbox.NewUploads(nil, store, nil, nil, nil)
	t.Cleanup(uploads.Stop)

	f := &uploadFinish{}
	f.pins.Add(1)
	upload, err := sandbox.NewUpload(t.Context(), uploads, &sandbox.Snapshot{
		BuildID:            buildID,
		FilesystemSnapshot: true,
		MemorySnapshot: sandbox.MemorySnapshot{
			Diff:       &build.NoDiff{},
			DiffHeader: sandbox.NewResolvedDiffHeader(nil),
		},
		RootfsDiff:       &build.NoDiff{},
		RootfsDiffHeader: sandbox.NewResolvedDiffHeader(nil),
		Metafile:         template.NewLocalFileLink(metaPath),
	}, store, storage.CompressConfig{}, nil, storage.UseCasePause, nil, f.finish)
	require.NoError(t, err)

	return &snapshotResult{
		upload:         upload,
		completeUpload: upload.Finish,
		abandonUpload:  upload.Abandon,
	}, f
}

func asyncCheckpointFlags(t *testing.T) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PeerToPeerAsyncCheckpointFlag.Key()).VariationForAll(true))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// Every checkpoint upload returns its pin. One that fails is discarded, so it
// ends abandoned, not failed: nothing will read it, and it must not count
// among the layers that did not land. One that lands says so.
func TestRunCheckpointUpload_ReturnsThePinWhenTheCheckpointIsDiscarded(t *testing.T) {
	t.Parallel()

	in := &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-1"}

	t.Run("sync upload fails", func(t *testing.T) {
		t.Parallel()

		s := &Server{info: &service.ServiceInfo{}, featureFlags: admissionFlagClient(t, nil)}
		res, f := checkpointUpload(t, storage.ErrObjectNotExist)

		failed := false
		err := s.runCheckpointUpload(t.Context(), nil, res, in, codes.Internal, func() { failed = true })
		require.Error(t, err)
		assert.True(t, failed)
		f.returned(t, template.UploadAbandoned)
		require.Error(t, res.upload.Wait(t.Context()), "its waiters still see the failure")
	})

	t.Run("sync upload lands", func(t *testing.T) {
		t.Parallel()

		s := &Server{info: &service.ServiceInfo{}, featureFlags: admissionFlagClient(t, nil)}
		res, f := checkpointUpload(t, nil)

		require.NoError(t, s.runCheckpointUpload(t.Context(), nil, res, in, codes.Internal, nil))
		f.returned(t, template.UploadLanded)
		require.NoError(t, res.upload.Wait(t.Context()))
	})

	t.Run("deferred memory seal fails", func(t *testing.T) {
		t.Parallel()

		s := &Server{info: &service.ServiceInfo{}, featureFlags: asyncCheckpointFlags(t)}
		res, f := checkpointUpload(t, nil)
		res.memoryExportDeferred = true
		res.waitMemorySealed = func(context.Context) error { return build.ErrDeferredSealFailed }

		err := s.runCheckpointUpload(t.Context(), nil, res, in, codes.FailedPrecondition, nil)
		require.Error(t, err)
		f.returned(t, template.UploadAbandoned)
		require.ErrorIs(t, res.upload.Wait(t.Context()), build.ErrDeferredSealFailed)
	})
}

// A resume-fresh checkpoint that returns before handing its upload to
// runCheckpointUpload — its template lookup, resume, envd upgrade or
// registration failing — abandons the upload: its waiters fail with the
// checkpoint's error instead of hanging, and its pin is returned. Once handed
// off, the upload is left to runCheckpointUpload.
func TestCheckpointHandoff(t *testing.T) {
	t.Parallel()

	t.Run("returned before the handoff", func(t *testing.T) {
		t.Parallel()

		res, f := checkpointUpload(t, nil)
		h := checkpointHandoff{res: res}
		h.settle(t.Context(), errors.New("error getting template for resume"))

		f.returned(t, template.UploadAbandoned)
		require.ErrorContains(t, res.upload.Wait(t.Context()), "error getting template for resume")
	})

	t.Run("returned before the handoff without an error", func(t *testing.T) {
		t.Parallel()

		res, f := checkpointUpload(t, nil)
		h := checkpointHandoff{res: res}
		h.settle(t.Context(), nil)

		f.returned(t, template.UploadAbandoned)
		err := res.upload.Wait(t.Context())
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "%!")
	})

	t.Run("handed off", func(t *testing.T) {
		t.Parallel()

		res, f := checkpointUpload(t, nil)
		h := checkpointHandoff{res: res}
		h.handOff()
		h.settle(t.Context(), errors.New("returned after the handoff"))

		assert.Equal(t, int64(1), f.pins.Load(), "the upload still owns its pin")
		res.completeUpload(t.Context(), nil)
		f.returned(t, template.UploadLanded)
	})
}
