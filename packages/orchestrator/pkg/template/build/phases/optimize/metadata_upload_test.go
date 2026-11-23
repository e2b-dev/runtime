//go:build linux

package optimize

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/storageopts"
)

// A layer upload writes the metadata object the optimize phase rewrites, so the
// rewrite starts only once every upload of the build has returned.
func TestUploadMetadataWaitsForLayerUploads(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		uploads := &errgroup.Group{}
		finishUpload := make(chan struct{})
		uploads.Go(func() error {
			<-finishUpload

			return nil
		})

		var written atomic.Bool
		blob := storage.NewMockBlob(t)
		blob.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(context.Context, []byte, ...storageopts.PutOption) error {
				written.Store(true)

				return nil
			})
		store := storage.NewMockStorageProvider(t)
		store.EXPECT().OpenBlob(mock.Anything, storage.Paths{BuildID: "build"}.Metadata()).Return(blob, nil)

		pb := &OptimizeBuilder{
			BuildContext:    buildcontext.BuildContext{UploadErrGroup: uploads},
			templateStorage: store,
		}

		done := make(chan error, 1)
		go func() {
			done <- pb.uploadMetadata(t.Context(), metadata.Template{Template: metadata.TemplateMetadata{BuildID: "build"}})
		}()

		synctest.Wait()
		assert.False(t, written.Load(), "metadata was written while a layer upload was in flight")

		close(finishUpload)
		require.NoError(t, <-done)
		assert.True(t, written.Load())
	})
}

// A failed layer upload fails the build, so its metadata is left alone and the
// build's own wait still returns the upload's error.
func TestUploadMetadataSkipsAfterFailedLayerUpload(t *testing.T) {
	t.Parallel()

	uploadErr := errors.New("storage down")
	uploads := &errgroup.Group{}
	uploads.Go(func() error { return uploadErr })

	pb := &OptimizeBuilder{
		BuildContext:    buildcontext.BuildContext{UploadErrGroup: uploads},
		templateStorage: storage.NewMockStorageProvider(t),
	}

	err := pb.uploadMetadata(t.Context(), metadata.Template{Template: metadata.TemplateMetadata{BuildID: "build"}})
	require.ErrorIs(t, err, uploadErr)
	require.ErrorIs(t, uploads.Wait(), uploadErr)
}
