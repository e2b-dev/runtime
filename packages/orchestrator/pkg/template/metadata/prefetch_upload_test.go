package metadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// The metadata object already exists when it is uploaded here, so the put has
// to tell a cache in front of the store to drop its older copy.
func TestUploadMetadataOverwrites(t *testing.T) {
	t.Parallel()

	var got storage.PutOptions
	blob := storage.NewMockBlob(t)
	blob.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ []byte, opts ...storage.PutOption) error {
			got = storage.ApplyPutOptions(opts)

			return nil
		})
	store := storage.NewMockStorageProvider(t)
	store.EXPECT().OpenBlob(mock.Anything, storage.Paths{BuildID: "build"}.Metadata()).Return(blob, nil)

	require.NoError(t, UploadMetadata(t.Context(), store, Template{Template: TemplateMetadata{BuildID: "build"}}, nil))
	assert.True(t, got.Overwrite)
}
