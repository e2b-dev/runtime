//go:build linux

package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/paths"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const (
	testTemplateID = "tmpl-init-layer-upload"
	testFilesHash  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func newInitLayerFileUploadServer(t *testing.T, exists bool, upload storage.UploadURL, signErr error) *ServerStore {
	t.Helper()

	blob := storage.NewMockBlob(t)
	blob.EXPECT().Exists(mock.Anything).Return(exists, nil)

	provider := storage.NewMockStorageProvider(t)
	path := paths.GetLayerFilesCachePath(testTemplateID, testFilesHash)
	provider.EXPECT().OpenBlob(mock.Anything, path).Return(blob, nil)
	provider.EXPECT().UploadSignedURL(mock.Anything, path, signedUrlExpiration).Return(upload, signErr)

	return &ServerStore{buildStorage: provider}
}

func initLayerFileUploadRequest() *templatemanager.InitLayerFileUploadRequest {
	return &templatemanager.InitLayerFileUploadRequest{
		TemplateID: testTemplateID,
		Hash:       testFilesHash,
	}
}

// The hash becomes part of the signed storage key, so a malformed one must be
// refused before storage is touched; the mock fails the test on any call.
func TestInitLayerFileUploadRejectsInvalidHash(t *testing.T) {
	t.Parallel()

	for name, hash := range map[string]string{
		"empty":                "",
		"uppercase":            strings.ToUpper(testFilesHash),
		"relative path":        "../" + testFilesHash,
		"absolute":             "/" + testFilesHash,
		"truncated":            testFilesHash[:63],
		"trailing path suffix": testFilesHash + "/x",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := &ServerStore{buildStorage: storage.NewMockStorageProvider(t)}
			req := initLayerFileUploadRequest()
			req.Hash = hash

			_, err := s.InitLayerFileUpload(t.Context(), req)
			require.Equal(t, codes.InvalidArgument, status.Code(err), err)
		})
	}
}

func TestInitLayerFileUploadUnsignableProvider(t *testing.T) {
	t.Parallel()

	unsupported := fmt.Errorf("%w: test provider", storage.ErrSignedUploadURLUnsupported)

	t.Run("cache hit reports present without a url", func(t *testing.T) {
		t.Parallel()

		s := newInitLayerFileUploadServer(t, true, storage.UploadURL{}, unsupported)

		resp, err := s.InitLayerFileUpload(t.Context(), initLayerFileUploadRequest())
		require.NoError(t, err)
		assert.True(t, resp.GetPresent())
		assert.Nil(t, resp.Url, "a cache hit must not hand back an unusable url")
	})

	t.Run("cache miss still fails", func(t *testing.T) {
		t.Parallel()

		s := newInitLayerFileUploadServer(t, false, storage.UploadURL{}, unsupported)

		_, err := s.InitLayerFileUpload(t.Context(), initLayerFileUploadRequest())
		require.ErrorIs(t, err, storage.ErrSignedUploadURLUnsupported)
	})
}

// Any signing failure — transient credential or network errors included — is tolerated on a
// cache hit: the hit needs no upload URL, and failing it would break builds on provider blips.
func TestInitLayerFileUploadSigningErrorOnCacheHit(t *testing.T) {
	t.Parallel()

	signErr := errors.New("failed to get Azure user delegation key")
	s := newInitLayerFileUploadServer(t, true, storage.UploadURL{}, signErr)

	resp, err := s.InitLayerFileUpload(t.Context(), initLayerFileUploadRequest())
	require.NoError(t, err)
	assert.True(t, resp.GetPresent())
	assert.Nil(t, resp.Url, "a cache hit must not hand back a url that failed to sign")
}

// The same failure on a cache miss stays fatal: there is nothing to serve without an upload.
func TestInitLayerFileUploadSigningErrorOnCacheMiss(t *testing.T) {
	t.Parallel()

	signErr := errors.New("failed to get Azure user delegation key")
	s := newInitLayerFileUploadServer(t, false, storage.UploadURL{}, signErr)

	_, err := s.InitLayerFileUpload(t.Context(), initLayerFileUploadRequest())
	require.ErrorIs(t, err, signErr)
}

// The url is returned on a cache hit as well: the SDK gates forceUpload re-uploads on
// url != nil, so dropping it there would silently turn forceUpload into a no-op.
func TestInitLayerFileUploadKeepsURLOnCacheHit(t *testing.T) {
	t.Parallel()

	for _, exists := range []bool{true, false} {
		t.Run(fmt.Sprintf("exists=%v", exists), func(t *testing.T) {
			t.Parallel()

			s := newInitLayerFileUploadServer(t, exists, storage.UploadURL{URL: "https://bucket.example/signed"}, nil)

			resp, err := s.InitLayerFileUpload(t.Context(), initLayerFileUploadRequest())
			require.NoError(t, err)
			assert.Equal(t, exists, resp.GetPresent())
			assert.Equal(t, "https://bucket.example/signed", resp.GetUrl())
			assert.Empty(t, resp.GetHeaders(), "providers that need no request headers must not send any")
		})
	}
}

func TestInitLayerFileUploadForwardsUploadHeaders(t *testing.T) {
	t.Parallel()

	headers := map[string]string{"x-ms-blob-type": "BlockBlob"}
	s := newInitLayerFileUploadServer(t, false, storage.UploadURL{URL: "https://account.example/signed", Headers: headers}, nil)

	resp, err := s.InitLayerFileUpload(t.Context(), initLayerFileUploadRequest())
	require.NoError(t, err)
	assert.Equal(t, headers, resp.GetHeaders())
}
