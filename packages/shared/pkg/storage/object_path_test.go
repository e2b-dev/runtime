package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Every provider must refuse a malformed path before touching its backend: the
// cloud providers here have no client, and the cache's inner provider is a mock
// with no expectations, so a path that reaches any I/O fails the test.
func TestProvidersRejectInvalidObjectPaths(t *testing.T) {
	t.Parallel()

	providers := map[string]func(t *testing.T) StorageProvider{
		"fs": func(t *testing.T) StorageProvider {
			t.Helper()

			return newFileSystemStorage(t.TempDir(), "http://upload.invalid", []byte("key"))
		},
		"gcp":   func(*testing.T) StorageProvider { return &gcpStorage{} },
		"aws":   func(*testing.T) StorageProvider { return &awsStorage{} },
		"azure": func(*testing.T) StorageProvider { return &azureStorage{} },
		"cache": func(t *testing.T) StorageProvider {
			t.Helper()

			return &cache{rootPath: t.TempDir(), inner: NewMockStorageProvider(t)}
		},
	}
	ops := map[string]func(ctx context.Context, p StorageProvider, path string) error{
		"open_blob": func(ctx context.Context, p StorageProvider, path string) error {
			_, err := p.OpenBlob(ctx, path)

			return err
		},
		"open_seekable": func(ctx context.Context, p StorageProvider, path string) error {
			_, err := p.OpenSeekable(ctx, path)

			return err
		},
		"delete_prefix": func(ctx context.Context, p StorageProvider, path string) error {
			return p.DeleteObjectsWithPrefix(ctx, path)
		},
		"upload_signed_url": func(ctx context.Context, p StorageProvider, path string) error {
			_, err := p.UploadSignedURL(ctx, path, time.Minute)

			return err
		},
	}
	paths := map[string]string{
		"empty":             "",
		"dot":               ".",
		"absolute":          "/build/memfile",
		"parent":            "../build/memfile",
		"nested_parent":     "build/../../memfile",
		"resolvable_parent": "build/../memfile",
		"dot_segment":       "build/./memfile",
		"double_slash":      "build//memfile",
		"trailing_slash":    "build/",
		"invalid_utf8":      "build/\xffmemfile",
	}

	for providerName, newProvider := range providers {
		for opName, op := range ops {
			for pathName, path := range paths {
				t.Run(providerName+"/"+opName+"/"+pathName, func(t *testing.T) {
					t.Parallel()

					require.ErrorIs(t, op(t.Context(), newProvider(t), path), errInvalidObjectPath)
				})
			}
		}
	}
}

// The paths templates are stored under must stay acceptable to the check.
func TestFSAcceptsTemplateObjectPaths(t *testing.T) {
	t.Parallel()

	p := newFileSystemStorage(t.TempDir(), "http://upload.invalid", []byte("key"))
	build := Paths{BuildID: uuid.NewString()}

	for _, path := range []string{
		build.Memfile(),
		build.MemfileHeader(),
		build.Rootfs(),
		build.RootfsHeader(),
		build.Snapfile(),
		build.Metadata(),
		build.DataFile(MemfileName, CompressionZstd),
		build.DataFile(RootfsName, CompressionLZ4),
	} {
		_, err := p.OpenBlob(t.Context(), path)
		require.NoError(t, err, path)
		_, err = p.OpenSeekable(t.Context(), path)
		require.NoError(t, err, path)
		_, err = p.UploadSignedURL(t.Context(), path, time.Minute)
		require.NoError(t, err, path)
	}

	require.NoError(t, p.DeleteObjectsWithPrefix(t.Context(), build.StorageDir()))
}
