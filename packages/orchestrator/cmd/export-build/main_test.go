//go:build linux

//nolint:paralleltest // env-driven tests use t.Setenv
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestResolveSpec_FlagURL(t *testing.T) {
	tests := []struct {
		name         string
		storageFlag  string
		wantProvider storage.Provider
		wantBucket   string
		wantEndpoint string
		wantPathStyle bool
	}{
		{
			name:         "s3 url with endpoint",
			storageFlag:  "s3://my-bucket?endpoint=http://minio:9000&s3ForcePathStyle=true&region=us-east-1",
			wantProvider: storage.AWSStorageProvider,
			wantBucket:   "my-bucket",
			wantEndpoint: "http://minio:9000",
			wantPathStyle: true,
		},
		{
			name:         "s3 url without path style (TOS-compatible)",
			storageFlag:  "s3://tos-bucket?endpoint=https://tos-s3-cn-shanghai.ivolces.com&region=cn-shanghai",
			wantProvider: storage.AWSStorageProvider,
			wantBucket:   "tos-bucket",
			wantEndpoint: "https://tos-s3-cn-shanghai.ivolces.com",
			wantPathStyle: false,
		},
		{
			name:         "gcs url",
			storageFlag:  "gs://my-gcs-bucket",
			wantProvider: storage.GCPStorageProvider,
			wantBucket:   "my-gcs-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Env vars must not interfere when a flag URL is provided.
			t.Setenv("TEMPLATE_STORAGE_URL", "gs://should-not-be-used")

			spec, err := resolveSpec(tt.storageFlag)
			require.NoError(t, err)
			assert.Equal(t, tt.wantProvider, spec.Provider)
			assert.Equal(t, tt.wantBucket, spec.Bucket)
			assert.Equal(t, tt.wantEndpoint, spec.Endpoint)
			assert.Equal(t, tt.wantPathStyle, spec.UsePathStyle)
		})
	}
}

func TestResolveSpec_FlagLocalPath(t *testing.T) {
	// A plain path (no ://) is treated as a local directory.
	spec, err := resolveSpec(".local-build")
	require.NoError(t, err)
	assert.Equal(t, storage.LocalStorageProvider, spec.Provider)
}

func TestResolveSpec_EnvVar(t *testing.T) {
	// When no flag is given, TEMPLATE_STORAGE_URL is used.
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://env-bucket?endpoint=http://minio:9000&s3ForcePathStyle=true&region=us-east-1")
	t.Setenv("STORAGE_PROVIDER", "")

	spec, err := resolveSpec("")
	require.NoError(t, err)
	assert.Equal(t, storage.AWSStorageProvider, spec.Provider)
	assert.Equal(t, "env-bucket", spec.Bucket)
}

func TestResolveSpec_FlagTakesPrecedenceOverEnv(t *testing.T) {
	// Explicit -storage flag wins over TEMPLATE_STORAGE_URL env var.
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://env-bucket")

	spec, err := resolveSpec("gs://flag-bucket")
	require.NoError(t, err)
	assert.Equal(t, storage.GCPStorageProvider, spec.Provider)
	assert.Equal(t, "flag-bucket", spec.Bucket)
}

func TestResolveSpec_LegacyEnvFallback(t *testing.T) {
	// When both TEMPLATE_STORAGE_URL and -storage are absent, the legacy env
	// vars (STORAGE_PROVIDER + TEMPLATE_BUCKET_NAME) are used.
	t.Setenv("TEMPLATE_STORAGE_URL", "")
	t.Setenv("STORAGE_PROVIDER", "GCPBucket")
	t.Setenv("TEMPLATE_BUCKET_NAME", "legacy-bucket")

	spec, err := resolveSpec("")
	require.NoError(t, err)
	assert.Equal(t, storage.GCPStorageProvider, spec.Provider)
	assert.Equal(t, "legacy-bucket", spec.Bucket)
}
