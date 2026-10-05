package cfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestTemplateStorage_URLOverridesLegacyEnv(t *testing.T) {
	t.Setenv("STORAGE_PROVIDER", "GCPBucket")
	t.Setenv("TEMPLATE_BUCKET_NAME", "legacy-bucket")
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://url-bucket?endpoint=http://minio:9000&s3ForcePathStyle=true")

	spec, err := TemplateStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.Spec{
		Provider:     storage.AWSStorageProvider,
		Bucket:       "url-bucket",
		Endpoint:     "http://minio:9000",
		UsePathStyle: true,
	}, spec)
}

// TestTemplateStorage_Legacy drives the legacy env style through the template
// role: STORAGE_PROVIDER + TEMPLATE_BUCKET_NAME / LOCAL_TEMPLATE_STORAGE_BASE_PATH
// (+ S3_USE_PATH_STYLE) are converted into a storage URL and resolved through
// the same pipeline as explicit URLs.
func TestTemplateStorage_Legacy(t *testing.T) {
	tests := []struct {
		name        string
		provider    string // STORAGE_PROVIDER ("" behaves as unset → GCPBucket)
		bucket      string // TEMPLATE_BUCKET_NAME
		basePath    string // LOCAL_TEMPLATE_STORAGE_BASE_PATH ("" → default)
		s3PathStyle string // S3_USE_PATH_STYLE
		want        storage.Spec
		wantErr     string
	}{
		{
			name:     "gcp bucket",
			provider: "GCPBucket",
			bucket:   "fc-templates",
			want:     storage.Spec{Provider: storage.GCPStorageProvider, Bucket: "fc-templates"},
		},
		{
			name:     "unset provider defaults to gcp",
			provider: "",
			bucket:   "fc-templates",
			want:     storage.Spec{Provider: storage.GCPStorageProvider, Bucket: "fc-templates"},
		},
		{
			// Endpoint/region stay empty in the legacy path: the AWS SDK
			// resolves AWS_ENDPOINT_URL / AWS_REGION itself.
			name:     "aws bucket",
			provider: "AWSBucket",
			bucket:   "legacy-bucket",
			want:     storage.Spec{Provider: storage.AWSStorageProvider, Bucket: "legacy-bucket"},
		},
		{
			// Our own legacy env (S3_USE_PATH_STYLE) is folded into the
			// synthesized URL, so the resolved spec is fully transparent.
			name:        "aws bucket with path style env",
			provider:    "AWSBucket",
			bucket:      "legacy-bucket",
			s3PathStyle: "True",
			want:        storage.Spec{Provider: storage.AWSStorageProvider, Bucket: "legacy-bucket", UsePathStyle: true},
		},
		{
			name:     "azure container",
			provider: "AzureBucket",
			bucket:   "fc-templates",
			want:     storage.Spec{Provider: storage.AzureStorageProvider, Bucket: "fc-templates"},
		},
		{
			name:     "local absolute path",
			provider: "Local",
			basePath: "/tmp/some-cache",
			want:     storage.Spec{Provider: storage.LocalStorageProvider, BasePath: "/tmp/some-cache"},
		},
		{
			name:     "local path with spaces",
			provider: "Local",
			basePath: "/var/lib/e2b storage",
			want:     storage.Spec{Provider: storage.LocalStorageProvider, BasePath: "/var/lib/e2b storage"},
		},
		{
			// The CLI tools (create-build, resume-build) configure relative
			// base paths like ".local-build"; the legacy→URL conversion keeps
			// them intact via the opaque file: form.
			name:     "local relative path",
			provider: "Local",
			basePath: ".local-build",
			want:     storage.Spec{Provider: storage.LocalStorageProvider, BasePath: ".local-build"},
		},
		{
			name:     "local default base path",
			provider: "Local",
			want:     storage.Spec{Provider: storage.LocalStorageProvider, BasePath: "/tmp/templates"},
		},
		{
			name:     "unknown provider",
			provider: "FloppyDisk",
			wantErr:  "unknown storage provider: FloppyDisk",
		},
		{
			name:     "cloud provider without bucket",
			provider: "GCPBucket",
			bucket:   "",
			wantErr:  "template storage bucket not configured: set TEMPLATE_BUCKET_NAME",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEMPLATE_STORAGE_URL", "")
			t.Setenv("STORAGE_PROVIDER", tt.provider)
			t.Setenv("TEMPLATE_BUCKET_NAME", tt.bucket)
			t.Setenv("LOCAL_TEMPLATE_STORAGE_BASE_PATH", tt.basePath)
			t.Setenv("S3_USE_PATH_STYLE", tt.s3PathStyle)

			spec, err := TemplateStorage()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, spec)
		})
	}
}

func TestTemplateStorage_URLIgnoresLegacyPathStyleEnv(t *testing.T) {
	// A defined storage URL is fully self-describing: the legacy
	// S3_USE_PATH_STYLE env must not leak into URL-configured roles.
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://url-bucket")
	t.Setenv("S3_USE_PATH_STYLE", "true")

	spec, err := TemplateStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.Spec{Provider: storage.AWSStorageProvider, Bucket: "url-bucket"}, spec)
}

func TestTemplateStorage_LegacySSEEnv(t *testing.T) {
	// The legacy SSE envs are folded into the synthesized URL for both roles,
	// alongside path style.
	t.Setenv("TEMPLATE_STORAGE_URL", "")
	t.Setenv("BUILD_CACHE_STORAGE_URL", "")
	t.Setenv("STORAGE_PROVIDER", "AWSBucket")
	t.Setenv("TEMPLATE_BUCKET_NAME", "legacy-templates")
	t.Setenv("BUILD_CACHE_BUCKET_NAME", "legacy-cache")
	t.Setenv("S3_USE_PATH_STYLE", "true")
	t.Setenv("S3_SSE_TYPE", "aws:kms")
	t.Setenv("S3_SSE_KMS_KEY_ID", "arn:aws:kms:us-east-1:111111111111:key/test")

	want := storage.Spec{
		Provider:             storage.AWSStorageProvider,
		UsePathStyle:         true,
		ServerSideEncryption: storage.SSEAWSKMS,
		SSEKMSKeyID:          "arn:aws:kms:us-east-1:111111111111:key/test",
	}

	spec, err := TemplateStorage()
	require.NoError(t, err)
	want.Bucket = "legacy-templates"
	assert.Equal(t, want, spec)

	spec, err = BuildCacheStorage()
	require.NoError(t, err)
	want.Bucket = "legacy-cache"
	assert.Equal(t, want, spec)
}

func TestTemplateStorage_LegacyInvalidSSEEnvFails(t *testing.T) {
	t.Setenv("TEMPLATE_STORAGE_URL", "")
	t.Setenv("STORAGE_PROVIDER", "AWSBucket")
	t.Setenv("TEMPLATE_BUCKET_NAME", "legacy-templates")
	t.Setenv("S3_SSE_TYPE", "yes")

	_, err := TemplateStorage()
	require.ErrorContains(t, err, `invalid ssetype "yes"`)
}

func TestTemplateStorage_URLIgnoresLegacySSEEnv(t *testing.T) {
	// A defined storage URL is self-describing: SSE comes from its query only.
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://url-bucket")
	t.Setenv("S3_SSE_TYPE", "AES256")

	spec, err := TemplateStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.Spec{Provider: storage.AWSStorageProvider, Bucket: "url-bucket"}, spec)
}

func TestStorage_MalformedPathStyleEnvFailsFast(t *testing.T) {
	// Deliberate: a malformed S3_USE_PATH_STYLE fails resolution loudly
	// (even for URL-configured roles) instead of silently meaning false.
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://url-bucket")
	t.Setenv("S3_USE_PATH_STYLE", "yes-please")

	_, err := TemplateStorage()
	require.ErrorContains(t, err, "parse storage environment")
}

func TestStorageRolesDiverge(t *testing.T) {
	// The template role reads through an S3-compatible cache while the build
	// cache stays on GCS — the scenario per-role URLs exist for.
	t.Setenv("STORAGE_PROVIDER", "GCPBucket")
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://fc-templates?endpoint=http://minio:9000&s3ForcePathStyle=true")
	t.Setenv("BUILD_CACHE_STORAGE_URL", "")
	t.Setenv("BUILD_CACHE_BUCKET_NAME", "fc-build-cache")

	templateSpec, err := TemplateStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.AWSStorageProvider, templateSpec.Provider)

	buildCacheSpec, err := BuildCacheStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.Spec{Provider: storage.GCPStorageProvider, Bucket: "fc-build-cache"}, buildCacheSpec)
}

func TestBuildCacheStorage_Legacy(t *testing.T) {
	t.Setenv("BUILD_CACHE_STORAGE_URL", "")
	t.Setenv("STORAGE_PROVIDER", "Local")
	t.Setenv("LOCAL_BUILD_CACHE_STORAGE_BASE_PATH", "/tmp/some-cache")

	spec, err := BuildCacheStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.Spec{Provider: storage.LocalStorageProvider, BasePath: "/tmp/some-cache"}, spec)

	t.Setenv("LOCAL_BUILD_CACHE_STORAGE_BASE_PATH", "")

	spec, err = BuildCacheStorage()
	require.NoError(t, err)
	assert.Equal(t, storage.Spec{Provider: storage.LocalStorageProvider, BasePath: "/tmp/build-cache"}, spec)
}

func TestTemplateStorage_InvalidURL(t *testing.T) {
	t.Setenv("TEMPLATE_STORAGE_URL", "s3://bucket?bogus=1")

	_, err := TemplateStorage()
	require.ErrorContains(t, err, "unsupported query parameter")
}
