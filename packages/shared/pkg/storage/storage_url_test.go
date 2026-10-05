package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStorageURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		want    Spec
		wantErr string
	}{
		{
			name: "gcs bucket",
			url:  "gs://my-bucket",
			want: Spec{Provider: GCPStorageProvider, Bucket: "my-bucket"},
		},
		{
			name: "gcs bucket trailing slash",
			url:  "gs://my-bucket/",
			want: Spec{Provider: GCPStorageProvider, Bucket: "my-bucket"},
		},
		{
			name: "plain s3 bucket",
			url:  "s3://my-bucket",
			want: Spec{Provider: AWSStorageProvider, Bucket: "my-bucket"},
		},
		{
			name: "s3 with region",
			url:  "s3://my-bucket?region=eu-west-1",
			want: Spec{Provider: AWSStorageProvider, Bucket: "my-bucket", Region: "eu-west-1"},
		},
		{
			name: "s3-compatible endpoint with path style",
			url:  "s3://templates?endpoint=http://minio:9000&s3ForcePathStyle=true",
			want: Spec{
				Provider:     AWSStorageProvider,
				Bucket:       "templates",
				Endpoint:     "http://minio:9000",
				UsePathStyle: true,
			},
		},
		{
			name: "s3 path style false",
			url:  "s3://my-bucket?s3ForcePathStyle=false",
			want: Spec{Provider: AWSStorageProvider, Bucket: "my-bucket"},
		},
		{
			name: "s3 with sse-s3",
			url:  "s3://my-bucket?ssetype=AES256",
			want: Spec{Provider: AWSStorageProvider, Bucket: "my-bucket", ServerSideEncryption: SSEAES256},
		},
		{
			name: "s3 with sse-kms and key",
			url:  "s3://my-bucket?ssetype=aws:kms&kmskeyid=arn:aws:kms:us-east-1:111111111111:key/test",
			want: Spec{
				Provider:             AWSStorageProvider,
				Bucket:               "my-bucket",
				ServerSideEncryption: SSEAWSKMS,
				SSEKMSKeyID:          "arn:aws:kms:us-east-1:111111111111:key/test",
			},
		},
		{
			name: "s3 with sse-kms and aws managed key",
			url:  "s3://my-bucket?ssetype=aws:kms",
			want: Spec{Provider: AWSStorageProvider, Bucket: "my-bucket", ServerSideEncryption: SSEAWSKMS},
		},
		{
			name:    "s3 with unknown ssetype",
			url:     "s3://my-bucket?ssetype=aes256",
			wantErr: `invalid ssetype "aes256"`,
		},
		{
			name:    "s3 kms key without aws:kms",
			url:     "s3://my-bucket?ssetype=AES256&kmskeyid=arn:aws:kms:us-east-1:111111111111:key/test",
			wantErr: "kmskeyid requires ssetype=aws:kms",
		},
		{
			name:    "gcs rejects ssetype",
			url:     "gs://my-bucket?ssetype=AES256",
			wantErr: "does not accept query parameters",
		},
		{
			name: "azure blob container",
			url:  "azblob://my-container",
			want: Spec{Provider: AzureStorageProvider, Bucket: "my-container"},
		},
		{
			name: "azure blob container trailing slash",
			url:  "azblob://my-container/",
			want: Spec{Provider: AzureStorageProvider, Bucket: "my-container"},
		},
		{
			name:    "azblob rejects query parameters",
			url:     "azblob://my-container?endpoint=http://azurite:10000",
			wantErr: "azblob:// does not accept query parameters",
		},
		{
			name: "local filesystem",
			url:  "file:///var/lib/storage",
			want: Spec{Provider: LocalStorageProvider, BasePath: "/var/lib/storage"},
		},
		{
			name: "local filesystem localhost authority",
			url:  "file://localhost/var/lib/storage",
			want: Spec{Provider: LocalStorageProvider, BasePath: "/var/lib/storage"},
		},
		{
			name: "local filesystem relative path",
			url:  "file:.local-build",
			want: Spec{Provider: LocalStorageProvider, BasePath: ".local-build"},
		},
		{
			name: "local filesystem nested relative path",
			url:  "file:build/cache",
			want: Spec{Provider: LocalStorageProvider, BasePath: "build/cache"},
		},
		{
			name: "surrounding whitespace",
			url:  "  gs://my-bucket\n",
			want: Spec{Provider: GCPStorageProvider, Bucket: "my-bucket"},
		},
		{
			name:    "unknown scheme",
			url:     "ftp://my-bucket",
			wantErr: "unsupported scheme",
		},
		{
			name:    "missing bucket",
			url:     "s3://",
			wantErr: "missing bucket name",
		},
		{
			name:    "key prefix rejected",
			url:     "gs://my-bucket/some/prefix",
			wantErr: "key prefixes are not supported",
		},
		{
			name:    "gcs query params rejected",
			url:     "gs://my-bucket?endpoint=http://x",
			wantErr: "does not accept query parameters",
		},
		{
			name:    "unknown s3 param",
			url:     "s3://my-bucket?s3ForcePathStyle=true&pathStyle=true",
			wantErr: `unsupported query parameter "pathStyle"`,
		},
		{
			name:    "invalid path style value",
			url:     "s3://my-bucket?s3ForcePathStyle=yep",
			wantErr: "invalid s3ForcePathStyle",
		},
		{
			name:    "endpoint without scheme",
			url:     "s3://my-bucket?endpoint=minio:9000",
			wantErr: "endpoint must be an absolute http(s) URL",
		},
		{
			name:    "endpoint with credentials rejected",
			url:     "s3://my-bucket?endpoint=http://key:secret@minio:9000",
			wantErr: "credentials in URLs are not supported",
		},
		{
			name:    "credentials rejected",
			url:     "s3://key:secret@my-bucket",
			wantErr: "credentials in URLs are not supported",
		},
		{
			name:    "file with remote host",
			url:     "file://nfs-server/export",
			wantErr: "must not have a host",
		},
		{
			name:    "file without path",
			url:     "file://",
			wantErr: "requires a path",
		},
		{
			name:    "file query params rejected",
			url:     "file:///var/lib/storage?mode=fast",
			wantErr: "does not accept query parameters",
		},
		{
			name:    "file relative with query params rejected",
			url:     "file:.local-build?mode=fast",
			wantErr: "does not accept query parameters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseStorageURL(tt.url)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestParseStorageURLDoesNotLeakCredentials asserts that credentials pasted
// into a storage URL never round-trip into error messages (which propagate to
// logs and observability sinks).
func TestParseStorageURLDoesNotLeakCredentials(t *testing.T) {
	t.Parallel()

	urls := []string{
		"s3://AKIAKEY:supersecret@bucket",                    // credentials rejection
		"azblob://AKIAKEY:supersecret@bucket",                // credentials rejection
		"ftps://AKIAKEY:supersecret@bucket",                  // unsupported-scheme branch must redact too
		"s3://AKIAKEY:supersecret@bucket/prefix",             // key prefix rejection
		"s3://AKIAKEY:supersecret@bucket?bogus=1",            // unknown query parameter
		"gs://AKIAKEY:supersecret@bucket?x=1",                // gs query parameter rejection
		"s3://AKIAKEY:supersecret@bucket?endpoint=x",         // invalid endpoint
		"s3://AKIAKEY:supersecret@bucket?s3ForcePathStyle=y", // invalid bool
		"s3://AKIAKEY:supersecret@bucket\x00",                // unparseable (control char)

		// Credentials hidden inside the endpoint query parameter must not
		// leak either — via the endpoint errors or any other query-echoing
		// error (redactedURL strips userinfo from URL-valued params).
		"s3://bucket?endpoint=http://AKIAKEY:supersecret@minio:9000&bogus=1",
		"s3://bucket?endpoint=ftp://AKIAKEY:supersecret@minio:9000",
		"s3://bucket?endpoint=http://AKIAKEY:supersecret@minio:9000",
		"s3://bucket?endpoint=http://AKIAKEY:supersecret@minio:9000&s3ForcePathStyle=maybe",
	}

	for _, raw := range urls {
		_, err := ParseStorageURL(raw)
		require.Error(t, err, raw)
		assert.NotContains(t, err.Error(), "supersecret", "error must not leak the password: %s", err)
		assert.NotContains(t, err.Error(), "AKIAKEY", "error must not leak the username: %s", err)
	}
}
