package storage

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sseHeader      = "X-Amz-Server-Side-Encryption"
	sseKMSKeyIDHdr = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	testKMSKeyID   = "arn:aws:kms:us-east-1:111111111111:key/test"
)

var sseCases = []struct {
	name     string
	spec     Spec
	wantAlg  string
	wantKey  string
	wantHdrs map[string]string
}{
	{
		name: "none",
		spec: Spec{},
	},
	{
		name:     "sse-s3",
		spec:     Spec{ServerSideEncryption: SSEAES256},
		wantAlg:  SSEAES256,
		wantHdrs: map[string]string{sseHeader: SSEAES256},
	},
	{
		name:     "sse-kms with key",
		spec:     Spec{ServerSideEncryption: SSEAWSKMS, SSEKMSKeyID: testKMSKeyID},
		wantAlg:  SSEAWSKMS,
		wantKey:  testKMSKeyID,
		wantHdrs: map[string]string{sseHeader: SSEAWSKMS, sseKMSKeyIDHdr: testKMSKeyID},
	},
}

// sseRecorder captures the SSE headers of every write request the fake S3
// server sees, keyed by the S3 operation.
type sseRecorder struct {
	mu   sync.Mutex
	seen map[string]http.Header
}

func (r *sseRecorder) record(op string, h http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string]http.Header{}
	}
	r.seen[op] = h.Clone()
}

func (r *sseRecorder) assertSSE(t *testing.T, op, wantAlg, wantKey string) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.seen[op]
	require.True(t, ok, "no %s request recorded", op)
	assert.Equal(t, wantAlg, h.Get(sseHeader), "%s: %s", op, sseHeader)
	assert.Equal(t, wantKey, h.Get(sseKMSKeyIDHdr), "%s: %s", op, sseKMSKeyIDHdr)
}

func newSSETestStorage(t *testing.T, spec Spec, rec *sseRecorder) *awsStorage {
	t.Helper()

	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
			rec.record("CreateMultipartUpload", r.Header)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>sse-upload-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber="):
			w.Header().Set("ETag", `"etag1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=sse-upload-id"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-object</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodPut:
			rec.record("PutObject", r.Header)
			w.Header().Set("ETag", `"put-etag"`)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected AWS request: %s %s", r.Method, r.URL.String())
		}
	})

	spec.Bucket = testBucketName

	return &awsStorage{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucketName:    spec.Bucket,
		sse:           newAWSSSE(spec),
	}
}

func TestAWSPutRequestsServerSideEncryption(t *testing.T) {
	t.Parallel()

	for _, tc := range sseCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &sseRecorder{}
			obj, err := newSSETestStorage(t, tc.spec, rec).OpenBlob(t.Context(), testObjectName)
			require.NoError(t, err)

			require.NoError(t, obj.Put(t.Context(), []byte("payload")))
			rec.assertSSE(t, "PutObject", tc.wantAlg, tc.wantKey)
		})
	}
}

func TestAWSUncompressedStoreFileRequestsServerSideEncryption(t *testing.T) {
	t.Parallel()

	for _, tc := range sseCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &sseRecorder{}
			obj, err := newSSETestStorage(t, tc.spec, rec).OpenSeekable(t.Context(), testObjectName)
			require.NoError(t, err)

			// Below the multipart part size, manager.Uploader issues one PutObject.
			_, _, err = obj.StoreFile(t.Context(), writeTempFile(t, []byte("small-uncompressed-file")))
			require.NoError(t, err)
			rec.assertSSE(t, "PutObject", tc.wantAlg, tc.wantKey)
		})
	}
}

func TestAWSCompressedStoreFileRequestsServerSideEncryption(t *testing.T) {
	t.Parallel()

	for _, tc := range sseCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &sseRecorder{}
			obj, err := newSSETestStorage(t, tc.spec, rec).OpenSeekable(t.Context(), testObjectName)
			require.NoError(t, err)

			input := writeTempFile(t, []byte(strings.Repeat("compressible-data", 1024)))
			_, _, err = obj.StoreFile(t.Context(), input, WithCompressConfig(testCompressConfig()))
			require.NoError(t, err)
			rec.assertSSE(t, "CreateMultipartUpload", tc.wantAlg, tc.wantKey)
		})
	}
}

func TestAWSUploadSignedURLReturnsServerSideEncryptionHeaders(t *testing.T) {
	t.Parallel()

	for _, tc := range sseCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider := newSSETestStorage(t, tc.spec, &sseRecorder{})
			upload, err := provider.UploadSignedURL(t.Context(), testObjectName, time.Hour)
			require.NoError(t, err)

			// The client replays Headers verbatim; they must match what was signed.
			assert.Equal(t, tc.wantHdrs, upload.Headers)

			u, err := url.Parse(upload.URL)
			require.NoError(t, err)
			signed := strings.Split(u.Query().Get("X-Amz-SignedHeaders"), ";")
			for name := range tc.wantHdrs {
				assert.Contains(t, signed, strings.ToLower(name), "header must be signed, not hoisted into the query")
				assert.Empty(t, u.Query().Get(name), "%s must not be a query parameter", name)
			}
		})
	}
}
