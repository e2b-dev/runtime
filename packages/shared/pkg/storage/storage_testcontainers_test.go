package storage

// Shared testcontainer scaffolding for the storage server-backed tests
// (RustFS / fake-gcs-server). Split out from #3113 so the GCS server tests and
// the AWS server tests can share it.

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RustFS stands in for S3: MinIO no longer publishes container images.
const (
	s3EmulatorImage     = "rustfs/rustfs:1.0.1"
	s3EmulatorAccessKey = "rustfsadmin"
	s3EmulatorSecretKey = "rustfsadmin"
)

// s3TestBackend describes where the tests run: a real AWS bucket (endpoint
// empty) or an S3-compatible container endpoint.
type s3TestBackend struct {
	bucket   string
	endpoint string
}

// startS3Backend starts a per-test S3 emulator container and creates a bucket
// in it (same pattern as redis_utils.SetupInstance — Docker required, torn
// down via t.Cleanup). Also used by the GCS XML multipart tests, which run
// against it because it implements the S3/GCS XML multipart dialect.
func startS3Backend(t *testing.T) *s3TestBackend {
	t.Helper()

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: s3EmulatorImage,
			Env: map[string]string{
				"RUSTFS_ACCESS_KEY": s3EmulatorAccessKey,
				"RUSTFS_SECRET_KEY": s3EmulatorSecretKey,
			},
			ExposedPorts: []string{"9000/tcp"},
			// /health is liveness only; S3 returns 503 until storage, IAM and locks are up, as /health/ready reports.
			WaitingFor: wait.ForHTTP("/health/ready").WithPort("9000/tcp"),
		},
		Started: true,
	})
	// Registered before the error check: a failed startup wait still returns a running container.
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("cleanup: failed to terminate s3 emulator container: %v", err)
		}
	})
	require.NoError(t, err, "start s3 emulator container")

	host, err := container.Host(t.Context())
	require.NoError(t, err)
	port, err := container.MappedPort(t.Context(), "9000")
	require.NoError(t, err)

	backend := &s3TestBackend{
		bucket:   "s3-test-bucket",
		endpoint: fmt.Sprintf("http://%s:%s", host, port.Port()),
	}

	_, err = backend.newClient(t, nil).CreateBucket(t.Context(),
		&s3.CreateBucketInput{Bucket: aws.String(backend.bucket)})
	require.NoError(t, err, "create bucket")

	return backend
}

// newClient builds an S3 client for the backend. httpClient is optional and
// lets tests install a fault-injecting transport; optFns lets them tune
// client options such as the retryer.
func (b *s3TestBackend) newClient(t *testing.T, httpClient *http.Client, optFns ...func(*s3.Options)) *s3.Client {
	t.Helper()

	if b.endpoint != "" {
		cfg := aws.Config{
			Credentials: credentials.NewStaticCredentialsProvider(s3EmulatorAccessKey, s3EmulatorSecretKey, ""),
			Region:      "us-east-1",
		}
		if httpClient != nil {
			cfg.HTTPClient = httpClient
		}

		return s3.NewFromConfig(cfg, append([]func(*s3.Options){func(o *s3.Options) {
			o.BaseEndpoint = aws.String(b.endpoint)
			o.UsePathStyle = true
		}}, optFns...)...)
	}

	var opts []func(*config.LoadOptions) error
	if httpClient != nil {
		opts = append(opts, config.WithHTTPClient(httpClient))
	}
	cfg, err := config.LoadDefaultConfig(t.Context(), opts...)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, optFns...)
}

func testKey(name string) string {
	return fmt.Sprintf("s3-test/%d/%s", time.Now().UnixNano(), name)
}

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "input")
	require.NoError(t, os.WriteFile(path, data, 0o644))

	return path
}

func testCompressConfig() CompressConfig {
	return CompressConfig{Enabled: true, Type: CompressionLZ4.String(), FrameSizeKB: 1, MinPartSizeMB: 1, FrameEncodeWorkers: 1}
}

// countingReadCloser counts bytes read through it and reports the total to
// onClose when the body is closed. The count is read in Close (after the
// transport is done with the body) rather than after RoundTrip returns, which
// would race the transport's still-running body reads.
type countingReadCloser struct {
	rc      io.ReadCloser
	n       atomic.Int64
	onClose func(int64)
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.n.Add(int64(n))

	return n, err
}

func (c *countingReadCloser) Close() error {
	err := c.rc.Close()
	if c.onClose != nil {
		c.onClose(c.n.Load())
	}

	return err
}

// faultInjectingTransport sits below the AWS SDK (after signing): the first
// attempt of every distinct S3 request consumes the full request body and
// gets a synthetic 500 InternalError, forcing the SDK's real retry machinery
// — including body rewind via Seek — before the retry passes through to real
// S3. Per-part body sizes are recorded per attempt so a broken rewind (short
// or empty re-send) is directly observable.
type faultInjectingTransport struct {
	inner http.RoundTripper

	mu            sync.Mutex
	seen          map[string]bool
	injected      int
	partBodySizes map[string][]int64 // partNumber -> body bytes per attempt
}

func newFaultInjectingTransport() *faultInjectingTransport {
	return &faultInjectingTransport{
		inner:         http.DefaultTransport,
		seen:          make(map[string]bool),
		partBodySizes: make(map[string][]int64),
	}
}

func (f *faultInjectingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Distinct request identity: retries repeat all of these exactly.
	key := req.Method + " " + req.URL.Path + "?" + req.URL.RawQuery + " range=" + req.Header.Get("Range")
	partNum := req.URL.Query().Get("partNumber")

	f.mu.Lock()
	first := !f.seen[key]
	f.seen[key] = true
	f.mu.Unlock()

	if first {
		var consumed int64
		if req.Body != nil {
			consumed, _ = io.Copy(io.Discard, req.Body)
			req.Body.Close()
		}

		f.mu.Lock()
		f.injected++
		if partNum != "" {
			f.partBodySizes[partNum] = append(f.partBodySizes[partNum], consumed)
		}
		f.mu.Unlock()

		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Status:     "500 Internal Server Error",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body: io.NopCloser(strings.NewReader(
				`<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>injected fault</Message></Error>`)),
			Request: req,
		}, nil
	}

	if partNum != "" && req.Body != nil {
		counter := &countingReadCloser{
			rc: req.Body,
			// Record on Close, not after RoundTrip returns: the transport may
			// still be reading/closing the body when RoundTrip returns.
			onClose: func(n int64) {
				f.mu.Lock()
				f.partBodySizes[partNum] = append(f.partBodySizes[partNum], n)
				f.mu.Unlock()
			},
		}
		clone := req.Clone(req.Context())
		clone.Body = counter

		return f.inner.RoundTrip(clone)
	}

	return f.inner.RoundTrip(req)
}
