package storage

// Tests exercising gcpObject (reads, Put, Size/metadata) against a
// fake-gcs-server container over the GCS JSON API. Note production uses the
// gRPC client (NewGCP); tests use the HTTP client — no emulator speaks the
// GCS gRPC protocol. The XML multipart upload side (MultipartUploader) is
// covered in gcp_multipart_test.go against RustFS.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const fakeGCSImage = "fsouza/fake-gcs-server:1.54.0"

// emulatorRedirectTransport sends every request to the emulator's host while
// keeping the originally targeted host in the Host header, so fake-gcs-server
// can route both JSON API and XML-style (host-based) requests correctly.
type emulatorRedirectTransport struct {
	emulatorHost string
	inner        http.RoundTripper
}

func (e emulatorRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if clone.Host == "" {
		clone.Host = clone.URL.Host
	}
	clone.URL.Scheme = "http"
	clone.URL.Host = e.emulatorHost

	return e.inner.RoundTrip(clone)
}

// startFakeGCSBackend starts a fake-gcs-server container, creates a bucket,
// and returns a gcpStorage wired to it over the JSON API.
func startFakeGCSBackend(t *testing.T) *gcpStorage {
	t.Helper()

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        fakeGCSImage,
			Cmd:          []string{"-scheme", "http", "-backend", "memory"},
			ExposedPorts: []string{"4443/tcp"},
			WaitingFor:   wait.ForListeningPort("4443/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err, "start fake-gcs-server container")

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(t.Context())); err != nil {
			t.Logf("cleanup: failed to terminate fake-gcs-server container: %v", err)
		}
	})

	host, err := container.Host(t.Context())
	require.NoError(t, err)
	port, err := container.MappedPort(t.Context(), "4443")
	require.NoError(t, err)

	// fake-gcs-server routes XML-style downloads (used by NewRangeReader) by
	// Host header, so redirect connections to the emulator while preserving
	// the original storage.googleapis.com host — the approach fake-gcs-server
	// recommends for the Go client.
	httpClient := &http.Client{Transport: emulatorRedirectTransport{
		emulatorHost: fmt.Sprintf("%s:%s", host, port.Port()),
		inner:        http.DefaultTransport,
	}}

	client, err := gcs.NewClient(t.Context(), option.WithHTTPClient(httpClient))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	const bucketName = "gcs-test-bucket"
	require.NoError(t, client.Bucket(bucketName).Create(t.Context(), "test-project", nil))

	return &gcpStorage{
		client: client,
		bucket: client.Bucket(bucketName),
	}
}

// TestGCSObjectReadPath covers the gcpObject side against fake-gcs-server:
// Put with metadata, Size() preferring the uncompressed-size metadata, and
// OpenRangeReader frame-by-frame decompression of a compressed blob.
func TestGCSObjectReadPath(t *testing.T) {
	t.Parallel()

	storage := startFakeGCSBackend(t)

	const dataSize = 8 * megabyte
	data := generateSemiRandomData(dataSize)

	// Compress in memory with the shared pipeline, then store the assembled
	// blob the way the multipart path would have laid it out.
	up := &memPartUploader{}
	fullFT, checksum, err := compressStream(t.Context(), bytes.NewReader(data), defaultCfg(CompressionZstd, 4, 2*megabyte), up, 4, nil)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(data), checksum)
	ft := fullFT.Table()

	seekable, err := storage.OpenSeekable(t.Context(), testKey("gcs-read-compressed"))
	require.NoError(t, err)
	obj, ok := seekable.(*gcpObject)
	require.True(t, ok)

	metadata := ObjectMetadata{}.WithUncompressedSize(int64(dataSize))
	require.NoError(t, obj.Put(t.Context(), up.Assemble(), WithMetadata(metadata)))

	// Size() must come from the uncompressed-size metadata, not the
	// (smaller) compressed object size.
	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(dataSize), size)

	stored, err := obj.Metadata(t.Context())
	require.NoError(t, err)
	uncompressed, ok := stored.UncompressedSize()
	require.True(t, ok)
	require.Equal(t, int64(dataSize), uncompressed)

	// Read everything back through frame-aligned range reads.
	var got bytes.Buffer
	for off := int64(0); off < int64(dataSize); {
		rr, src, err := obj.OpenRangeReader(t.Context(), off, 0, ft)
		require.NoError(t, err)
		require.Equal(t, SourceGCS, src)

		n, err := got.ReadFrom(rr)
		require.NoError(t, err)
		require.Positive(t, n)
		_, err = rr.Close(t.Context())
		require.NoError(t, err)

		off += n
	}
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(got.Bytes()),
		"read-back data differs from original")
}

// TestGCSObjectStoreFileSmallUncompressed covers gcpObject.StoreFile's
// small-file path (single-shot Put, no multipart, no credentials needed)
// against fake-gcs-server.
func TestGCSObjectStoreFileSmallUncompressed(t *testing.T) {
	t.Parallel()

	storage := startFakeGCSBackend(t)

	const dataSize = 4 * megabyte // < gcpMultipartUploadChunkSize -> Put path
	data := generateSemiRandomData(dataSize)
	inputPath := writeTempFile(t, data)

	seekable, err := storage.OpenSeekable(t.Context(), testKey("gcs-storefile-small"))
	require.NoError(t, err)
	obj, ok := seekable.(*gcpObject)
	require.True(t, ok)

	fullFT, checksum, err := obj.StoreFile(t.Context(), inputPath, WithChecksumSHA256())
	require.NoError(t, err)
	require.Nil(t, fullFT, "uncompressed uploads have no frame table")
	require.Equal(t, sha256.Sum256(data), checksum)

	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(dataSize), size)

	var got bytes.Buffer
	_, err = obj.WriteTo(t.Context(), &got)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(got.Bytes()))
}
