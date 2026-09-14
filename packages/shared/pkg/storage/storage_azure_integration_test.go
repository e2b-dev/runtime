package storage

// Integration test for the Azure Blob storage backend, running against a
// per-test Azurite container (the official Azure Storage emulator) — same
// pattern as the MinIO-backed S3 tests in storage_testcontainers_test.go
// (Docker required, torn down via t.Cleanup).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const azuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.35.0"

// azuriteAccountKey is Azurite's well-known dev account key (public, not a
// secret — it only ever authenticates against the local emulator).
const azuriteAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

// startAzuriteBackend starts a per-test Azurite container and returns a
// connection string for its blob endpoint.
func startAzuriteBackend(t *testing.T) string {
	t.Helper()

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: azuriteImage,
			// --skipApiVersionCheck: the pinned azblob SDK negotiates a service
			// API version newer than the emulator's validation list; Azurite
			// documents this flag for exactly that case.
			Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck"},
			ExposedPorts: []string{"10000/tcp"},
			WaitingFor:   wait.ForListeningPort("10000/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err, "start azurite container")

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(t.Context())); err != nil {
			t.Logf("cleanup: failed to terminate azurite container: %v", err)
		}
	})

	host, err := container.Host(t.Context())
	require.NoError(t, err)
	port, err := container.MappedPort(t.Context(), "10000")
	require.NoError(t, err)

	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=%s;BlobEndpoint=http://%s:%s/devstoreaccount1;",
		azuriteAccountKey, host, port.Port(),
	)
}

//nolint:paralleltest // t.Setenv
func TestAzureIntegration(t *testing.T) {
	connectionString := startAzuriteBackend(t)
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", connectionString)

	ctx := t.Context()

	// Container names must be 3-63 lowercase alphanumeric/hyphen characters.
	containerName := fmt.Sprintf("e2b-it-%d", time.Now().UnixNano())

	// Create the container with a raw SDK client; the provider under test
	// assumes the container already exists.
	adminClient, err := azblob.NewClientFromConnectionString(connectionString, nil)
	require.NoError(t, err)

	_, err = adminClient.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err, "failed to create test container %q", containerName)
	t.Cleanup(func() {
		_, err := adminClient.DeleteContainer(context.WithoutCancel(t.Context()), containerName, nil)
		if err != nil && !bloberror.HasCode(err, bloberror.ContainerNotFound) {
			t.Logf("failed to delete test container %q: %v", containerName, err)
		}
	})

	// Build the provider through the public entry points: URL parse + factory.
	spec, err := ParseStorageURL(fmt.Sprintf("azblob://%s", containerName))
	require.NoError(t, err)
	require.Equal(t, AzureStorageProvider, spec.Provider)

	provider, err := NewProvider(ctx, spec)
	require.NoError(t, err)
	require.IsType(t, (*azureStorage)(nil), provider)

	t.Run("PutExistsWriteToRoundTrip", func(t *testing.T) {
		blob, err := provider.OpenBlob(ctx, "round-trip/blob.bin")
		require.NoError(t, err)

		exists, err := blob.Exists(ctx)
		require.NoError(t, err)
		assert.False(t, exists, "blob must not exist before Put")

		content := []byte("hello from the azure integration test \x00\x01\x02")
		require.NoError(t, blob.Put(ctx, content))

		exists, err = blob.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, exists, "blob must exist after Put")

		var buf bytes.Buffer
		n, err := blob.WriteTo(ctx, &buf)
		require.NoError(t, err)
		assert.Equal(t, int64(len(content)), n)
		assert.Equal(t, content, buf.Bytes())
	})

	t.Run("MetadataRoundTrip", func(t *testing.T) {
		blob, err := provider.OpenBlob(ctx, "metadata/blob.bin")
		require.NoError(t, err)

		// ObjectMetadataSoftDeleted ("storage-index-soft-deleted") exercises
		// the hyphen -> "__" encoding against a real server, which also
		// case-normalizes metadata keys. ObjectMetadataTeamID ("team_id")
		// checks that a single pre-existing underscore survives decoding.
		metadata := ObjectMetadata{
			ObjectMetadataSoftDeleted: "reclaim:action-123",
			ObjectMetadataTeamID:      "team-42",
		}
		require.NoError(t, blob.Put(ctx, []byte("with metadata"), WithMetadata(metadata)))

		got, err := BlobCustomMetadata(ctx, blob)
		require.NoError(t, err)
		assert.Equal(t, metadata, got)
	})

	t.Run("StoreFileRangeReads", func(t *testing.T) {
		// 25 MB > 2x the 10 MB azureUploadBlockSize, forcing a staged
		// multi-block upload (Put Block + Put Block List) rather than a single
		// Put Blob, and giving us two block boundaries to read across.
		const fileSize = 25 * 1024 * 1024
		require.Greater(t, fileSize, 2*azureUploadBlockSize, "file must span multiple upload blocks")

		content := make([]byte, fileSize)
		_, err := rand.New(rand.NewSource(42)).Read(content)
		require.NoError(t, err)

		localPath := filepath.Join(t.TempDir(), "seekable.bin")
		require.NoError(t, os.WriteFile(localPath, content, 0o644))

		seekable, err := provider.OpenSeekable(ctx, "seekable/blob.bin")
		require.NoError(t, err)

		_, _, err = seekable.StoreFile(ctx, localPath)
		require.NoError(t, err)

		size, err := seekable.Size(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(fileSize), size)

		ranges := []struct {
			name string
			off  int64
			len  int64
		}{
			{name: "first byte", off: 0, len: 1},
			{name: "last byte", off: fileSize - 1, len: 1},
			{name: "spanning first block boundary", off: azureUploadBlockSize - 64, len: 128},
			{name: "spanning second block boundary", off: 2*azureUploadBlockSize - 1000, len: 2000},
			{name: "mid-block chunk", off: 5 * 1024 * 1024, len: 256 * 1024},
		}

		for _, r := range ranges {
			t.Run(r.name, func(t *testing.T) {
				reader, source, err := seekable.OpenRangeReader(ctx, r.off, r.len, nil)
				require.NoError(t, err)
				assert.Equal(t, SourceAzure, source)

				got, err := io.ReadAll(reader)
				require.NoError(t, err)

				_, err = reader.Close(ctx)
				require.NoError(t, err)

				assert.Equal(t, content[r.off:r.off+r.len], got, "range %d+%d must match the source file", r.off, r.len)
			})
		}
	})

	t.Run("StoreFileCompressedRoundTrip", func(t *testing.T) {
		// Exercises azurePartUploader (Put Block / Put Block List via
		// storeFileCompressed) and the compressed read path (frame lookup +
		// decompression over a range reader).
		const fileSize = 8 * 1024 * 1024

		content := make([]byte, fileSize)
		_, err := rand.New(rand.NewSource(7)).Read(content)
		require.NoError(t, err)
		// Zero a stretch so the file actually compresses.
		copy(content[1024*1024:5*1024*1024], make([]byte, 4*1024*1024))

		localPath := filepath.Join(t.TempDir(), "compressed.bin")
		require.NoError(t, os.WriteFile(localPath, content, 0o644))

		seekable, err := provider.OpenSeekable(ctx, "seekable/compressed.bin")
		require.NoError(t, err)

		cfg := CompressConfig{
			Enabled:            true,
			Type:               CompressionLZ4.String(),
			EncoderConcurrency: 1,
			FrameEncodeWorkers: 2,
			FrameSizeKB:        512,
			MinPartSizeMB:      1,
		}
		fullTable, _, err := seekable.StoreFile(ctx, localPath, WithCompressConfig(cfg))
		require.NoError(t, err)
		require.NotNil(t, fullTable)

		frameTable := fullTable.Table()

		// OpenRangeReader requires frame-aligned offsets on a compressed
		// object (see LocateCompressed); align each probe via
		// LocateUncompressed, the same dance the read path does.
		probes := []struct {
			name string
			off  int64
		}{
			{name: "first frame", off: 0},
			{name: "zeroed stretch", off: 2 * 1024 * 1024},
			{name: "tail", off: fileSize - 4096},
		}

		for _, p := range probes {
			t.Run(p.name, func(t *testing.T) {
				frame, err := frameTable.LocateUncompressed(p.off)
				require.NoError(t, err)

				reader, source, err := seekable.OpenRangeReader(ctx, frame.Offset, int64(frame.Length), frameTable)
				require.NoError(t, err)
				assert.Equal(t, SourceAzure, source)

				got, err := io.ReadAll(reader)
				require.NoError(t, err)

				_, err = reader.Close(ctx)
				require.NoError(t, err)

				assert.Equal(t, content[frame.Offset:frame.Offset+int64(frame.Length)], got, "frame at %d must match the source file", p.off)
			})
		}
	})

	t.Run("StoreFileCompressedEmpty", func(t *testing.T) {
		// Zero-byte input makes compressStream emit one empty part (S3 and the
		// GCS XML API reject zero-part completes). Azure is the mirror image:
		// Put Block rejects an empty block but an empty block list is a legal
		// empty blob, so the uploader must drop the part instead of staging it.
		localPath := filepath.Join(t.TempDir(), "empty.bin")
		require.NoError(t, os.WriteFile(localPath, nil, 0o644))

		seekable, err := provider.OpenSeekable(ctx, "seekable/empty.bin")
		require.NoError(t, err)

		cfg := CompressConfig{
			Enabled:            true,
			Type:               CompressionLZ4.String(),
			EncoderConcurrency: 1,
			FrameEncodeWorkers: 1,
			FrameSizeKB:        512,
			MinPartSizeMB:      1,
		}
		_, _, err = seekable.StoreFile(ctx, localPath, WithCompressConfig(cfg))
		require.NoError(t, err)

		size, err := seekable.Size(ctx)
		require.NoError(t, err)
		assert.Zero(t, size)
	})

	t.Run("RangeReaderZeroLength", func(t *testing.T) {
		// In azblob a Count of 0 means CountToEnd; the backend must reject it
		// instead of silently returning the whole blob tail.
		seekable, err := provider.OpenSeekable(ctx, "seekable/blob.bin")
		require.NoError(t, err)

		reader, _, err := seekable.OpenRangeReader(ctx, 0, 0, nil)
		require.ErrorContains(t, err, "invalid range length")
		assert.Nil(t, reader)
	})

	t.Run("DeleteObjectsWithPrefix", func(t *testing.T) {
		putBlob := func(path string) {
			t.Helper()
			blob, err := provider.OpenBlob(ctx, path)
			require.NoError(t, err)
			require.NoError(t, blob.Put(ctx, []byte("delete-me")))
		}

		var doomed, kept []string
		for i := range 7 {
			doomed = append(doomed, fmt.Sprintf("prefix-del/obj-%d", i))
		}
		for i := range 2 {
			kept = append(kept, fmt.Sprintf("prefix-keep/obj-%d", i))
		}
		for _, path := range append(append([]string{}, doomed...), kept...) {
			putBlob(path)
		}

		require.NoError(t, provider.DeleteObjectsWithPrefix(ctx, "prefix-del/"))

		for _, path := range doomed {
			blob, err := provider.OpenBlob(ctx, path)
			require.NoError(t, err)
			exists, err := blob.Exists(ctx)
			require.NoError(t, err)
			assert.False(t, exists, "blob %q must be deleted", path)
		}
		for _, path := range kept {
			blob, err := provider.OpenBlob(ctx, path)
			require.NoError(t, err)
			exists, err := blob.Exists(ctx)
			require.NoError(t, err)
			assert.True(t, exists, "sibling blob %q must survive", path)
		}

		// An empty prefix would delete the entire container; it must error out.
		require.ErrorContains(t, provider.DeleteObjectsWithPrefix(ctx, ""), "empty prefix")
	})

	t.Run("UploadSignedURLRequiresTheReturnedHeaders", func(t *testing.T) {
		path := "signed/upload.bin"
		body := []byte("signed-upload-body")

		upload, err := provider.UploadSignedURL(ctx, path, time.Hour)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"x-ms-blob-type": "BlockBlob"}, upload.Headers)

		// Azurite rejects the header-less PUT with a bare 400 where real Azure names MissingRequiredHeader.
		status, _, err := putSignedURL(ctx, t, upload.URL, nil, body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, status)

		status, responseBody, err := putSignedURL(ctx, t, upload.URL, upload.Headers, body)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, status, responseBody)

		object, err := provider.OpenBlob(ctx, path)
		require.NoError(t, err)
		var read bytes.Buffer
		_, err = object.WriteTo(ctx, &read)
		require.NoError(t, err)
		assert.Equal(t, body, read.Bytes())
	})

	t.Run("DeleteNonexistentIsIdempotent", func(t *testing.T) {
		object, err := provider.OpenSeekable(ctx, "never/created.bin")
		require.NoError(t, err)

		deleter, ok := object.(interface {
			Delete(ctx context.Context) error
		})
		require.True(t, ok, "azure seekable must support Delete")
		assert.NoError(t, deleter.Delete(ctx), "deleting a missing blob must succeed (S3 DeleteObject semantics)")
	})
}

// A missing container must NOT look like a missing object. Mapping ContainerNotFound to
// ErrObjectNotExist makes a misconfigured cluster indistinguishable from empty storage:
// the orchestrator reads "no snapshot" and starts fresh instead of failing, so a typo in
// the container name looks like normal operation. AWS surfaces NoSuchBucket and GCP
// ErrBucketNotExist for the same reason.
//
//nolint:paralleltest // t.Setenv
func TestAzureIntegrationMissingContainerIsNotObjectNotExist(t *testing.T) {
	connectionString := startAzuriteBackend(t)
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", connectionString)

	ctx := t.Context()

	// Deliberately never created.
	spec, err := ParseStorageURL(fmt.Sprintf("azblob://e2b-absent-%d", time.Now().UnixNano()))
	require.NoError(t, err)

	provider, err := NewProvider(ctx, spec)
	require.NoError(t, err, "construction must not probe the container")

	blob, err := provider.OpenBlob(ctx, "some/blob")
	require.NoError(t, err)

	// The sharpest case: Exists must not answer "no, and that is fine".
	t.Run("Exists", func(t *testing.T) {
		_, err := blob.Exists(ctx)
		require.Error(t, err, "a missing container must be an error, not a confident false")
		assert.NotErrorIs(t, err, ErrObjectNotExist)
	})

	t.Run("WriteTo", func(t *testing.T) {
		_, err := blob.WriteTo(ctx, io.Discard)
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrObjectNotExist)
	})

	t.Run("Size", func(t *testing.T) {
		seekable, err := provider.OpenSeekable(ctx, "some/blob")
		require.NoError(t, err)

		_, err = seekable.Size(ctx)
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrObjectNotExist)
	})
}

// putSignedURL PUTs body to a signed upload URL with exactly the given headers.
func putSignedURL(ctx context.Context, t *testing.T, url string, headers map[string]string, body []byte) (int, string, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}

	req.ContentLength = int64(len(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer res.Body.Close()

	responseBody, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, "", err
	}

	return res.StatusCode, string(responseBody), nil
}
