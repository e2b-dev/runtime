package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAWSPartUploader_PartUploaderContract(t *testing.T) {
	t.Parallel()

	testPartUploaderContract(t, partUploaderTestAdapter{
		abortsOnClose: true,
		new: func(t *testing.T, recorder *partUploaderRecorder) partUploader {
			t.Helper()

			client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
					recorder.started = true
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>contract-upload-id</UploadId></InitiateMultipartUploadResult>`))
				case r.Method == http.MethodPut:
					recordUploadedPart(t, recorder, w, r)
				case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=contract-upload-id"):
					var complete completeMultipartUploadRequest
					if err := xml.NewDecoder(r.Body).Decode(&complete); err != nil {
						t.Fatalf("decode complete upload request: %v", err)
					}
					recorder.completed = true
					if len(complete.Parts) == 2 && (complete.Parts[0].PartNumber != 1 || complete.Parts[1].PartNumber != 2) {
						t.Fatalf("complete upload parts not sorted: %+v", complete.Parts)
					}
					for _, p := range complete.Parts {
						if p.ChecksumCRC32 == "" {
							t.Fatalf("complete upload part %d missing CRC32 checksum", p.PartNumber)
						}
					}
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-object</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
				case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId=contract-upload-id"):
					recorder.aborted = true
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Fatalf("unexpected AWS multipart request: %s %s", r.Method, r.URL.String())
				}
			})

			return &awsPartUploader{client: client, bucketName: testBucketName, objectName: testObjectName}
		},
	})
}

func TestAWSCompressedStoreFileSetsUncompressedSizeMetadataAndSizeUsesIt(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, []byte(strings.Repeat("compressible-data", 1024)))
	inputSize := int64(len(strings.Repeat("compressible-data", 1024)))
	var metadata map[string]string

	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
			metadata = s3MetadataFromHeaders(r.Header)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>metadata-upload-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber="):
			w.Header().Set("ETag", `"etag1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=metadata-upload-id"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-object</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "17")
			if size := metadata[ObjectMetadataUncompressedSize]; size != "" {
				w.Header().Set("x-amz-meta-"+ObjectMetadataUncompressedSize, size)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected AWS request: %s %s", r.Method, r.URL.String())
		}
	})

	obj := &awsObject{client: client, bucketName: testBucketName, path: testObjectName}
	_, _, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(testCompressConfig()))
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(inputSize, 10), metadata[ObjectMetadataUncompressedSize])

	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Equal(t, inputSize, size)
}

func TestAWSCompressedStoreFileAbortsMultipartUploadOnFailure(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, []byte(strings.Repeat("compressible-data", 1024)))
	var aborted atomic.Bool

	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>abort-upload-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber="):
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId=abort-upload-id"):
			aborted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected AWS request: %s %s", r.Method, r.URL.String())
		}
	})

	obj := &awsObject{client: client, bucketName: testBucketName, path: testObjectName}
	_, _, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(testCompressConfig()))
	require.Error(t, err)
	require.True(t, aborted.Load(), "failed compressed upload should abort the multipart upload")
}

func TestAWSCompressedStoreFileRoundTripsThroughOpenRangeReader(t *testing.T) {
	t.Parallel()

	input := []byte(strings.Repeat("compressible-data", 1024))
	inputPath := writeTempFile(t, input)

	var mu sync.Mutex
	parts := map[int][]byte{}
	var object []byte

	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>rt-upload-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && r.URL.Query().Get("partNumber") != "":
			n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
			assert.NoError(t, err)
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			mu.Lock()
			parts[n] = body
			mu.Unlock()
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=rt-upload-id"):
			mu.Lock()
			for _, n := range slices.Sorted(maps.Keys(parts)) {
				object = append(object, parts[n]...)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-object</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodGet:
			var from, to int64
			_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to)
			assert.NoError(t, err)
			mu.Lock()
			frame := object[from : to+1]
			mu.Unlock()
			w.WriteHeader(http.StatusPartialContent)
			w.Write(frame)
		default:
			t.Fatalf("unexpected AWS request: %s %s", r.Method, r.URL.String())
		}
	})

	obj := &awsObject{client: client, bucketName: testBucketName, path: testObjectName}
	ft, _, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(testCompressConfig()))
	require.NoError(t, err)

	var got bytes.Buffer
	for off := int64(0); off < int64(len(input)); {
		rr, src, err := obj.OpenRangeReader(t.Context(), off, 0, ft.Table())
		require.NoError(t, err)
		require.Equal(t, SourceAWS, src)

		n, err := got.ReadFrom(rr)
		require.NoError(t, err)
		require.Positive(t, n)
		_, err = rr.Close(t.Context())
		require.NoError(t, err)

		off += n
	}
	require.Equal(t, input, got.Bytes())
}

func TestAWSCompressedStoreFileEmptyFile(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, nil)
	var partBodies []string
	var completed atomic.Bool

	client := newTestS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads=":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>empty-upload-id</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && r.URL.Query().Get("partNumber") != "":
			partBodies = append(partBodies, readAllString(t, r.Body))
			w.Header().Set("ETag", `"etag1"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=empty-upload-id"):
			completed.Store(true)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-object</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
		default:
			t.Fatalf("unexpected AWS request: %s %s", r.Method, r.URL.String())
		}
	})

	obj := &awsObject{client: client, bucketName: testBucketName, path: testObjectName}
	ft, _, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(testCompressConfig()))
	require.NoError(t, err)
	require.True(t, completed.Load(), "empty compressed upload should complete the multipart upload")
	require.Equal(t, []string{""}, partBodies, "should ship exactly one empty part")
	require.Equal(t, 0, ft.Table().NumFrames())
}

type completeMultipartUploadRequest struct {
	Parts []struct {
		PartNumber    int    `xml:"PartNumber"`
		ChecksumCRC32 string `xml:"ChecksumCRC32"`
	} `xml:"Part"`
}

func newTestS3Client(t *testing.T, handler http.HandlerFunc) *s3.Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return s3.NewFromConfig(aws.Config{
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		HTTPClient:   server.Client(),
	}, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func s3MetadataFromHeaders(h http.Header) map[string]string {
	metadata := make(map[string]string)
	for key, values := range h {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") && len(values) > 0 {
			metadata[strings.TrimPrefix(strings.ToLower(key), "x-amz-meta-")] = values[0]
		}
	}

	return metadata
}

// ---------------------------------------------------------------------------
// Tests below exercise awsObject and awsPartUploader against a real S3 server
// (TestS3*), unlike the fake-httptest tests above.
//
// By default each test starts its own MinIO container via testcontainers
// (same pattern as pkg/redis: Docker is required, teardown via t.Cleanup so
// it works with TESTCONTAINERS_RYUK_DISABLED=true in CI). These run with the
// regular unit test suite.
//
// Setting E2B_LIVE_S3_BUCKET switches the same tests to a real AWS bucket
// (credentials from the standard SDK chain: AWS_PROFILE, env vars, SSO, ...):
//
//	AWS_PROFILE=<profile> E2B_LIVE_S3_BUCKET=<bucket> \
//	  go test ./pkg/storage -run TestS3 -v -timeout 30m
//
// MinIO reimplements S3 semantics (5 MiB part minimum, multipart lifecycle,
// CRC32 checksums) with high fidelity, but it is not AWS: real-S3 quirks like
// metadata-key normalization and checksum-trailer validation should still be
// verified against a real bucket before relying on them.
// ---------------------------------------------------------------------------

const liveBucketEnv = "E2B_LIVE_S3_BUCKET"

// testBackend returns the S3 backend for a test: the real AWS bucket from
// E2B_LIVE_S3_BUCKET if set, otherwise a per-test MinIO container.
func testBackend(t *testing.T) *s3TestBackend {
	t.Helper()

	if bucket := os.Getenv(liveBucketEnv); bucket != "" {
		return &s3TestBackend{bucket: bucket}
	}

	return startMinioBackend(t)
}

// object wraps an awsObject on the backend and deletes it on cleanup.
func (b *s3TestBackend) object(t *testing.T, client *s3.Client, key string) *awsObject {
	t.Helper()

	obj := &awsObject{
		client:     client,
		bucketName: b.bucket,
		path:       key,
	}

	t.Cleanup(func() {
		// t.Context() is done by cleanup time; use a fresh one.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if err := obj.Delete(ctx); err != nil {
			t.Logf("cleanup: failed to delete s3://%s/%s: %v", b.bucket, key, err)
		}
	})

	return obj
}

// testS3Object is the common case: backend + default client + one object.
func testS3Object(t *testing.T, key string) *awsObject {
	t.Helper()

	backend := testBackend(t)

	return backend.object(t, backend.newClient(t, nil), key)
}

// TestS3CompressedRoundTrip stores a compressed file via the real
// multipart path (multiple >=5MiB parts), verifies the uncompressed-size
// metadata round-trips through real S3 HeadObject, and reads all data back
// through OpenRangeReader frame decompression.
func TestS3CompressedRoundTrip(t *testing.T) {
	t.Parallel()

	codecs := []struct {
		codec CompressionType
		level int
	}{
		{CompressionZstd, 2},
		{CompressionLZ4, 0},
	}

	for _, tc := range codecs {
		t.Run(tc.codec.String(), func(t *testing.T) {
			t.Parallel()

			const dataSize = 32 * megabyte
			data := generateSemiRandomData(dataSize)
			inputPath := writeTempFile(t, data)

			obj := testS3Object(t, testKey("compressed-"+tc.codec.String()))

			cfg := CompressConfig{
				Enabled:            true,
				Type:               tc.codec.String(),
				Level:              tc.level,
				FrameSizeKB:        2 * 1024, // 2 MiB frames, production default
				MinPartSizeMB:      5,        // S3 minimum -> forces multiple parts
				FrameEncodeWorkers: 4,
				EncoderConcurrency: 1,
			}

			fullFT, checksum, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(cfg))
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(data), checksum)

			ft := fullFT.Table()
			require.Equal(t, dataSize/(2*megabyte), ft.NumFrames())
			require.Equal(t, int64(dataSize), ft.UncompressedSize())
			require.Less(t, ft.CompressedSize(), int64(dataSize), "semi-random data should compress")
			t.Logf("compressed %d -> %d bytes (ratio %.2f), %d frames",
				dataSize, ft.CompressedSize(),
				float64(ft.CompressedSize())/float64(dataSize), ft.NumFrames())

			// Size() must come from the uncompressed-size metadata, not the
			// (smaller) object Content-Length. Real S3 lowercases metadata
			// keys, which the fake server can't fully replicate.
			size, err := obj.Size(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(dataSize), size)

			// Read everything back through frame-aligned range reads.
			var got bytes.Buffer
			for off := int64(0); off < int64(dataSize); {
				rr, src, err := obj.OpenRangeReader(t.Context(), off, 0, ft)
				require.NoError(t, err)
				require.Equal(t, SourceAWS, src)

				n, err := got.ReadFrom(rr)
				require.NoError(t, err)
				require.Positive(t, n)
				_, err = rr.Close(t.Context())
				require.NoError(t, err)

				off += n
			}
			require.Equal(t, len(data), got.Len())
			require.Equal(t, sha256.Sum256(data), sha256.Sum256(got.Bytes()),
				"read-back data differs from original")
		})
	}
}

// TestS3PartUploaderContract drives awsPartUploader directly against
// real S3: out-of-order part numbers, a multi-slice part body (exercises
// multiSliceReader's Seek for SDK payload hashing), CRC32 part checksums, and
// ordered reassembly on Complete. Real S3 enforces the 5 MiB non-final part
// minimum and validates checksums, which the fake server does not.
func TestS3PartUploaderContract(t *testing.T) {
	t.Parallel()

	obj := testS3Object(t, testKey("part-uploader-contract"))

	// Part 1 (non-final) must be >= 5 MiB on real S3; split it into two
	// slices to exercise the multi-slice streaming path. Part 2 (final) is
	// deliberately tiny.
	part1a := bytes.Repeat([]byte{0xA1}, 3*megabyte)
	part1b := bytes.Repeat([]byte{0xB2}, 2*megabyte+512)
	part2 := []byte("final-part-tail")

	up := &awsPartUploader{client: obj.client, bucketName: obj.bucketName, objectName: obj.path}
	require.NoError(t, up.Start(t.Context()))
	// Upload out of order: final part first.
	require.NoError(t, up.UploadPart(t.Context(), 2, part2))
	require.NoError(t, up.UploadPart(t.Context(), 1, part1a, part1b))
	require.NoError(t, up.Complete(t.Context()))
	require.NoError(t, up.Close(), "Close after Complete must not abort")

	want := slices.Concat(part1a, part1b, part2)
	var got bytes.Buffer
	n, err := obj.WriteTo(t.Context(), &got)
	require.NoError(t, err)
	require.Equal(t, int64(len(want)), n)
	require.Equal(t, sha256.Sum256(want), sha256.Sum256(got.Bytes()),
		"parts must reassemble in part-number order, not upload order")
}

// TestS3PartUploaderAbortOnClose verifies Close on an incomplete upload
// aborts it on real S3: no object is created and no orphaned multipart upload
// (which would accrue storage costs) is left behind.
func TestS3PartUploaderAbortOnClose(t *testing.T) {
	t.Parallel()

	obj := testS3Object(t, testKey("part-uploader-abort"))

	up := &awsPartUploader{client: obj.client, bucketName: obj.bucketName, objectName: obj.path}
	require.NoError(t, up.Start(t.Context()))
	require.NoError(t, up.UploadPart(t.Context(), 1, []byte("abandoned-part")))
	require.NoError(t, up.Close())

	exists, err := obj.Exists(t.Context())
	require.NoError(t, err)
	require.False(t, exists, "aborted upload must not create an object")

	list, err := obj.client.ListMultipartUploads(t.Context(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(obj.bucketName),
		Prefix: aws.String(obj.path),
	})
	require.NoError(t, err)
	require.Empty(t, list.Uploads, "abort must leave no orphaned multipart upload")
}

// TestS3CompressedEmptyFile verifies real S3 accepts the single empty
// part that the compressed path ships for zero-byte inputs (the 5 MiB part
// minimum does not apply to the final part).
func TestS3CompressedEmptyFile(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, nil)
	obj := testS3Object(t, testKey("compressed-empty"))

	cfg := CompressConfig{
		Enabled:            true,
		Type:               CompressionZstd.String(),
		Level:              2,
		FrameSizeKB:        2 * 1024,
		MinPartSizeMB:      5,
		FrameEncodeWorkers: 4,
		EncoderConcurrency: 1,
	}

	fullFT, checksum, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(cfg))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(nil), checksum)
	require.Equal(t, 0, fullFT.Table().NumFrames())

	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Zero(t, size)
}

// TestS3CompressedRetryOnTransientFailure fails the first attempt of
// every S3 request (CreateMultipartUpload, each UploadPart, Complete, and the
// read-back HEAD/GETs) with an injected 500 and verifies the whole
// compressed upload/download cycle still succeeds via SDK retries against
// real S3. The per-part body byte counts prove the multi-slice body was fully
// rewound and re-sent, and the final SHA-256 catches any rewind corruption.
func TestS3CompressedRetryOnTransientFailure(t *testing.T) {
	t.Parallel()

	const dataSize = 32 * megabyte
	data := generateSemiRandomData(dataSize)
	inputPath := writeTempFile(t, data)

	ft := newFaultInjectingTransport()
	backend := testBackend(t)
	// Cap retry backoff so the injected failures don't stall the unit suite
	// on the SDK's default exponential backoff. Rewind correctness — the
	// point of this test — is unaffected by backoff timing.
	client := backend.newClient(t, &http.Client{Transport: ft}, func(o *s3.Options) {
		o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxBackoff = 50 * time.Millisecond
		})
	})
	obj := backend.object(t, client, testKey("retry-transient"))

	// lz4 compresses semi-random data to ~53% -> ~17 MB -> 4 parts at the
	// 5 MiB minimum, so several UploadPart retries are exercised.
	compCfg := CompressConfig{
		Enabled:            true,
		Type:               CompressionLZ4.String(),
		FrameSizeKB:        2 * 1024,
		MinPartSizeMB:      5,
		FrameEncodeWorkers: 4,
		EncoderConcurrency: 1,
	}

	fullFT, checksum, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(compCfg))
	require.NoError(t, err, "upload must survive one injected 500 per request")
	require.Equal(t, sha256.Sum256(data), checksum)

	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(dataSize), size)

	table := fullFT.Table()
	var got bytes.Buffer
	for off := int64(0); off < int64(dataSize); {
		rr, _, err := obj.OpenRangeReader(t.Context(), off, 0, table)
		require.NoError(t, err)

		n, err := got.ReadFrom(rr)
		require.NoError(t, err)
		require.Positive(t, n)
		_, err = rr.Close(t.Context())
		require.NoError(t, err)

		off += n
	}
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(got.Bytes()),
		"read-back after injected faults differs from original")

	ft.mu.Lock()
	defer ft.mu.Unlock()

	require.NotEmpty(t, ft.partBodySizes)
	require.Greater(t, ft.injected, len(ft.partBodySizes),
		"should have injected faults beyond UploadPart (create/complete/reads)")
	t.Logf("injected %d faults across %d distinct requests (%d parts)",
		ft.injected, len(ft.seen), len(ft.partBodySizes))

	for part, sizes := range ft.partBodySizes {
		require.Len(t, sizes, 2, "part %s: expected exactly one failed and one successful attempt", part)
		require.Positive(t, sizes[0], "part %s: injected attempt consumed no body", part)
		require.Equal(t, sizes[0], sizes[1],
			"part %s: retry sent %d bytes but first attempt sent %d — body rewind is broken",
			part, sizes[1], sizes[0])
	}
}

// TestS3LargeCompressedUpload is an opt-in scale test: set
// E2B_LIVE_S3_LARGE_MB (e.g. 2048) to upload that much data through the
// compressed multipart path, producing hundreds of real parts. Verifies
// checksum, metadata size, and sparse read-back at the first, middle, and
// last frames. Note clampCloudMinPartSize part-count clamping only activates
// around ~500 GB, which stays unit-test-only territory.
func TestS3LargeCompressedUpload(t *testing.T) {
	t.Parallel()

	sizeMBEnv := os.Getenv("E2B_LIVE_S3_LARGE_MB")
	if sizeMBEnv == "" {
		t.Skip("E2B_LIVE_S3_LARGE_MB not set, skipping large-scale live test")
	}
	sizeMB, err := strconv.Atoi(sizeMBEnv)
	require.NoError(t, err)
	require.Positive(t, sizeMB)

	dataSize := sizeMB * megabyte
	data := generateSemiRandomData(dataSize)
	inputPath := writeTempFile(t, data)

	obj := testS3Object(t, testKey(fmt.Sprintf("large-%dmb", sizeMB)))

	cfg := CompressConfig{
		Enabled:            true,
		Type:               CompressionZstd.String(),
		Level:              2,
		FrameSizeKB:        2 * 1024,
		MinPartSizeMB:      5, // small parts -> many parts
		FrameEncodeWorkers: 4,
		EncoderConcurrency: 1,
	}

	start := time.Now()
	fullFT, checksum, err := obj.StoreFile(t.Context(), inputPath, WithCompressConfig(cfg))
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(data), checksum)

	table := fullFT.Table()
	require.Equal(t, int64(dataSize), table.UncompressedSize())
	t.Logf("uploaded %d MB in %s (%.1f MB/s uncompressed): %d bytes compressed (ratio %.2f), %d frames, ~%d parts",
		sizeMB, elapsed.Round(time.Second), float64(dataSize)/megabyte/elapsed.Seconds(),
		table.CompressedSize(), float64(table.CompressedSize())/float64(dataSize),
		table.NumFrames(), table.CompressedSize()/(5*megabyte)+1)

	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(dataSize), size)

	// Sparse read-back: first, middle, and last frame.
	frameSize := int64(2 * megabyte)
	offsets := []int64{
		0,
		int64(table.NumFrames()/2) * frameSize,
		int64(table.NumFrames()-1) * frameSize,
	}
	for _, off := range offsets {
		rr, _, err := obj.OpenRangeReader(t.Context(), off, 0, table)
		require.NoError(t, err)

		var buf bytes.Buffer
		n, err := buf.ReadFrom(rr)
		require.NoError(t, err)
		require.Positive(t, n)
		_, err = rr.Close(t.Context())
		require.NoError(t, err)

		require.True(t, bytes.Equal(data[off:off+n], buf.Bytes()),
			"read-back mismatch at offset %d", off)
	}
}

// TestS3UncompressedStoreFile verifies the plain (non-compressed)
// StoreFile path still works against real S3 and round-trips the data.
func TestS3UncompressedStoreFile(t *testing.T) {
	t.Parallel()

	const dataSize = 24 * megabyte // > awsMultipartUploadPartSize -> multipart
	data := generateSemiRandomData(dataSize)
	inputPath := writeTempFile(t, data)

	obj := testS3Object(t, testKey("uncompressed"))

	fullFT, _, err := obj.StoreFile(t.Context(), inputPath)
	require.NoError(t, err)
	require.Nil(t, fullFT, "uncompressed uploads have no frame table")

	size, err := obj.Size(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(dataSize), size)

	var got bytes.Buffer
	_, err = obj.WriteTo(t.Context(), &got)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(got.Bytes()))
}

func TestAWSDeleteObjectsWithPrefixRejectsEmptyPrefix(t *testing.T) {
	t.Parallel()

	s := &awsStorage{bucketName: "test-bucket"}

	err := s.DeleteObjectsWithPrefix(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty prefix")
}

// An Azure-shaped header map leaking into the S3/GCS response would break any upload that echoed it into the presigned signature.
func TestS3UploadSignedURLNeedsNoRequestHeaders(t *testing.T) {
	t.Parallel()

	backend := testBackend(t)
	client := backend.newClient(t, nil)
	provider := &awsStorage{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucketName:    backend.bucket,
	}

	key := testKey("presigned-upload.bin")
	obj := backend.object(t, client, key)

	upload, err := provider.UploadSignedURL(t.Context(), key, time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, upload.URL)
	assert.Nil(t, upload.Headers)

	body := []byte("presigned-upload-body")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, upload.URL, bytes.NewReader(body))
	require.NoError(t, err)
	req.ContentLength = int64(len(body))

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusOK, res.StatusCode)

	var read bytes.Buffer
	_, err = obj.WriteTo(t.Context(), &read)
	require.NoError(t, err)
	assert.Equal(t, body, read.Bytes())
}
