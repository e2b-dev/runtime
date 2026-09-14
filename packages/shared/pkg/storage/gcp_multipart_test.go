package storage

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test constants
const (
	testBucketName = "test-bucket"
	testObjectName = "test-object"
	testToken      = "test-token"
	uploadsPath    = "uploads"
)

// createTestMultipartUploader creates a test uploader with a mock HTTP client
func createTestMultipartUploader(t *testing.T, handler http.HandlerFunc, retryConfig ...RetryConfig) *MultipartUploader {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	config := DefaultRetryConfig()
	if len(retryConfig) > 0 {
		config = retryConfig[0]
	}

	// Create retryable client using the test server's client
	retryableClient := createRetryableClient(t.Context(), config)
	retryableClient.HTTPClient = server.Client()

	uploader := &MultipartUploader{
		bucketName:  testBucketName,
		objectName:  testObjectName,
		token:       testToken,
		client:      retryableClient,
		retryConfig: config,
		baseURL:     server.URL, // Override to use test server
	}

	return uploader
}

func TestMultipartUploader_PartUploaderContract(t *testing.T) {
	t.Parallel()

	testPartUploaderContract(t, partUploaderTestAdapter{
		abortsOnClose: true,
		new: func(t *testing.T, recorder *partUploaderRecorder) partUploader {
			t.Helper()

			return createTestMultipartUploader(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.RawQuery == uploadsPath:
					recorder.started = true
					response := InitiateMultipartUploadResult{Bucket: testBucketName, Key: testObjectName, UploadID: "contract-upload-id"}
					xmlData, _ := xml.Marshal(response)
					w.WriteHeader(http.StatusOK)
					w.Write(xmlData)
				case r.Method == http.MethodPut:
					recordUploadedPart(t, recorder, w, r)
				case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId=contract-upload-id"):
					recorder.completed = true
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodDelete && strings.Contains(r.URL.RawQuery, "uploadId=contract-upload-id"):
					recorder.aborted = true
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Fatalf("unexpected GCP multipart request: %s %s", r.Method, r.URL.String())
				}
			})
		},
	})
}

// abortRecordingUploader builds a test uploader whose part uploads always
// fail, recording whether the failure path issued the abort DELETE.
func abortRecordingUploader(t *testing.T, uploadID string, aborted *atomic.Bool) *MultipartUploader {
	t.Helper()

	return createTestMultipartUploader(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == uploadsPath:
			xmlData, _ := xml.Marshal(InitiateMultipartUploadResult{Bucket: testBucketName, Key: testObjectName, UploadID: uploadID})
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)
		case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber="):
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodDelete && r.URL.RawQuery == "uploadId="+uploadID:
			aborted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected GCP multipart request: %s %s", r.Method, r.URL.String())
		}
	}, fastRetryConfig())
}

func TestGCPCompressedStoreFileAbortsMultipartUploadOnFailure(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, []byte(strings.Repeat("compressible-data", 1024)))
	var aborted atomic.Bool
	uploader := abortRecordingUploader(t, "compressed-abort-upload-id", &aborted)

	_, _, err := storeFileCompressed(t.Context(), inputPath, testCompressConfig(), 4, PutOptions{},
		func(ObjectMetadata) (partUploader, error) { return uploader, nil })
	require.Error(t, err)
	require.True(t, aborted.Load(), "failed compressed upload should abort the multipart upload")
}

// The uncompressed >=50MB path has no deferred Close from compressStream and
// must abort on its own.
func TestGCPUploadFileInParallelAbortsOnPartFailure(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, []byte("parallel-upload-that-fails"))
	var aborted atomic.Bool
	uploader := abortRecordingUploader(t, "parallel-abort-upload-id", &aborted)

	_, err := uploader.UploadFileInParallel(t.Context(), inputPath, 2, nil)
	require.Error(t, err)
	require.True(t, aborted.Load(), "failed parallel upload should abort the multipart upload")
}

func TestMultipartUploader_InitiateUpload_Success(t *testing.T) {
	t.Parallel()
	expectedUploadID := "test-upload-id-123"

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, testObjectName)
		assert.Contains(t, r.URL.RawQuery, uploadsPath)
		assert.Equal(t, "Bearer "+testToken, r.Header.Get("Authorization"))
		assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))

		response := InitiateMultipartUploadResult{
			Bucket:   testBucketName,
			Key:      testObjectName,
			UploadID: expectedUploadID,
		}

		xmlData, _ := xml.Marshal(response)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		w.Write(xmlData)
	})

	uploader := createTestMultipartUploader(t, handler)
	uploadID, err := uploader.initiateUpload(t.Context())

	require.NoError(t, err)
	require.Equal(t, expectedUploadID, uploadID)
}

func TestMultipartUploader_UploadPart_Success(t *testing.T) {
	t.Parallel()
	expectedETag := `"abcd1234"`
	testData := []byte("test part data")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Contains(t, r.URL.RawQuery, "partNumber=1")
		assert.Contains(t, r.URL.RawQuery, "uploadId=test-upload-id")
		assert.Equal(t, "Bearer "+testToken, r.Header.Get("Authorization"))

		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, testData, body)
		sum := md5.Sum(testData) //nolint:gosec // verifying GCS Content-MD5 header.
		assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), r.Header.Get("Content-MD5"))

		w.Header().Set("ETag", expectedETag)
		w.WriteHeader(http.StatusOK)
	})

	uploader := createTestMultipartUploader(t, handler)
	etag, err := uploader.uploadPart(t.Context(), "test-upload-id", 1, testData)

	require.NoError(t, err)
	require.Equal(t, expectedETag, etag)
}

func TestMultipartUploader_UploadPartSlices_Success(t *testing.T) {
	t.Parallel()
	expectedETag := `"slice-etag"`
	slices := [][]byte{[]byte("hello "), []byte("world"), []byte("!")}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Contains(t, r.URL.RawQuery, "partNumber=3")
		assert.Contains(t, r.URL.RawQuery, "uploadId=test-upload-id")
		// Verify body is the concatenation of all slices.
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		expected := []byte("hello world!")
		assert.Equal(t, expected, body)
		sum := md5.Sum(expected) //nolint:gosec // verifying GCS Content-MD5 header.
		assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), r.Header.Get("Content-MD5"))

		w.Header().Set("ETag", expectedETag)
		w.WriteHeader(http.StatusOK)
	})

	uploader := createTestMultipartUploader(t, handler)
	etag, err := uploader.uploadPartSlices(t.Context(), "test-upload-id", 3, slices)

	require.NoError(t, err)
	require.Equal(t, expectedETag, etag)
}

func TestMultipartUploader_UploadPart_MissingETag(t *testing.T) {
	t.Parallel()
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Don't set ETag header
		w.WriteHeader(http.StatusOK)
	})

	uploader := createTestMultipartUploader(t, handler)
	etag, err := uploader.uploadPart(t.Context(), "test-upload-id", 1, []byte("test"))

	require.Error(t, err)
	require.Contains(t, err.Error(), "no ETag returned for part 1")
	require.Empty(t, etag)
}

func TestMultipartUploader_CompleteUpload_Success(t *testing.T) {
	t.Parallel()
	parts := []Part{
		{PartNumber: 1, ETag: `"etag1"`},
		{PartNumber: 2, ETag: `"etag2"`},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.RawQuery, "uploadId=test-upload-id")
		assert.Equal(t, "Bearer "+testToken, r.Header.Get("Authorization"))
		assert.Equal(t, "application/xml", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)

		var completeReq CompleteMultipartUpload
		err = xml.Unmarshal(body, &completeReq)
		assert.NoError(t, err)
		assert.Len(t, completeReq.Parts, 2)
		assert.Equal(t, 1, completeReq.Parts[0].PartNumber)
		assert.Equal(t, `"etag1"`, completeReq.Parts[0].ETag)

		w.WriteHeader(http.StatusOK)
	})

	uploader := createTestMultipartUploader(t, handler)
	err := uploader.completeUpload(t.Context(), "test-upload-id", parts)
	require.NoError(t, err)
}

func TestMultipartUploader_UploadFileInParallel_Success(t *testing.T) {
	t.Parallel()
	// Create a test file
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	testContent := strings.Repeat("test data content ", 1000) // Make it large enough for multiple chunks
	err := os.WriteFile(testFile, []byte(testContent), 0o644)
	require.NoError(t, err)

	var initiateCount, uploadPartCount, completeCount int32
	receivedParts := sync.Map{}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			// Initiate upload
			atomic.AddInt32(&initiateCount, 1)
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "test-upload-id-123",
			}
			xmlData, _ := xml.Marshal(response)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			// Upload part
			partNum := atomic.AddInt32(&uploadPartCount, 1)
			body, _ := io.ReadAll(r.Body)
			receivedParts.Store(int(partNum), string(body))

			w.Header().Set("ETag", fmt.Sprintf(`"etag%d"`, partNum))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			// Complete upload
			atomic.AddInt32(&completeCount, 1)
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 2, nil)
	require.NoError(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&initiateCount))
	require.Equal(t, int32(1), atomic.LoadInt32(&completeCount))
	require.Positive(t, atomic.LoadInt32(&uploadPartCount))

	// Verify all parts were uploaded and content matches
	var reconstructed strings.Builder
	for i := 1; i <= int(atomic.LoadInt32(&uploadPartCount)); i++ {
		if part, ok := receivedParts.Load(i); ok {
			reconstructed.WriteString(part.(string))
		}
	}
	require.Equal(t, testContent, reconstructed.String())
}

func TestMultipartUploader_UploadFileInParallel_Checksum(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	testContent := strings.Repeat("checksummed payload ", 5000) // multiple chunks
	require.NoError(t, os.WriteFile(testFile, []byte(testContent), 0o644))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "checksum-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)
		case strings.Contains(r.URL.RawQuery, "partNumber"):
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)
	hasher := sha256.New()
	_, err := uploader.UploadFileInParallel(t.Context(), testFile, 4, hasher)
	require.NoError(t, err)

	require.Equal(t, sha256.Sum256([]byte(testContent)), sum256(hasher))
}

func TestMultipartUploader_InitiateUpload_WithRetries(t *testing.T) {
	t.Parallel()
	var requestCount atomic.Int32
	expectedUploadID := "retry-upload-id"

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := requestCount.Add(1)
		if count < 2 {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		response := InitiateMultipartUploadResult{
			Bucket:   testBucketName,
			Key:      testObjectName,
			UploadID: expectedUploadID,
		}
		xmlData, _ := xml.Marshal(response)
		w.WriteHeader(http.StatusOK)
		w.Write(xmlData)
	})

	config := RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2,
	}

	uploader := createTestMultipartUploader(t, handler, config)
	uploadID, err := uploader.initiateUpload(t.Context())

	require.NoError(t, err)
	require.Equal(t, expectedUploadID, uploadID)
	require.Equal(t, int32(2), requestCount.Load())
}

// STRESS TESTS AND EDGE CASES

func TestMultipartUploader_HighConcurrency_StressTest(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "large.txt")
	fileSize := 2 * gcpMultipartUploadChunkSize
	testContent := strings.Repeat("0123456789abcdef", fileSize/16)
	err := os.WriteFile(testFile, []byte(testContent), 0o644)
	require.NoError(t, err)

	const wantConcurrent = int32(2) // == numParts (fileSize / chunk size)

	var initiateCalls, partCalls, completeCalls int32
	var maxConcurrentParts atomic.Int32
	var currentConcurrentParts atomic.Int32
	var arrivedParts atomic.Int32
	allPartsInFlight := make(chan struct{})
	var releaseOnce sync.Once
	var barrierTimedOut atomic.Bool
	receivedParts := sync.Map{}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			atomic.AddInt32(&initiateCalls, 1)
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "stress-test-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			// Track concurrent part uploads
			current := currentConcurrentParts.Add(1)
			defer currentConcurrentParts.Add(-1)

			// Update max concurrent parts
			for {
				maxConcurrent := maxConcurrentParts.Load()
				if current <= maxConcurrent || maxConcurrentParts.CompareAndSwap(maxConcurrent, current) {
					break
				}
			}

			// Hold every part until all are concurrently in flight, then release.
			if arrivedParts.Add(1) == wantConcurrent {
				releaseOnce.Do(func() { close(allPartsInFlight) })
			}
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			select {
			case <-allPartsInFlight:
			case <-timer.C:
				// require/FailNow can't be called from this non-test goroutine.
				barrierTimedOut.Store(true)
			}

			partNum := atomic.AddInt32(&partCalls, 1)
			body, _ := io.ReadAll(r.Body)
			receivedParts.Store(int(partNum), string(body))

			w.Header().Set("ETag", fmt.Sprintf(`"etag%d"`, partNum))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			atomic.AddInt32(&completeCalls, 1)
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)

	// Use high concurrency to stress test
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 50, nil)
	require.NoError(t, err)

	// Verify all calls were made
	require.Equal(t, int32(1), atomic.LoadInt32(&initiateCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&completeCalls))
	require.Positive(t, atomic.LoadInt32(&partCalls))
	require.False(t, barrierTimedOut.Load(),
		"parts did not upload concurrently: barrier timed out waiting for all parts to be in flight")
	require.Greater(t, maxConcurrentParts.Load(), int32(1), "Should have concurrent uploads")

	// Verify content integrity
	var reconstructed strings.Builder
	for i := 1; i <= int(atomic.LoadInt32(&partCalls)); i++ {
		if part, ok := receivedParts.Load(i); ok {
			reconstructed.WriteString(part.(string))
		}
	}
	require.Equal(t, testContent, reconstructed.String())
}

func TestMultipartUploader_RandomFailures_ChaosTest(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "chaos.txt")
	testContent := strings.Repeat("chaos test data ", 3200000) // ~100MB file for multiple parts
	err := os.WriteFile(testFile, []byte(testContent), 0o644)
	require.NoError(t, err)

	var attemptCount, successCount int32
	failureRate := 0.3 // 30% failure rate

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "chaos-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			atomic.AddInt32(&attemptCount, 1)

			// Randomly fail some requests
			if rand.Float64() < failureRate {
				w.WriteHeader(http.StatusInternalServerError)

				return
			}

			atomic.AddInt32(&successCount, 1)
			partNum := r.URL.Query().Get("partNumber")
			w.Header().Set("ETag", fmt.Sprintf(`"chaos-etag-%s"`, partNum))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	// Use aggressive retry config for chaos test
	config := RetryConfig{
		MaxAttempts:       10,
		InitialBackoff:    1 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2,
	}

	uploader := createTestMultipartUploader(t, handler, config)
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 10, nil)
	require.NoError(t, err)

	t.Logf("Chaos test: %d total attempts, %d successes",
		atomic.LoadInt32(&attemptCount), atomic.LoadInt32(&successCount))

	// Should have more attempts than successes due to retries
	require.GreaterOrEqual(t, atomic.LoadInt32(&attemptCount), atomic.LoadInt32(&successCount))
}

func TestMultipartUploader_PartialFailures_Recovery(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "partial.txt")
	testContent := strings.Repeat("partial failure test ", 2500000) // 100MB+ file
	err := os.WriteFile(testFile, []byte(testContent), 0o644)
	require.NoError(t, err)

	var partAttempts sync.Map
	maxAttempts := 3

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "partial-fail-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			partNumStr := r.URL.Query().Get("partNumber")

			// Track attempts per part
			val, _ := partAttempts.LoadOrStore(partNumStr, new(int32(0)))
			attempts := val.(*int32)
			currentAttempts := atomic.AddInt32(attempts, 1)

			// Fail first few attempts for each part, then succeed
			if currentAttempts < int32(maxAttempts-1) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("simulated failure"))

				return
			}

			w.Header().Set("ETag", fmt.Sprintf(`"recovery-etag-%s"`, partNumStr))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	config := RetryConfig{
		MaxAttempts:       maxAttempts,
		InitialBackoff:    5 * time.Millisecond,
		MaxBackoff:        50 * time.Millisecond,
		BackoffMultiplier: 2,
	}

	uploader := createTestMultipartUploader(t, handler, config)
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 5, nil)
	require.NoError(t, err)

	// Verify that all parts eventually succeeded after retries
	partAttempts.Range(func(key, value any) bool {
		attempts := atomic.LoadInt32(value.(*int32))
		require.Equal(t, int32(maxAttempts-1), attempts, "Part %s should have exactly %d attempts", key, maxAttempts-1)

		return true
	})
}

func TestMultipartUploader_EdgeCases_EmptyFile(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	emptyFile := filepath.Join(tempDir, "empty.txt")
	err := os.WriteFile(emptyFile, []byte(""), 0o644)
	require.NoError(t, err)

	var initiateCalls, partCalls, completeCalls int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			atomic.AddInt32(&initiateCalls, 1)
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "empty-file-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			atomic.AddInt32(&partCalls, 1)
			body, _ := io.ReadAll(r.Body)
			assert.Empty(t, body, "Empty file should result in empty part")

			w.Header().Set("ETag", `"empty-etag"`)
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			atomic.AddInt32(&completeCalls, 1)
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)
	_, err = uploader.UploadFileInParallel(t.Context(), emptyFile, 5, nil)
	require.NoError(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&initiateCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&partCalls)) // Should have exactly 1 part for empty file
	require.Equal(t, int32(1), atomic.LoadInt32(&completeCalls))
}

func TestMultipartUploader_EdgeCases_VerySmallFile(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	smallFile := filepath.Join(tempDir, "small.txt")
	smallContent := "small"
	err := os.WriteFile(smallFile, []byte(smallContent), 0o644)
	require.NoError(t, err)

	var receivedParts sync.Map

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "small-file-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			body, _ := io.ReadAll(r.Body)
			partNum := r.URL.Query().Get("partNumber")
			receivedParts.Store(partNum, string(body))

			w.Header().Set("ETag", `"small-etag"`)
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)
	_, err = uploader.UploadFileInParallel(t.Context(), smallFile, 10, nil) // High concurrency for small file
	require.NoError(t, err)

	// Small file should produce exactly one part
	var partCount int
	receivedParts.Range(func(_, _ any) bool {
		partCount++

		return true
	})
	require.Equal(t, 1, partCount)
	data, ok := receivedParts.Load("1")
	require.True(t, ok)
	require.Equal(t, smallContent, data.(string))
}

type repeatReader struct {
	char      byte
	remaining int
}

var _ io.Reader = (*repeatReader)(nil)

func (r *repeatReader) Read(p []byte) (n int, err error) {
	toWrite := int(math.Min(float64(len(p)), float64(r.remaining)))
	if toWrite <= 0 {
		return 0, io.EOF
	}
	r.remaining -= toWrite

	for index := range p[:toWrite] {
		p[index] = r.char
	}

	return toWrite, nil
}

func newRepeatReader(b byte, count int) io.Reader {
	return &repeatReader{char: b, remaining: count}
}

func TestMultipartUploader_ResourceExhaustion_TooManyConcurrentUploads(t *testing.T) {
	t.Parallel()

	totalChunks := 10

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "resource.txt")
	file, err := os.Create(testFile)
	require.NoError(t, err)
	count, err := io.Copy(file, newRepeatReader('a', gcpMultipartUploadChunkSize*totalChunks))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(gcpMultipartUploadChunkSize*totalChunks))
	err = file.Close()
	require.NoError(t, err)

	var activeConcurrency atomic.Int32
	var maxObservedConcurrency atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "resource-test-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			current := activeConcurrency.Add(1)
			defer activeConcurrency.Add(-1)

			// Track max observed concurrency
			for {
				maxObserved := maxObservedConcurrency.Load()
				if current <= maxObserved || maxObservedConcurrency.CompareAndSwap(maxObserved, current) {
					break
				}
			}

			// Simulate work that takes time
			time.Sleep(10 * time.Millisecond)

			partNum := r.URL.Query().Get("partNumber")
			w.Header().Set("ETag", fmt.Sprintf(`"resource-etag-%s"`, partNum))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)

	// Try with extremely high concurrency
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 1000, nil)
	require.NoError(t, err)

	// Should have observed significant concurrency but not necessarily 1000
	// (due to file size and chunk limitations)
	t.Logf("Max observed concurrency: %d", maxObservedConcurrency.Load())
	require.Greater(t, maxObservedConcurrency.Load(), int32(1))
}

func TestMultipartUploader_BoundaryConditions_ExactChunkSize(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "exact.txt")
	// Create file that's exactly 2 chunks
	testContent := strings.Repeat("x", gcpMultipartUploadChunkSize*2)
	err := os.WriteFile(testFile, []byte(testContent), 0o644)
	require.NoError(t, err)

	var partSizes []int
	var partSizesMu sync.Mutex

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "boundary-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			body, _ := io.ReadAll(r.Body)
			partSizesMu.Lock()
			partSizes = append(partSizes, len(body))
			partSizesMu.Unlock()

			partNum := r.URL.Query().Get("partNumber")
			w.Header().Set("ETag", fmt.Sprintf(`"boundary-etag-%s"`, partNum))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	uploader := createTestMultipartUploader(t, handler)
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 5, nil)
	require.NoError(t, err)

	// Should have exactly 2 parts, each of ChunkSize
	require.Len(t, partSizes, 2)
	for _, size := range partSizes {
		require.Equal(t, gcpMultipartUploadChunkSize, size)
	}
}

func TestMultipartUploader_FileNotFound_Error(t *testing.T) {
	t.Parallel()
	uploader := createTestMultipartUploader(t, func(http.ResponseWriter, *http.Request) {
		t.Error("Should not make any HTTP requests for missing file")
	})

	_, err := uploader.UploadFileInParallel(t.Context(), "/nonexistent/file.txt", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to open file")
}

func TestMultipartUploader_ConcurrentRetries_RaceCondition(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "race.txt")
	testContent := strings.Repeat("race condition test ", 2500000) // 100MB+ file
	err := os.WriteFile(testFile, []byte(testContent), 0o644)
	require.NoError(t, err)

	var retryAttempts sync.Map
	var totalRequests atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalRequests.Add(1)

		switch {
		case r.URL.RawQuery == uploadsPath:
			response := InitiateMultipartUploadResult{
				Bucket:   testBucketName,
				Key:      testObjectName,
				UploadID: "race-upload-id",
			}
			xmlData, _ := xml.Marshal(response)
			w.WriteHeader(http.StatusOK)
			w.Write(xmlData)

		case strings.Contains(r.URL.RawQuery, "partNumber"):
			partNumStr := r.URL.Query().Get("partNumber")

			// Track retry attempts per part with race-safe operations
			val, _ := retryAttempts.LoadOrStore(partNumStr, new(int32(0)))
			attempts := val.(*int32)
			currentAttempt := atomic.AddInt32(attempts, 1)

			// Fail first 2 attempts to force retries under high concurrency
			if currentAttempt <= 2 {
				// Add random delay to increase race condition probability
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				w.WriteHeader(http.StatusInternalServerError)

				return
			}

			w.Header().Set("ETag", fmt.Sprintf(`"race-etag-%s"`, partNumStr))
			w.WriteHeader(http.StatusOK)

		case strings.Contains(r.URL.RawQuery, "uploadId"):
			w.WriteHeader(http.StatusOK)
		}
	})

	config := RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    1 * time.Millisecond, // Very fast retries to increase race probability
		MaxBackoff:        10 * time.Millisecond,
		BackoffMultiplier: 2,
	}

	uploader := createTestMultipartUploader(t, handler, config)
	_, err = uploader.UploadFileInParallel(t.Context(), testFile, 20, nil) // High concurrency
	require.NoError(t, err)

	t.Logf("Total HTTP requests made: %d", totalRequests.Load())

	// Verify that retries happened correctly under concurrent conditions
	retryAttempts.Range(func(key, value any) bool {
		attempts := atomic.LoadInt32(value.(*int32))
		require.GreaterOrEqual(t, attempts, int32(3), "Part %s should have at least 3 attempts", key)

		return true
	})
}

// TestCreateRetryableClient_JitterBehavior tests that the jittered backoff works correctly
func TestCreateRetryableClient_JitterBehavior(t *testing.T) {
	t.Parallel()
	config := RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
	}

	client := createRetryableClient(t.Context(), config)
	require.NotNil(t, client)
	require.NotNil(t, client.Backoff)

	// Test jitter produces values within expected range
	t.Run("JitterRange", func(t *testing.T) {
		t.Parallel()
		// Test first attempt (attemptNum = 0)
		for range 10 {
			backoff := client.Backoff(config.InitialBackoff, config.MaxBackoff, 0, nil)
			require.GreaterOrEqual(t, backoff, time.Duration(0))
			require.Less(t, backoff, config.InitialBackoff)
		}

		// Test second attempt (attemptNum = 1) - should be jittered version of 200ms
		expectedBase := time.Duration(float64(config.InitialBackoff) * config.BackoffMultiplier)
		for range 10 {
			backoff := client.Backoff(config.InitialBackoff, config.MaxBackoff, 1, nil)
			require.GreaterOrEqual(t, backoff, time.Duration(0))
			require.Less(t, backoff, expectedBase)
		}
	})

	// Test that jitter produces different values (randomness)
	t.Run("JitterRandomness", func(t *testing.T) {
		t.Parallel()
		values := make(map[time.Duration]bool)

		// Collect 20 jittered values
		for range 20 {
			backoff := client.Backoff(config.InitialBackoff, config.MaxBackoff, 1, nil)
			values[backoff] = true
		}

		// Should have at least some variation (not all the same value)
		// With a range of 0-200ms, getting 20 identical values is highly unlikely
		require.Greater(t, len(values), 1, "Jitter should produce varied values")
	})

	// Test exponential backoff base calculation (before jitter)
	t.Run("ExponentialBackoffBase", func(t *testing.T) {
		t.Parallel()
		// We can't directly test the base calculation due to jitter,
		// but we can verify the max possible value matches our expectation

		// For attemptNum=0: base should be 100ms, jitter: 0-100ms
		// For attemptNum=1: base should be 200ms, jitter: 0-200ms
		// For attemptNum=2: base should be 400ms, jitter: 0-400ms

		// Test attempt 2 multiple times and verify max range
		var maxSeen time.Duration
		for range 100 {
			backoff := client.Backoff(config.InitialBackoff, config.MaxBackoff, 2, nil)
			if backoff > maxSeen {
				maxSeen = backoff
			}
		}

		expectedBase := time.Duration(float64(config.InitialBackoff) * config.BackoffMultiplier * config.BackoffMultiplier)
		// The max we should ever see is just under the expected base (due to jitter being 0 to base-1)
		require.Less(t, maxSeen, expectedBase)
		// But we should see values reasonably close to the base in 100 attempts
		require.Greater(t, maxSeen, expectedBase/2)
	})

	// Test max backoff cap
	t.Run("MaxBackoffCap", func(t *testing.T) {
		t.Parallel()
		// With high attempt numbers, backoff should be capped at MaxBackoff
		for range 10 {
			backoff := client.Backoff(config.InitialBackoff, config.MaxBackoff, 10, nil)
			require.GreaterOrEqual(t, backoff, time.Duration(0))
			require.Less(t, backoff, config.MaxBackoff)
		}
	})
}

// TestCreateRetryableClient_Configuration tests the retry client configuration
func TestCreateRetryableClient_Configuration(t *testing.T) {
	t.Parallel()
	config := RetryConfig{
		MaxAttempts:       5,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		BackoffMultiplier: 3.0,
	}

	client := createRetryableClient(t.Context(), config)

	// Verify retry configuration
	require.Equal(t, config.MaxAttempts-1, client.RetryMax) // go-retryablehttp counts retries, not total attempts
	require.Equal(t, config.InitialBackoff, client.RetryWaitMin)
	require.Equal(t, config.MaxBackoff, client.RetryWaitMax)
	require.NotNil(t, client.Logger)
	require.NotNil(t, client.Backoff)
}

// TestCreateRetryableClient_ZeroBackoff tests edge case of zero backoff
func TestCreateRetryableClient_ZeroBackoff(t *testing.T) {
	t.Parallel()
	config := RetryConfig{
		MaxAttempts:       2,
		InitialBackoff:    0, // Zero initial backoff
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
	}

	client := createRetryableClient(t.Context(), config)

	// With zero initial backoff, jitter should also return zero
	backoff := client.Backoff(config.InitialBackoff, config.MaxBackoff, 0, nil)
	require.Equal(t, time.Duration(0), backoff)
}

// TestRetryableClient_ActualRetryBehavior tests the retry behavior in practice
func TestRetryableClient_ActualRetryBehavior(t *testing.T) {
	t.Parallel()
	var requestCount atomic.Int32
	var retryDelays []time.Duration
	var retryTimes []time.Time
	var retryMu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := requestCount.Add(1)
		retryMu.Lock()
		retryTimes = append(retryTimes, time.Now())
		retryMu.Unlock()

		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("success"))
		}
	}))
	defer server.Close()

	config := RetryConfig{
		MaxAttempts:       3,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        500 * time.Millisecond,
		BackoffMultiplier: 2.0,
	}

	client := createRetryableClient(t.Context(), config)
	client.HTTPClient = server.Client()

	startTime := time.Now()
	req, err := retryablehttp.NewRequestWithContext(t.Context(), "GET", server.URL, nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Should have made 3 requests (initial + 2 retries)
	require.Equal(t, int32(3), requestCount.Load())
	require.Len(t, retryTimes, 3)

	// Calculate actual delays between requests
	for i := 1; i < len(retryTimes); i++ {
		delay := retryTimes[i].Sub(retryTimes[i-1])
		retryDelays = append(retryDelays, delay)
	}

	// Verify we had some delays due to backoff (but jittered, so variable)
	require.Len(t, retryDelays, 2)

	// First retry delay should be jittered version of 50ms (0-50ms range)
	// But in practice, with network overhead and CI scheduling, it might be much higher
	require.Greater(t, retryDelays[0], time.Duration(0))
	require.Less(t, retryDelays[0], 2*time.Second)

	// Second retry delay should be jittered version of 100ms (0-100ms range)
	require.Greater(t, retryDelays[1], time.Duration(0))
	require.Less(t, retryDelays[1], 2*time.Second)

	totalTime := time.Since(startTime)
	t.Logf("Total time: %v, Retry delays: %v", totalTime, retryDelays)
}

// TestGCPCompleteRejects200WithErrorBody verifies CompleteMultipartUpload
// treats an HTTP 200 carrying an <Error> body as a failure. S3-dialect
// servers (including the GCS XML API) can return 200 with an error payload on
// internal timeouts; accepting it would record a frame table for an object
// that was never committed.
// fastRetryConfig retries a few times with negligible backoff so retry-path
// tests stay fast.
func fastRetryConfig() RetryConfig {
	return RetryConfig{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2}
}

func newCompleteErrorUploader(t *testing.T, completeAttempts *atomic.Int32, complete http.HandlerFunc) *MultipartUploader {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "uploads":
			w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>id-1</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut && r.URL.Query().Get("partNumber") != "":
			io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"e1"`)
		case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
			completeAttempts.Add(1)
			complete(w, r)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
	}))
	t.Cleanup(server.Close)

	return &MultipartUploader{
		bucketName: "b", objectName: "o", token: "t",
		client:  createRetryableClient(t.Context(), fastRetryConfig()),
		baseURL: server.URL + "/b",
	}
}

func TestGCPCompleteRejects200WithErrorBody(t *testing.T) {
	t.Parallel()

	// Persistent 200-with-<Error>: retried up to the budget, then reported as
	// a failed commit (never treated as success).
	var attempts atomic.Int32
	up := newCompleteErrorUploader(t, &attempts, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK) // 200 OK, but the body is an error — commit did NOT happen.
		w.Write([]byte(`<?xml version="1.0"?><Error><Code>InternalError</Code><Message>please try again</Message></Error>`))
	})
	require.NoError(t, up.Start(t.Context()))
	require.NoError(t, up.UploadPart(t.Context(), 1, []byte("data")))

	err := up.Complete(t.Context())
	require.Error(t, err, "200-with-<Error>-body must be treated as a failed commit, never as success")
	require.Equal(t, int32(3), attempts.Load(), "a transient 200-error must be retried up to the configured budget")
}

func TestGCPCompleteRetriesTransient200Error(t *testing.T) {
	t.Parallel()

	// First complete attempt returns a 200-with-<Error>; the retry succeeds.
	var attempts atomic.Int32
	up := newCompleteErrorUploader(t, &attempts, func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Load() == 1 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0"?><Error><Code>InternalError</Code><Message>please try again</Message></Error>`))

			return
		}
		w.Write([]byte(`<CompleteMultipartUploadResult><ETag>"final"</ETag></CompleteMultipartUploadResult>`))
	})
	require.NoError(t, up.Start(t.Context()))
	require.NoError(t, up.UploadPart(t.Context(), 1, []byte("data")))

	require.NoError(t, up.Complete(t.Context()), "a transient 200-error should be retried to success")
	require.Equal(t, int32(2), attempts.Load())
}

func TestGCPCompleteRetriesTruncatedBody(t *testing.T) {
	t.Parallel()

	// 200 headers, but the body is truncated (declared Content-Length exceeds
	// what's written), so reading it fails after the headers. This must be
	// retried and ultimately fail — never masked as a clean commit.
	var attempts atomic.Int32
	up := newCompleteErrorUploader(t, &attempts, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<partial"))
	})
	require.NoError(t, up.Start(t.Context()))
	require.NoError(t, up.UploadPart(t.Context(), 1, []byte("data")))

	err := up.Complete(t.Context())
	require.Error(t, err, "a truncated complete response must fail, not commit")
	require.Equal(t, int32(3), attempts.Load(), "a failed body read must be retried to the budget")
}

// gcsXMLBackend starts MinIO and opens its bucket for anonymous access so the
// GCS XML MultipartUploader (bearer-token auth, unverifiable by MinIO) can
// upload without request signing.
func gcsXMLBackend(t *testing.T) *s3TestBackend {
	t.Helper()

	backend := startMinioBackend(t)

	policy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS": ["*"]},
			"Action": ["s3:*"],
			"Resource": ["arn:aws:s3:::%s", "arn:aws:s3:::%s/*"]
		}]
	}`, backend.bucket, backend.bucket)

	_, err := backend.newClient(t, nil).PutBucketPolicy(t.Context(), &s3.PutBucketPolicyInput{
		Bucket: aws.String(backend.bucket),
		Policy: aws.String(policy),
	})
	require.NoError(t, err, "allow anonymous access on minio bucket")

	return backend
}

// authStrippingTransport removes the Authorization header (the uploader's
// bearer token) so requests reach MinIO as anonymous — MinIO cannot validate
// Google OAuth tokens. Nothing else is adapted: in particular, MinIO's 411
// rejection of chunked uploads guards the uploader's explicit Content-Length
// (via multiSliceReader.Len) against regressions.
type authStrippingTransport struct {
	inner http.RoundTripper
}

func (a authStrippingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Del("Authorization")

	return a.inner.RoundTrip(clone)
}

// gcsXMLUploader builds a MultipartUploader pointed at the MinIO backend,
// bypassing NewMultipartUploaderWithRetryConfig (which requires real Google
// credentials and hardcodes the production URL). transport is optional and
// sits between the retryable client and the network (e.g. fault injection).
func gcsXMLUploader(t *testing.T, backend *s3TestBackend, key string, metadata ObjectMetadata, transport http.RoundTripper) *MultipartUploader {
	t.Helper()

	if transport == nil {
		transport = http.DefaultTransport
	}

	rc := retryablehttp.NewClient()
	rc.RetryMax = 3
	rc.RetryWaitMin = 10 * time.Millisecond
	rc.RetryWaitMax = 50 * time.Millisecond
	rc.Logger = nil
	rc.HTTPClient = &http.Client{Transport: authStrippingTransport{inner: transport}}

	return &MultipartUploader{
		bucketName:  backend.bucket,
		objectName:  key,
		token:       "test-token", // stripped by authStrippingTransport
		client:      rc,
		retryConfig: DefaultRetryConfig(),
		metadata:    metadata,
		baseURL:     backend.endpoint + "/" + backend.bucket,
	}
}

// anonymousGet fetches an object's raw bytes via unauthenticated HTTP.
func anonymousGet(t *testing.T, backend *s3TestBackend, key string) []byte {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, backend.endpoint+"/"+backend.bucket+"/"+key, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return body
}

// TestGCSXMLPartUploaderContract drives MultipartUploader directly against
// MinIO's XML multipart implementation: out-of-order part numbers, a
// multi-slice part body, Content-MD5 validation by a real server, and
// ordered reassembly on Complete.
func TestGCSXMLPartUploaderContract(t *testing.T) {
	t.Parallel()

	backend := gcsXMLBackend(t)
	key := testKey("gcs-part-contract")
	up := gcsXMLUploader(t, backend, key, nil, nil)

	// Part 1 (non-final) must be >= 5 MiB; two slices exercise the
	// multi-slice hashing/streaming path. Part 2 (final) is tiny.
	part1a := bytes.Repeat([]byte{0xC3}, 3*megabyte)
	part1b := bytes.Repeat([]byte{0xD4}, 2*megabyte+512)
	part2 := []byte("gcs-final-part")

	require.NoError(t, up.Start(t.Context()))
	// Upload out of order: final part first.
	require.NoError(t, up.UploadPart(t.Context(), 2, part2))
	require.NoError(t, up.UploadPart(t.Context(), 1, part1a, part1b))
	require.NoError(t, up.Complete(t.Context()))
	require.NoError(t, up.Close())

	want := slices.Concat(part1a, part1b, part2)
	got := anonymousGet(t, backend, key)
	require.Equal(t, sha256.Sum256(want), sha256.Sum256(got),
		"parts must reassemble in part-number order, not upload order")
}

// Mirrors TestS3PartUploaderAbortOnClose for the GCS XML uploader.
func TestGCSXMLPartUploaderAbortOnClose(t *testing.T) {
	t.Parallel()

	backend := gcsXMLBackend(t)
	key := testKey("gcs-part-abort")
	up := gcsXMLUploader(t, backend, key, nil, nil)

	require.NoError(t, up.Start(t.Context()))
	require.NoError(t, up.UploadPart(t.Context(), 1, []byte("abandoned-part")))
	require.NoError(t, up.Close())

	list, err := backend.newClient(t, nil).ListMultipartUploads(t.Context(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(backend.bucket),
		Prefix: aws.String(key),
	})
	require.NoError(t, err)
	require.Empty(t, list.Uploads, "abort must leave no orphaned multipart upload")
}

// TestGCSXMLCompressedRoundTrip runs the production compressed upload path
// (storeFileCompressed) with the GCS XML uploader against MinIO, then
// verifies the stored blob decompresses back to the original.
func TestGCSXMLCompressedRoundTrip(t *testing.T) {
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

			backend := gcsXMLBackend(t)
			key := testKey("gcs-compressed-" + tc.codec.String())

			cfg := CompressConfig{
				Enabled:            true,
				Type:               tc.codec.String(),
				Level:              tc.level,
				FrameSizeKB:        2 * 1024,
				MinPartSizeMB:      5,
				FrameEncodeWorkers: 4,
				EncoderConcurrency: 1,
			}

			fullFT, checksum, err := storeFileCompressed(t.Context(), inputPath, cfg, 4, PutOptions{},
				func(metadata ObjectMetadata) (partUploader, error) {
					return gcsXMLUploader(t, backend, key, metadata, nil), nil
				})
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(data), checksum)

			ft := fullFT.Table()
			require.Equal(t, dataSize/(2*megabyte), ft.NumFrames())
			require.Equal(t, int64(dataSize), ft.UncompressedSize())

			blob := anonymousGet(t, backend, key)
			require.Equal(t, ft.CompressedSize(), int64(len(blob)))

			decompressed, err := decompressAll(ft, blob)
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(data), sha256.Sum256(decompressed),
				"stored blob must decompress back to the original")
		})
	}
}

// TestGCSXMLCompressedEmptyFile verifies the single empty part shipped for
// zero-byte inputs completes against a real XML multipart implementation.
func TestGCSXMLCompressedEmptyFile(t *testing.T) {
	t.Parallel()

	inputPath := writeTempFile(t, nil)
	backend := gcsXMLBackend(t)
	key := testKey("gcs-compressed-empty")

	fullFT, checksum, err := storeFileCompressed(t.Context(), inputPath, testCompressConfig(), 4, PutOptions{},
		func(metadata ObjectMetadata) (partUploader, error) {
			return gcsXMLUploader(t, backend, key, metadata, nil), nil
		})
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(nil), checksum)
	require.Equal(t, 0, fullFT.Table().NumFrames())
	require.Empty(t, anonymousGet(t, backend, key))
}

// TestGCSXMLRetryOnTransientFailure fails the first attempt of every request
// (initiate, each part, complete) with an injected 500 and verifies
// retryablehttp's retry with ReaderFunc body replay: the whole upload
// succeeds, retried parts re-send byte-identical bodies, and the stored blob
// decompresses cleanly.
func TestGCSXMLRetryOnTransientFailure(t *testing.T) {
	t.Parallel()

	const dataSize = 32 * megabyte
	data := generateSemiRandomData(dataSize)
	inputPath := writeTempFile(t, data)

	backend := gcsXMLBackend(t)
	key := testKey("gcs-retry-transient")
	ft := newFaultInjectingTransport()

	cfg := CompressConfig{
		Enabled:            true,
		Type:               CompressionLZ4.String(),
		FrameSizeKB:        2 * 1024,
		MinPartSizeMB:      5,
		FrameEncodeWorkers: 4,
		EncoderConcurrency: 1,
	}

	fullFT, checksum, err := storeFileCompressed(t.Context(), inputPath, cfg, 4, PutOptions{},
		func(metadata ObjectMetadata) (partUploader, error) {
			return gcsXMLUploader(t, backend, key, metadata, ft), nil
		})
	require.NoError(t, err, "upload must survive one injected 500 per request")
	require.Equal(t, sha256.Sum256(data), checksum)

	blob := anonymousGet(t, backend, key)
	decompressed, err := decompressAll(fullFT.Table(), blob)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(decompressed),
		"stored blob after injected faults must decompress to the original")

	ft.mu.Lock()
	defer ft.mu.Unlock()

	require.NotEmpty(t, ft.partBodySizes)
	require.Greater(t, ft.injected, len(ft.partBodySizes),
		"should have injected faults beyond part uploads (initiate/complete)")
	t.Logf("injected %d faults across %d distinct requests (%d parts)",
		ft.injected, len(ft.seen), len(ft.partBodySizes))

	for part, sizes := range ft.partBodySizes {
		require.Len(t, sizes, 2, "part %s: expected exactly one failed and one successful attempt", part)
		require.Positive(t, sizes[0], "part %s: injected attempt consumed no body", part)
		require.Equal(t, sizes[0], sizes[1],
			"part %s: retry sent %d bytes but first attempt sent %d — ReaderFunc body replay is broken",
			part, sizes[1], sizes[0])
	}
}

// TestGCSXMLUploadFileInParallel exercises the uncompressed >=50MB multipart
// path (UploadFileInParallel) against MinIO: fixed 50 MB chunks uploaded
// concurrently, with the overlapped checksum hasher.
func TestGCSXMLUploadFileInParallel(t *testing.T) {
	t.Parallel()

	const dataSize = 120 * megabyte // 3 parts at the fixed 50 MB chunk size
	data := generateSemiRandomData(dataSize)
	inputPath := writeTempFile(t, data)

	backend := gcsXMLBackend(t)
	key := testKey("gcs-parallel-upload")
	up := gcsXMLUploader(t, backend, key, nil, nil)

	hasher := sha256.New()
	n, err := up.UploadFileInParallel(t.Context(), inputPath, 4, hasher)
	require.NoError(t, err)
	require.Equal(t, int64(dataSize), n)
	require.Equal(t, sha256.Sum256(data), [32]byte(hasher.Sum(nil)),
		"overlapped hasher must checksum the whole file")

	got := anonymousGet(t, backend, key)
	require.Equal(t, sha256.Sum256(data), sha256.Sum256(got))
}

// TestGCPUploadFileInParallelEmptyFile verifies the empty-file path ships its
// single zero-byte part with an explicit Content-Length rather than chunked
// transfer encoding, which S3-compatible XML backends (MinIO here) reject with
// 411 MissingContentLength.
func TestGCPUploadFileInParallelEmptyFile(t *testing.T) {
	t.Parallel()

	backend := gcsXMLBackend(t)
	key := testKey("gcp-parallel-empty")
	up := gcsXMLUploader(t, backend, key, nil, nil)

	inputPath := writeTempFile(t, nil)
	n, err := up.UploadFileInParallel(t.Context(), inputPath, 2, nil)
	require.NoError(t, err, "empty file must upload without a 411 chunked-encoding rejection")
	require.Zero(t, n)
	require.Empty(t, anonymousGet(t, backend, key))
}
