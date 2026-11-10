package storage

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	gcpMultipartUploadChunkSize = 50 * 1024 * 1024 // 50MB chunks
	gcpAbortUploadTimeout       = 5 * time.Second
)

// RetryConfig holds the configuration for retry logic
type RetryConfig struct {
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
}

// DefaultRetryConfig returns the default retry configuration matching storage_google.go
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:       googleMaxAttempts,
		InitialBackoff:    googleInitialBackoff,
		MaxBackoff:        googleMaxBackoff,
		BackoffMultiplier: googleBackoffMultiplier,
	}
}

func createRetryableClient(ctx context.Context, config RetryConfig) *retryablehttp.Client {
	client := retryablehttp.NewClient()

	client.RetryMax = config.MaxAttempts - 1 // go-retryablehttp counts retries, not total attempts
	client.RetryWaitMin = config.InitialBackoff
	client.RetryWaitMax = config.MaxBackoff
	client.CheckRetry = retryOnCompleteError

	// Custom backoff function with full jitter to avoid thundering herd
	client.Backoff = func(start, maxBackoff time.Duration, attemptNum int, _ *http.Response) time.Duration {
		// Calculate exponential backoff
		backoff := start
		for range attemptNum {
			backoff = time.Duration(float64(backoff) * config.BackoffMultiplier)
			if backoff > maxBackoff {
				backoff = maxBackoff

				break
			}
		}

		// Apply full jitter: random(0, backoff)
		// This implements the "full jitter" strategy recommended by AWS:
		// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
		// Benefits:
		// - Spreads retry attempts across time to avoid thundering herd
		// - Reduces peak load on servers during outages
		// - Improves overall system stability under high retry scenarios
		if backoff > 0 {
			return time.Duration(rand.Int63n(int64(backoff)))
		}

		return backoff
	}

	// add otel instrumentation
	originalTransport := client.HTTPClient.Transport
	client.HTTPClient.Transport = otelhttp.NewTransport(originalTransport)

	// Use zap logger
	client.Logger = &leveledLogger{
		logger: logger.L().Detach(ctx),
	}

	return client
}

// retryOnCompleteError extends the default retry policy so a transient
// CompleteMultipartUpload failure is retried like a 5xx. S3 and the GCS XML
// API can return HTTP 200 with an <Error> body (e.g. InternalError) for a
// commit that didn't happen; the default policy only looks at the status code,
// so without this such a response fails on the first attempt and ignores the
// configured retry budget. Only the complete request (URL query "uploadId=…",
// small XML body) is inspected, so buffering the body to peek + restore it is
// cheap.
func retryOnCompleteError(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if retry, rerr := retryablehttp.DefaultRetryPolicy(ctx, resp, err); retry || rerr != nil {
		return retry, rerr
	}

	if resp == nil || resp.StatusCode != http.StatusOK || resp.Request == nil ||
		!strings.HasPrefix(resp.Request.URL.RawQuery, "uploadId=") || resp.Body == nil {
		return false, nil
	}

	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	// Restore the body so the caller (or a retry) can read it.
	resp.Body = io.NopCloser(bytes.NewReader(body))

	// Retry when the body couldn't be fully read (transient truncation/reset
	// after the headers) or when it carries a transient <Error> (e.g.
	// InternalError). Crucially, a read failure must not fall through as
	// no-retry: that would hand completeUpload a clean, buffered partial body
	// and let it commit on a truncated response.
	var apiErr completeMultipartError
	retry := readErr != nil || (xml.Unmarshal(body, &apiErr) == nil && apiErr.Code != "")

	return retry, nil //nolint:nilerr // decision is carried by the bool; returning readErr would abort the retry loop instead of retrying
}

// zapLogger adapts zap.Logger to retryablehttp.LeveledLogger interface
var _ retryablehttp.LeveledLogger = &leveledLogger{}

type leveledLogger struct {
	logger *zap.Logger
}

func (z *leveledLogger) Error(msg string, keysAndValues ...any) {
	z.logger.Error(msg, zap.Any("details", keysAndValues))
}

func (z *leveledLogger) Info(msg string, keysAndValues ...any) {
	z.logger.Info(msg, zap.Any("details", keysAndValues))
}

func (z *leveledLogger) Debug(string, ...any) {
	// Ignore debug logs
}

func (z *leveledLogger) Warn(msg string, keysAndValues ...any) {
	z.logger.Warn(msg, zap.Any("details", keysAndValues))
}

type InitiateMultipartUploadResult struct {
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

type CompleteMultipartUpload struct {
	XMLName string `xml:"CompleteMultipartUpload"`
	Parts   []Part `xml:"Part"`
}

type Part struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// completeMultipartError matches the S3/GCS XML error payload that can arrive
// with an HTTP 200 status on CompleteMultipartUpload. Unmarshal only succeeds
// (Code populated) when the response root is <Error>.
type completeMultipartError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

type MultipartUploader struct {
	bucketName  string
	objectName  string
	token       string
	client      *retryablehttp.Client
	retryConfig RetryConfig
	metadata    ObjectMetadata
	baseURL     string // Allow overriding for testing

	// Fields for partUploader interface
	uploadID string
	mu       sync.Mutex
	parts    []Part
	// completed is guarded by mu; set only by completeUpload, read by Close.
	completed bool
}

var _ partUploader = (*MultipartUploader)(nil)

// Start initiates the GCS multipart upload.
func (m *MultipartUploader) Start(ctx context.Context) error {
	uploadID, err := m.initiateUpload(ctx)
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	m.uploadID = uploadID

	return nil
}

// UploadPart uploads a single part to GCS. Multiple data slices are hashed
// and uploaded without copying into a single contiguous buffer.
func (m *MultipartUploader) UploadPart(ctx context.Context, partIndex int, data ...[]byte) error {
	etag, err := m.uploadPartSlices(ctx, m.uploadID, partIndex, data)
	if err != nil {
		return fmt.Errorf("failed to upload part %d: %w", partIndex, err)
	}

	m.mu.Lock()
	m.parts = append(m.parts, Part{
		PartNumber: partIndex,
		ETag:       etag,
	})
	m.mu.Unlock()

	return nil
}

// Complete finalizes the GCS multipart upload with all collected parts.
func (m *MultipartUploader) Complete(ctx context.Context) error {
	m.mu.Lock()
	parts := make([]Part, len(m.parts))
	copy(parts, m.parts)
	m.mu.Unlock()

	return m.completeUpload(ctx, m.uploadID, parts)
}

// Close aborts an upload that was started but never committed, discarding
// its staged parts.
func (m *MultipartUploader) Close() error {
	m.mu.Lock()
	completed := m.completed
	m.mu.Unlock()

	if completed || m.uploadID == "" {
		return nil
	}

	// The upload's context is usually already canceled here, so abort on a
	// detached context with its own deadline.
	ctx, cancel := context.WithTimeout(context.Background(), gcpAbortUploadTimeout)
	defer cancel()

	return m.abortUpload(ctx, m.uploadID)
}

func NewMultipartUploaderWithRetryConfig(ctx context.Context, bucketName, objectName string, retryConfig RetryConfig, metadata ObjectMetadata) (*MultipartUploader, error) {
	creds, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials: %w", err)
	}

	token, err := creds.TokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	return &MultipartUploader{
		bucketName:  bucketName,
		objectName:  objectName,
		token:       token.AccessToken,
		client:      createRetryableClient(ctx, retryConfig),
		retryConfig: retryConfig,
		metadata:    metadata,
		baseURL:     fmt.Sprintf("https://%s.storage.googleapis.com", bucketName),
	}, nil
}

func (m *MultipartUploader) initiateUpload(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/%s?uploads", m.baseURL, m.objectName)

	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+m.token)
	req.Header.Set("Content-Length", "0")
	req.Header.Set("Content-Type", "application/octet-stream")
	// Custom user metadata is set on initiate; the final object inherits it.
	for k, v := range m.metadata {
		req.Header.Set("x-goog-meta-"+k, v)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return "", fmt.Errorf("failed to initiate upload (status %d): %s", resp.StatusCode, string(body))
	}

	var result InitiateMultipartUploadResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to parse initiate response: %w", err)
	}

	return result.UploadID, nil
}

func (m *MultipartUploader) uploadPart(ctx context.Context, uploadID string, partNumber int, data []byte) (string, error) {
	url := fmt.Sprintf("%s/%s?partNumber=%d&uploadId=%s",
		m.baseURL, m.objectName, partNumber, uploadID)

	// A non-nil zero-length body counts as "unknown length" to net/http and
	// is sent chunked, which S3-compatible XML backends reject with 411. Pass
	// no body for empty parts so Content-Length: 0 is sent instead.
	var body any
	if len(data) > 0 {
		body = bytes.NewReader(data)
	}

	req, err := retryablehttp.NewRequestWithContext(ctx, "PUT", url, body)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+m.token)
	sum := md5.Sum(data) //nolint:gosec // GCS multipart uses Content-MD5 for transport integrity.
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))

	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return "", fmt.Errorf("failed to upload part %d (status %d): %s", partNumber, resp.StatusCode, string(body))
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("no ETag returned for part %d", partNumber)
	}

	return etag, nil
}

// uploadPartSlices uploads a part from multiple byte slices without concatenating them.
func (m *MultipartUploader) uploadPartSlices(ctx context.Context, uploadID string, partNumber int, slices [][]byte) (string, error) {
	totalLen := 0
	for _, s := range slices {
		totalLen += len(s)
	}

	url := fmt.Sprintf("%s/%s?partNumber=%d&uploadId=%s",
		m.baseURL, m.objectName, partNumber, uploadID)

	// Use a ReaderFunc so the retryable client can replay the body on
	// retries. The multiSliceReader's Len makes retryablehttp set the
	// request's ContentLength, so parts are sent with an explicit
	// Content-Length rather than chunked transfer encoding (which GCS
	// tolerates but S3-compatible XML backends reject with 411). Empty parts
	// send no body at all for the same reason: a zero-length reader still
	// counts as "unknown length" to net/http and would be chunked.
	var body any
	if totalLen > 0 {
		body = retryablehttp.ReaderFunc(func() (io.Reader, error) {
			return newMultiSliceReader(slices), nil
		})
	}

	req, err := retryablehttp.NewRequestWithContext(ctx, "PUT", url, body)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+m.token)
	h := md5.New() //nolint:gosec // GCS multipart uses Content-MD5 for transport integrity.
	for _, s := range slices {
		_, _ = h.Write(s)
	}
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(h.Sum(nil)))

	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return "", fmt.Errorf("failed to upload part %d (status %d): %s", partNumber, resp.StatusCode, string(body))
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("no ETag returned for part %d", partNumber)
	}

	return etag, nil
}

func (m *MultipartUploader) completeUpload(ctx context.Context, uploadID string, parts []Part) error {
	// Sort parts by part number
	slices.SortFunc(parts, func(a, b Part) int {
		return cmp.Compare(a.PartNumber, b.PartNumber)
	})

	completeReq := CompleteMultipartUpload{Parts: parts}
	xmlData, err := xml.Marshal(completeReq)
	if err != nil {
		return fmt.Errorf("failed to marshal complete request: %w", err)
	}

	url := fmt.Sprintf("%s/%s?uploadId=%s",
		m.baseURL, m.objectName, uploadID)

	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(xmlData))
	if err != nil {
		return fmt.Errorf("failed to create complete request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.token)
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(xmlData)))

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("read complete upload response (status %d): %w", resp.StatusCode, readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to complete upload (status %d): %s", resp.StatusCode, string(body))
	}

	// S3 and the GCS XML API can return HTTP 200 with an <Error> body when the
	// commit fails server-side (documented for internal errors/timeouts).
	// Treating that as success would record a frame table for an object that
	// was never committed, so reject any response whose root is an Error.
	var apiErr completeMultipartError
	if xml.Unmarshal(body, &apiErr) == nil && apiErr.Code != "" {
		return fmt.Errorf("failed to complete upload (status %d, code %s): %s", resp.StatusCode, apiErr.Code, apiErr.Message)
	}

	// Set here, not in Complete: UploadFileInParallel also commits through
	// this path, and Close must never abort a live object.
	m.mu.Lock()
	m.completed = true
	m.mu.Unlock()

	return nil
}

// abortUpload discards an in-flight multipart upload and its staged parts.
// GCS answers 204 on success and 404 when the upload is already gone; both
// mean nothing is left to clean up.
func (m *MultipartUploader) abortUpload(ctx context.Context, uploadID string) error {
	url := fmt.Sprintf("%s/%s?uploadId=%s",
		m.baseURL, m.objectName, uploadID)

	req, err := retryablehttp.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create abort request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.token)

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("failed to abort upload (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func (m *MultipartUploader) UploadFileInParallel(ctx context.Context, filePath string, maxConcurrency int, hasher hash.Hash) (int64, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate number of parts
	numParts := int(math.Ceil(float64(fileSize) / float64(gcpMultipartUploadChunkSize)))
	if numParts == 0 {
		numParts = 1 // Always upload at least 1 part, even for empty files
	}

	// Initiate multipart upload
	uploadID, err := m.initiateUpload(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to initiate upload: %w", err)
	}

	// Unlike the compressed path, nothing wraps this in compressStream's
	// deferred Close, so abort here on failure; Close needs m.uploadID and is
	// a no-op after commit.
	m.uploadID = uploadID
	defer func() {
		if abortErr := m.Close(); abortErr != nil { //nolint:contextcheck // Close aborts on a detached context by design.
			logger.L().Warn(ctx, "failed to abort GCS multipart upload",
				zap.String("bucket", m.bucketName),
				zap.String("object", m.objectName),
				zap.Error(abortErr),
			)
		}
	}()

	// Hash on a sibling goroutine while parts upload — the read overlaps the
	// upload, adding no wall-clock latency. Own file handle (separate from the
	// part uploaders' ReadAt); opened here so a failed upload can close it.
	var hashFile *os.File
	if hasher != nil {
		hashFile, err = os.Open(filePath)
		if err != nil {
			return 0, fmt.Errorf("failed to open file for checksum: %w", err)
		}
		defer hashFile.Close()
	}

	var eg errgroup.Group
	if hashFile != nil {
		eg.Go(func() error {
			if _, err := io.Copy(hasher, hashFile); err != nil {
				return fmt.Errorf("failed to checksum file: %w", err)
			}

			return nil
		})
	}

	parts, err := m.uploadParts(ctx, maxConcurrency, numParts, fileSize, file, uploadID)
	if hashFile != nil && err != nil {
		hashFile.Close() // cancel the now-pointless io.Copy
	}
	if hashErr := eg.Wait(); err == nil {
		err = hashErr
	}
	if err != nil {
		return 0, fmt.Errorf("failed to upload file: %w", err)
	}

	if err := m.completeUpload(ctx, uploadID, parts); err != nil {
		return 0, fmt.Errorf("failed to complete upload: %w", err)
	}

	return fileSize, nil
}

func (m *MultipartUploader) uploadParts(ctx context.Context, maxConcurrency int, numParts int, fileSize int64, file *os.File, uploadID string) ([]Part, error) {
	g, ctx := errgroup.WithContext(ctx) // Context ONLY for waitgroup goroutines; canceled after errgroup finishes
	// SetLimit(0) blocks every Go call, while a negative limit disables the
	// bound. Always retain at least one admitted uploader.
	g.SetLimit(max(maxConcurrency, 1))

	// Thread-safe map to collect parts
	var partsMu sync.Mutex
	parts := make([]Part, numParts)

	// Upload each part concurrently
	for partNumber := 1; partNumber <= numParts; partNumber++ {
		g.Go(func() error {
			// Check if context was cancelled
			select {
			case <-ctx.Done():
				return fmt.Errorf("part %d failed: %w", partNumber, ctx.Err())
			default:
			}

			// Read chunk from file
			offset := int64(partNumber-1) * gcpMultipartUploadChunkSize
			chunkSize := gcpMultipartUploadChunkSize
			if offset+int64(chunkSize) > fileSize {
				chunkSize = int(fileSize - offset)
			}

			chunk := make([]byte, chunkSize)
			_, err := file.ReadAt(chunk, offset)
			if err != nil {
				return fmt.Errorf("failed to read chunk for part %d: %w", partNumber, err)
			}

			// Upload part
			etag, err := m.uploadPart(ctx, uploadID, partNumber, chunk)
			if err != nil {
				return fmt.Errorf("failed to upload part %d: %w", partNumber, err)
			}

			// Store result thread-safely
			partsMu.Lock()
			parts[partNumber-1] = Part{
				PartNumber: partNumber,
				ETag:       etag,
			}
			partsMu.Unlock()

			return nil
		})
	}

	// Wait for all parts to complete or first error
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	return parts, nil
}
