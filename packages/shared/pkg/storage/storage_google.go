package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	googleReadTimeout             = 10 * time.Second
	googleOperationTimeout        = 5 * time.Second
	googleBufferSize              = 4 << 20 // 4 MiB
	googleInitialBackoff          = 10 * time.Millisecond
	googleMaxBackoff              = 10 * time.Second
	googleBackoffMultiplier       = 2
	googleMaxAttempts             = 10
	defaultGRPCConnectionPoolSize = 8
	defaultGCSEnableDirectPath    = false

	gcsOperationAttr                    = "operation"
	gcsOperationAttrWrite               = "Write"
	gcsOperationAttrWriteFromFileSystem = "WriteFromFileSystem"
)

type gcpStorage struct {
	client *storage.Client
	bucket *storage.BucketHandle

	limiter *limit.Limiter
}

var _ StorageProvider = (*gcpStorage)(nil)

type gcpObject struct {
	storage *gcpStorage
	path    string
	handle  *storage.ObjectHandle
	objType SeekableObjectType

	limiter *limit.Limiter
}

var (
	_ Seekable    = (*gcpObject)(nil)
	_ Blob        = (*gcpObject)(nil)
	_ RangeOpener = (*gcpObject)(nil)
)

func NewGCP(ctx context.Context, bucketName string, limiter *limit.Limiter) (StorageProvider, error) {
	grpcPoolSize, err := env.GetEnvAsInt("GCS_GRPC_CONNECTION_POOL_SIZE", defaultGRPCConnectionPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GCS_GRPC_CONNECTION_POOL_SIZE: %w", err)
	}

	opts := []option.ClientOption{
		option.WithGRPCConnectionPool(grpcPoolSize),
		option.WithGRPCDialOption(grpc.WithInitialConnWindowSize(32 * megabyte)),
		option.WithGRPCDialOption(grpc.WithInitialWindowSize(4 * megabyte)),
		internaloption.EnableDirectPath(defaultGCSEnableDirectPath),
	}

	client, err := storage.NewGRPCClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &gcpStorage{
		client:  client,
		bucket:  client.Bucket(bucketName),
		limiter: limiter,
	}, nil
}

func (s *gcpStorage) DeleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	if err := validateObjectPath(prefix); err != nil {
		return err
	}

	objects := s.bucket.Objects(ctx, &storage.Query{Prefix: prefix + "/"})

	for {
		object, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return fmt.Errorf("error when iterating over template objects: %w", err)
		}

		err = s.bucket.Object(object.Name).Delete(ctx)
		if err != nil {
			return fmt.Errorf("error when deleting template object: %w", err)
		}
	}

	return nil
}

func (s *gcpStorage) GetDetails() string {
	return fmt.Sprintf("[GCP Storage, bucket set to %s]", s.bucket.BucketName())
}

func (s *gcpStorage) UploadSignedURL(_ context.Context, path string, ttl time.Duration) (UploadURL, error) {
	if err := validateObjectPath(path); err != nil {
		return UploadURL{}, err
	}

	token, err := parseServiceAccountBase64(consts.GoogleServiceAccountSecret)
	if err != nil {
		return UploadURL{}, fmt.Errorf("failed to parse GCP service account: %w", err)
	}

	opts := &storage.SignedURLOptions{
		GoogleAccessID: token.ClientEmail,
		PrivateKey:     []byte(token.PrivateKey),
		Method:         http.MethodPut,
		Expires:        time.Now().Add(ttl),
	}

	url, err := storage.SignedURL(s.bucket.BucketName(), path, opts)
	if err != nil {
		return UploadURL{}, fmt.Errorf("failed to create signed URL for GCS object (%s): %w", path, err)
	}

	return UploadURL{URL: url}, nil
}

func (s *gcpStorage) OpenSeekable(_ context.Context, path string) (Seekable, error) {
	if err := validateObjectPath(path); err != nil {
		return nil, err
	}

	handle := s.bucket.Object(path).Retryer(
		storage.WithMaxAttempts(googleMaxAttempts),
		storage.WithPolicy(storage.RetryAlways),
		storage.WithBackoff(
			gax.Backoff{
				Initial:    googleInitialBackoff,
				Max:        googleMaxBackoff,
				Multiplier: googleBackoffMultiplier,
			},
		),
	)

	objType, _ := seekableObjectType(path)

	return &gcpObject{
		storage: s,
		path:    path,
		handle:  handle,
		objType: objType,

		limiter: s.limiter,
	}, nil
}

func (s *gcpStorage) OpenBlob(_ context.Context, path string) (Blob, error) {
	if err := validateObjectPath(path); err != nil {
		return nil, err
	}

	handle := s.bucket.Object(path).Retryer(
		storage.WithMaxAttempts(googleMaxAttempts),
		storage.WithPolicy(storage.RetryAlways),
		storage.WithBackoff(
			gax.Backoff{
				Initial:    googleInitialBackoff,
				Max:        googleMaxBackoff,
				Multiplier: googleBackoffMultiplier,
			},
		),
	)

	return &gcpObject{
		storage: s,
		path:    path,
		handle:  handle,

		limiter: s.limiter,
	}, nil
}

func (o *gcpObject) Delete(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, googleOperationTimeout)
	defer cancel()

	if err := o.handle.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete %q: %w", o.path, err)
	}

	return nil
}

func (o *gcpObject) Exists(ctx context.Context) (bool, error) {
	_, err := o.Size(ctx)

	return err == nil, ignoreNotExists(err)
}

func (o *gcpObject) Size(ctx context.Context) (_ int64, err error) {
	start := time.Now()
	defer func() { RecordReadSize(ctx, time.Since(start), o.objType, SourceGCS, err) }()

	ctx, cancel := context.WithTimeout(ctx, googleOperationTimeout)
	defer cancel()

	attrs, err := o.handle.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// use ours instead of theirs
			return 0, fmt.Errorf("failed to get GCS object (%q) attributes: %w", o.path, ErrObjectNotExist)
		}

		return 0, fmt.Errorf("failed to get GCS object (%q) attributes: %w", o.path, err)
	}

	if size, ok := ObjectMetadata(attrs.Metadata).UncompressedSize(); ok {
		return size, nil
	}

	return attrs.Size, nil
}

// Metadata implements MetadataReader via Attrs (always hits GCS).
func (o *gcpObject) Metadata(ctx context.Context) (ObjectMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, googleOperationTimeout)
	defer cancel()

	attrs, err := o.handle.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("failed to get GCS object (%q) attributes: %w", o.path, ErrObjectNotExist)
		}

		return nil, fmt.Errorf("failed to get GCS object (%q) attributes: %w", o.path, err)
	}

	return ObjectMetadata(attrs.Metadata), nil
}

func (o *gcpObject) openRangeReader(ctx context.Context, off, length int64) (RangeReader, error) {
	readCtx, cancel := context.WithCancel(ctx)

	openTimer := time.AfterFunc(googleReadTimeout, cancel)
	reader, err := o.handle.NewRangeReader(readCtx, off, length)
	// Stop returning false means the timer fired and cancel was/is being called,
	// so readCtx is cancelled and reader (if any) is unusable.
	if !openTimer.Stop() && err == nil {
		reader.Close()
		err = context.DeadlineExceeded
	}
	if err != nil {
		cancel()

		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("failed to create GCS range reader for %q at %d+%d: %w", o.path, off, length, ErrObjectNotExist)
		}

		return nil, fmt.Errorf("failed to create GCS range reader for %q at %d+%d: %w", o.path, off, length, err)
	}

	return &idleTimeoutReader{
		ReadCloser: reader,
		cancel:     cancel,
		timer:      time.AfterFunc(googleReadTimeout, cancel),
	}, nil
}

// idleTimeoutReader fires cancel() after googleReadTimeout with no Read
// activity (in-flight Read with no progress, or no Read called).
type idleTimeoutReader struct {
	readMeter
	io.ReadCloser

	cancel context.CancelFunc
	timer  *time.Timer
}

func (r *idleTimeoutReader) Read(p []byte) (int, error) {
	r.timer.Reset(googleReadTimeout)

	t0 := time.Now()
	n, err := r.ReadCloser.Read(p)
	r.observe(n, t0)
	if err != nil {
		r.timer.Stop()
	} else {
		r.timer.Reset(googleReadTimeout)
	}

	return n, err
}

func (r *idleTimeoutReader) Close(_ context.Context) (*ReadStats, error) {
	r.timer.Stop()
	defer r.cancel()

	return r.stats(), r.ReadCloser.Close()
}

func (o *gcpObject) Put(ctx context.Context, data []byte, opts ...PutOption) error {
	timer := googleWriteTimerFactory.Begin(attribute.String(gcsOperationAttr, gcsOperationAttrWrite))

	w := o.handle.NewWriter(ctx)
	if putOpts := ApplyPutOptions(opts); len(putOpts.Metadata) > 0 {
		w.Metadata = putOpts.Metadata
	}

	c, err := io.Copy(w, bytes.NewReader(data))
	if err != nil && !errors.Is(err, io.EOF) {
		closeErr := w.Close()
		if closeErr != nil {
			logger.L().Warn(ctx, "failed to close GCS writer after copy error",
				zap.String("object", o.path),
				zap.NamedError("error_copy", err),
				zap.Error(closeErr),
			)
		}

		timer.Failure(ctx, c)

		// ResourceExhausted from GCS means per-object mutation rate limiting —
		// multiple concurrent writers racing to write the same content-addressed object.
		if isResourceExhausted(err) {
			return ErrObjectRateLimited
		}

		return fmt.Errorf("failed to write to %q: %w", o.path, err)
	}

	// For small objects the GCS Writer buffers data in memory during Write()
	// and performs the actual upload during Close(). ResourceExhausted errors
	// from per-object mutation rate limiting will surface here.
	if err := w.Close(); err != nil {
		timer.Failure(ctx, c)

		if isResourceExhausted(err) {
			return ErrObjectRateLimited
		}

		return fmt.Errorf("failed to write to %q: %w", o.path, err)
	}

	timer.Success(ctx, c)

	return nil
}

func (o *gcpObject) WriteTo(ctx context.Context, dst io.Writer) (n int64, err error) {
	start := time.Now()
	defer func() { RecordReadBlob(ctx, time.Since(start), n, o.path, SourceGCS, err) }()

	ctx, cancel := context.WithTimeout(ctx, googleReadTimeout)
	defer cancel()

	reader, err := o.handle.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return 0, fmt.Errorf("failed to create reader for %q: %w", o.path, ErrObjectNotExist)
		}

		return 0, fmt.Errorf("failed to create reader for %q: %w", o.path, err)
	}

	defer reader.Close()

	buff := make([]byte, googleBufferSize)
	n, err = io.CopyBuffer(dst, reader, buff)
	if err != nil {
		return n, fmt.Errorf("failed to copy %q to buffer: %w", o.path, err)
	}

	return n, nil
}

func (o *gcpObject) StoreFile(ctx context.Context, path string, opts ...PutOption) (_ *FullFrameTable, _ [32]byte, e error) {
	ctx, span := tracer.Start(ctx, "write to gcp from file system")
	defer func() {
		recordError(span, e)
		span.End()
	}()

	putOpts := ApplyPutOptions(opts)

	bucketName := o.storage.bucket.BucketName()
	objectName := o.path

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to get file size: %w", err)
	}

	cfg := CompressConfigFromOpts(putOpts)
	compressionType := cfg.CompressionType()
	compressionMetricType := compressionType.String()
	if cfg.IsCompressionEnabled() && compressionType == CompressionZstd {
		compressionMetricType = fmt.Sprintf("%s-%d", compressionMetricType, cfg.Level)
	}

	timer := googleWriteTimerFactory.Begin(
		attribute.String(gcsOperationAttr, gcsOperationAttrWriteFromFileSystem),
		attribute.String("compression.type", compressionMetricType),
	)

	release, err := o.limiter.AcquireUploadSlot(ctx)
	if err != nil {
		timer.Failure(ctx, 0)

		return nil, [32]byte{}, err
	}
	defer release()

	maxConcurrency := o.limiter.MaxUploadTasks(ctx)

	// Compressed uploads always go through the multipart compressed path,
	// regardless of file size.
	if cfg.IsCompressionEnabled() {
		start := time.Now()
		ft, checksum, err := storeFileCompressed(ctx, path, cfg, maxConcurrency, putOpts, func(metadata ObjectMetadata) (partUploader, error) {
			return NewMultipartUploaderWithRetryConfig(ctx, bucketName, objectName, DefaultRetryConfig(), metadata)
		})
		if err != nil {
			timer.Failure(ctx, fileInfo.Size())
			logger.L().Error(ctx, "Failed to upload file to GCS",
				zap.String("bucket", bucketName),
				zap.String("object", objectName),
				zap.String("source", path),
				zap.Int64("size_uncompressed", fileInfo.Size()),
				zap.String("compression", cfg.CompressionType().String()),
				zap.Int64("duration_ms", time.Since(start).Milliseconds()),
				zap.Error(err),
			)
		} else {
			timer.Success(ctx, fileInfo.Size())

			t := ft.Table()
			logger.L().Debug(ctx, "Uploaded file to GCS",
				zap.String("bucket", bucketName),
				zap.String("object", objectName),
				zap.String("source", path),
				zap.Int64("size_uncompressed", fileInfo.Size()),
				zap.Int64("size_compressed", t.CompressedSize()),
				zap.String("compression", cfg.CompressionType().String()),
				zap.Int("frames", t.NumFrames()),
				zap.Int64("duration_ms", time.Since(start).Milliseconds()),
			)
		}

		return ft, checksum, err
	}

	// Uncompressed uploads only hash when the caller asked for a checksum.
	var hasher hash.Hash
	if putOpts.Checksum {
		hasher = sha256.New()
	}

	// If the file is too small, the overhead of writing in parallel isn't worth the effort.
	// Write it in one shot instead.
	if fileInfo.Size() < gcpMultipartUploadChunkSize {
		data, err := os.ReadFile(path)
		if err != nil {
			timer.Failure(ctx, 0)

			return nil, [32]byte{}, fmt.Errorf("failed to read file: %w", err)
		}

		err = o.Put(ctx, data, opts...)
		if err != nil {
			timer.Failure(ctx, int64(len(data)))

			return nil, [32]byte{}, fmt.Errorf("failed to write file (%d bytes): %w", len(data), err)
		}

		timer.Success(ctx, int64(len(data)))

		logger.L().Debug(ctx, "Uploaded file to GCS",
			zap.String("bucket", bucketName),
			zap.String("object", objectName),
			zap.String("source", path),
			zap.Int64("size_uncompressed", int64(len(data))),
			zap.String("compression", "none"),
		)

		if hasher != nil {
			hasher.Write(data)
		}

		return nil, sum256(hasher), e
	}

	uploader, err := NewMultipartUploaderWithRetryConfig(
		ctx,
		bucketName,
		objectName,
		DefaultRetryConfig(),
		putOpts.Metadata,
	)
	if err != nil {
		timer.Failure(ctx, 0)

		return nil, [32]byte{}, fmt.Errorf("failed to create multipart uploader: %w", err)
	}

	start := time.Now()
	count, err := uploader.UploadFileInParallel(ctx, path, maxConcurrency, hasher)
	if err != nil {
		timer.Failure(ctx, count)

		return nil, [32]byte{}, fmt.Errorf("failed to upload file in parallel: %w", err)
	}

	logger.L().Debug(ctx, "Uploaded file to GCS",
		zap.String("bucket", bucketName),
		zap.String("object", objectName),
		zap.String("source", path),
		zap.Int64("size_uncompressed", fileInfo.Size()),
		zap.String("compression", "none"),
		zap.Int("max_concurrency", maxConcurrency),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()),
	)

	timer.Success(ctx, count)

	return nil, sum256(hasher), e
}

type gcpServiceToken struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func parseServiceAccountBase64(serviceAccount string) (*gcpServiceToken, error) {
	decoded, err := base64.StdEncoding.DecodeString(serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	var sa gcpServiceToken
	if err := json.Unmarshal(decoded, &sa); err != nil {
		return nil, fmt.Errorf("failed to parse service account JSON: %w", err)
	}

	return &sa, nil
}

func (o *gcpObject) OpenRangeReader(ctx context.Context, offsetU int64, length int64, frameTable *FrameTable) (_ RangeReader, _ Source, err error) {
	start := time.Now()
	defer func() {
		RecordReadOpen(ctx, time.Since(start), o.objType, SourceGCS, frameTable.CompressionType(), err)
	}()

	if !frameTable.IsCompressed() {
		rc, err := o.openRangeReader(ctx, offsetU, length)
		if err != nil {
			return nil, SourceGCS, err
		}

		return rc, SourceGCS, nil
	}

	r, err := frameTable.LocateCompressed(offsetU)
	if err != nil {
		return nil, SourceGCS, fmt.Errorf("get frame for offset %d, GCS:%s: %w", offsetU, o.path, err)
	}

	raw, err := o.openRangeReader(ctx, r.Offset, int64(r.Length))
	if err != nil {
		return nil, SourceGCS, err
	}

	dec, err := NewDecompressReader(raw, frameTable.CompressionType(), SourceGCS, o.objType)
	if err != nil {
		raw.Close(ctx)

		return nil, SourceGCS, err
	}

	return dec, SourceGCS, nil
}

func isResourceExhausted(err error) bool {
	type grpcStatusProvider interface {
		GRPCStatus() *status.Status
	}

	var se grpcStatusProvider
	if errors.As(err, &se) {
		return se.GRPCStatus().Code() == codes.ResourceExhausted
	}

	return false
}
