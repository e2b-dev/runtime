package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	awsOperationTimeout        = 5 * time.Second
	awsWriteTimeout            = 30 * time.Second
	awsReadTimeout             = 15 * time.Second
	awsMultipartUploadPartSize = 10 * 1024 * 1024
)

type awsStorage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucketName    string
	limiter       *limit.Limiter
}

var _ StorageProvider = (*awsStorage)(nil)

type awsObject struct {
	client     *s3.Client
	path       string
	bucketName string
	limiter    *limit.Limiter
}

var (
	_ Seekable = (*awsObject)(nil)
	_ Blob     = (*awsObject)(nil)
)

func newAWSStorage(ctx context.Context, spec Spec, limiter *limit.Limiter) (*awsStorage, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	otelaws.AppendMiddlewares(&cfg.APIOptions)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Options the spec leaves unset fall back to the AWS SDK's own
		// resolution (AWS_ENDPOINT_URL, AWS_REGION, credentials, …).
		if spec.Endpoint != "" {
			o.BaseEndpoint = aws.String(spec.Endpoint)
		}
		if spec.Region != "" {
			o.Region = spec.Region
		}

		// Path-style addressing (https://host/bucket/key instead of the SDK's
		// default virtual-host style https://bucket.host/key) is required by
		// most S3-compatible backends (MinIO, Ceph, …).
		o.UsePathStyle = spec.UsePathStyle

		// S3-compatible backends (MinIO, Ceph, etc.) do not return x-amz-checksum-*
		// response headers. Without this setting the SDK logs a WARN for every
		// GetObject response because it cannot validate the payload checksum.
		// WhenRequired tells the SDK to only validate when the caller explicitly
		// requested a checksum — matching the actual contract with these backends.
		// Standard AWS S3 returns checksums and benefits from WhenSupported (the
		// SDK default), so this opt-out is gated on UsePathStyle which already
		// identifies S3-compatible deployments.
		if spec.UsePathStyle {
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})
	presignClient := s3.NewPresignClient(client)

	return &awsStorage{
		client:        client,
		presignClient: presignClient,
		bucketName:    spec.Bucket,
		limiter:       limiter,
	}, nil
}

func (s *awsStorage) DeleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, awsOperationTimeout)
	defer cancel()

	list, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &s.bucketName, Prefix: &prefix})
	if err != nil {
		return err
	}

	objects := make([]types.ObjectIdentifier, 0, len(list.Contents))
	for _, obj := range list.Contents {
		objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
	}

	// AWS S3 delete operation requires at least one object to delete.
	if len(objects) == 0 {
		logger.L().Warn(ctx, "No objects found to delete with the given prefix", zap.String("prefix", prefix), zap.String("bucket", s.bucketName))

		return nil
	}

	output, err := s.client.DeleteObjects(
		ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucketName),
			Delete: &types.Delete{Objects: objects},
		},
	)
	if err != nil {
		return err
	}

	if len(output.Errors) > 0 {
		var errStr strings.Builder
		for _, delErr := range output.Errors {
			fmt.Fprintf(&errStr, "Key: %s, Code: %s, Message: %s; ", aws.ToString(delErr.Key), aws.ToString(delErr.Code), aws.ToString(delErr.Message))
		}

		return errors.New("errors occurred during deletion: " + errStr.String())
	}

	if len(output.Deleted) != len(objects) {
		return errors.New("not all objects listed were deleted")
	}

	return nil
}

func (s *awsStorage) GetDetails() string {
	return fmt.Sprintf("[AWS Storage, bucket set to %s]", s.bucketName)
}

func (s *awsStorage) UploadSignedURL(ctx context.Context, path string, ttl time.Duration) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(path),
	}
	resp, err := s.presignClient.PresignPutObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = ttl
	})
	if err != nil {
		return "", fmt.Errorf("failed to presign PUT URL: %w", err)
	}

	return resp.URL, nil
}

func (s *awsStorage) OpenSeekable(_ context.Context, path string) (Seekable, error) {
	return &awsObject{
		client:     s.client,
		bucketName: s.bucketName,
		path:       path,
		limiter:    s.limiter,
	}, nil
}

func (s *awsStorage) OpenBlob(_ context.Context, path string) (Blob, error) {
	return &awsObject{
		client:     s.client,
		bucketName: s.bucketName,
		path:       path,
		limiter:    s.limiter,
	}, nil
}

func (o *awsObject) WriteTo(ctx context.Context, dst io.Writer) (n int64, err error) {
	start := time.Now()
	defer func() { RecordReadBlob(ctx, time.Since(start), n, o.path, SourceAWS, err) }()

	ctx, cancel := context.WithTimeout(ctx, awsReadTimeout)
	defer cancel()

	resp, err := o.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &o.bucketName, Key: &o.path})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return 0, ErrObjectNotExist
		}

		return 0, err
	}

	defer resp.Body.Close()

	n, err = io.Copy(dst, resp.Body)

	return n, err
}

func (o *awsObject) StoreFile(ctx context.Context, path string, opts ...PutOption) (*FullFrameTable, [32]byte, error) {
	p := ApplyPutOptions(opts)

	release, err := o.limiter.AcquireUploadSlot(ctx)
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer release()

	cfg := CompressConfigFromOpts(p)
	if cfg.IsCompressionEnabled() {
		return storeFileCompressed(ctx, path, cfg, o.limiter.MaxUploadTasks(ctx), p, func(metadata ObjectMetadata) (partUploader, error) {
			return &awsPartUploader{client: o.client, bucketName: o.bucketName, objectName: o.path, metadata: metadata}, nil
		})
	}

	// Inherit the caller's context for the multipart upload. The AWS SDK's
	// manager.Uploader reuses the same ctx for CreateMultipartUpload, every
	// UploadPart, and the final Complete/Abort —
	// a tight static timeout here would cancel an in-flight multi-GB snapshot
	// upload and surface as "S3: UploadPart ... StatusCode: 0, canceled,
	// context deadline exceeded". The caller (pkg/server/sandboxes.go) already
	// scopes a per-attempt deadline (uploadTimeout = 20m) with retry budget on
	// top, matching the GCP path which also inherits the caller's ctx.
	f, err := os.Open(path)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer f.Close()

	uploader := manager.NewUploader(
		o.client,
		func(u *manager.Uploader) {
			u.PartSize = awsMultipartUploadPartSize
			u.Concurrency = o.limiter.MaxUploadTasks(ctx)
		},
	)

	_, err = uploader.Upload(
		ctx,
		&s3.PutObjectInput{
			Bucket:   &o.bucketName,
			Key:      &o.path,
			Body:     f,
			Metadata: p.Metadata,
		},
	)
	if err == nil {
		fi, _ := f.Stat()
		var size int64
		if fi != nil {
			size = fi.Size()
		}

		logger.L().Debug(ctx, "Uploaded file to S3",
			zap.String("bucket", o.bucketName),
			zap.String("object", o.path),
			zap.String("source", path),
			zap.Int64("size_uncompressed", size),
			zap.String("compression", "none"),
		)
	}

	return nil, [32]byte{}, err
}

func (o *awsObject) Put(ctx context.Context, data []byte, opts ...PutOption) error {
	ctx, cancel := context.WithTimeout(ctx, awsWriteTimeout)
	defer cancel()

	_, err := o.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:   &o.bucketName,
			Key:      &o.path,
			Body:     bytes.NewReader(data),
			Metadata: ApplyPutOptions(opts).Metadata,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (o *awsObject) OpenRangeReader(ctx context.Context, off, length int64, frameTable *FrameTable) (_ RangeReader, _ Source, err error) {
	start := time.Now()
	objType, _ := seekableObjectType(o.path)
	defer func() {
		RecordReadOpen(ctx, time.Since(start), objType, SourceAWS, frameTable.CompressionType(), err)
	}()

	if !frameTable.IsCompressed() {
		rc, err := o.openRangeReader(ctx, off, length)
		if err != nil {
			return nil, SourceAWS, err
		}

		return rc, SourceAWS, nil
	}

	r, err := frameTable.LocateCompressed(off)
	if err != nil {
		return nil, SourceAWS, fmt.Errorf("get frame for offset %d, S3:%s: %w", off, o.path, err)
	}

	raw, err := o.openRangeReader(ctx, r.Offset, int64(r.Length))
	if err != nil {
		return nil, SourceAWS, err
	}

	dec, err := NewDecompressReader(raw, frameTable.CompressionType(), SourceAWS, objType)
	if err != nil {
		raw.Close(ctx)

		return nil, SourceAWS, err
	}

	return dec, SourceAWS, nil
}

func (o *awsObject) openRangeReader(ctx context.Context, off, length int64) (RangeReader, error) {
	resp, err := o.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(o.bucketName),
		Key:    aws.String(o.path),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+length-1)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrObjectNotExist
		}

		return nil, fmt.Errorf("failed to create S3 range reader for %q: %w", o.path, err)
	}

	return NewRangeReader(resp.Body), nil
}

func (o *awsObject) Size(ctx context.Context) (_ int64, err error) {
	start := time.Now()
	objType, _ := seekableObjectType(o.path)
	defer func() { RecordReadSize(ctx, time.Since(start), objType, SourceAWS, err) }()

	ctx, cancel := context.WithTimeout(ctx, awsOperationTimeout)
	defer cancel()

	resp, err := o.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &o.bucketName, Key: &o.path})
	if err != nil {
		var nsk *types.NoSuchKey
		var nfd *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &nfd) {
			return 0, ErrObjectNotExist
		}

		return 0, err
	}

	if size, ok := ObjectMetadata(resp.Metadata).UncompressedSize(); ok {
		return size, nil
	}

	return *resp.ContentLength, nil
}

func (o *awsObject) Exists(ctx context.Context) (bool, error) {
	_, err := o.Size(ctx)

	return err == nil, ignoreNotExists(err)
}

func (o *awsObject) Delete(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, awsOperationTimeout)
	defer cancel()

	_, err := o.client.DeleteObject(
		ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(o.bucketName),
			Key:    aws.String(o.path),
		},
	)

	return err
}

func ignoreNotExists(err error) error {
	if errors.Is(err, ErrObjectNotExist) {
		return nil
	}

	return err
}

type awsPartUploader struct {
	client     *s3.Client
	bucketName string
	objectName string
	metadata   ObjectMetadata

	mu       sync.Mutex
	uploadID string
	parts    []types.CompletedPart
	// completed needs no lock: compressStream calls Complete and the deferred
	// Close sequentially from one goroutine, after all UploadPart calls finish.
	completed bool
}

var _ partUploader = (*awsPartUploader)(nil)

func (m *awsPartUploader) Start(ctx context.Context) error {
	out, err := m.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(m.bucketName),
		Key:      aws.String(m.objectName),
		Metadata: m.metadata,
		// The SDK's default integrity protections attach CRC32 checksums to
		// UploadPart requests; S3 requires the algorithm to be declared at
		// initiation and echoed per part in Complete. Declare it explicitly on
		// every call so the flow is consistent regardless of SDK/env config
		// (manager.Uploader does the same for the uncompressed path).
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	m.uploadID = aws.ToString(out.UploadId)

	return nil
}

// UploadPart uploads a single part. Multiple data slices are streamed without
// copying into a contiguous buffer; the section reader's Seek lets the SDK
// compute the payload hash/length and rewind on retries.
func (m *awsPartUploader) UploadPart(ctx context.Context, partIndex int, data ...[]byte) error {
	body := newMultiSliceReader(data)
	out, err := m.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:            aws.String(m.bucketName),
		Key:               aws.String(m.objectName),
		UploadId:          aws.String(m.uploadID),
		PartNumber:        aws.Int32(int32(partIndex)),
		Body:              body,
		ContentLength:     aws.Int64(body.Size()),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return fmt.Errorf("failed to upload part %d: %w", partIndex, err)
	}

	m.mu.Lock()
	m.parts = append(m.parts, types.CompletedPart{
		ETag:          out.ETag,
		ChecksumCRC32: out.ChecksumCRC32,
		PartNumber:    aws.Int32(int32(partIndex)),
	})
	m.mu.Unlock()

	return nil
}

func (m *awsPartUploader) Complete(ctx context.Context) error {
	m.mu.Lock()
	parts := make([]types.CompletedPart, len(m.parts))
	copy(parts, m.parts)
	m.mu.Unlock()

	slices.SortFunc(parts, func(a, b types.CompletedPart) int {
		return int(aws.ToInt32(a.PartNumber) - aws.ToInt32(b.PartNumber))
	})

	_, err := m.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(m.bucketName),
		Key:      aws.String(m.objectName),
		UploadId: aws.String(m.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return err
	}

	m.completed = true

	return nil
}

func (m *awsPartUploader) Close() error {
	if m.completed || m.uploadID == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), awsOperationTimeout)
	defer cancel()

	_, err := m.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(m.bucketName),
		Key:      aws.String(m.objectName),
		UploadId: aws.String(m.uploadID),
	})

	return err
}
