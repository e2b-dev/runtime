package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/storageopts"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/storage")

var ErrObjectNotExist = errors.New("object does not exist")

// ErrSignedUploadURLUnsupported means the provider cannot sign an upload URL usable by an
// external client. Callers that only need the URL on a cache miss may tolerate it.
var ErrSignedUploadURLUnsupported = errors.New("signed upload URLs are not supported by this storage provider")

// ErrObjectRateLimited means per-object mutation rate limiting —
// multiple concurrent writers racing to write the same content-addressed object.
var ErrObjectRateLimited = errors.New("object access rate limited")

// ErrObjectSoftDeleted means the storage index has marked this object for
// deletion (soft-delete tombstone in custom metadata) and enforcement is on.
var ErrObjectSoftDeleted = errors.New("object soft-deleted by storage index")

// ErrMetadataUnsupported means the blob's backend cannot read custom metadata
// (no MetadataReader). It is distinct from "read succeeded, no metadata" so
// callers (e.g. soft-delete enforcement) can fail closed on an unverifiable
// backend instead of assuming there is no tombstone.
var ErrMetadataUnsupported = errors.New("blob does not support reading custom metadata")

// ObjectMetadataSoftDeleted is the storage-index soft-delete tombstone key.
const ObjectMetadataSoftDeleted = storageopts.ObjectMetadataSoftDeleted

// MemoryChunkSize must always be bigger or equal to the block size.
const MemoryChunkSize = 4 * 1024 * 1024 // 4 MB

type SeekableObjectType int

const (
	UnknownSeekableObjectType SeekableObjectType = iota
	MemfileObjectType
	RootFSObjectType
	numSeekableObjectTypes
)

func (t SeekableObjectType) String() string {
	switch t {
	case MemfileObjectType:
		return "memfile"
	case RootFSObjectType:
		return "rootfs"
	default:
		return "unknown"
	}
}

// UploadURL is a signed upload target for an external client; Headers go verbatim on the PUT (a SAS can pin response headers but never require a request one).
type UploadURL struct {
	URL     string
	Headers map[string]string
}

type StorageProvider interface {
	DeleteObjectsWithPrefix(ctx context.Context, prefix string) error
	UploadSignedURL(ctx context.Context, path string, ttl time.Duration) (UploadURL, error)
	OpenBlob(ctx context.Context, path string) (Blob, error)
	OpenSeekable(ctx context.Context, path string) (Seekable, error)
	GetDetails() string
}

type (
	ObjectMetadata = storageopts.ObjectMetadata
	ObjectOrigin   = storageopts.ObjectOrigin
	PutOptions     = storageopts.PutOptions
	PutOption      = storageopts.PutOption
	FrameSink      = storageopts.FrameSink
)

const (
	ObjectMetadataTeamID           = storageopts.ObjectMetadataTeamID
	ObjectMetadataTemplateID       = storageopts.ObjectMetadataTemplateID
	ObjectMetadataBuildOrigin      = storageopts.ObjectMetadataBuildOrigin
	ObjectMetadataUncompressedSize = storageopts.ObjectMetadataUncompressedSize
	ObjectMetadataLogicalSize      = storageopts.ObjectMetadataLogicalSize
	ObjectMetadataMappedSize       = storageopts.ObjectMetadataMappedSize
	ObjectMetadataDiffSize         = storageopts.ObjectMetadataDiffSize

	ObjectOriginPause              = storageopts.ObjectOriginPause
	ObjectOriginTemplateBuild      = storageopts.ObjectOriginTemplateBuild
	ObjectOriginTemplateBuildCache = storageopts.ObjectOriginTemplateBuildCache
	ObjectOriginSnapshotTemplate   = storageopts.ObjectOriginSnapshotTemplate
)

func WithMetadata(metadata ObjectMetadata) PutOption { return storageopts.WithMetadata(metadata) }

// WithCompressConfig threads a typed CompressConfig through PutOptions. It is
// stored as `any` in storageopts to avoid importing storage from there;
// backends use CompressConfigFromOpts to pull it back out.
func WithCompressConfig(cfg CompressConfig) PutOption { return storageopts.WithCompression(cfg) }

func WithFrameSink(s FrameSink) PutOption { return storageopts.WithFrameSink(s) }

func WithChecksumSHA256() PutOption {
	return func(o *PutOptions) { o.Checksum = true }
}

// sum256 finalizes h into a SHA-256 digest, or the zero digest when h is nil.
func sum256(h hash.Hash) [32]byte {
	var sum [32]byte
	if h != nil {
		copy(sum[:], h.Sum(nil))
	}

	return sum
}

func ApplyPutOptions(opts []PutOption) PutOptions { return storageopts.Apply(opts) }

// CompressConfigFromOpts returns the typed CompressConfig set by
// WithCompressConfig, or the zero value if absent.
func CompressConfigFromOpts(p PutOptions) CompressConfig {
	if c, ok := p.Compression.(CompressConfig); ok {
		return c
	}

	return CompressConfig{}
}

type Blob interface {
	WriteTo(ctx context.Context, dst io.Writer) (int64, error)
	Put(ctx context.Context, data []byte, opts ...storageopts.PutOption) error
	Exists(ctx context.Context) (bool, error)
}

// MetadataReader is an optional Blob capability: read the object's custom
// metadata without downloading it. Backends that can't answer cheaply omit it.
type MetadataReader interface {
	Metadata(ctx context.Context) (ObjectMetadata, error)
}

// BlobCustomMetadata returns the blob's custom metadata, or ErrMetadataUnsupported
// when the backend can't read it — so callers can distinguish "no tombstone"
// from "couldn't check" and fail closed under enforcement.
func BlobCustomMetadata(ctx context.Context, b Blob) (ObjectMetadata, error) {
	mr, ok := b.(MetadataReader)
	if !ok {
		return nil, ErrMetadataUnsupported
	}

	return mr.Metadata(ctx)
}

// ReadStats is what a RangeReader did over its lifetime; returned from Close.
type ReadStats struct {
	StoredBytes    int64
	DeliveredBytes int64
	Read           time.Duration // source I/O wall, excluding open and decompression
	Decompress     time.Duration
}

type RangeReader interface {
	io.Reader
	// Close returns the reader's lifetime stats, or nil if it doesn't meter.
	Close(ctx context.Context) (*ReadStats, error)
}

// RangeOpener supports progressive reads via a streaming range reader.
// OpenRangeReader returns the Source that served the bytes.
type RangeOpener interface {
	OpenRangeReader(ctx context.Context, offsetU int64, length int64, frameTable *FrameTable) (RangeReader, Source, error)
}

type SeekableWriter interface {
	// Store entire file. Compression is opt-in via WithCompressConfig.
	StoreFile(ctx context.Context, path string, opts ...PutOption) (*FullFrameTable, [32]byte, error)
}

type Seekable interface {
	RangeOpener
	SeekableWriter
	Size(ctx context.Context) (int64, error)
}

func UploadFramed(ctx context.Context, provider StorageProvider, remotePath string, localPath string, opts ...PutOption) (*FullFrameTable, [32]byte, error) {
	object, err := provider.OpenSeekable(ctx, remotePath)
	if err != nil {
		return nil, [32]byte{}, err
	}

	return object.StoreFile(ctx, localPath, opts...)
}

func UploadBlob(ctx context.Context, provider StorageProvider, remotePath string, localPath string, opts ...PutOption) error {
	blob, err := provider.OpenBlob(ctx, remotePath)
	if err != nil {
		return err
	}

	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", localPath, err)
	}

	return blob.Put(ctx, data, opts...)
}

// PeerTransitionedError is returned by the peer Seekable when the remote
// storage upload has completed; the caller should re-load the V4 header from
// storage.
type PeerTransitionedError struct {
	RetryAfter time.Duration
}

func (e *PeerTransitionedError) Error() string {
	return "peer upload completed, reload header from storage"
}

func recordError(span trace.Span, err error) {
	if ignoreEOF(err) == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// GetBlob is a convenience wrapper that wraps b.WriteTo interface to return a
// byte slice.
func GetBlob(ctx context.Context, b Blob) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := b.WriteTo(ctx, &buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
