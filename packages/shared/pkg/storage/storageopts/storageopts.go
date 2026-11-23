// Package storageopts holds option types for the storage package, kept
// separate so generated mocks can reference them without an import cycle.
package storageopts

import (
	"context"
	"maps"
	"strconv"
)

type ObjectMetadata map[string]string

// Custom object metadata keys for the storage index (immutable, set-once).
const (
	ObjectMetadataTeamID      = "team_id"
	ObjectMetadataTemplateID  = "template_id"
	ObjectMetadataBuildOrigin = "build_origin"

	// ObjectMetadataUncompressedSize stores the original size of a compressed
	// object so that Size() can report it without fetching the frame table.
	ObjectMetadataUncompressedSize = "uncompressed-size"
)

// WithUncompressedSize returns a copy of the metadata with the original
// (uncompressed) size recorded. Internal key wins on collision.
func (m ObjectMetadata) WithUncompressedSize(size int64) ObjectMetadata {
	out := make(ObjectMetadata, len(m)+1)
	maps.Copy(out, m)
	out[ObjectMetadataUncompressedSize] = strconv.FormatInt(size, 10)

	return out
}

// UncompressedSize reports the original size of a compressed object, or false
// if absent (uncompressed object) or malformed.
func (m ObjectMetadata) UncompressedSize() (int64, bool) {
	n, err := strconv.ParseInt(m[ObjectMetadataUncompressedSize], 10, 64)

	return n, err == nil
}

// ObjectOrigin is the immutable operation that created a build, stored as the
// ObjectMetadataBuildOrigin value.
type ObjectOrigin string

const (
	ObjectOriginPause              ObjectOrigin = "pause"
	ObjectOriginTemplateBuild      ObjectOrigin = "template_build"
	ObjectOriginTemplateBuildCache ObjectOrigin = "template_build_cache"
	ObjectOriginSnapshotTemplate   ObjectOrigin = "snapshot_template"
)

// ObjectMetadataSoftDeleted is a mutable tombstone written by the storage index
// (not at upload time) to mark a layer for deletion. Value is
// "<reason>:<action_id>". Consumers fail closed on it behind a feature flag.
const ObjectMetadataSoftDeleted = "storage-index-soft-deleted"

// Layer-size metadata keys, written on each data object as decimal byte counts
// (all uncompressed, from the diff header).
const (
	// ObjectMetadataLogicalSize is the layer's logical (virtual device) size.
	ObjectMetadataLogicalSize = "logical-size"
	// ObjectMetadataMappedSize is the bytes mapped to non-empty builds.
	ObjectMetadataMappedSize = "mapped-size"
	// ObjectMetadataDiffSize is the bytes this build itself contributes.
	ObjectMetadataDiffSize = "diff-size"
)

// FrameSink fires once per compressed frame with its absolute C-space offset.
// Best-effort; implementations should return quickly and bound their own I/O.
type FrameSink func(ctx context.Context, cOffset int64, compressed []byte)

// PutOptions holds parameters for blob/seekable writes. Compression is held
// as `any` so that storage.CompressConfig (which has heavy storage-internal
// dependencies) doesn't have to be moved here. Backends type-assert it back.
type PutOptions struct {
	Metadata    ObjectMetadata
	Compression any
	FrameSink   FrameSink
	Checksum    bool
	// Overwrite marks a blob put whose object may already hold other content,
	// so a cache in front of the store must not keep its older copy.
	Overwrite bool
}

func WithFrameSink(s FrameSink) PutOption { return func(o *PutOptions) { o.FrameSink = s } }

type PutOption func(*PutOptions)

func WithMetadata(metadata ObjectMetadata) PutOption {
	return func(o *PutOptions) {
		if len(metadata) == 0 {
			return
		}
		if o.Metadata == nil {
			o.Metadata = make(ObjectMetadata, len(metadata))
		}
		maps.Copy(o.Metadata, metadata)
	}
}

// WithCompression stashes a compression config (typed in the storage package)
// into PutOptions. The storage package wraps this with a typed helper.
func WithCompression(cfg any) PutOption {
	return func(o *PutOptions) { o.Compression = cfg }
}

func WithOverwrite() PutOption { return func(o *PutOptions) { o.Overwrite = true } }

func Apply(opts []PutOption) PutOptions {
	var p PutOptions
	for _, opt := range opts {
		opt(&p)
	}

	return p
}
