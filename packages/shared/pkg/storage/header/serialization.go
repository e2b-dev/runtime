package header

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const metadataVersionMask = 0xFFFF

func metadataFormatVersion(version uint64) uint64 {
	return version & metadataVersionMask
}

// SerializeHeader serializes a header, dispatching to the version-specific format.
//
// V3 (Version <= 3): [Metadata] [v3 mappings…]
// V4 (Version >= 4): [Metadata] [uint8 flags] [uint32 uncompressedSize] [LZ4( Builds + v4 mappings )]
func SerializeHeader(h *Header) ([]byte, error) {
	switch metadataFormatVersion(h.Metadata.Version) {
	case 1, 2, 3:
		return serializeV3(h.Metadata, h.Mapping)
	case MetadataVersionV4:
		data, _, err := serializeV4(h.Metadata, h.Builds, h.Mapping, h.IncompletePendingUpload)

		return data, err
	case MetadataVersionV5:
		data, _, err := serializeV5(h.Metadata, h.Builds, h.Mapping, h.IncompletePendingUpload)

		return data, err
	default:
		return nil, fmt.Errorf("unsupported header version %d", h.Metadata.Version)
	}
}

// DeserializeBytes auto-detects the header version and deserializes accordingly.
// See SerializeHeader for the binary layout.
func DeserializeBytes(data []byte) (*Header, error) {
	if len(data) < metadataSize {
		return nil, fmt.Errorf("header too short: %d bytes", len(data))
	}

	metadata, err := deserializeMetadata(data[:metadataSize])
	if err != nil {
		return nil, err
	}

	blockData := data[metadataSize:]

	switch metadataFormatVersion(metadata.Version) {
	case MetadataVersionV5:
		return deserializeV5(metadata, blockData)
	case MetadataVersionV4:
		return deserializeV4(metadata, blockData)
	case 1, 2, 3:
		return deserializeV3(metadata, blockData)
	default:
		return nil, fmt.Errorf("unsupported header version %d", metadata.Version)
	}
}

// backfillMissingV3UncompressedBuilds materializes the zero BuildData ("uncompressed,
// size unknown") for mapping-referenced builds with no entry. On V4+ only the
// header's own build gets one: runV3 inherits its parent's V4/V5 version, writes no
// self entry, and only runs with compression off. Ancestor gaps stay absent — the
// information is lost, claiming uncompressed would 404 the suffix-less object name
// forever on a compressed build, and createDiff instead resolves the build's own
// header.
func backfillMissingV3UncompressedBuilds(h *Header) {
	v4plus := metadataFormatVersion(h.Metadata.Version) >= MetadataVersionV4

	for _, bid := range h.Mapping.Builds() {
		if _, ok := h.Builds[bid]; ok {
			continue
		}
		if v4plus && bid != h.Metadata.BuildId {
			continue
		}
		if h.Builds == nil {
			h.Builds = make(map[uuid.UUID]BuildData)
		}
		h.Builds[bid] = BuildData{}
	}
}

// LoadHeader fetches a serialized header from storage and deserializes it.
// Returns the on-wire byte count alongside the header so callers can attribute
// it to throughput telemetry. Errors (including storage.ErrObjectNotExist) are
// returned as-is.
func LoadHeader(ctx context.Context, s storage.StorageProvider, path string) (*Header, int, error) {
	h, n, err := LoadStoredHeader(ctx, s, path)
	if err != nil {
		return nil, n, err
	}

	if !h.IncompletePendingUpload {
		backfillMissingV3UncompressedBuilds(h)
	}

	return h, n, nil
}

// LoadStoredHeader is LoadHeader without the V3 uncompressed-build backfill:
// its Builds map holds exactly the entries the serialized header carried.
func LoadStoredHeader(ctx context.Context, s storage.StorageProvider, path string) (*Header, int, error) {
	blob, err := s.OpenBlob(ctx, path)
	if err != nil {
		return nil, 0, fmt.Errorf("open blob %s: %w", path, err)
	}

	// read.blob (the transfer) is emitted per-layer inside each backend WriteTo;
	// only the deserialize/decompress phase below is single-layer.
	data, err := storage.GetBlob(ctx, blob)
	if err != nil {
		return nil, 0, err
	}

	decStart := time.Now()
	h, err := DeserializeBytes(data)
	storage.RecordReadBlobDecompress(ctx, time.Since(decStart), int64(len(data)), path, headerCodec(h), err)
	if err != nil {
		return nil, len(data), err
	}

	return h, len(data), nil
}

// headerCodec reports a header format's inner compression (V4/V5 use LZ4).
// Nil-safe for the failed-deserialize path.
func headerCodec(h *Header) storage.CompressionType {
	if h == nil {
		return storage.CompressionNone
	}
	switch metadataFormatVersion(h.Metadata.Version) {
	case MetadataVersionV4, MetadataVersionV5:
		return storage.CompressionLZ4
	default:
		return storage.CompressionNone
	}
}

// StoreHeader serializes a header, uploads it, and returns the effective
// compression config plus the stored and pre-compression byte counts. V3 has
// no inner compression so the counts match and cfg is the zero value. Refuses
// to persist a header still flagged as in-flight.
func StoreHeader(ctx context.Context, s storage.StorageProvider, path string, h *Header, opts ...storage.PutOption) (cfg storage.CompressConfig, stored, uncompressed int64, err error) {
	if h == nil {
		return storage.CompressConfig{}, 0, 0, errors.New("header is nil")
	}

	if h.IncompletePendingUpload {
		return storage.CompressConfig{}, 0, 0, fmt.Errorf("refusing to persist incomplete header for %s", path)
	}

	var data []byte
	switch metadataFormatVersion(h.Metadata.Version) {
	case 1, 2, 3:
		data, err = serializeV3(h.Metadata, h.Mapping)
		if err != nil {
			return storage.CompressConfig{}, 0, 0, fmt.Errorf("serialize header: %w", err)
		}
		uncompressed = int64(len(data))
	case MetadataVersionV4, MetadataVersionV5:
		var blockUncompressed int64
		if metadataFormatVersion(h.Metadata.Version) == MetadataVersionV5 {
			data, blockUncompressed, err = serializeV5(h.Metadata, h.Builds, h.Mapping, h.IncompletePendingUpload)
		} else {
			data, blockUncompressed, err = serializeV4(h.Metadata, h.Builds, h.Mapping, h.IncompletePendingUpload)
		}
		if err != nil {
			return storage.CompressConfig{}, 0, 0, fmt.Errorf("serialize header: %w", err)
		}

		// Guard the read-side cap on the write path. The cap is enforced in
		// deserializeV4; without this symmetric check an oversize header would
		// upload successfully and then fail every restore, permanently bricking
		// the snapshot. Fail the Pause loudly instead.
		if blockUncompressed > int64(v4MaxUncompressedHeaderSize) {
			return storage.CompressConfig{}, 0, 0, fmt.Errorf("refusing to persist header for %s: uncompressed block %d exceeds cap %d", path, blockUncompressed, v4MaxUncompressedHeaderSize)
		}

		uncompressed = int64(metadataSize+v4FlagsLen+v4SizePrefixLen) + blockUncompressed
		cfg.Type = storage.CompressionLZ4.String()
	default:
		return storage.CompressConfig{}, 0, 0, fmt.Errorf("unsupported header version %d", h.Metadata.Version)
	}

	blob, err := s.OpenBlob(ctx, path)
	if err != nil {
		return storage.CompressConfig{}, 0, 0, fmt.Errorf("open blob %s: %w", path, err)
	}

	if err := blob.Put(ctx, data, opts...); err != nil {
		return storage.CompressConfig{}, 0, 0, fmt.Errorf("put blob %s: %w", path, err)
	}

	return cfg, int64(len(data)), uncompressed, nil
}

// Deserialize reads a header from a storage Blob (legacy API).
func Deserialize(ctx context.Context, in storage.Blob) (*Header, error) {
	data, err := storage.GetBlob(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to write to buffer: %w", err)
	}

	return DeserializeBytes(data)
}
