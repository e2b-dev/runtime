//go:build linux

package peerserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// ErrUnknownFile is returned when the requested file name is not recognised.
var ErrUnknownFile = errors.New("unknown file")

// ResolveSeekable maps (buildID, fileName) to a SeekableSource.
// Supported file names: memfile, rootfs.ext4.
// Returns ErrNotAvailable when the build is not in the local cache.
// Returns ErrUnknownFile for unrecognised file names.
func ResolveSeekable(cache Cache, buildID, fileName string) (SeekableSource, error) {
	stripped := storage.StripCompression(fileName)
	switch stripped {
	case storage.MemfileName, storage.RootfsName:
		diff, ok := cache.LookupDiff(buildID, build.DiffType(stripped))
		if !ok {
			return nil, ErrNotAvailable
		}

		return &seekableSource{diff: diff}, nil

	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFile, fileName)
	}
}

// ResolveBlob maps (buildID, fileName) to a BlobSource.
// Supported file names: snapfile, metadata.json, memfile.header, rootfs.ext4.header.
// Returns ErrNotAvailable when the build is not in the local cache.
// Returns ErrUnknownFile for unrecognised file names.
//
// The source reads the cached template, which is pinned until the returned
// release is called; the caller releases it once it is done with the source,
// on every path. release is never nil.
func ResolveBlob(ctx context.Context, cache Cache, buildID, fileName string) (BlobSource, func(), error) {
	t, release, ok := cache.LookupPinned(ctx, buildID)
	if !ok {
		return nil, release, ErrNotAvailable
	}

	switch fileName {
	case storage.SnapfileName:
		return &fileSource{getFile: t.Snapfile}, release, nil

	case storage.MetadataName:
		return &metadataSource{getMetadata: t.Metadata}, release, nil

	case storage.MemfileName + storage.HeaderSuffix:
		return &headerSource{getDevice: t.Memfile}, release, nil

	case storage.RootfsName + storage.HeaderSuffix:
		return &headerSource{getDevice: func(_ context.Context) (block.ReadonlyDevice, error) { return t.Rootfs() }}, release, nil

	default:
		release()

		return nil, func() {}, fmt.Errorf("%w: %q", ErrUnknownFile, fileName)
	}
}
