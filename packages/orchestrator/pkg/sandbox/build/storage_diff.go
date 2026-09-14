//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	refreshCauseProactive        = "proactive"
	refreshCausePeerTransitioned = "peer_transitioned"
	refreshCauseZeroEntryMiss    = "zero_entry_miss"
)

// source carries the StorageDiff's current routing state. upstream is always
// non-nil after construction but may be switched once over the lifetime; ft
// nil = not authoritative, non-nil = authoritative and immutable.
// ft is *FullFrameTable rather than *FrameTable — see
// (*header.Header).SelfBuildData for the invariant justifying the upcast.
type source struct {
	upstream           storage.RangeOpener
	fullDiffFrameTable *storage.FullFrameTable
	// dataPath is the storage object upstream reads from; the soft-delete check
	// reads the tombstone off it directly (the object that gets pruned), not the
	// header. Empty when there is no storage path to check. It travels with the
	// upstream it was opened from, so a peer transition (uncompressed probe ->
	// authoritative compressed object) swaps both atomically.
	dataPath string
}

func isPeerRouted(v any) bool {
	_, ok := v.(peerclient.PeerRouted)

	return ok
}

// zeroEntryRefreshCause maps a size lookup that a zero Builds entry could not
// answer to the cause of the header refresh that recovers it, or "" when the
// error is not recoverable that way.
func zeroEntryRefreshCause(err error) string {
	var transErr *storage.PeerTransitionedError
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		return refreshCauseZeroEntryMiss
	case errors.As(err, &transErr):
		return refreshCausePeerTransitioned
	default:
		return ""
	}
}

type StorageDiff struct {
	chunker           *block.Chunker
	cachePath         string
	cacheKey          DiffStoreKey
	buildID           string
	diffType          DiffType
	storageObjectType storage.SeekableObjectType
	flags             *featureflags.Client

	blockSize   int64
	metrics     blockmetrics.Metrics
	persistence storage.StorageProvider

	source    atomic.Pointer[source]
	refreshMu sync.Mutex

	// softDeletedPath holds the storage path the background check found
	// tombstoned (only under enforcement); reads fail closed while it equals the
	// current source's dataPath. Comparing by path value (not a bool) makes the
	// latch race-free: a stale check that recorded a superseded probe path can
	// never match the live dataPath, and a peer-transition repoint to a different
	// object auto-disables the old verdict without a separate clear.
	softDeletedPath atomic.Pointer[string]
}

var _ Diff = (*StorageDiff)(nil)

type UnknownDiffTypeError struct {
	DiffType DiffType
}

func (e UnknownDiffTypeError) Error() string {
	return fmt.Sprintf("unknown diff type: %s", e.DiffType)
}

// newStorageDiff assembles a StorageDiff from a fully-resolved upstream, size,
// and full-file FrameTable. All regime decisioning (peer-active bootstrap, V3
// fallback, authoritative-refresh recovery) lives in the caller (createDiff);
// this constructor is intentionally pure.
func newStorageDiff(
	basePath string,
	buildID string,
	diffType DiffType,
	storageObjectType storage.SeekableObjectType,
	blockSize int64,
	metrics blockmetrics.Metrics,
	persistence storage.StorageProvider,
	upstream storage.Seekable,
	uncompressedSize int64,
	initialFT *storage.FullFrameTable,
	dataPath string,
	ff *featureflags.Client,
) (*StorageDiff, error) {
	cachePath := GenerateDiffCachePath(basePath, buildID, diffType)
	c, err := block.NewChunker(ff, uncompressedSize, blockSize, cachePath, metrics, storageObjectType)
	if err != nil {
		return nil, fmt.Errorf("create chunker for build %s: %w", buildID, err)
	}

	d := &StorageDiff{
		buildID:           buildID,
		diffType:          diffType,
		storageObjectType: storageObjectType,
		flags:             ff,
		cachePath:         cachePath,
		blockSize:         blockSize,
		metrics:           metrics,
		persistence:       persistence,
		chunker:           c,
		cacheKey:          GetDiffStoreKey(buildID, diffType),
	}
	d.source.Store(&source{upstream: upstream, fullDiffFrameTable: initialFT, dataPath: dataPath})

	return d, nil
}

func (b *File) createDiff(ctx context.Context, buildID uuid.UUID) (Diff, error) {
	h := b.Header()
	blockSize := int64(h.Metadata.BlockSize)

	objType, ok := storageObjectType(b.fileType)
	if !ok {
		return nil, UnknownDiffTypeError{b.fileType}
	}

	bd, hasEntry := h.Builds[buildID]

	var (
		upstream  storage.Seekable
		size      int64
		initialFT *storage.FullFrameTable
		dataPath  string
		err       error
	)
	switch {
	case hasEntry:
		// bd.FrameData is per-mapping trimmed, not the ancestor's full table —
		// don't latch it; first read will refresh. Exception: a zero bd marks
		// an uncompressed V3-or-older ancestor; UncompressedFullFrameTable IS
		// that ancestor's full table, latch it to skip a refresh whose header
		// file may not exist. If the mark proves false, the size lookup below
		// recovers via the build's own header.
		upstream, dataPath, err = b.openDataFile(ctx, buildID, bd.FrameData.CompressionType())
		if err != nil {
			return nil, err
		}
		size = bd.Size
		if bd == (header.BuildData{}) {
			initialFT = storage.UncompressedFullFrameTable
		}

	default:
		// hasEntry=false: a peer-served incomplete header, or a V4+ header whose
		// Builds section lost an ancestor (LoadHeader leaves the gap absent on
		// purpose — see backfillMissingV3UncompressedBuilds). Probe basic-name to
		// detect peer routing; otherwise refresh from storage to resolve the
		// build's real path and frame table.
		upstream, dataPath, err = b.openDataFile(ctx, buildID, storage.CompressionNone)
		if err != nil {
			return nil, err
		}
		if isPeerRouted(upstream) {
			peerSize, peerErr := upstream.Size(ctx)
			if peerErr == nil {
				size = peerSize

				break
			}
			if _, ok := errors.AsType[*storage.PeerTransitionedError](peerErr); !ok {
				return nil, fmt.Errorf("createDiff: peer Size for build %s: %w", buildID, peerErr)
			}
		}
		loaded, lerr := refreshHeader(ctx, b.persistence, buildID, b.fileType, refreshCauseProactive)
		if lerr != nil {
			if errors.Is(lerr, storage.ErrObjectNotExist) {
				// Legacy template: data file exists at the basic uncompressed
				// path but no header file was ever uploaded; keep the upstream
				// we already opened at the basic path and latch as uncompressed.
				initialFT = storage.UncompressedFullFrameTable

				break
			}

			return nil, fmt.Errorf("createDiff: proactive header load for build %s: %w", buildID, lerr)
		}
		// Promote loaded header on self-match so future pauses inherit the
		// populated Builds map.
		if loaded.Metadata.BuildId == h.Metadata.BuildId {
			if _, hasSelf := loaded.Builds[loaded.Metadata.BuildId]; hasSelf {
				b.SwapHeader(loaded)
			}
		}
		upstream, size, initialFT, dataPath, err = openFromLoadedHeader(ctx, b.persistence, loaded, b.fileType)
		if err != nil {
			return nil, err
		}
	}

	if size == 0 {
		size, err = upstream.Size(ctx)
		if cause := zeroEntryRefreshCause(err); hasEntry && initialFT == storage.UncompressedFullFrameTable && cause != "" {
			// The zero entry claimed an uncompressed V3-era ancestor, but the
			// basic-name object cannot answer: it does not exist, or the peer
			// serving it is gone. Resolve the build's own header like the
			// no-entry branch instead of failing every fault permanently.
			loaded, lerr := refreshHeader(ctx, b.persistence, buildID, b.fileType, cause)
			if lerr == nil {
				upstream, size, initialFT, dataPath, err = openFromLoadedHeader(ctx, b.persistence, loaded, b.fileType)
				if err == nil && size == 0 {
					size, err = upstream.Size(ctx)
				}
			} else {
				// Report why the recovery failed, not just the object miss.
				err = errors.Join(err, lerr)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("createDiff: size lookup for build %s: %w", buildID, err)
		}
	}

	if isPeerRouted(upstream) {
		initialFT = nil
	}

	d, err := newStorageDiff(
		b.store.cachePath,
		buildID.String(),
		b.fileType,
		objType,
		blockSize,
		b.metrics,
		b.persistence,
		upstream,
		size,
		initialFT,
		dataPath,
		b.store.flags,
	)
	if err != nil {
		return nil, err
	}

	d.startSoftDeleteCheck(context.WithoutCancel(ctx))

	return d, nil
}

func (b *File) openDataFile(ctx context.Context, buildID uuid.UUID, ct storage.CompressionType) (storage.Seekable, string, error) {
	path := storage.Paths{BuildID: buildID.String()}.DataFile(string(b.fileType), ct)
	upstream, err := b.persistence.OpenSeekable(ctx, path)
	if err != nil {
		return nil, "", fmt.Errorf("createDiff: open data file for build %s at %s: %w", buildID, path, err)
	}

	return upstream, path, nil
}

func openFromLoadedHeader(
	ctx context.Context,
	persistence storage.StorageProvider,
	loaded *header.Header,
	fileType DiffType,
) (storage.Seekable, int64, *storage.FullFrameTable, string, error) {
	buildID := loaded.Metadata.BuildId
	paths := storage.Paths{BuildID: buildID.String()}
	if loaded.Metadata.Version < header.MetadataVersionV4 {
		path := paths.DataFile(string(fileType), storage.CompressionNone)
		upstream, err := persistence.OpenSeekable(ctx, path)
		if err != nil {
			return nil, 0, nil, "", fmt.Errorf("reopen uncompressed upstream for pre-V4 build %s at %s: %w", buildID, path, err)
		}

		return upstream, 0, storage.UncompressedFullFrameTable, path, nil
	}
	size, ft, err := loaded.SelfBuildData()
	if err != nil {
		return nil, 0, nil, "", err
	}
	path := paths.DataFile(string(fileType), ft.Table().CompressionType())
	upstream, err := persistence.OpenSeekable(ctx, path)
	if err != nil {
		return nil, 0, nil, "", fmt.Errorf("reopen upstream for build %s at %s: %w", buildID, path, err)
	}

	return upstream, size, ft, path, nil
}

func storageObjectType(diffType DiffType) (storage.SeekableObjectType, bool) {
	switch diffType {
	case Memfile:
		return storage.MemfileObjectType, true
	case Rootfs:
		return storage.RootFSObjectType, true
	default:
		return storage.UnknownSeekableObjectType, false
	}
}

func (b *StorageDiff) CacheKey() DiffStoreKey {
	return b.cacheKey
}

func (b *StorageDiff) Close() error {
	return b.chunker.Close()
}

func (b *StorageDiff) ReadAt(ctx context.Context, p []byte, off int64, callerFT *storage.FrameTable) (int, error) {
	if err := b.softDeleteErr(); err != nil {
		return 0, err
	}
	up, ft, err := b.resolve(ctx, callerFT)
	if err != nil {
		return 0, err
	}
	n, err := b.chunker.ReadAt(ctx, p, off, up, ft)
	var transErr *storage.PeerTransitionedError
	if !errors.As(err, &transErr) {
		return n, err
	}
	up, ft, err = b.recoverFromPeerTransition(ctx, transErr, callerFT)
	if err != nil {
		return 0, err
	}

	return b.chunker.ReadAt(ctx, p, off, up, ft)
}

// recoverFromPeerTransition handles a PeerTransitionedError surfaced by a read:
// back off, reload the source, re-check the soft-delete latch (the source's
// dataPath may have changed during the reload), and re-resolve the upstream/FT.
func (b *StorageDiff) recoverFromPeerTransition(ctx context.Context, transErr *storage.PeerTransitionedError, callerFT *storage.FrameTable) (storage.RangeOpener, *storage.FrameTable, error) {
	if err := waitTransitionBackoff(ctx, transErr); err != nil {
		return nil, nil, err
	}
	if err := b.reloadAfterPeerTransition(ctx); err != nil {
		return nil, nil, fmt.Errorf("refresh after peer transition: %w", err)
	}
	if err := b.softDeleteErr(); err != nil {
		return nil, nil, err
	}

	return b.resolve(ctx, callerFT)
}

func (b *StorageDiff) Slice(ctx context.Context, off, length int64, callerFT *storage.FrameTable) ([]byte, error) {
	if err := b.softDeleteErr(); err != nil {
		return nil, err
	}
	up, ft, err := b.resolve(ctx, callerFT)
	if err != nil {
		return nil, err
	}
	out, err := b.chunker.Slice(ctx, off, length, up, ft)
	var transErr *storage.PeerTransitionedError
	if !errors.As(err, &transErr) {
		return out, err
	}
	up, ft, err = b.recoverFromPeerTransition(ctx, transErr, callerFT)
	if err != nil {
		return nil, err
	}

	return b.chunker.Slice(ctx, off, length, up, ft)
}

// The local file might not be synced.
func (b *StorageDiff) CachePath(context.Context) (string, error) {
	return b.cachePath, nil
}

func (b *StorageDiff) FileSize(ctx context.Context) (int64, error) {
	return b.chunker.FileSize(ctx)
}

func (b *StorageDiff) Size(_ context.Context) (int64, error) {
	return b.chunker.Size(), nil
}

func (b *StorageDiff) BlockSize() int64 {
	return b.blockSize
}

// IsCached reports whether [off, off+length) is in the chunker's local cache.
// Side-effect-free.
func (b *StorageDiff) IsCached(ctx context.Context, off, length int64) bool {
	return b.chunker.IsCached(ctx, off, length)
}

func refreshHeader(ctx context.Context, persistence storage.StorageProvider, buildID uuid.UUID, diffType DiffType, cause string) (*header.Header, error) {
	timer := frameTableRefreshTimer.Begin(
		attribute.String("cause", cause),
		attribute.String("file_type", string(diffType)),
	)

	headerPath := storage.Paths{BuildID: buildID.String()}.HeaderFile(string(diffType))
	h, bytesLoaded, err := header.LoadHeader(ctx, persistence, headerPath)
	if err != nil {
		timer.Failure(ctx, int64(bytesLoaded))

		return nil, fmt.Errorf("load header for build %s: %w", buildID, err)
	}
	timer.Success(ctx, int64(bytesLoaded))

	return h, nil
}

// resolve picks the (upstream, ft) the next read should use, given the
// caller's per-mapping FT hint. The contract: if there is no authoritative FT
// latched AND no peer currently serving this build, we MUST refresh before
// reading. The latched upstream was opened at the bootstrap-guessed CT path
// and may return wrong bytes once the peer is gone; only the authoritative
// header tells us where to read from.
func (b *StorageDiff) resolve(ctx context.Context, callerFT *storage.FrameTable) (storage.RangeOpener, *storage.FrameTable, error) {
	cur := b.source.Load()
	if cur.fullDiffFrameTable != nil {
		return cur.upstream, cur.fullDiffFrameTable.Table(), nil
	}
	if callerFT != nil {
		return cur.upstream, callerFT, nil
	}
	if isPeerRouted(cur.upstream) {
		return cur.upstream, storage.UncompressedFrameTable, nil
	}
	if err := b.reloadProactive(ctx); err != nil {
		return nil, nil, fmt.Errorf("resolve: %w", err)
	}
	cur = b.source.Load()

	return cur.upstream, cur.fullDiffFrameTable.Table(), nil
}

// reloadAfterPeerTransition refreshes the source after a peerSeekable signaled
// PeerTransitionedError. Short-circuits if a concurrent goroutine already
// swapped the upstream to non-peer.
func (b *StorageDiff) reloadAfterPeerTransition(ctx context.Context) error {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	if !isPeerRouted(b.source.Load().upstream) {
		return nil
	}

	return b.reloadSourceLocked(ctx, refreshCausePeerTransitioned)
}

// reloadProactive refreshes the source when resolve has no authoritative FT
// and no peer to ask. Short-circuits if a concurrent goroutine already latched
// an FT.
func (b *StorageDiff) reloadProactive(ctx context.Context) error {
	b.refreshMu.Lock()
	defer b.refreshMu.Unlock()
	if b.source.Load().fullDiffFrameTable != nil {
		return nil
	}

	return b.reloadSourceLocked(ctx, refreshCauseProactive)
}

// reloadSourceLocked re-fetches the header and reopens upstream. Caller must
// hold refreshMu. The cause is propagated to refreshHeader's telemetry label.
//
// A missing header here is always a real error: legacy ancestors are caught
// at createDiff time (backfill-marker or !hasEntry branches), so any runtime
// ErrObjectNotExist is a race or genuine miss — propagate, do not assume
// uncompressed (would serve compressed bytes raw for any modern build).
func (b *StorageDiff) reloadSourceLocked(ctx context.Context, cause string) error {
	bid, err := uuid.Parse(b.buildID)
	if err != nil {
		return fmt.Errorf("parse build id %s: %w", b.buildID, err)
	}
	loaded, err := refreshHeader(ctx, b.persistence, bid, b.diffType, cause)
	if err != nil {
		return fmt.Errorf("reloadSourceLocked: load header for build %s: %w", b.buildID, err)
	}
	upstream, _, ft, dataPath, err := openFromLoadedHeader(ctx, b.persistence, loaded, b.diffType)
	if err != nil {
		return fmt.Errorf("reloadSourceLocked: build %s: %w", b.buildID, err)
	}
	pathChanged := b.source.Load().dataPath != dataPath
	b.source.Store(&source{upstream: upstream, fullDiffFrameTable: ft, dataPath: dataPath})

	// Recheck on any path change, and also on a peer transition even if the path
	// is unchanged: a peer-served object that wasn't in storage at first open
	// (not_found) may now exist and carry a tombstone the initial check missed.
	if pathChanged || cause == refreshCausePeerTransitioned {
		b.startSoftDeleteCheck(context.WithoutCancel(ctx))
	}

	return nil
}
