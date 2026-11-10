//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	blockmocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func newV4HeaderFF(t *testing.T, on bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.V4HeaderForUncompressedFlag.Key()).VariationForAll(on))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ff.Close(context.WithoutCancel(t.Context()))
	})

	return ff
}

func resolveV4(t *testing.T, ff *featureflags.Client) bool {
	t.Helper()
	_, useV4, err := resolveCompressConfig(t.Context(), storage.CompressConfig{}, ff, storage.MemfileName, 4096, storage.UseCaseBuild)
	require.NoError(t, err)

	return useV4
}

func TestResolveCompressConfig_V4_NilClient(t *testing.T) {
	t.Parallel()

	require.False(t, resolveV4(t, nil))
}

func TestResolveCompressConfig_V4_FlagOff(t *testing.T) {
	t.Parallel()

	ff := newV4HeaderFF(t, false)
	require.False(t, resolveV4(t, ff))
}

func TestResolveCompressConfig_V4_FlagOn(t *testing.T) {
	t.Parallel()

	ff := newV4HeaderFF(t, true)
	require.True(t, resolveV4(t, ff))
}

// putV3Header registers a V3 ancestor in the fake cache. V3 headers carry no
// Builds map at all — appendAncestorBuilds must synthesize a placeholder from
// Metadata.Size so descendants don't pay a refresh roundtrip per cold read.
func putV3Header(t *testing.T, cache *fakeCache, buildID uuid.UUID, fileType build.DiffType, size uint64) {
	t.Helper()
	tpl := templatemocks.NewMockTemplate(t)
	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(&headers.Header{
		Metadata: &headers.Metadata{Version: 3, Size: size},
	}).Maybe()

	switch fileType {
	case build.Memfile:
		tpl.EXPECT().Memfile(mock.Anything).Return(dev, nil).Maybe()
	case build.Rootfs:
		tpl.EXPECT().Rootfs().Return(dev, nil).Maybe()
	}

	cache.put(buildID.String(), tpl)
}

func mappingTo(t *testing.T, ancestorID uuid.UUID) headers.Mapping {
	t.Helper()
	m, err := headers.NewMapping(testBlockSize, []headers.BuildMap{{
		Offset: 0, Length: testBlockSize, BuildId: ancestorID, BuildStorageOffset: 0,
	}})
	require.NoError(t, err)

	return m
}

// V4 descendant of a V3 ancestor: appendAncestorBuilds writes a sentinel
// empty BuildData{} so descendants take createDiff's hasEntry branch and
// resolve() short-circuits on the UncompressedFrameTable hint from
// GetBuildFrameData. Size is left zero on purpose — Metadata.Size is the
// virtual size, not the diff size, and asking storage at upload time across
// a long chain would multiply roundtrips. createDiff already falls back to
// upstream.Size when bd.Size == 0.
func TestAppendAncestorBuilds_V3AncestorSynthesizesEntry(t *testing.T) {
	t.Parallel()

	uploads, cache := newUploads(t)
	ancestorID := uuid.New()
	putV3Header(t, cache, ancestorID, build.Memfile, 512*1024*1024)

	u := &Upload{buildID: uuid.New(), uploads: uploads}
	dst := map[uuid.UUID]headers.BuildData{}

	delta := watchAncestorResolutions(t)
	err := u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile)
	require.Equal(t, map[string]int64{"entry/legacy": 1}, delta())
	require.NoError(t, err)

	bd, ok := dst[ancestorID]
	require.True(t, ok, "V3 ancestor must produce a Builds entry")
	require.Equal(t, int64(0), bd.Size, "Size must stay 0 — diff size is queried at read time via upstream.Size")
	require.Nil(t, bd.FrameData, "FrameData must be nil so GetBuildFrameData returns UncompressedFrameTable")
}

// V4 ancestor: existing behavior — copy the ancestor's own self-entry verbatim.
func TestAppendAncestorBuilds_V4AncestorCopiesEntry(t *testing.T) {
	t.Parallel()

	uploads, cache := newUploads(t)
	ancestorID := uuid.New()
	putHeader(t, cache, ancestorID, build.Memfile, false)

	u := &Upload{buildID: uuid.New(), uploads: uploads}
	dst := map[uuid.UUID]headers.BuildData{}

	delta := watchAncestorResolutions(t)
	err := u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile)
	require.Equal(t, map[string]int64{"entry/overwrite": 1}, delta())
	require.NoError(t, err)

	_, ok := dst[ancestorID]
	require.True(t, ok, "V4 ancestor's self-entry must be copied into dst")
}

// V3 caller (dst=nil): the barrier still runs, but no entry is written —
// preserves the existing contract.
func TestAppendAncestorBuilds_NilDstSkipsSynthesis(t *testing.T) {
	t.Parallel()

	uploads, cache := newUploads(t)
	ancestorID := uuid.New()
	putV3Header(t, cache, ancestorID, build.Memfile, 1024)

	u := &Upload{buildID: uuid.New(), uploads: uploads}
	delta := watchAncestorResolutions(t)
	err := u.appendAncestorBuilds(t.Context(), nil, mappingTo(t, ancestorID), build.Memfile)
	require.Equal(t, map[string]int64{"entry/none": 1}, delta())
	require.NoError(t, err)
}

// gapUpload builds an Upload whose ancestor is neither cached locally nor
// peer-served, so Wait returns nil — the inherited-gap case.
func gapUpload(t *testing.T, provider storage.StorageProvider) *Upload {
	t.Helper()
	uploads, _ := newUploads(t)
	uploads.p2p = peerclient.NopResolver()

	return &Upload{buildID: uuid.New(), uploads: uploads, store: provider}
}

// A mapping-referenced build missing from both dst and the local cache (a gap
// inherited from the source header) must not be persisted as a gap — the entry
// is recovered from the build's own stored header so it stops propagating to
// descendant headers.
func TestAppendAncestorBuilds_RecoversInheritedGapFromStoredHeader(t *testing.T) {
	t.Parallel()

	ancestorID := uuid.New()
	ancestorHeader, err := headers.NewHeader(
		&headers.Metadata{Version: headers.MetadataVersionV4, BlockSize: 4096, Size: 4096, BuildId: ancestorID, BaseBuildId: ancestorID},
		nil,
	)
	require.NoError(t, err)
	want := headers.BuildData{Size: 12345}
	ancestorHeader.SetBuild(ancestorID, want)
	ancestorHeaderBytes, err := headers.SerializeHeader(ancestorHeader)
	require.NoError(t, err)

	provider := storage.NewMockStorageProvider(t)
	headerBlob := storage.NewMockBlob(t)
	headerBlob.EXPECT().
		WriteTo(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, w io.Writer) (int64, error) {
			return io.Copy(w, bytes.NewReader(ancestorHeaderBytes))
		}).Once()
	provider.EXPECT().
		OpenBlob(mock.Anything, storage.Paths{BuildID: ancestorID.String()}.HeaderFile(storage.MemfileName)).
		Return(headerBlob, nil).Once()

	dst := map[uuid.UUID]headers.BuildData{}
	delta := watchAncestorResolutions(t)
	require.NoError(t, gapUpload(t, provider).appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[string]int64{"no_future/storage_heal": 1}, delta())
	require.Equal(t, want, dst[ancestorID])
}

// A gap whose build has no stored header (legacy uncompressed build) stays
// absent — the read path resolves it — and must not fail the upload.
func TestAppendAncestorBuilds_LeavesGapAbsentWithoutStoredHeader(t *testing.T) {
	t.Parallel()

	ancestorID := uuid.New()
	provider := storage.NewMockStorageProvider(t)
	provider.EXPECT().
		OpenBlob(mock.Anything, storage.Paths{BuildID: ancestorID.String()}.HeaderFile(storage.MemfileName)).
		Return(nil, storage.ErrObjectNotExist).Once()

	dst := map[uuid.UUID]headers.BuildData{}
	delta := watchAncestorResolutions(t)
	require.NoError(t, gapUpload(t, provider).appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[string]int64{"no_future/absent": 1}, delta())
	require.NotContains(t, dst, ancestorID)
}

// A transient failure loading the ancestor's header must not fail the pause:
// the heal is an optimization, and leaving the gap absent is what every release
// before it did — createDiff still resolves the build's own header per fault.
func TestAppendAncestorBuilds_TransientLoadFailureKeepsUploadAlive(t *testing.T) {
	t.Parallel()

	ancestorID := uuid.New()
	provider := storage.NewMockStorageProvider(t)
	provider.EXPECT().
		OpenBlob(mock.Anything, storage.Paths{BuildID: ancestorID.String()}.HeaderFile(storage.MemfileName)).
		Return(nil, errors.New("storage unavailable")).Once()

	dst := map[uuid.UUID]headers.BuildData{}
	delta := watchAncestorResolutions(t)
	require.NoError(t, gapUpload(t, provider).appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[string]int64{"no_future/absent": 1}, delta())
	require.NotContains(t, dst, ancestorID)
}

// A load failure on an already-cancelled context still fails: continuing would
// bury the real cause under a store-header error further down the pause.
func TestAppendAncestorBuilds_CancelledContextFailsUpload(t *testing.T) {
	t.Parallel()

	ancestorID := uuid.New()
	provider := storage.NewMockStorageProvider(t)
	provider.EXPECT().
		OpenBlob(mock.Anything, storage.Paths{BuildID: ancestorID.String()}.HeaderFile(storage.MemfileName)).
		Return(nil, context.Canceled).Once()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	dst := map[uuid.UUID]headers.BuildData{}
	delta := watchAncestorResolutions(t)
	err := gapUpload(t, provider).appendAncestorBuilds(ctx, dst, mappingTo(t, ancestorID), build.Memfile)
	require.Equal(t, map[string]int64{"error/none": 1}, delta())
	require.ErrorIs(t, err, context.Canceled)
}

// An entry already carried through the source header is kept as-is, with no
// storage round-trip (the mock provider fails on any unexpected call).
func TestAppendAncestorBuilds_ExistingEntrySkipsStorage(t *testing.T) {
	t.Parallel()

	ancestorID := uuid.New()
	want := headers.BuildData{Size: 777}
	dst := map[uuid.UUID]headers.BuildData{ancestorID: want}
	delta := watchAncestorResolutions(t)
	require.NoError(t, gapUpload(t, storage.NewMockStorageProvider(t)).appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[string]int64{"no_future/inherited": 1}, delta())
	require.Equal(t, want, dst[ancestorID])
}

// A filesystem-only snapshot has no memfile, so its MemorySnapshot.BlockSize is
// 0. NewUpload must skip resolving the memfile compress config for it —
// otherwise, with compression enabled, validateCompressConfig would reject the
// zero block size and fail the upload. FrameSizeKB is a multiple of the 4 KiB
// rootfs block so the rootfs config (which is always resolved) stays valid.
func TestNewUpload_FilesystemSnapshotSkipsMemfileCompressConfig(t *testing.T) {
	t.Parallel()

	cfg := storage.CompressConfig{Enabled: true, Type: "zstd", FrameSizeKB: 256}

	t.Run("filesystem-only snapshot with zero memfile block size succeeds", func(t *testing.T) {
		t.Parallel()
		snap := &Snapshot{
			BuildID:            uuid.New(),
			FilesystemSnapshot: true,
			RootfsBlockSize:    4096,
		}

		u, err := NewUpload(t.Context(), nil, snap, nil, cfg, nil, storage.UseCaseBuild, storage.ObjectMetadata{}, nil)
		require.NoError(t, err)
		require.NotNil(t, u)
	})

	t.Run("memory snapshot with zero memfile block size still errors", func(t *testing.T) {
		t.Parallel()
		snap := &Snapshot{
			BuildID:            uuid.New(),
			FilesystemSnapshot: false,
			RootfsBlockSize:    4096,
		}

		_, err := NewUpload(t.Context(), nil, snap, nil, cfg, nil, storage.UseCaseBuild, storage.ObjectMetadata{}, nil)
		require.Error(t, err)
	})
}

// serveStoredHeader makes provider serve h, serialized, as buildID's stored
// memfile header, for as many loads as the test makes.
func serveStoredHeader(t *testing.T, provider *storage.MockStorageProvider, buildID uuid.UUID, h *headers.Header) {
	t.Helper()

	data, err := headers.SerializeHeader(h)
	require.NoError(t, err)
	provider.EXPECT().
		OpenBlob(mock.Anything, storage.Paths{BuildID: buildID.String()}.HeaderFile(storage.MemfileName)).
		RunAndReturn(func(context.Context, string) (storage.Blob, error) {
			blob := storage.NewMockBlob(t)
			blob.EXPECT().WriteTo(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, w io.Writer) (int64, error) {
				return io.Copy(w, bytes.NewReader(data))
			})

			return blob, nil
		})
}

// putResidentHeader caches buildID with h as its finalized memfile header —
// what an entry holds after its own upload published h.
func putResidentHeader(t *testing.T, cache *fakeCache, buildID uuid.UUID, h *headers.Header) {
	t.Helper()

	tpl := templatemocks.NewMockTemplate(t)
	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(h).Maybe()
	tpl.EXPECT().Memfile(mock.Anything).Return(dev, nil).Maybe()
	cache.put(buildID.String(), tpl)
}

// ancestorHeader is an ancestor's own header as its upload publishes it: a
// mapping onto itself, at version, with builds as its Builds map.
func ancestorHeader(t *testing.T, ancestorID uuid.UUID, version uint64, builds map[uuid.UUID]headers.BuildData) *headers.Header {
	t.Helper()

	h, err := headers.NewHeader(&headers.Metadata{
		Version: version, BlockSize: testBlockSize, Size: testBlockSize,
		BuildId: ancestorID, BaseBuildId: ancestorID,
	}, []headers.BuildMap{{Offset: 0, Length: testBlockSize, BuildId: ancestorID}})
	require.NoError(t, err)
	h.Builds = builds

	return h
}

// ancestorShape is one kind of ancestor header whose Builds entry the child
// derives differently, with the Builds map the child ends up with.
type ancestorShape struct {
	name   string
	header func(t *testing.T, ancestorID uuid.UUID) *headers.Header
	want   func(ancestorID uuid.UUID) map[uuid.UUID]headers.BuildData
	// resident and released are the resolutions counted when the ancestor's
	// entry is cached, and when it is gone after its upload.
	resident, released string
}

var ancestorShapes = []ancestorShape{
	{
		name: "V4 header with its own entry",
		header: func(t *testing.T, id uuid.UUID) *headers.Header {
			t.Helper()

			return ancestorHeader(t, id, headers.MetadataVersionV4, map[uuid.UUID]headers.BuildData{id: {Size: 12345}})
		},
		want: func(id uuid.UUID) map[uuid.UUID]headers.BuildData {
			return map[uuid.UUID]headers.BuildData{id: {Size: 12345}}
		},
		resident: "overwrite",
		released: "storage_heal",
	},
	{
		// runV3 under a V4 parent: V4 version, no entry for its own build.
		name: "V4 header without its own entry",
		header: func(t *testing.T, id uuid.UUID) *headers.Header {
			t.Helper()

			return ancestorHeader(t, id, headers.MetadataVersionV4, nil)
		},
		want: func(uuid.UUID) map[uuid.UUID]headers.BuildData {
			return map[uuid.UUID]headers.BuildData{}
		},
		resident: "absent",
		released: "self_entry_absent",
	},
	{
		name: "pre-V4 header",
		header: func(t *testing.T, id uuid.UUID) *headers.Header {
			t.Helper()

			return ancestorHeader(t, id, 3, nil)
		},
		want: func(id uuid.UUID) map[uuid.UUID]headers.BuildData {
			return map[uuid.UUID]headers.BuildData{id: {}}
		},
		resident: "legacy",
		released: "legacy",
	},
}

// With the fallback on, an ancestor whose upload future fired but whose entry
// is gone writes the same Builds entry into the child as when its entry held
// the header its upload published, for every ancestor header shape —
// including the V4 header with no entry for its own build, which LoadHeader's
// backfill would turn into an empty "uncompressed" entry that published header
// does not carry.
func TestAppendAncestorBuilds_ReleasedAncestorMatchesResident(t *testing.T) {
	t.Parallel()

	ff := newFallbackFF(t, true)

	for _, shape := range ancestorShapes {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			ancestorID := uuid.New()
			ancestor := shape.header(t, ancestorID)
			delta := watchAncestorResolutions(t)

			resident, residentCache := newUploads(t)
			resident.ff = ff
			putResidentHeader(t, residentCache, ancestorID, ancestor)
			firedFuture(t, resident, ancestorID, nil)
			residentDst := map[uuid.UUID]headers.BuildData{}
			u := &Upload{buildID: uuid.New(), uploads: resident, store: storage.NewMockStorageProvider(t)}
			require.NoError(t, u.appendAncestorBuilds(t.Context(), residentDst, mappingTo(t, ancestorID), build.Memfile))

			released, _ := newUploads(t)
			released.ff = ff
			firedFuture(t, released, ancestorID, nil)
			provider := storage.NewMockStorageProvider(t)
			serveStoredHeader(t, provider, ancestorID, ancestor)
			releasedDst := map[uuid.UUID]headers.BuildData{}
			u = &Upload{buildID: uuid.New(), uploads: released, store: provider}
			require.NoError(t, u.appendAncestorBuilds(t.Context(), releasedDst, mappingTo(t, ancestorID), build.Memfile))

			require.Equal(t, shape.want(ancestorID), residentDst, "resident")
			require.Equal(t, residentDst, releasedDst, "released must write what resident writes")
			require.Equal(t, map[string]int64{"entry/" + shape.resident: 1, "future_no_entry/" + shape.released: 1}, delta())
		})
	}
}

// With the fallback off, the same released ancestor fails the child's upload
// as on main, without touching storage.
func TestAppendAncestorBuilds_ReleasedAncestorFailsWithFallbackOff(t *testing.T) {
	t.Parallel()

	ff := newFallbackFF(t, false)
	uploads, _ := newUploads(t)
	uploads.ff = ff
	ancestorID := uuid.New()
	firedFuture(t, uploads, ancestorID, nil)

	dst := map[uuid.UUID]headers.BuildData{}
	u := &Upload{buildID: uuid.New(), uploads: uploads, store: storage.NewMockStorageProvider(t)}
	delta := watchAncestorResolutions(t)
	err := u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile)
	require.Equal(t, map[string]int64{"error/none": 1}, delta())
	require.ErrorContains(t, err, "not in template cache")
	require.Empty(t, dst)
}

// A released ancestor the child already carries keeps the inherited entry,
// with no storage read.
func TestAppendAncestorBuilds_ReleasedAncestorKeepsInheritedEntry(t *testing.T) {
	t.Parallel()

	ff := newFallbackFF(t, true)
	uploads, _ := newUploads(t)
	uploads.ff = ff
	ancestorID := uuid.New()
	firedFuture(t, uploads, ancestorID, nil)

	want := headers.BuildData{Size: 777}
	dst := map[uuid.UUID]headers.BuildData{ancestorID: want}
	u := &Upload{buildID: uuid.New(), uploads: uploads, store: storage.NewMockStorageProvider(t)}
	delta := watchAncestorResolutions(t)
	require.NoError(t, u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[string]int64{"future_no_entry/inherited": 1}, delta())
	require.Equal(t, map[uuid.UUID]headers.BuildData{ancestorID: want}, dst)
}

// A resident ancestor overwrites the entry the child already carries with the
// one its published header holds — the case where resident and released
// ancestors differ, since a released one keeps the inherited entry.
func TestAppendAncestorBuilds_ResidentAncestorOverwritesInheritedEntry(t *testing.T) {
	t.Parallel()

	ff := newFallbackFF(t, true)
	uploads, cache := newUploads(t)
	uploads.ff = ff
	ancestorID := uuid.New()
	want := headers.BuildData{Size: 12345}
	putResidentHeader(t, cache, ancestorID, ancestorHeader(t, ancestorID, headers.MetadataVersionV4, map[uuid.UUID]headers.BuildData{ancestorID: want}))
	firedFuture(t, uploads, ancestorID, nil)

	dst := map[uuid.UUID]headers.BuildData{ancestorID: {Size: 777}}
	u := &Upload{buildID: uuid.New(), uploads: uploads, store: storage.NewMockStorageProvider(t)}
	delta := watchAncestorResolutions(t)
	require.NoError(t, u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[string]int64{"entry/overwrite": 1}, delta())
	require.Equal(t, map[uuid.UUID]headers.BuildData{ancestorID: want}, dst)
}

// The heal for an ancestor with no upload future is main's, fallback on or
// off: it keeps LoadHeader's backfill, so a V4 header without its own entry
// still gives the child an empty entry.
func TestAppendAncestorBuilds_NoFutureHealKeepsBackfill(t *testing.T) {
	t.Parallel()

	for _, on := range []bool{false, true} {
		t.Run(map[bool]string{false: "fallback off", true: "fallback on"}[on], func(t *testing.T) {
			t.Parallel()

			ff := newFallbackFF(t, on)
			ancestorID := uuid.New()
			provider := storage.NewMockStorageProvider(t)
			serveStoredHeader(t, provider, ancestorID, ancestorHeader(t, ancestorID, headers.MetadataVersionV4, nil))

			u := gapUpload(t, provider)
			u.uploads.ff = ff
			dst := map[uuid.UUID]headers.BuildData{}
			delta := watchAncestorResolutions(t)
			require.NoError(t, u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
			require.Equal(t, map[string]int64{"no_future/storage_heal": 1}, delta())
			require.Equal(t, map[uuid.UUID]headers.BuildData{ancestorID: {}}, dst)
		})
	}
}

// ancestorResolutionsMu serializes every test that walks ancestors: the
// counter's attributes carry no build identifier, so a test can read only its
// own increments as a delta, and only while no other walk moves the counter.
var ancestorResolutionsMu sync.Mutex

// watchAncestorResolutions takes ancestorResolutionsMu for the rest of the
// test and returns the counter's increments since the call, keyed
// "verdict/resolution".
func watchAncestorResolutions(t *testing.T) func() map[string]int64 {
	t.Helper()

	ancestorResolutionsMu.Lock()
	t.Cleanup(ancestorResolutionsMu.Unlock)

	before := ancestorResolutionCounts(t)

	return func() map[string]int64 {
		t.Helper()

		delta := map[string]int64{}
		for k, v := range ancestorResolutionCounts(t) {
			if d := v - before[k]; d != 0 {
				delta[k] = d
			}
		}

		return delta
	}
}

func ancestorResolutionCounts(t *testing.T) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorTemplateCacheAncestorResolutionsCounterName) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not an int64 sum", m.Name)
			for _, dp := range sum.DataPoints {
				verdict, ok := dp.Attributes.Value("verdict")
				require.True(t, ok, "point without a verdict")
				resolution, ok := dp.Attributes.Value("resolution")
				require.True(t, ok, "point without a resolution")
				require.Equal(t, 2, dp.Attributes.Len(), "unexpected attributes: %v", dp.Attributes)
				out[verdict.AsString()+"/"+resolution.AsString()] += dp.Value
			}
		}
	}

	return out
}

// mappingToAll maps one block onto each build, in order.
func mappingToAll(t *testing.T, buildIDs ...uuid.UUID) headers.Mapping {
	t.Helper()

	maps := make([]headers.BuildMap, len(buildIDs))
	for i, id := range buildIDs {
		maps[i] = headers.BuildMap{Offset: uint64(i) * testBlockSize, Length: testBlockSize, BuildId: id}
	}
	m, err := headers.NewMapping(testBlockSize, maps)
	require.NoError(t, err)

	return m
}

// A walk counts each ancestor once and skips the child's own build and the
// nil build; the ancestor whose wait fails is counted, and the ones after it
// are not reached.
func TestAppendAncestorBuilds_CountsEachAncestorOnce(t *testing.T) {
	t.Parallel()

	uploads, cache := newUploads(t)
	uploads.p2p = peerclient.NopResolver()
	selfID, cachedID, failingID, unreachedID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	putHeader(t, cache, cachedID, build.Memfile, false)
	uploads.ff = newFallbackFF(t, false)
	firedFuture(t, uploads, failingID, nil) // no entry, fallback off: the wait fails

	u := &Upload{buildID: selfID, uploads: uploads, store: storage.NewMockStorageProvider(t)}
	delta := watchAncestorResolutions(t)
	err := u.appendAncestorBuilds(t.Context(), map[uuid.UUID]headers.BuildData{},
		mappingToAll(t, selfID, cachedID, uuid.Nil, failingID, unreachedID), build.Memfile)
	require.ErrorContains(t, err, failingID.String())
	require.Equal(t, map[string]int64{"entry/overwrite": 1, "error/none": 1}, delta())
}

// A pending local entry with no future is polled from storage, and counted
// under that verdict.
func TestAppendAncestorBuilds_CountsRemotePoll(t *testing.T) {
	t.Parallel()

	uploads, cache := newUploads(t)
	ancestorID := uuid.New()
	finalized := ancestorHeader(t, ancestorID, headers.MetadataVersionV4, map[uuid.UUID]headers.BuildData{ancestorID: {Size: 9}})

	tpl := templatemocks.NewMockTemplate(t)
	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(&headers.Header{
		Metadata:                &headers.Metadata{Version: headers.MetadataVersionV4},
		IncompletePendingUpload: true,
	})
	dev.EXPECT().SwapHeader(mock.Anything).Once()
	tpl.EXPECT().Memfile(mock.Anything).Return(dev, nil)
	cache.put(ancestorID.String(), tpl)

	provider := storage.NewMockStorageProvider(t)
	serveStoredHeader(t, provider, ancestorID, finalized)
	uploads.persistence = provider

	dst := map[uuid.UUID]headers.BuildData{}
	u := &Upload{buildID: uuid.New(), uploads: uploads, store: storage.NewMockStorageProvider(t)}
	delta := watchAncestorResolutions(t)
	require.NoError(t, u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
	require.Equal(t, map[uuid.UUID]headers.BuildData{ancestorID: {Size: 9}}, dst)
	require.Equal(t, map[string]int64{"p2p_poll/overwrite": 1}, delta())
}

func newDropProvisionalFF(t *testing.T, on bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SnapshotCacheDropProvisionalHeaderFlag.Key()).VariationForAll(on))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// The upload keeps its snapshot for the whole retry budget but never reads the
// provisional header, which AddSnapshot has already consumed. With the flag on
// NewUpload clears it; with the flag off, with no flags client (resume-build),
// or with nothing to clear, the snapshot is left as it was. Each call records
// one outcome.
//
// The counter is process-wide and carries no per-run label, so the test reads
// it as a delta and does not run in parallel. Only this test records dropped
// or flag_off on the upload's header, but every other NewUpload records none,
// so the none case requires at least one.
//
//nolint:paralleltest // reads a process-wide counter as a delta
func TestNewUpload_DropsProvisionalHeader(t *testing.T) {
	provisional, err := headers.NewHeader(&headers.Metadata{Version: 3, BlockSize: 4096, Size: 4096}, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		ff          *featureflags.Client
		provisional *headers.Header
		wantCleared bool
		outcome     string
	}{
		{name: "flag on", ff: newDropProvisionalFF(t, true), provisional: provisional, wantCleared: true, outcome: "dropped"},
		{name: "flag off", ff: newDropProvisionalFF(t, false), provisional: provisional, wantCleared: false, outcome: "flag_off"},
		{name: "no flags client", ff: nil, provisional: provisional, wantCleared: false, outcome: "flag_off"},
		{name: "no provisional header", ff: newDropProvisionalFF(t, true), provisional: nil, wantCleared: false, outcome: "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := &Snapshot{
				BuildID:            uuid.New(),
				FilesystemSnapshot: true,
				RootfsBlockSize:    4096,
				MemorySnapshot:     MemorySnapshot{ProvisionalDiffHeader: tc.provisional},
			}

			before := map[string]int64{}
			for _, o := range []string{"dropped", "flag_off", "none"} {
				before[o] = deadStructureOutcomeTotal(t, "upload_provisional_header", o)
			}
			_, err := NewUpload(t.Context(), nil, snap, nil, storage.CompressConfig{}, tc.ff, storage.UseCasePause, storage.ObjectMetadata{}, nil)
			require.NoError(t, err)

			if tc.wantCleared {
				require.Nil(t, snap.MemorySnapshot.ProvisionalDiffHeader)
			} else {
				require.Same(t, tc.provisional, snap.MemorySnapshot.ProvisionalDiffHeader)
			}
			for _, o := range []string{"dropped", "flag_off"} {
				want := int64(0)
				if o == tc.outcome {
					want = 1
				}
				require.Equal(t, want, deadStructureOutcomeTotal(t, "upload_provisional_header", o)-before[o], o)
			}
			if tc.outcome == "none" {
				require.GreaterOrEqual(t, deadStructureOutcomeTotal(t, "upload_provisional_header", "none")-before["none"], int64(1))
			}
		})
	}
}
