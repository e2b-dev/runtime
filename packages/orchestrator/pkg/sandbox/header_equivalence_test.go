//go:build linux

package sandbox

import (
	"context"
	"maps"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// newReleaseFF returns a flags client serving SnapshotCacheReleaseSupersededFlag
// at release and SnapshotCacheAncestorStorageFallbackFlag at fallback.
func newReleaseFF(t *testing.T, release, fallback bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SnapshotCacheReleaseSupersededFlag.Key()).VariationForAll(release))
	td.Update(td.Flag(featureflags.SnapshotCacheAncestorStorageFallbackFlag.Key()).VariationForAll(fallback))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ff.Close(context.WithoutCancel(t.Context()))
	})

	return ff
}

// countingProvider counts the blob opens per path it passes to the wrapped
// provider.
type countingProvider struct {
	storage.StorageProvider

	mu    sync.Mutex
	opens map[string]int
}

func newCountingProvider(inner storage.StorageProvider) *countingProvider {
	return &countingProvider{StorageProvider: inner, opens: map[string]int{}}
}

func (p *countingProvider) OpenBlob(ctx context.Context, path string) (storage.Blob, error) {
	p.mu.Lock()
	p.opens[path]++
	p.mu.Unlock()

	return p.StorageProvider.OpenBlob(ctx, path)
}

func (p *countingProvider) count(path string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.opens[path]
}

func memfileHeaderPath(buildID uuid.UUID) string {
	return storage.Paths{BuildID: buildID.String()}.HeaderFile(storage.MemfileName)
}

// v3WrittenAncestor is the header runV3 publishes under a V4 parent: V4
// version, no entry for its own build.
func v3WrittenAncestor(t *testing.T, id uuid.UUID) *headers.Header {
	t.Helper()

	return ancestorHeader(t, id, headers.MetadataVersionV4, nil)
}

// framedShape is a compressed V4 ancestor: its own entry carries a frame table.
var framedShape = ancestorShape{
	name: "V4 header with its own framed entry",
	header: func(t *testing.T, id uuid.UUID) *headers.Header {
		t.Helper()

		return ancestorHeader(t, id, headers.MetadataVersionV4, map[uuid.UUID]headers.BuildData{id: framedBuildData()})
	},
	want: func(id uuid.UUID) map[uuid.UUID]headers.BuildData {
		return map[uuid.UUID]headers.BuildData{id: framedBuildData()}
	},
	resident: "overwrite",
	released: "storage_heal",
}

func framedBuildData() headers.BuildData {
	return headers.BuildData{
		Size:     testBlockSize,
		Checksum: [32]byte{1, 2, 3},
		FrameData: storage.NewFullFrameTable(storage.CompressionZstd, []storage.FrameSize{
			{U: testBlockSize, C: 1000},
		}).Table(),
	}
}

// serializeChild serializes the V4 header a child build publishes over
// ancestorID with builds as its ancestor entries.
func serializeChild(t *testing.T, childID, ancestorID uuid.UUID, builds map[uuid.UUID]headers.BuildData) []byte {
	t.Helper()

	h, err := headers.NewHeader(&headers.Metadata{
		Version: headers.MetadataVersionV4, BlockSize: testBlockSize, Size: 2 * testBlockSize,
		Generation: 1, BuildId: childID, BaseBuildId: ancestorID,
	}, []headers.BuildMap{
		{Offset: 0, Length: testBlockSize, BuildId: ancestorID},
		{Offset: testBlockSize, Length: testBlockSize, BuildId: childID},
	})
	require.NoError(t, err)
	h.Builds = maps.Clone(builds)
	h.Builds[childID] = headers.BuildData{Size: testBlockSize}

	data, err := headers.SerializeHeader(h)
	require.NoError(t, err)

	return data
}

// A child's Builds entry for its ancestor, and so its serialized header, is
// the same whether the ancestor's cache entry still holds the header its
// upload published or has been released and is healed from storage — for
// each heal verdict a release can produce, whatever the flags read at the
// heal, for every ancestor shape.
func TestHeaderEquivalence_ResidentVsReleased(t *testing.T) {
	t.Parallel()

	releases := []struct {
		name    string
		ff      *featureflags.Client
		future  bool
		verdict string
	}{
		{name: "future fired, both flags on", ff: newReleaseFF(t, true, true), future: true, verdict: "future_no_entry"},
		{name: "future fired, both flags off", ff: newReleaseFF(t, false, false), future: true, verdict: "future_no_entry"},
		{name: "future expired, both flags on", ff: newReleaseFF(t, true, true), future: false, verdict: "no_future"},
		{name: "future expired, both flags off", ff: newReleaseFF(t, false, false), future: false, verdict: "no_future"},
		{name: "future expired, nil client", future: false, verdict: "no_future"},
	}

	shapes := append([]ancestorShape{framedShape}, ancestorShapes...)

	for _, rel := range releases {
		for _, shape := range shapes {
			t.Run(rel.name+"/"+shape.name, func(t *testing.T) {
				t.Parallel()

				ancestorID, childID := uuid.New(), uuid.New()
				ancestor := shape.header(t, ancestorID)
				delta := watchAncestorResolutions(t)

				resident, residentCache := newUploads(t)
				resident.ff = rel.ff
				putResidentHeader(t, residentCache, ancestorID, ancestor)
				firedFuture(t, resident, ancestorID, nil)
				residentDst := map[uuid.UUID]headers.BuildData{}
				u := &Upload{buildID: childID, uploads: resident, store: storage.NewMockStorageProvider(t)}
				require.NoError(t, u.appendAncestorBuilds(t.Context(), residentDst, mappingTo(t, ancestorID), build.Memfile))

				released, releasedCache := newUploads(t)
				released.ff = rel.ff
				released.p2p = peerclient.NopResolver()
				releasedCache.release(ancestorID.String())
				if rel.future {
					firedFuture(t, released, ancestorID, nil)
				}
				mock := storage.NewMockStorageProvider(t)
				serveStoredHeader(t, mock, ancestorID, ancestor)
				provider := newCountingProvider(mock)
				releasedDst := map[uuid.UUID]headers.BuildData{}
				u = &Upload{buildID: childID, uploads: released, store: provider}
				require.NoError(t, u.appendAncestorBuilds(t.Context(), releasedDst, mappingTo(t, ancestorID), build.Memfile))

				require.Equal(t, shape.want(ancestorID), residentDst, "resident")
				require.Equal(t, residentDst, releasedDst, "released must write what resident writes")
				require.Equal(t,
					serializeChild(t, childID, ancestorID, residentDst),
					serializeChild(t, childID, ancestorID, releasedDst),
					"serialized child headers must be byte-identical")
				if bd, ok := shape.want(ancestorID)[ancestorID]; ok && bd.FrameData != nil {
					require.NotNil(t, releasedDst[ancestorID].FrameData, "released heal must keep the frame table")
				}
				assert.Equal(t, 1, provider.count(memfileHeaderPath(ancestorID)), "released run reads the stored header once")
				require.Equal(t, map[string]int64{"entry/" + shape.resident: 1, rel.verdict + "/" + shape.released: 1}, delta())
			})
		}
	}
}

// The no-future heal of a V3-written ancestor skips LoadHeader's backfill
// exactly when this node released the ancestor, leaving the child without an
// entry as a resident ancestor would. No flag moves it either way: an
// ancestor this node did not release is backfilled as before, whatever the
// flags read.
func TestAppendAncestorBuilds_NoFutureHealFollowsTheReleaseRecord(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name               string
		nilClient          bool
		release, fallback  bool
		releasedHere       bool
		want               func(id uuid.UUID) map[uuid.UUID]headers.BuildData
		wantResolutionKind string
	}{
		{name: "nil client", nilClient: true, want: backfilled, wantResolutionKind: "no_future/storage_heal"},
		{name: "both off", want: backfilled, wantResolutionKind: "no_future/storage_heal"},
		{name: "release off, fallback on", fallback: true, want: backfilled, wantResolutionKind: "no_future/storage_heal"},
		{name: "release on, fallback off", release: true, want: backfilled, wantResolutionKind: "no_future/storage_heal"},
		{name: "both on", release: true, fallback: true, want: backfilled, wantResolutionKind: "no_future/storage_heal"},
		{name: "released here, both on", release: true, fallback: true, releasedHere: true, want: noEntry, wantResolutionKind: "no_future/self_entry_absent"},
		{name: "released here, both off", releasedHere: true, want: noEntry, wantResolutionKind: "no_future/self_entry_absent"},
		{name: "released here, nil client", nilClient: true, releasedHere: true, want: noEntry, wantResolutionKind: "no_future/self_entry_absent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var ff *featureflags.Client
			if !tc.nilClient {
				ff = newReleaseFF(t, tc.release, tc.fallback)
			}
			ancestorID := uuid.New()
			provider := storage.NewMockStorageProvider(t)
			serveStoredHeader(t, provider, ancestorID, v3WrittenAncestor(t, ancestorID))

			u := gapUpload(t, provider)
			u.uploads.ff = ff
			if tc.releasedHere {
				u.uploads.tc.(*fakeCache).release(ancestorID.String())
			}
			dst := map[uuid.UUID]headers.BuildData{}
			delta := watchAncestorResolutions(t)
			require.NoError(t, u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
			require.Equal(t, map[string]int64{tc.wantResolutionKind: 1}, delta())
			require.Equal(t, tc.want(ancestorID), dst)
		})
	}
}

func backfilled(id uuid.UUID) map[uuid.UUID]headers.BuildData {
	return map[uuid.UUID]headers.BuildData{id: {}}
}

func noEntry(uuid.UUID) map[uuid.UUID]headers.BuildData {
	return map[uuid.UUID]headers.BuildData{}
}

// Three pauses of one chain over a non-resident V3-written ancestor with no
// upload future, each child inheriting the previous child's Builds map, with
// both flags on. A released ancestor never gets an entry, so every pause reads
// its stored header once; a never-released one is backfilled by the first
// pause, and later pauses inherit that entry and read nothing.
func TestAppendAncestorBuilds_RepeatedPausesHeaderReads(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		released   bool
		wantReads  []int
		wantCounts map[string]int64
	}{
		{
			name: "released here", released: true,
			wantReads:  []int{1, 1, 1},
			wantCounts: map[string]int64{"no_future/self_entry_absent": 3},
		},
		{
			name: "never released", released: false,
			wantReads:  []int{1, 0, 0},
			wantCounts: map[string]int64{"no_future/storage_heal": 1, "no_future/inherited": 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ff := newReleaseFF(t, true, true)
			ancestorID := uuid.New()
			mock := storage.NewMockStorageProvider(t)
			serveStoredHeader(t, mock, ancestorID, v3WrittenAncestor(t, ancestorID))
			provider := newCountingProvider(mock)
			path := memfileHeaderPath(ancestorID)

			delta := watchAncestorResolutions(t)
			inherited := map[uuid.UUID]headers.BuildData{}
			for pause, want := range tc.wantReads {
				u := gapUpload(t, provider)
				u.uploads.ff = ff
				if tc.released {
					u.uploads.tc.(*fakeCache).release(ancestorID.String())
				}
				dst := maps.Clone(inherited)
				before := provider.count(path)
				require.NoError(t, u.appendAncestorBuilds(t.Context(), dst, mappingTo(t, ancestorID), build.Memfile))
				require.Equal(t, want, provider.count(path)-before, "pause %d: header reads for the ancestor", pause+1)
				dst[u.buildID] = headers.BuildData{Size: testBlockSize}
				inherited = dst
			}
			require.Equal(t, tc.wantCounts, delta())
		})
	}
}

// The differential above can tell the two loads apart: for a V3-written
// ancestor, LoadHeader backfills an entry LoadStoredHeader does not carry,
// so a released heal through LoadHeader would diverge from the resident run.
func TestHeaderEquivalence_BackfillDistinguishesV3WrittenAncestor(t *testing.T) {
	t.Parallel()

	ancestorID := uuid.New()
	provider := storage.NewMockStorageProvider(t)
	serveStoredHeader(t, provider, ancestorID, v3WrittenAncestor(t, ancestorID))
	path := memfileHeaderPath(ancestorID)

	loaded, _, err := headers.LoadHeader(t.Context(), provider, path)
	require.NoError(t, err)
	stored, _, err := headers.LoadStoredHeader(t.Context(), provider, path)
	require.NoError(t, err)

	require.Contains(t, loaded.Builds, ancestorID, "LoadHeader backfills the self entry")
	require.NotContains(t, stored.Builds, ancestorID, "LoadStoredHeader keeps the stored Builds map")
	require.NotEqual(t, loaded.Builds, stored.Builds)

	childID := uuid.New()
	require.NotEqual(t,
		serializeChild(t, childID, ancestorID, map[uuid.UUID]headers.BuildData{ancestorID: loaded.Builds[ancestorID]}),
		serializeChild(t, childID, ancestorID, map[uuid.UUID]headers.BuildData{}),
		"a backfilled entry changes the serialized child header")
}
