//go:build linux

package template

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	blockmocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// durableMemfileDevice is a ReadonlyDevice that also exposes DurableHeaderNow,
// like the real *Storage. Header() stands in for the live (provisional) header;
// DurableHeaderNow returns the deduped header scheduling metadata must use, and
// its ready flag models whether the deduped header has resolved yet.
type durableMemfileDevice struct {
	*blockmocks.MockReadonlyDevice

	durable *header.Header
	ready   bool
}

func (d durableMemfileDevice) DurableHeaderNow() (*header.Header, bool) {
	return d.durable, d.ready
}

func schedulingTemplate(t *testing.T, mem block.ReadonlyDevice, rootfsBase uuid.UUID) *storageTemplate {
	t.Helper()
	rootfsHdr, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096, BaseBuildId: rootfsBase}, nil)
	require.NoError(t, err)
	rootfsDev := blockmocks.NewMockReadonlyDevice(t)
	rootfsDev.EXPECT().Header().Return(rootfsHdr)

	tmpl := &storageTemplate{
		memfile: utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:  utils.NewSetOnce[block.ReadonlyDevice](),
	}
	require.NoError(t, tmpl.rootfs.SetValue(rootfsDev))
	require.NoError(t, tmpl.memfile.SetValue(mem))

	return tmpl
}

// When the deduped header has resolved, SchedulingMetadata reports it (never the
// live/provisional header) — so memfile build ids reflect the real build id.
func TestStorageTemplate_SchedulingMetadataUsesDurableHeader(t *testing.T) {
	t.Parallel()

	dedupedBase := uuid.New()
	dedupedHdr, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096, BaseBuildId: dedupedBase}, nil)
	require.NoError(t, err)
	memMock := blockmocks.NewMockReadonlyDevice(t)
	memMock.EXPECT().Header().Return(nil).Maybe()
	memDev := durableMemfileDevice{MockReadonlyDevice: memMock, durable: dedupedHdr, ready: true}

	md := schedulingTemplate(t, memDev, uuid.New()).SchedulingMetadata(t.Context())
	require.NotNil(t, md)
	assert.Equal(t, dedupedBase.String(), md.GetMemfileBaseBuildId())
}

// While the deduped header is still pending (provisional window), SchedulingMetadata
// must NOT block and must NOT emit the provisional build id: it reports rootfs-only
// metadata (empty memfile base build id).
func TestStorageTemplate_SchedulingMetadataSkipsPendingMemfile(t *testing.T) {
	t.Parallel()

	rootfsBase := uuid.New()
	memMock := blockmocks.NewMockReadonlyDevice(t)
	memMock.EXPECT().Header().Return(nil).Maybe()
	memDev := durableMemfileDevice{MockReadonlyDevice: memMock, durable: nil, ready: false}

	md := schedulingTemplate(t, memDev, rootfsBase).SchedulingMetadata(t.Context())
	require.NotNil(t, md)
	assert.Empty(t, md.GetMemfileBaseBuildId())
	assert.Equal(t, rootfsBase.String(), md.GetRootfsBaseBuildId())
}

// footprintHeader builds a header whose mapping has exactly n entries, using a
// distinct build id per entry so nothing merges.
func footprintHeader(t *testing.T, n int) *header.Header {
	t.Helper()

	const blockSize = uint64(4096)

	maps := make([]header.BuildMap, n)
	for i := range maps {
		maps[i] = header.BuildMap{
			Offset:  uint64(i) * blockSize,
			Length:  blockSize,
			BuildId: uuid.New(),
		}
	}

	h, err := header.NewHeader(&header.Metadata{
		Version:     3,
		BlockSize:   blockSize,
		Size:        uint64(n) * blockSize,
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}, maps)
	require.NoError(t, err)
	require.Equal(t, n, h.Mapping.Len())

	return h
}

// The gauge exists to size a bound, so it has to see every mapping the template
// keeps alive. After a pause the provisional header stays referenced by
// memfileHeader while the device has already moved on to the deduped one, so a
// gauge that reads only the devices reports one mapping where two are resident
// — understating exactly the footprint it is used to size.
func TestStorageTemplate_HeaderFootprintCountsRetainedHolders(t *testing.T) {
	t.Parallel()

	provisional := footprintHeader(t, 12)
	deduped := footprintHeader(t, 3)
	rootfsHdr := footprintHeader(t, 5)

	memDev := blockmocks.NewMockReadonlyDevice(t)
	memDev.EXPECT().Header().Return(deduped)
	rootfsDev := blockmocks.NewMockReadonlyDevice(t)
	rootfsDev.EXPECT().Header().Return(rootfsHdr)

	tmpl := &storageTemplate{
		memfile:              utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:               utils.NewSetOnce[block.ReadonlyDevice](),
		rootfsHeader:         resolvedHeader(rootfsHdr),
		durableMemfileHeader: resolvedHeader(deduped),
	}
	tmpl.memfileHeader.Store(resolvedHeader(provisional))
	require.NoError(t, tmpl.memfile.SetValue(memDev))
	require.NoError(t, tmpl.rootfs.SetValue(rootfsDev))

	entries, bytes := tmpl.headerFootprint()

	wantEntries := provisional.Mapping.Len() + deduped.Mapping.Len() + rootfsHdr.Mapping.Len()
	wantBytes := provisional.Mapping.ByteSize() + deduped.Mapping.ByteSize() + rootfsHdr.Mapping.ByteSize()

	assert.Equal(t, wantEntries, entries, "the provisional header is still resident and must be counted")
	assert.Equal(t, wantBytes, bytes)
}

// The holders usually resolve to the same headers the devices carry. Counting
// those twice would overstate the number the sizing decisions are made from.
func TestStorageTemplate_HeaderFootprintCountsEachMappingOnce(t *testing.T) {
	t.Parallel()

	memHdr := footprintHeader(t, 7)
	rootfsHdr := footprintHeader(t, 4)

	memDev := blockmocks.NewMockReadonlyDevice(t)
	memDev.EXPECT().Header().Return(memHdr)
	rootfsDev := blockmocks.NewMockReadonlyDevice(t)
	rootfsDev.EXPECT().Header().Return(rootfsHdr)

	tmpl := &storageTemplate{
		memfile:      utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:       utils.NewSetOnce[block.ReadonlyDevice](),
		rootfsHeader: resolvedHeader(rootfsHdr),
	}
	tmpl.memfileHeader.Store(resolvedHeader(memHdr))
	require.NoError(t, tmpl.memfile.SetValue(memDev))
	require.NoError(t, tmpl.rootfs.SetValue(rootfsDev))

	entries, bytes := tmpl.headerFootprint()

	assert.Equal(t, memHdr.Mapping.Len()+rootfsHdr.Mapping.Len(), entries)
	assert.Equal(t, memHdr.Mapping.ByteSize()+rootfsHdr.Mapping.ByteSize(), bytes)
}

// A template that is still fetching must contribute nothing rather than block
// the metrics collection goroutine on its unresolved futures.
func TestStorageTemplate_HeaderFootprintSkipsUnresolved(t *testing.T) {
	t.Parallel()

	tmpl := &storageTemplate{
		memfile:      utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:       utils.NewSetOnce[block.ReadonlyDevice](),
		rootfsHeader: utils.NewSetOnce[*header.Header](),
	}
	tmpl.memfileHeader.Store(utils.NewSetOnce[*header.Header]())

	done := make(chan struct{})
	go func() {
		defer close(done)

		entries, bytes := tmpl.headerFootprint()
		assert.Zero(t, entries)
		assert.Zero(t, bytes)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("headerFootprint blocked on an unresolved template")
	}
}

// One allocation reaches headerFootprint by two routes once a snapshot's upload
// publishes: CloneForUpload copies the Header struct, so the clone shares the
// source's Mapping slices, and publish installs the clone on the device while
// the holder keeps the source. Header identity does not see that, so the gauge
// would step up as uploads land — reading like retention growth rather than a
// counting artifact, on exactly the population a byte budget gets sized from.
func TestStorageTemplate_HeaderFootprintCountsSharedMappingOnce(t *testing.T) {
	t.Parallel()

	source := footprintHeader(t, 9)
	published := source.CloneForUpload(source.Metadata.Version + 1)
	require.NotSame(t, source, published, "the clone is a distinct header")
	require.True(t, source.Mapping.SharesStorageWith(published.Mapping),
		"the clone must share the source's mapping, or this test proves nothing")

	rootfsHdr := footprintHeader(t, 4)

	memDev := blockmocks.NewMockReadonlyDevice(t)
	memDev.EXPECT().Header().Return(published)
	rootfsDev := blockmocks.NewMockReadonlyDevice(t)
	rootfsDev.EXPECT().Header().Return(rootfsHdr)

	tmpl := &storageTemplate{
		memfile:      utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:       utils.NewSetOnce[block.ReadonlyDevice](),
		rootfsHeader: resolvedHeader(rootfsHdr),
	}
	tmpl.memfileHeader.Store(resolvedHeader(source))
	require.NoError(t, tmpl.memfile.SetValue(memDev))
	require.NoError(t, tmpl.rootfs.SetValue(rootfsDev))

	entries, bytes := tmpl.headerFootprint()

	assert.Equal(t, source.Mapping.Len()+rootfsHdr.Mapping.Len(), entries,
		"the shared mapping must be counted once, not once per header")
	assert.Equal(t, source.Mapping.ByteSize()+rootfsHdr.Mapping.ByteSize(), bytes)
}

type countingFile struct {
	path   string
	closes atomic.Int32
}

func (f *countingFile) Path() string { return f.path }

func (f *countingFile) Close() error {
	f.closes.Add(1)

	return nil
}

// The cache can close one instance from two paths at once: a retired entry's
// last release and the eviction callback Invalidate queued. The teardown must
// run once however many callers race for it, and each caller must see it
// finished before Close returns.
func TestStorageTemplate_CloseRunsOnce(t *testing.T) {
	t.Parallel()

	paths, err := storage.Paths{BuildID: uuid.NewString()}.Cache(storage.Config{TemplateCacheDir: t.TempDir()})
	require.NoError(t, err)

	var memCloses, rootfsCloses atomic.Int32
	memDev := blockmocks.NewMockReadonlyDevice(t)
	memDev.EXPECT().Close().RunAndReturn(func() error {
		memCloses.Add(1)

		return nil
	}).Maybe()
	rootfsDev := blockmocks.NewMockReadonlyDevice(t)
	rootfsDev.EXPECT().Close().RunAndReturn(func() error {
		rootfsCloses.Add(1)

		return nil
	}).Maybe()
	snapfile := &countingFile{path: paths.CacheSnapfile()}

	tmpl := &storageTemplate{
		paths:    paths,
		memfile:  utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:   utils.NewSetOnce[block.ReadonlyDevice](),
		snapfile: utils.NewSetOnce[File](),
	}
	require.NoError(t, tmpl.memfile.SetValue(memDev))
	require.NoError(t, tmpl.rootfs.SetValue(rootfsDev))
	require.NoError(t, tmpl.snapfile.SetValue(snapfile))

	const callers = 8

	start := make(chan struct{})
	errs := make([]error, callers)

	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			<-start
			errs[i] = tmpl.Close(t.Context())
		})
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "caller %d", i)
	}

	assert.Equal(t, int32(1), memCloses.Load(), "memfile closed more than once")
	assert.Equal(t, int32(1), rootfsCloses.Load(), "rootfs closed more than once")
	assert.Equal(t, int32(1), snapfile.closes.Load(), "snapfile closed more than once")
	assert.NoDirExists(t, filepath.Dir(paths.CacheSnapfile()))
}

// swapDeadStructureMetrics points the package-level dead-structure counters
// at a manual reader for the duration of the test. NOT parallel-safe.
func swapDeadStructureMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).
		Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template")

	prevOutcome, prevBytes := deadStructureOutcomeMetric, deadStructureBytesMetric
	deadStructureOutcomeMetric = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureOutcomeCounterName))
	deadStructureBytesMetric = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureBytesCounterName))
	t.Cleanup(func() { deadStructureOutcomeMetric, deadStructureBytesMetric = prevOutcome, prevBytes })

	return reader
}

// deadStructureTotals returns the dead-structure outcome counts and bytes,
// each keyed by "structure/outcome".
func deadStructureTotals(t *testing.T, reader *sdkmetric.ManualReader) (outcomes, bytes map[string]int64) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	outcomes, bytes = map[string]int64{}, map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			var out map[string]int64
			switch m.Name {
			case string(telemetry.OrchestratorDeadStructureOutcomeCounterName):
				out = outcomes
			case string(telemetry.OrchestratorDeadStructureBytesCounterName):
				out = bytes
			default:
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				structure, ok := dp.Attributes.Value("structure")
				require.True(t, ok, "datapoint missing structure attribute")
				outcome, ok := dp.Attributes.Value("outcome")
				require.True(t, ok, "datapoint missing outcome attribute")
				out[structure.AsString()+"/"+outcome.AsString()] += dp.Value
			}
		}
	}

	return outcomes, bytes
}

// fetchedTemplate runs a real Fetch over local files and resolved headers. With
// provisional set it is shaped as AddSnapshot shapes a template served from a
// provisional header: the holder carries the provisional header, the deduped
// one arrives as the durable header, and drop carries the flag AddSnapshot
// read. Otherwise the holder carries the header the device is built from.
func fetchedTemplate(t *testing.T, provisional, drop bool) (tmpl *storageTemplate, served, deduped, rootfsHdr *header.Header) {
	t.Helper()

	served = footprintHeader(t, 12)
	deduped = footprintHeader(t, 3)
	rootfsHdr = footprintHeader(t, 5)

	holder := resolvedHeader(served)
	var durable *utils.SetOnce[*header.Header]
	if provisional {
		// Both are a pause's headers, which stay incomplete until the upload.
		served.IncompletePendingUpload = true
		deduped.IncompletePendingUpload = true
		durable = resolvedHeader(deduped)
	}

	dir := t.TempDir()
	tmpl, err := newTemplateFromStorage(
		cfg.BuilderConfig{StorageConfig: storage.Config{TemplateCacheDir: dir}},
		uuid.NewString(),
		holder,
		resolvedHeader(rootfsHdr),
		nil,
		blockmetrics.Metrics{},
		&countingFile{path: filepath.Join(dir, "snapfile")},
		&countingFile{path: filepath.Join(dir, "metadata.json")},
		durable,
	)
	require.NoError(t, err)
	tmpl.dropProvisionalHeader = drop

	tmpl.Fetch(t.Context(), nil)

	return tmpl, served, deduped, rootfsHdr
}

// With the flag on, Fetch drops the template's hold on the provisional header
// and changes nothing a reader of the template sees: the device still serves
// the provisional header until the swap, and a pause and scheduling metadata
// still use the deduped one. Once the swap lands, the footprint stops counting
// the provisional mapping. With the flag off the holder keeps it, as it always
// has, and the footprint keeps counting it. Either way Fetch records one
// outcome and the header's size.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestStorageTemplate_FetchDropsProvisionalHeader(t *testing.T) {
	for _, tc := range []struct {
		name          string
		drop          bool
		wantHeld      bool
		outcome       string
		wantCountsOld bool
	}{
		{name: "flag on", drop: true, wantHeld: false, outcome: "dropped", wantCountsOld: false},
		{name: "flag off", drop: false, wantHeld: true, outcome: "flag_off", wantCountsOld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := swapDeadStructureMetrics(t)

			tmpl, provisional, deduped, rootfsHdr := fetchedTemplate(t, true, tc.drop)

			held := tmpl.memfileHeader.Load()
			assert.Equal(t, tc.wantHeld, held != nil, "the template's hold on the provisional header after Fetch")
			if held != nil {
				h, err := held.Result()
				require.NoError(t, err)
				assert.Same(t, provisional, h)
			}
			key := "template_provisional_header/" + tc.outcome
			outcomes, bytes := deadStructureTotals(t, reader)
			assert.Equal(t, map[string]int64{key: 1}, outcomes, "one outcome per Fetch")
			assert.Equal(t, map[string]int64{key: int64(provisional.Mapping.ByteSize())}, bytes)

			mem, err := tmpl.memfile.Result()
			require.NoError(t, err)
			assert.Same(t, provisional, mem.Header(), "the device serves the provisional header until the swap")

			durable, ok := mem.(interface {
				DurableHeaderNow() (*header.Header, bool)
			})
			require.True(t, ok)
			dh, ready := durable.DurableHeaderNow()
			require.True(t, ready)
			assert.Same(t, deduped, dh, "a pause still parents off the deduped header")

			sm := tmpl.SchedulingMetadata(t.Context())
			require.NotNil(t, sm)
			assert.Equal(t, deduped.Metadata.BaseBuildId.String(), sm.GetMemfileBaseBuildId(),
				"scheduling metadata reports the deduped header, not the provisional one")

			cas, ok := mem.(interface {
				SwapHeaderIfCurrent(old, next *header.Header) bool
			})
			require.True(t, ok)
			require.True(t, cas.SwapHeaderIfCurrent(provisional, deduped), "the swap still finds the provisional header")

			entries, bytesHeld := tmpl.headerFootprint()
			wantEntries := deduped.Mapping.Len() + rootfsHdr.Mapping.Len()
			wantBytes := deduped.Mapping.ByteSize() + rootfsHdr.Mapping.ByteSize()
			if tc.wantCountsOld {
				wantEntries += provisional.Mapping.Len()
				wantBytes += provisional.Mapping.ByteSize()
			}
			assert.Equal(t, wantEntries, entries)
			assert.Equal(t, wantBytes, bytesHeld)
		})
	}
}

// A template not built from a provisional header keeps its holder, which
// aliases the header the device is built from, whatever the flag says; Fetch
// records none and no size.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestStorageTemplate_FetchKeepsNonProvisionalHolder(t *testing.T) {
	reader := swapDeadStructureMetrics(t)

	tmpl, served, _, _ := fetchedTemplate(t, false, true)

	held := tmpl.memfileHeader.Load()
	require.NotNil(t, held)
	h, err := held.Result()
	require.NoError(t, err)
	assert.Same(t, served, h)
	outcomes, bytes := deadStructureTotals(t, reader)
	assert.Equal(t, map[string]int64{"template_provisional_header/none": 1}, outcomes)
	assert.Empty(t, bytes)
}

// A Fetch after the one that dropped the holder finds it gone. It must fail
// the memfile leg rather than dereference it, and leave the device the first
// Fetch published in place.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestStorageTemplate_SecondFetchAfterDrop(t *testing.T) {
	swapDeadStructureMetrics(t)

	tmpl, provisional, _, _ := fetchedTemplate(t, true, true)
	require.Nil(t, tmpl.memfileHeader.Load())

	require.NotPanics(t, func() { tmpl.Fetch(t.Context(), nil) })

	mem, err := tmpl.memfile.Result()
	require.NoError(t, err)
	assert.Same(t, provisional, mem.Header(), "the first Fetch's device is untouched")
}

// The footprint gauge reads the holder from the metrics goroutine while Fetch
// may be clearing it. Under -race, every concurrent load of the holder must
// see either the provisional header or nothing, and the gauge must never
// count more than the headers the template can reference.
func TestStorageTemplate_FootprintRacesProvisionalDrop(t *testing.T) {
	t.Parallel()

	provisional := footprintHeader(t, 12)
	deduped := footprintHeader(t, 3)
	rootfsHdr := footprintHeader(t, 5)

	dir := t.TempDir()
	tmpl, err := newTemplateFromStorage(
		cfg.BuilderConfig{StorageConfig: storage.Config{TemplateCacheDir: dir}},
		uuid.NewString(),
		resolvedHeader(provisional),
		resolvedHeader(rootfsHdr),
		nil,
		blockmetrics.Metrics{},
		&countingFile{path: filepath.Join(dir, "snapfile")},
		&countingFile{path: filepath.Join(dir, "metadata.json")},
		resolvedHeader(deduped),
	)
	require.NoError(t, err)
	tmpl.dropProvisionalHeader = true

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if holder := tmpl.memfileHeader.Load(); holder != nil {
					h, err := holder.Result()
					if assert.NoError(t, err) {
						assert.Same(t, provisional, h)
					}
				}
				entries, _ := tmpl.headerFootprint()
				assert.LessOrEqual(t, entries, provisional.Mapping.Len()+deduped.Mapping.Len()+rootfsHdr.Mapping.Len())
			}
		})
	}

	tmpl.Fetch(t.Context(), nil)
	close(stop)
	wg.Wait()

	assert.Nil(t, tmpl.memfileHeader.Load())
}
