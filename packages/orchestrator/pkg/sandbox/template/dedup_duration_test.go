//go:build linux

package template

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// swapDedupDurationMetric points the package-level dedup-duration histogram at
// a manual reader for the duration of the test. NOT parallel-safe.
func swapDedupDurationMetric(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := mp.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template")

	prev := memfileDedupDuration
	memfileDedupDuration = utils.Must(telemetry.GetHistogram(m, telemetry.OrchestratorSandboxMemfileDedupDurationName))
	t.Cleanup(func() { memfileDedupDuration = prev })

	return reader
}

func dedupDurationSamples(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.HistogramDataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var out []metricdata.HistogramDataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxMemfileDedupDurationName) {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			out = append(out, hist.DataPoints...)
		}
	}

	return out
}

func newDedupTestCache(t *testing.T) *Cache {
	t.Helper()

	return &Cache{
		config: cfg.Config{BuilderConfig: cfg.BuilderConfig{
			StorageConfig: storage.Config{TemplateCacheDir: t.TempDir()},
		}},
		cache: ttlcache.New(ttlcache.WithTTL[string, Template](time.Hour)),
	}
}

func mustHeader(t *testing.T, base uuid.UUID) *header.Header {
	t.Helper()

	h, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096, BaseBuildId: base}, nil)
	require.NoError(t, err)

	return h
}

// dedupTestDevice completes *build.File into a block.ReadonlyDevice; the swap
// goroutine only touches the promoted header CAS methods.
type dedupTestDevice struct {
	*build.File
}

func (dedupTestDevice) BlockSize() int64                    { return int64(header.PageSize) }
func (dedupTestDevice) Size(context.Context) (int64, error) { return 0, nil }
func (dedupTestDevice) Close() error                        { return nil }

// residentTemplate pre-inserts a template for buildID whose memfile is a real
// *build.File serving the provisional header, so AddSnapshot takes the cache-hit
// path (no Fetch) and its swap goroutine runs the real CAS against the device.
func residentTemplate(t *testing.T, c *Cache, buildID string, memfile dedupTestDevice) {
	t.Helper()

	tmpl, err := newTemplateFromStorage(c.config.BuilderConfig, buildID, nil, nil, nil, blockmetrics.Metrics{}, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, tmpl.memfile.SetValue(memfile))
	c.cache.Set(tmpl.Files().CacheKey(), tmpl, ttlcache.DefaultTTL)
}

// A pending provisional swap that resolves must record one dedup-duration
// sample covering the pause-to-swap latency.
//
//nolint:paralleltest // swaps the package-level dedup-duration histogram
func TestAddSnapshot_RecordsDedupDurationAtSwap(t *testing.T) {
	reader := swapDedupDurationMetric(t)

	c := newDedupTestCache(t)
	buildID := uuid.NewString()

	provisionalHdr := mustHeader(t, uuid.New())
	dedupedHdr := mustHeader(t, uuid.New())
	memfile := dedupTestDevice{build.NewFile(provisionalHdr, nil, build.Memfile, nil, blockmetrics.Metrics{})}
	residentTemplate(t, c, buildID, memfile)

	dedupedFuture := utils.NewSetOnce[*header.Header]()
	swapDone := make(chan struct{})

	createdAt := time.Now().Add(-50 * time.Millisecond)
	finishUpload, err := c.AddSnapshot(t.Context(), buildID, SnapshotLineage{Origin: storage.ObjectOriginPause},
		dedupedFuture, resolvedHeader(mustHeader(t, uuid.New())),
		nil, nil,
		&build.NoDiff{}, &build.NoDiff{},
		provisionalHdr, &build.NoDiff{},
		createdAt,
		func() { close(swapDone) },
	)
	require.NoError(t, err)
	defer finishUpload(UploadLanded)

	// The dedup is still pending: nothing recorded yet.
	assert.Empty(t, dedupDurationSamples(t, reader))

	require.NoError(t, dedupedFuture.SetValue(dedupedHdr))
	select {
	case <-swapDone:
	case <-time.After(5 * time.Second):
		t.Fatal("swap goroutine did not complete")
	}

	assert.Same(t, dedupedHdr, memfile.Header(), "swap must install the deduped header")

	samples := dedupDurationSamples(t, reader)
	require.Len(t, samples, 1)
	require.EqualValues(t, 1, samples[0].Count)
	assert.GreaterOrEqual(t, samples[0].Sum, int64(50), "duration must cover the provisional's age at swap")
	assert.Less(t, samples[0].Sum, int64(5000))
	assert.Equal(t, 0, samples[0].Attributes.Len(), "dedup duration carries no attributes")
}

// With no provisional in play there is no swap and no sample.
//
//nolint:paralleltest // swaps the package-level dedup-duration histogram
func TestAddSnapshot_NoDedupDurationWithoutProvisional(t *testing.T) {
	reader := swapDedupDurationMetric(t)

	c := newDedupTestCache(t)
	buildID := uuid.NewString()

	memfile := dedupTestDevice{build.NewFile(mustHeader(t, uuid.New()), nil, build.Memfile, nil, blockmetrics.Metrics{})}
	residentTemplate(t, c, buildID, memfile)

	finishUpload, err := c.AddSnapshot(t.Context(), buildID, SnapshotLineage{Origin: storage.ObjectOriginPause},
		resolvedHeader(mustHeader(t, uuid.New())), resolvedHeader(mustHeader(t, uuid.New())),
		nil, nil,
		&build.NoDiff{}, &build.NoDiff{},
		nil, nil,
		time.Time{},
		nil,
	)
	require.NoError(t, err)
	defer finishUpload(UploadLanded)

	assert.Empty(t, dedupDurationSamples(t, reader))
}
