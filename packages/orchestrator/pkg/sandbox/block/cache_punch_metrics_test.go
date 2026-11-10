//go:build linux

package block

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// swapCacheInstruments points the package-level punch and close instruments
// at a fresh reader for one test.
func swapCacheInstruments(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	meter := provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block")

	prevDuration, prevBytes, prevClose := punchDuration, punchBytes, closeDuration
	punchDuration = utils.Must(telemetry.GetFloatHistogram(meter, telemetry.OrchestratorBlockCachePunchDuration))
	punchBytes = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorBlockCachePunchBytes))
	closeDuration = utils.Must(telemetry.GetFloatHistogram(meter, telemetry.OrchestratorBlockCacheCloseDuration))
	t.Cleanup(func() { punchDuration, punchBytes, closeDuration = prevDuration, prevBytes, prevClose })
	// The production callback, so the test reads the gauge the way the exporter does.
	_ = utils.Must(telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorBlockCachePunchInflight, observePunchesInFlight))

	return reader
}

type punchTotals struct {
	count uint64
	bytes int64
}

// punchesBySource keys the collected punches by "source/result".
func punchesBySource(t *testing.T, reader *sdkmetric.ManualReader) map[string]punchTotals {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := make(map[string]punchTotals)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Histogram[float64]:
				if m.Name != string(telemetry.OrchestratorBlockCachePunchDuration) {
					continue
				}
				for _, dp := range data.DataPoints {
					key := attrKey(t, dp.Attributes, "source", "result")
					totals := out[key]
					totals.count += dp.Count
					out[key] = totals
				}
			case metricdata.Sum[int64]:
				if m.Name != string(telemetry.OrchestratorBlockCachePunchBytes) {
					continue
				}
				for _, dp := range data.DataPoints {
					key := attrKey(t, dp.Attributes, "source", "result")
					totals := out[key]
					totals.bytes += dp.Value
					out[key] = totals
				}
			}
		}
	}

	return out
}

// punchMetrics collects the summed punch duration in ms and the in-flight gauge.
func punchMetrics(t *testing.T, reader *sdkmetric.ManualReader) (durationMs float64, inFlight int64) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	foundGauge := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Histogram[float64]:
				if m.Name != string(telemetry.OrchestratorBlockCachePunchDuration) {
					continue
				}
				for _, dp := range data.DataPoints {
					durationMs += dp.Sum
				}
			case metricdata.Sum[int64]:
				if m.Name != string(telemetry.OrchestratorBlockCachePunchInflight) {
					continue
				}
				require.Len(t, data.DataPoints, 1)
				inFlight, foundGauge = data.DataPoints[0].Value, true
			}
		}
	}
	require.True(t, foundGauge, "in-flight gauge not collected")

	return durationMs, inFlight
}

// histogramCounts collects one histogram's datapoint counts keyed by attrKey.
func histogramCounts(t *testing.T, reader *sdkmetric.ManualReader, name telemetry.HistogramType, attrs ...string) map[string]uint64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := make(map[string]uint64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			data, ok := m.Data.(metricdata.Histogram[float64])
			require.Truef(t, ok, "metric %s is not a float64 histogram", m.Name)
			for _, dp := range data.DataPoints {
				out[attrKey(t, dp.Attributes, attrs...)] += dp.Count
			}
		}
	}

	return out
}

// attrKey joins the named attribute values with "/".
func attrKey(t *testing.T, attrs attribute.Set, names ...string) string {
	t.Helper()

	values := make([]string, 0, len(names))
	for _, name := range names {
		value, ok := attrs.Value(attribute.Key(name))
		require.Truef(t, ok, "attribute %s missing", name)
		values = append(values, value.AsString())
	}

	return strings.Join(values, "/")
}

//nolint:paralleltest // swaps the package-level cache instruments
func TestCacheRecordsPunches(t *testing.T) {
	page := int64(os.Getpagesize())

	for name, tc := range map[string]struct {
		blockSize int64
		punch     func(t *testing.T, c *Cache, bs int64) error
		want      map[string]punchTotals
	}{
		"zeroes request": {
			blockSize: page,
			punch: func(_ *testing.T, c *Cache, bs int64) error {
				_, err := c.WriteZeroesAt(2*bs, 4*bs)

				return err
			},
			want: map[string]punchTotals{"zeroes_request/removed": {count: 1, bytes: 4 * page}},
		},
		"grown disk tail": {
			blockSize: page,
			punch: func(_ *testing.T, c *Cache, bs int64) error {
				_, err := c.ZeroGrownTail(8*bs, 8*bs)

				return err
			},
			want: map[string]punchTotals{"grow/removed": {count: 1, bytes: 8 * page}},
		},
		"zero runs in a write": {
			blockSize: page,
			punch: func(_ *testing.T, c *Cache, bs int64) error {
				write := make([]byte, 6*bs)
				copy(write[2*bs:], "data")
				_, err := c.WriteAt(write, 0)

				return err
			},
			want: map[string]punchTotals{"zero_run/removed": {count: 2, bytes: 5 * page}},
		},
		// A fold re-detects the older cache's zero blocks and punches them one block at a time.
		"fold": {
			blockSize: page,
			punch: func(t *testing.T, c *Cache, bs int64) error {
				t.Helper()

				older, err := NewCache(16*bs, bs, t.TempDir()+"/older", false)
				require.NoError(t, err)
				t.Cleanup(func() { _ = older.Close() })
				_, err = older.WriteZeroesAt(2*bs, 3*bs)
				require.NoError(t, err)

				return c.FillMissingFrom(older)
			},
			want: map[string]punchTotals{"zeroes_request/removed": {count: 1, bytes: 3 * page}, "fold/removed": {count: 3, bytes: 3 * page}},
		},
		"memory export": {
			blockSize: page,
			punch: func(_ *testing.T, c *Cache, bs int64) error {
				_, err := c.WriteAtShared(make([]byte, bs), 3*bs)

				return err
			},
			want: map[string]punchTotals{"export/removed": {count: 1, bytes: page}},
		},
		// A 4-byte block makes the range unaligned, so MADV_REMOVE refuses it and the cache zeroes it instead.
		"refused punch": {
			blockSize: 4,
			punch: func(_ *testing.T, c *Cache, bs int64) error {
				_, err := c.WriteZeroesAt(2*bs, 4*bs)

				return err
			},
			want: map[string]punchTotals{"zeroes_request/zeroed": {count: 1, bytes: 16}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			reader := swapCacheInstruments(t)

			cache, err := NewCache(16*tc.blockSize, tc.blockSize, t.TempDir()+"/cache", false)
			require.NoError(t, err)
			t.Cleanup(func() { _ = cache.Close() })

			require.NoError(t, tc.punch(t, cache, tc.blockSize))
			assert.Equal(t, tc.want, punchesBySource(t, reader))
			assert.Zero(t, punchesInFlight.Load())
		})
	}
}

// A zeroing that faults unwinds through punchHole into a RunFaultSafe caller;
// the punch must not stay in flight.
//
//nolint:paralleltest // swaps the package-level punch operations
func TestFaultedPunchLeavesNothingInFlight(t *testing.T) {
	page := int64(os.Getpagesize())

	prev := punchOps
	t.Cleanup(func() { punchOps = prev })
	punchOps.remove = func([]byte) error { return unix.EINVAL }
	punchOps.zero = func([]byte) { panic("fault") }

	cache, err := NewCache(16*page, page, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	assert.PanicsWithValue(t, "fault", func() { _, _ = cache.WriteZeroesAt(0, page) })
	assert.Zero(t, punchesInFlight.Load())
}

// The fake clock moves only inside the punch: 250 ms in MADV_REMOVE and 100 ms
// in the zeroing fallback. So the duration is exact, covers the whole punch and
// nothing else, and the gauge read inside each step shows the punch in flight.
//
//nolint:paralleltest // swaps the package-level cache instruments and operations
func TestPunchTimingAndInFlight(t *testing.T) {
	page := int64(os.Getpagesize())

	for name, tc := range map[string]struct {
		refuse       bool
		wantMs       float64
		wantInFlight []int64
	}{
		"removed": {wantMs: 250, wantInFlight: []int64{1}},
		"zeroed":  {refuse: true, wantMs: 350, wantInFlight: []int64{1, 1}},
	} {
		t.Run(name, func(t *testing.T) {
			reader := swapCacheInstruments(t)

			prev := punchOps
			t.Cleanup(func() { punchOps = prev })
			now := time.Unix(0, 0)
			var inFlight []int64
			step := func(d time.Duration) {
				now = now.Add(d)
				_, gauge := punchMetrics(t, reader)
				inFlight = append(inFlight, gauge)
			}
			punchOps.now = func() time.Time { return now }
			punchOps.remove = func(b []byte) error {
				step(250 * time.Millisecond)
				if tc.refuse {
					return unix.EINVAL
				}

				return prev.remove(b)
			}
			punchOps.zero = func(b []byte) {
				step(100 * time.Millisecond)
				prev.zero(b)
			}

			cache, err := NewCache(16*page, page, t.TempDir()+"/cache", false)
			require.NoError(t, err)
			t.Cleanup(func() { _ = cache.Close() })
			_, err = cache.WriteZeroesAt(2*page, 4*page)
			require.NoError(t, err)

			durationMs, gauge := punchMetrics(t, reader)
			assert.InDelta(t, tc.wantMs, durationMs, 0)
			assert.Equal(t, tc.wantInFlight, inFlight)
			assert.Zero(t, gauge)
		})
	}
}
