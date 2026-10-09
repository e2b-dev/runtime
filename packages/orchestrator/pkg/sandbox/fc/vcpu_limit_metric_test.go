//go:build linux

package fc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// A nil handle cannot be limited, which drives both paths to their worst outcome without root.
//
//nolint:paralleltest // otel.SetMeterProvider mutates process state
func TestVcpuLimitRecordsOutcome(t *testing.T) {
	reader := sharedMeterReader()
	before := vcpuLimitCounts(t, reader)
	p := &Process{files: &storage.SandboxFiles{SandboxID: "test"}}
	var noCgroup *cgroup.CgroupHandle

	require.NoError(t, p.limitVcpusBeforeResume(t.Context(), noCgroup, 2, 2), "a VM no bigger than the target needs no limit")
	require.Error(t, p.limitVcpusBeforeResume(t.Context(), noCgroup, 2, 16), "no confine and no fallback aborts the resume")
	p.moveLimitToVcpuThreads(t.Context(), noCgroup, 2, 16)

	after := vcpuLimitCounts(t, reader)
	for key := range after {
		after[key] -= before[key]
		if after[key] == 0 {
			delete(after, key)
		}
	}
	assert.Equal(t, map[[2]string]int64{
		{"resume", "failed"}:        1,
		{"boot", "cgroup_fallback"}: 1,
	}, after)
}

var (
	meterReaderOnce sync.Once
	meterReader     *sdkmetric.ManualReader
)

// sharedMeterReader installs one meter provider for the package's metric tests: the global provider
// binds this package's instruments to the first one set, so a second provider would see nothing.
func sharedMeterReader() *sdkmetric.ManualReader {
	meterReaderOnce.Do(func() {
		meterReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(meterReader)))
	})

	return meterReader
}

// vcpuLimitCounts is the SandboxVcpuLimit counter by (path, outcome).
func vcpuLimitCounts(t *testing.T, reader sdkmetric.Reader) map[[2]string]int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	counts := map[[2]string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			if recorded.Name != string(telemetry.SandboxVcpuLimit) {
				continue
			}
			sum, ok := recorded.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				path, _ := point.Attributes.Value(attribute.Key("path"))
				outcome, _ := point.Attributes.Value(attribute.Key("outcome"))
				counts[[2]string{path.Emit(), outcome.Emit()}] += point.Value
			}
		}
	}

	return counts
}
