//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type capacityOnlyTemplate struct {
	meta metadata.Template
}

func (t capacityOnlyTemplate) Metadata() (metadata.Template, error) { return t.meta, nil }
func (capacityOnlyTemplate) Files() storage.CachePaths              { panic("resource initialization") }

func (capacityOnlyTemplate) Memfile(context.Context) (block.ReadonlyDevice, error) {
	panic("resource initialization")
}

func (capacityOnlyTemplate) Rootfs() (block.ReadonlyDevice, error) { panic("resource initialization") }

func (capacityOnlyTemplate) Snapfile() (sbxtemplate.File, error)    { panic("resource initialization") }
func (capacityOnlyTemplate) UpdateMetadata(metadata.Template) error { return nil }
func (capacityOnlyTemplate) Close(context.Context) error            { return nil }

func vcpuSandbox(vcpu, maxVcpus int64) *Sandbox {
	return &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second},
			Config:         NewConfig(Config{Vcpu: vcpu, ConfiguredVmVcpus: maxVcpus}),
		},
		egressProxy: network.NewNoopEgressProxy(),
	}
}

func TestMetricsCarryCpuHotplug(t *testing.T) {
	t.Parallel()

	var m Metrics
	require.NoError(t, json.Unmarshal([]byte(`{"cpu_count":2,"cpu_possible":16,"cpu_target":2,"cpu_target_attempts":1,"cpu_write_pending_ms":0}`), &m))
	assert.Equal(t, Metrics{CPUCount: 2, CPUPossible: 16, CPUTarget: 2, CPUTargetAttempts: 1}, m)

	var old Metrics
	require.NoError(t, json.Unmarshal([]byte(`{"cpu_count":2}`), &old))
	assert.Zero(t, old.CPUPossible, "an envd without hotplug reports neither cpu_possible nor cpu_target, which gate the gauges")
	assert.Zero(t, old.CPUTarget)
}

func TestRebootMaxVcpus(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(16), rebootMaxVcpus(0, 16), "a request without max_vcpus keeps the lineage's VM size")
	assert.Equal(t, int64(16), rebootMaxVcpus(16, 16))
	assert.Equal(t, int64(2), rebootMaxVcpus(2, 16), "an explicit maximum may shrink a cold-booted VM")
	assert.Equal(t, int64(32), rebootMaxVcpus(32, 16), "a request may grow it")
	assert.Equal(t, int64(0), rebootMaxVcpus(0, 0), "an unrecorded lineage boots as requested")
}

func TestResumeVmVcpus(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		vcpu     int64
		maxVcpus int64
		recorded int64
		want     int64
		wantErr  bool
	}{
		"recorded capacity":                  {vcpu: 2, recorded: 16, want: 16},
		"recorded capacity is authoritative": {vcpu: 2, maxVcpus: 32, recorded: 16, want: 16},
		"legacy stored capacity":             {vcpu: 2, maxVcpus: 16, want: 16},
		"legacy unchanged request":           {vcpu: 2, want: 2},
		"impossible upsize while resizing":   {vcpu: 8, maxVcpus: 8, recorded: 4, wantErr: true},
		"mismatch without resizing resumes":  {vcpu: 8, recorded: 4, want: 4},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := resumeVmVcpus(tc.vcpu, tc.maxVcpus, tc.recorded)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrVcpuExceedsSnapshot)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResumeRejectsVcpuBeforeResourceInitialization(t *testing.T) {
	t.Parallel()

	_, err := (&Factory{}).ResumeSandbox(
		t.Context(),
		capacityOnlyTemplate{meta: metadata.Template{VcpuCount: 4}},
		NewConfig(Config{Vcpu: 8, ConfiguredVmVcpus: 8}),
		sandboxtypes.RuntimeMetadata{},
		time.Time{},
		time.Time{},
		nil,
	)
	require.ErrorIs(t, err, ErrVcpuExceedsSnapshot)
}

func TestVerifyEnvdCpuCount(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		header      string
		vmVcpus     int64
		wantOutcome envdCpuReportOutcome
		wantErr     bool
	}{
		"taken":                     {`{"online":2,"possible":16,"target":2}`, 16, envdCpuReportedOK, false},
		"rejected":                  {`{"online":2,"possible":2,"target":2,"rejected":"cpu target 4 outside 1..2"}`, 16, envdCpuRejected, true},
		"target mismatch":           {`{"online":2,"possible":16,"target":4}`, 16, envdCpuTargetMismatch, true},
		"possible below VM size":    {`{"online":2,"possible":2,"target":2}`, 16, envdCpuPossibleLow, true},
		"possible unavailable":      {`{"online":2,"possible":0,"target":2}`, 16, envdCpuPossibleUnknown, true},
		"malformed":                 {`{`, 16, envdCpuUnreadable, true},
		"oversized":                 {`{"online":2,"possible":16,"target":2,"rejected":"` + strings.Repeat("x", 600) + `"}`, 16, envdCpuUnreadable, true},
		"old envd, spare vcpus":     {"", 16, envdCpuNoHeader, true},
		"old envd, vm of vcpu size": {"", 2, envdCpuNoHeader, false},
		"old envd, unknown size":    {"", 0, envdCpuNoHeader, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			outcome, err := verifyEnvdCpuCount(tc.header, 2, tc.vmVcpus)
			assert.Equal(t, tc.wantOutcome, outcome)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Not parallel: overrides the package-level HTTP client and reads its metric provider
// after every subtest has run.
//
//nolint:paralleltest // see above
func TestInitEnvdCountsCpuReports(t *testing.T) {
	originalClient := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: 5 * time.Second}
	t.Cleanup(func() { sandboxHttpClient = originalClient })

	for _, tc := range []struct {
		name        string
		header      string
		status      int
		vmVcpus     int64
		wantFail    bool
		wantAnomaly bool
		first       bool
		reinit      bool
	}{
		{name: "first init", header: `{"online":2,"possible":8,"target":2}`, status: http.StatusNoContent, vmVcpus: 8, first: true},
		{name: "reinit", header: `{"online":2,"possible":8,"target":2}`, status: http.StatusNoContent, vmVcpus: 8, reinit: true},
		{name: "reported capable", header: `{"online":2,"possible":8,"target":2}`, status: http.StatusNoContent, vmVcpus: 8},
		{name: "missing header", status: http.StatusNoContent, vmVcpus: 8, wantAnomaly: true},
		{name: "too few possible", header: `{"online":2,"possible":2,"target":2}`, status: http.StatusNoContent, vmVcpus: 8, wantAnomaly: true},
		{name: "failed init is not counted", header: `{"online":2,"possible":8,"target":4}`, status: http.StatusServiceUnavailable, vmVcpus: 8, wantFail: true},
		{name: "failed init without header is not an anomaly", status: http.StatusServiceUnavailable, vmVcpus: 8, wantFail: true},
		{name: "no spare CPUs is not counted", status: http.StatusNoContent, vmVcpus: 2},
	} {
		t.Run(tc.name, func(t *testing.T) { //nolint:paralleltest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("X-Envd-Cpus", tc.header)
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()

			s, _ := newMemoryTestSandbox(t, server.URL)
			s.Config.Vcpu = 2
			s.resolvedVmVcpus = tc.vmVcpus
			// Isolate this test's cumulative metric points from other sandbox tests.
			s.Runtime.SandboxType = sandboxtypes.SandboxType("cpu-report-test")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			opts := envdInitOptions{}
			if tc.reinit {
				opts.reinit = newEnvdReinit(func(error) {})
			}
			err := s.initEnvd(ctx, StartTypeCreate, tc.first, opts)
			if tc.wantFail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			anomalies := testLogObserver.FilterMessage("envd CPU report anomaly").
				FilterField(zap.String("sandbox.id", s.Runtime.SandboxID)).All()
			if tc.wantAnomaly {
				assert.Len(t, anomalies, 1)
			} else {
				assert.Empty(t, anomalies)
			}
		})
	}

	var metrics metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &metrics))
	counts := map[string]int64{}
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != string(telemetry.EnvdCpuReport) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				if !hasAttr(point.Attributes, "sandbox_type", "cpu-report-test") || !hasAttr(point.Attributes, "start_type", string(StartTypeCreate)) {
					continue
				}
				outcome, ok := point.Attributes.Value("outcome")
				require.True(t, ok)
				init, ok := point.Attributes.Value("init")
				require.True(t, ok)
				counts[init.AsString()+"/"+outcome.AsString()] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"first/reported_ok": 1, "reinit/reported_ok": 1, "repeat/reported_ok": 1, "repeat/no_header": 1, "repeat/possible_too_low": 1}, counts)
}

// Not parallel: overrides the package-level sandboxHttpClient.
func TestEnvdInitSendsCpuCount(t *testing.T) { //nolint:paralleltest
	orig := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: 5 * time.Second}
	defer func() { sandboxHttpClient = orig }()

	// sentCpuCount is the cpuCount in the /init body, decoded raw so an absent field is not mistaken for 0.
	sentCpuCount := func(t *testing.T, s *Sandbox) any {
		t.Helper()

		var captured map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		resp, _, err := s.doRequestWithInfiniteRetries(ctx, s.newEnvdInitRequest(server.URL+"/init", ""))
		require.NoError(t, err)
		resp.Body.Close()

		if n, ok := captured["cpuCount"].(float64); ok {
			return int(n)
		}

		return captured["cpuCount"]
	}

	assert.Equal(t, 2, sentCpuCount(t, vcpuSandbox(2, 0)), "sent without a VM size too: the snapshot may have another count online")
	assert.Equal(t, 2, sentCpuCount(t, vcpuSandbox(2, 4)))
	assert.Equal(t, 4, sentCpuCount(t, vcpuSandbox(4, 4)), "the full size is sent too, to bring offlined CPUs back")
	smaller := vcpuSandbox(20, 0)
	smaller.resolvedVmVcpus = 16
	assert.Equal(t, 16, sentCpuCount(t, smaller), "a resume allowed onto a smaller VM asks envd for what it has")
}
