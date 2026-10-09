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

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
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
		header  string
		vmVcpus int64
		wantErr bool
	}{
		"taken":                     {`{"online":2,"possible":16,"target":2}`, 16, false},
		"rejected":                  {`{"online":2,"possible":2,"target":2,"rejected":"cpu target 4 outside 1..2"}`, 2, true},
		"other":                     {`{"online":2,"possible":16,"target":4}`, 16, true},
		"malformed":                 {`{`, 16, true},
		"oversized":                 {`{"online":2,"possible":16,"target":2,"rejected":"` + strings.Repeat("x", 600) + `"}`, 16, true},
		"old envd, spare vcpus":     {"", 16, true},
		"old envd, vm of vcpu size": {"", 2, false},
		"old envd, unknown size":    {"", 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := verifyEnvdCpuCount(tc.header, 2, tc.vmVcpus)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
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
		resp, _, err := s.doRequestWithInfiniteRetries(ctx, server.URL+"/init")
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
