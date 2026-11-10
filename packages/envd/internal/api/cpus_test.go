package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cpus"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

type fakeCPUManager struct {
	mu      sync.Mutex
	targets []int
	err     error
	state   cpus.State
}

func (f *fakeCPUManager) SetTarget(n int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.targets = append(f.targets, n)
	f.state.Target = n

	return nil
}

func (f *fakeCPUManager) Status() cpus.State {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.state
}

func (f *fakeCPUManager) Start(context.Context) {}

func (f *fakeCPUManager) recorded() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int(nil), f.targets...)
}

// newAPIWithCPUs is an /init-ready API: MMDS holds the hash of the empty token, so a
// body without an access token authenticates, and the fake manager stands in for sysfs.
func newAPIWithCPUs(f *fakeCPUManager) *API {
	api := newTestAPI(nil, &mockMMDSClient{hash: keys.HashAccessToken("")})
	api.cpuManager = f

	return api
}

func postInit(t *testing.T, api *API, body any) *httptest.ResponseRecorder {
	t.Helper()

	b, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", bytes.NewReader(b))
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	api.PostInit(rec, req)

	return rec
}

func cpuHeader(t *testing.T, rec *httptest.ResponseRecorder) cpuReport {
	t.Helper()

	var report cpuReport
	require.NoError(t, json.Unmarshal([]byte(rec.Header().Get(cpusHeader)), &report))

	return report
}

func TestPostInit_CpuCount(t *testing.T) {
	t.Parallel()

	t.Run("records the target and reports it on the header", func(t *testing.T) {
		t.Parallel()
		f := &fakeCPUManager{state: cpus.State{Online: 4, Possible: 16}}
		api := newAPIWithCPUs(f)

		rec := postInit(t, api, map[string]any{"cpuCount": 8})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, []int{8}, f.recorded())
		assert.JSONEq(t, `{"online":4,"possible":16,"target":8}`, rec.Header().Get(cpusHeader))
	})

	t.Run("without cpuCount the target is untouched but the state is still reported", func(t *testing.T) {
		t.Parallel()
		f := &fakeCPUManager{state: cpus.State{Online: 4, Possible: 16}}
		api := newAPIWithCPUs(f)

		rec := postInit(t, api, map[string]any{})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, f.recorded())
		assert.JSONEq(t, `{"online":4,"possible":16,"target":0}`, rec.Header().Get(cpusHeader))
	})

	t.Run("a rejected count does not fail the init and is named on the header", func(t *testing.T) {
		t.Parallel()
		f := &fakeCPUManager{err: errors.New("cpu target 32 outside 1..16"), state: cpus.State{Online: 4, Possible: 16}}
		api := newAPIWithCPUs(f)

		rec := postInit(t, api, map[string]any{"cpuCount": 32})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, f.recorded())
		assert.Equal(t, "cpu target 32 outside 1..16", cpuHeader(t, rec).Rejected)

		// The rejection belongs to that response only.
		rec = postInit(t, api, map[string]any{})
		assert.Empty(t, cpuHeader(t, rec).Rejected)
	})

	t.Run("a stale request does not move the target", func(t *testing.T) {
		t.Parallel()
		f := &fakeCPUManager{state: cpus.State{Online: 4, Possible: 16}}
		api := newAPIWithCPUs(f)
		now := time.Now()

		rec := postInit(t, api, map[string]any{"cpuCount": 8, "timestamp": now.Format(time.RFC3339Nano)})
		require.Equal(t, http.StatusNoContent, rec.Code)

		rec = postInit(t, api, map[string]any{"cpuCount": 2, "timestamp": now.Add(-time.Minute).Format(time.RFC3339Nano)})
		require.Equal(t, http.StatusNoContent, rec.Code)

		assert.Equal(t, []int{8}, f.recorded())
	})
}

// Retry bookkeeping describes attempts the worker has not made yet at /init time, so the
// header carries only the counts.
func TestPostInit_CPUHeaderCarriesOnlyCounts(t *testing.T) {
	t.Parallel()

	api := newAPIWithCPUs(&fakeCPUManager{state: cpus.State{Online: 6, Possible: 16, Target: 8, Attempts: 3}})

	rec := postInit(t, api, map[string]any{})

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.JSONEq(t, `{"online":6,"possible":16,"target":8}`, rec.Header().Get(cpusHeader))
}

func TestGetMetricsCarriesCPUTarget(t *testing.T) {
	t.Parallel()

	api := newAPIWithCPUs(&fakeCPUManager{state: cpus.State{Online: 8, Possible: 16, Target: 8, Attempts: 2, WritePending: 1500 * time.Millisecond}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody)
	require.NoError(t, err)
	rec := httptest.NewRecorder()

	api.GetMetrics(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.InDelta(t, 16, got["cpu_possible"], 0)
	assert.InDelta(t, 8, got["cpu_target"], 0)
	assert.InDelta(t, 2, got["cpu_target_attempts"], 0)
	assert.InDelta(t, 1500, got["cpu_write_pending_ms"], 0)
	assert.Contains(t, got, "cpu_count")
}
