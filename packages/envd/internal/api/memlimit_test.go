package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/memlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

type fakeMemLimits struct {
	mu       sync.Mutex
	requests []string
	state    memlimit.State
}

func (f *fakeMemLimits) Request(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, reason)
}

func (f *fakeMemLimits) Status() memlimit.State {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.state
}

func (f *fakeMemLimits) Start(context.Context) {}

func (f *fakeMemLimits) requested() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.requests...)
}

func newAPIWithMemLimits(f *fakeMemLimits, tokenHash string) *API {
	api := newTestAPI(nil, &mockMMDSClient{hash: keys.HashAccessToken(tokenHash)})
	api.memLimits = f

	return api
}

func TestPostInit_MemoryLimit(t *testing.T) {
	t.Parallel()

	t.Run("requests a recompute and reports the applied limit on the header", func(t *testing.T) {
		t.Parallel()
		f := &fakeMemLimits{state: memlimit.State{MemTotal: 3 << 30, Limit: 3<<30 - 128<<20}}
		api := newAPIWithMemLimits(f, "")

		rec := postInit(t, api, map[string]any{})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, []string{"init"}, f.requested())
		assert.JSONEq(t, `{"mem_total":3221225472,"applied":3087007744}`, memoryLimitJSON(t, rec))
	})

	t.Run("a recompute still running is marked pending", func(t *testing.T) {
		t.Parallel()
		f := &fakeMemLimits{state: memlimit.State{MemTotal: 1 << 30, Limit: 896 << 20, Pending: true}}
		api := newAPIWithMemLimits(f, "")

		rec := postInit(t, api, map[string]any{})

		assert.JSONEq(t, `{"mem_total":1073741824,"applied":939524096,"pending":true}`, memoryLimitJSON(t, rec))
	})

	t.Run("a stale request still requests a recompute", func(t *testing.T) {
		t.Parallel()
		f := &fakeMemLimits{}
		api := newAPIWithMemLimits(f, "")

		// Near-now timestamps, as in cpus_test.go: a run as root applies the first to
		// the system clock, so a far-future one would break every concurrent test.
		now := time.Now()
		postInit(t, api, map[string]any{"timestamp": now.Format(time.RFC3339Nano)})
		postInit(t, api, map[string]any{"timestamp": now.Add(-time.Minute).Format(time.RFC3339Nano)})

		assert.Equal(t, []string{"init", "init"}, f.requested())
	})

	t.Run("an unauthenticated request does not", func(t *testing.T) {
		t.Parallel()
		f := &fakeMemLimits{}
		api := newAPIWithMemLimits(f, "expected")

		postInit(t, api, map[string]any{"accessToken": "wrong"})

		assert.Empty(t, f.requested())
	})
}

// memoryLimitJSON is the limit object inside a served /init's X-Envd-Memory report.
func memoryLimitJSON(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()

	var report struct {
		Limit json.RawMessage `json:"limit"`
	}
	require.NoError(t, json.Unmarshal([]byte(rec.Header().Get(memoryHeader)), &report))

	return string(report.Limit)
}

func TestReportMemory_TruncatesTheError(t *testing.T) {
	t.Parallel()

	f := &fakeMemLimits{state: memlimit.State{Err: strings.Repeat("x", 4096)}}
	api := newAPIWithMemLimits(f, "")

	rec := postInit(t, api, map[string]any{})

	assert.Less(t, len(rec.Header().Get(memoryHeader)), 1<<10)
}
