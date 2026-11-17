//go:build linux

package fc

import (
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/testutils"
)

// newBalloonAPIStub serves a minimal FC balloon API on a unix socket:
// PATCH /balloon/reporting/{pause,resume} answer 204 — the status FC really
// returns (VmmData::Empty), regardless of what the vendored spec declares —
// GET /balloon serves the config, and GET /balloon/reporting/status the
// pause state. pauseStatus lets tests flip pause into an error.
func newBalloonAPIStub(t *testing.T, reporting bool, paused *bool, pauseStatus int) *apiClient {
	t.Helper()

	dir := testutils.SocketDir(t)
	socket := dir + "/fc.sock"
	ln, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /balloon/reporting/pause", func(w http.ResponseWriter, _ *http.Request) {
		if pauseStatus != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(pauseStatus)
			_, _ = w.Write([]byte(`{"fault_message":"free page reporting was not enabled"}`))

			return
		}
		*paused = true
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("PATCH /balloon/reporting/resume", func(w http.ResponseWriter, _ *http.Request) {
		*paused = false
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /balloon/reporting/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"paused": *paused})
	})
	mux.HandleFunc("GET /balloon", func(w http.ResponseWriter, _ *http.Request) {
		if !reporting {
			// FC answers 400 when no balloon device is installed.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"fault_message":"no balloon device"}`))

			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"amount_mib": 0, "deflate_on_oom": true, "stats_polling_interval_s": 0,
			"free_page_reporting": true, "free_page_hinting": false,
		})
	})

	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	return newApiClient(socket)
}

// TestBalloonReportingClient pins the go-swagger plumbing the deferred memory
// export's preflight rests on. The load-bearing case is 204-on-success: FC
// returns 204 (VmmData::Empty) where the vendored spec declares 200, so
// go-swagger surfaces success as an error the client must classify back to
// nil — get that wrong and pause/resume "fail" on every real success, and the
// feature silently no-ops into the synchronous copy while flagged on.
func TestBalloonReportingClient(t *testing.T) {
	t.Parallel()

	t.Run("pause and resume succeed on 204", func(t *testing.T) {
		t.Parallel()
		paused := false
		c := newBalloonAPIStub(t, true, &paused, 0)

		require.NoError(t, c.pauseFreePageReporting(t.Context()))
		assert.True(t, paused, "the stub must have observed the pause")
		got, err := c.freePageReportingPaused(t.Context())
		require.NoError(t, err)
		assert.True(t, got, "status must confirm the pause")

		require.NoError(t, c.resumeFreePageReporting(t.Context()))
		assert.False(t, paused)
		got, err = c.freePageReportingPaused(t.Context())
		require.NoError(t, err)
		assert.False(t, got)
	})

	t.Run("pause failure is an error", func(t *testing.T) {
		t.Parallel()
		paused := false
		c := newBalloonAPIStub(t, true, &paused, http.StatusBadRequest)
		require.Error(t, c.pauseFreePageReporting(t.Context()))
		assert.False(t, paused)
	})

	t.Run("balloon config reports reporting", func(t *testing.T) {
		t.Parallel()
		paused := false
		c := newBalloonAPIStub(t, true, &paused, 0)
		reporting, err := c.balloonFreePageReporting(t.Context())
		require.NoError(t, err)
		assert.True(t, reporting)
	})

	t.Run("no balloon device means no reporting, not an error", func(t *testing.T) {
		t.Parallel()
		paused := false
		c := newBalloonAPIStub(t, false, &paused, 0)
		reporting, err := c.balloonFreePageReporting(t.Context())
		require.NoError(t, err)
		assert.False(t, reporting)
	})
}
