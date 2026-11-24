//go:build linux

package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// TestUpgradeReadsEnvVarsBeforeDelivering: a live upgrade replaces envd with a process that
// starts without the sandbox's environment variables, so the variables are read before the
// binary is delivered, and an upgrade whose variables cannot be read is declined without
// touching the guest.
func TestUpgradeReadsEnvVarsBeforeDelivering(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		envsStatus  int
		configured  map[string]string
		wantGated   map[string]int64
		wantUpgrade bool
	}{
		"unreadable variables decline the upgrade": {
			envsStatus: http.StatusServiceUnavailable,
			wantGated:  map[string]int64{envdUpgradeGateEnvVarsUnavailable: 1},
		},
		"variables differing from the configured ones decline the upgrade": {
			envsStatus: http.StatusOK,
			configured: map[string]string{"USER_VAR": "configured"},
			wantGated:  map[string]int64{envdUpgradeGateEnvVarsConflict: 1},
		},
		"readable variables let delivery proceed": {
			envsStatus:  http.StatusOK,
			wantGated:   map[string]int64{},
			wantUpgrade: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var envsCalls, upgradeCalls atomic.Int32
			envd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/envs":
					envsCalls.Add(1)
					w.WriteHeader(tc.envsStatus)
					_, _ = io.WriteString(w, `{"USER_VAR":"value"}`)
				case "/upgrade":
					// Answering instead of exec'ing ends the attempt at delivery, which is
					// as far as a unit test can follow it.
					upgradeCalls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusInternalServerError)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(envd.Close)

			dir := warmScratchDir(t)
			src := filepath.Join(dir, "envd")
			// Runnable, because with the cache flag off the version comes from a fork+exec.
			require.NoError(t, os.WriteFile(src, []byte("#!/bin/sh\necho 0.9.0\n"), 0o755))
			s, reader := upgradeGateServer(t, src, filepath.Join(dir, "base"), false)

			sbx := upgradeSandbox("0.6.13")
			user := "user"
			sbx.Config.Envd.DefaultUser = &user
			sbx.Config.Envd.Vars = tc.configured
			sbx.SetEnvdServerURLForTest(envd.URL)

			upgraded, err := s.maybeUpgradeEnvd(t.Context(), sbx)
			require.NoError(t, err)
			require.False(t, upgraded)

			require.Equal(t, int32(1), envsCalls.Load())
			require.Equal(t, tc.wantGated, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeGated, "reason"))
			if tc.wantUpgrade {
				require.Equal(t, int32(1), upgradeCalls.Load())
				require.Equal(t, map[string]int64{"delivery_failed": 1},
					counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeAttempts, "result"))
			} else {
				require.Zero(t, upgradeCalls.Load(), "a declined upgrade must not reach the guest")
				require.Empty(t, counterPoints(t, reader, telemetry.OrchestratorEnvdUpgradeAttempts, "result"))
			}
		})
	}
}
