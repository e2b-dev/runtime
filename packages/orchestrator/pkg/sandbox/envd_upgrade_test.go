//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCaptureEnvVarsForUpgradeReplaysValuesOnInit(t *testing.T) {
	t.Parallel()

	const (
		accessToken = "test-access-token"
		sentinel    = "creation-time-value"
	)

	type initRequest struct {
		envVars map[string]string `json:"envVars"`
	}
	initRequests := make(chan initRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Access-Token") != accessToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/envs":
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"RESUME_SENTINEL":"`+sentinel+`","E2B_CUSTOM":"user-value","E2B_SANDBOX":"old","E2B_SANDBOX_ID":"old","E2B_TEMPLATE_ID":"old","E2B_EVENTS_ADDRESS":"old"}`)
		case "/init":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var body struct {
				EnvVars map[string]string `json:"envVars"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			initRequests <- initRequest{envVars: body.EnvVars}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	accessTokenValue := accessToken
	sbx := &Sandbox{Metadata: &Metadata{
		internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second, envdServerURLOverride: server.URL},
		Config:         NewConfig(Config{Envd: EnvdMetadata{AccessToken: &accessTokenValue}}),
	}}

	require.NoError(t, sbx.CaptureEnvVarsForUpgrade(t.Context()))
	resp, _, err := sbx.doRequestWithInfiniteRetries(t.Context(), http.MethodPost, server.URL+"/init")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	var request initRequest
	select {
	case request = <-initRequests:
	case <-time.After(5 * time.Second):
		t.Fatal("envd did not receive the /init request")
	}
	assert.Equal(t, sentinel, request.envVars["RESUME_SENTINEL"])
	assert.Equal(t, "user-value", request.envVars["E2B_CUSTOM"])
	assert.NotContains(t, request.envVars, "E2B_SANDBOX")
	assert.NotContains(t, request.envVars, "E2B_SANDBOX_ID")
	assert.NotContains(t, request.envVars, "E2B_TEMPLATE_ID")
	assert.NotContains(t, request.envVars, "E2B_EVENTS_ADDRESS")
}

func TestCaptureEnvVarsForUpgradeLeavesConfigUnchangedOnFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	existing := map[string]string{"EXISTING": "value"}
	sbx := &Sandbox{Metadata: &Metadata{
		internalConfig: internalConfig{envdServerURLOverride: server.URL},
		Config:         NewConfig(Config{Envd: EnvdMetadata{Vars: existing}}),
	}}

	err := sbx.CaptureEnvVarsForUpgrade(t.Context())
	require.Error(t, err)
	assert.Equal(t, existing, sbx.Config.Envd.Vars)
}

// TestIsUpgradeDeliveryFailure guards the distinction CallEnvdUpgrade relies on:
// a request that never reached a running envd (so no upgrade happened) is a
// failure, while the expected post-send connection drop when envd execs
// mid-response is a success. Misclassifying the former as success would record a
// false upgrade in the rollout metrics.
func TestIsUpgradeDeliveryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused -> failure", syscall.ECONNREFUSED, true},
		{"dialing connection refused -> failure", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, true},
		{"deadline exceeded -> not a delivery failure (ambiguous; confirmed by version)", context.DeadlineExceeded, false},
		{"dial timeout -> failure", &net.OpError{Op: "dial", Err: errors.New("i/o timeout")}, true},
		{"post-send reset -> success (envd exec'd)", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, false},
		{"EOF after body -> success (envd exec'd)", io.EOF, false},
		{"generic error -> success (assume exec'd)", errors.New("unexpected"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isUpgradeDeliveryFailure(tt.err))
		})
	}
}
