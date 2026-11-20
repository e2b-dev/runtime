//go:build linux

package sandbox

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// envVarsSandbox is a sandbox resumed from a memory snapshot, so its config carries no
// environment variables, with envd served at url.
func envVarsSandbox(url string, accessToken *string) *Sandbox {
	return &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second, envdServerURLOverride: url},
			Config:         NewConfig(Config{Envd: EnvdMetadata{AccessToken: accessToken}}),
			Runtime:        sandboxtypes.RuntimeMetadata{SandboxID: "sbx"},
		},
		egressProxy: network.NewNoopEgressProxy(),
	}
}

// initBodyEnvVars is the envVars field of the /init body s would send now.
func initBodyEnvVars(t *testing.T, s *Sandbox) map[string]string {
	t.Helper()

	raw, err := s.newEnvdInitRequest("http://envd/init", "").marshal(time.Now())
	require.NoError(t, err)

	var body envd.PostInitJSONBody
	require.NoError(t, json.Unmarshal(raw, &body))

	return body.EnvVars
}

// TestCaptureEnvdEnvVarsSendsThemOnInit: the variables a running envd holds reach the next
// /init, which is the one a live upgrade sends its replacement, without the variables envd
// sets for itself.
func TestCaptureEnvdEnvVarsSendsThemOnInit(t *testing.T) {
	t.Parallel()

	token := "token"
	served, err := json.Marshal(map[string]string{
		"USER_VAR":           "value",
		"EMPTY_VAR":          "",
		"E2B_SANDBOX":        "true",
		"E2B_SANDBOX_ID":     "sbx",
		"E2B_TEMPLATE_ID":    "tpl",
		"E2B_EVENTS_ADDRESS": "http://events",
	})
	require.NoError(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/envs" || r.Header.Get("X-Access-Token") != token {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(served)
	}))
	t.Cleanup(srv.Close)

	s := envVarsSandbox(srv.URL, &token)
	require.Nil(t, initBodyEnvVars(t, s), "a memory resume sends no variables until they are read")

	n, err := s.CaptureEnvdEnvVars(t.Context(), 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, map[string]string{"USER_VAR": "value", "EMPTY_VAR": ""}, initBodyEnvVars(t, s))
}

// TestCaptureEnvdEnvVarsKeepsNothingOnFailure: a read that fails keeps nothing, so the caller
// can decline the upgrade and leave the running envd alone.
func TestCaptureEnvdEnvVarsKeepsNothingOnFailure(t *testing.T) {
	t.Parallel()

	for name, respond := range map[string]func(w http.ResponseWriter){
		"error status": func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) },
		"null body":    func(w http.ResponseWriter) { _, _ = io.WriteString(w, "null") },
		"malformed":    func(w http.ResponseWriter) { _, _ = io.WriteString(w, `{"USER_VAR":`) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { respond(w) }))
			t.Cleanup(srv.Close)

			s := envVarsSandbox(srv.URL, nil)

			_, err := s.CaptureEnvdEnvVars(t.Context(), 5*time.Second)
			require.Error(t, err)
			assert.Nil(t, initBodyEnvVars(t, s))
		})
	}
}

// TestCaptureEnvdEnvVarsChecksTheConfiguredSet: a start configured with variables sends its
// own, which is only safe while they match the running envd's. Which set a live upgrade should
// keep when they differ is undecided, so a difference is an error the caller declines on.
func TestCaptureEnvdEnvVarsChecksTheConfiguredSet(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		configured map[string]string
		served     map[string]string
		wantErr    error
	}{
		"matching": {served: map[string]string{"USER_VAR": "configured", "E2B_SANDBOX_ID": "sbx"}},
		"configured with a name envd sets itself": {
			configured: map[string]string{"USER_VAR": "configured", "E2B_SANDBOX_ID": "user-set"},
			served:     map[string]string{"USER_VAR": "configured", "E2B_SANDBOX_ID": "sbx"},
		},
		"different value": {served: map[string]string{"USER_VAR": "other"}, wantErr: ErrEnvdEnvVarsConflict},
		"extra variable": {
			served:  map[string]string{"USER_VAR": "configured", "EMPTY_VAR": ""},
			wantErr: ErrEnvdEnvVarsConflict,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			served, err := json.Marshal(tc.served)
			require.NoError(t, err)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(served)
			}))
			t.Cleanup(srv.Close)

			configured := tc.configured
			if configured == nil {
				configured = map[string]string{"USER_VAR": "configured"}
			}
			s := envVarsSandbox(srv.URL, nil)
			s.Config.Envd.Vars = configured

			n, err := s.CaptureEnvdEnvVars(t.Context(), 5*time.Second)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, 1, n)
			}
			assert.Equal(t, configured, initBodyEnvVars(t, s))
		})
	}
}
