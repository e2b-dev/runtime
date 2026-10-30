//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestClassifyEnvdInitExit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want envdInitExitType
	}{
		{"nil", nil, envdInitExitSuccess},
		{"deadline_exceeded", context.DeadlineExceeded, envdInitExitTimeout},
		{"wrapped_deadline", fmt.Errorf("init: %w", context.DeadlineExceeded), envdInitExitTimeout},
		{"wait_for_envd_timeout", ErrWaitForEnvdTimeout, envdInitExitTimeout},
		{
			"wrapped_wait_for_envd_timeout",
			// Mirrors doRequestWithInfiniteRetries: ctx.Err() is Canceled, the
			// cause is the timeout sentinel, both wrapped together.
			fmt.Errorf("%w with cause: %w", context.Canceled, ErrWaitForEnvdTimeout),
			envdInitExitTimeout,
		},
		{"canceled", context.Canceled, envdInitExitCanceled},
		{"wrapped_canceled", fmt.Errorf("init: %w", context.Canceled), envdInitExitCanceled},
		{"fc_process_exited", ErrFcProcessExited, envdInitExitOther},
		{
			"wrapped_fc_process_exited",
			// Mirrors doRequestWithInfiniteRetries: ctx.Err() is Canceled, the
			// cause is the fc-exit sentinel, both wrapped together. Must not be
			// misclassified as canceled.
			fmt.Errorf("%w with cause: %w", context.Canceled, ErrFcProcessExited),
			envdInitExitOther,
		},
		{"other", errors.New("connection refused"), envdInitExitOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, classifyEnvdInitExit(tt.err))
		})
	}
}

// mockEgressProxy is a test EgressProxy whose CA bundle is bundle, followed by roots for an
// envd at mockRootsFromEnvdVersion or later.
type mockEgressProxy struct {
	network.NoopEgressProxy

	bundle string
	roots  string
}

// mockRootsFromEnvdVersion splits the test versions 0.10.0 and 0.10.2.
const mockRootsFromEnvdVersion = "0.10.1"

func (m *mockEgressProxy) CABundle(envdVersion string) string {
	if ok, err := utils.IsGTEVersion(envdVersion, mockRootsFromEnvdVersion); err != nil || !ok {
		return m.bundle
	}

	return m.bundle + m.roots
}

// newTestSandboxWithBundle builds a minimal Sandbox whose egress proxy gives every envd bundle.
func newTestSandboxWithBundle(bundle string) *Sandbox {
	return &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second},
			Config:         NewConfig(Config{}),
			Runtime:        sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
		},
		egressProxy: &mockEgressProxy{bundle: bundle},
	}
}

// TestEnvdInitSendsCaBundle verifies the full injection chain:
// EgressProxy.CABundle() → newEnvdInitRequest → POST /init body caBundle field.
//
// Not parallel: overrides the package-level sandboxHttpClient.
func TestEnvdInitSendsCaBundle(t *testing.T) { //nolint:paralleltest
	const pemBundle = "-----BEGIN CERTIFICATE-----\nPROXY\n-----END CERTIFICATE-----\n" +
		"-----BEGIN CERTIFICATE-----\nCUSTOM\n-----END CERTIFICATE-----\n"

	sbx := newTestSandboxWithBundle(pemBundle)

	var captured envd.PostInitJSONBody
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/init", r.URL.Path)

		err := json.NewDecoder(r.Body).Decode(&captured)
		assert.NoError(t, err)

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	orig := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: 5 * time.Second}
	defer func() { sandboxHttpClient = orig }()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	resp, _, err := sbx.doRequestWithInfiniteRetries(ctx, sbx.newEnvdInitRequest(server.URL+"/init", ""))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEmpty(t, captured.CaBundle, "caBundle should be non-empty")
	assert.Contains(t, captured.CaBundle, "-----BEGIN CERTIFICATE-----\nPROXY\n-----END CERTIFICATE-----")
	assert.Contains(t, captured.CaBundle, "-----BEGIN CERTIFICATE-----\nCUSTOM\n-----END CERTIFICATE-----")
}

func TestEnvdInitEmptyCaBundle(t *testing.T) { //nolint:paralleltest
	sbx := newTestSandboxWithBundle("")

	var captured envd.PostInitJSONBody
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&captured)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	orig := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: 5 * time.Second}
	defer func() { sandboxHttpClient = orig }()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	resp, _, err := sbx.doRequestWithInfiniteRetries(ctx, sbx.newEnvdInitRequest(server.URL+"/init", ""))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Empty(t, captured.CaBundle, "caBundle should be omitted when empty")
}
