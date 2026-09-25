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

// mockEgressProxy is a test EgressProxy that returns a fixed CA bundle string.
type mockEgressProxy struct {
	network.NoopEgressProxy

	bundle string
}

func (m *mockEgressProxy) CABundle() string { return m.bundle }

// newTestSandboxWithBundle builds a minimal Sandbox with CABundle set —
// mirroring what Factory.CreateSandbox does with f.egressProxy.CABundle().
func newTestSandboxWithBundle(bundle string) *Sandbox {
	return &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second},
			Config:         NewConfig(Config{}),
			Runtime:        sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
		},
		CABundle: bundle,
	}
}

// TestEnvdInitSendsCaBundle verifies the full injection chain:
// EgressProxy.CABundle() → Sandbox.CABundle → POST /init body caBundle field.
//
// Not parallel: overrides the package-level sandboxHttpClient.
func TestEnvdInitSendsCaBundle(t *testing.T) { //nolint:paralleltest
	const pemBundle = "-----BEGIN CERTIFICATE-----\nPROXY\n-----END CERTIFICATE-----\n" +
		"-----BEGIN CERTIFICATE-----\nCUSTOM\n-----END CERTIFICATE-----\n"

	proxy := &mockEgressProxy{bundle: pemBundle}
	sbx := newTestSandboxWithBundle(proxy.CABundle())

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

	resp, _, err := sbx.doRequestWithInfiniteRetries(ctx, http.MethodPost, server.URL+"/init")
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

	resp, _, err := sbx.doRequestWithInfiniteRetries(ctx, http.MethodPost, server.URL+"/init")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Empty(t, captured.CaBundle, "caBundle should be omitted when empty")
}

func TestConvertMounts(t *testing.T) {
	t.Parallel()

	const orchIP = "192.0.2.1"
	sbx := &Sandbox{
		Metadata: &Metadata{
			Config: NewConfig(Config{}),
		},
	}
	sbx.config.NetworkConfig.OrchestratorInSandboxIPAddress = orchIP

	t.Run("sync true is passed through", func(t *testing.T) {
		t.Parallel()
		mounts := []VolumeMountConfig{
			{Name: "vol-a", Path: "/data", Sync: true},
		}
		result := sbx.convertMounts(mounts)
		require.Len(t, result, 1)
		assert.Equal(t, orchIP+":/vol-a", result[0].NfsTarget)
		assert.Equal(t, "/data", result[0].Path)
		require.NotNil(t, result[0].Sync)
		assert.True(t, *result[0].Sync)
	})

	t.Run("sync false is passed through", func(t *testing.T) {
		t.Parallel()
		mounts := []VolumeMountConfig{
			{Name: "vol-b", Path: "/cache", Sync: false},
		}
		result := sbx.convertMounts(mounts)
		require.Len(t, result, 1)
		require.NotNil(t, result[0].Sync)
		assert.False(t, *result[0].Sync)
	})

	t.Run("multiple mounts have independent sync pointers", func(t *testing.T) {
		t.Parallel()
		mounts := []VolumeMountConfig{
			{Name: "vol-1", Path: "/a", Sync: true},
			{Name: "vol-2", Path: "/b", Sync: false},
			{Name: "vol-3", Path: "/c", Sync: true},
		}
		result := sbx.convertMounts(mounts)
		require.Len(t, result, 3)

		// Each pointer must be independent (not aliased)
		assert.True(t, *result[0].Sync)
		assert.False(t, *result[1].Sync)
		assert.True(t, *result[2].Sync)

		// Mutating one must not affect others
		*result[0].Sync = false
		assert.False(t, *result[0].Sync)
		assert.False(t, *result[1].Sync) // was already false
		assert.True(t, *result[2].Sync)  // must remain true
	})

	t.Run("empty mounts returns empty slice", func(t *testing.T) {
		t.Parallel()
		result := sbx.convertMounts(nil)
		assert.Empty(t, result)
	})
}
