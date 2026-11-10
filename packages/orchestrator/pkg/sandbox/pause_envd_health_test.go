//go:build linux

package sandbox

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// probeTimeout is what a rollout would set: generous next to the 100ms
// monitoring probe, because here a false refusal costs a customer a pause
// rather than a log line.
const probeTimeout = 500 * time.Millisecond

// envdAt serves envd for one sandbox and counts the probes that reach it:
// every request gets status, and a status of 0 drops the connection instead,
// the shape of a guest whose agent is gone.
func envdAt(t *testing.T, sbx *Sandbox, status int) *atomic.Int32 {
	t.Helper()

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if status == 0 {
			panic(http.ErrAbortHandler)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	sbx.internalConfig.envdServerURLOverride = srv.URL

	return &hits
}

func newHealthProbeSandbox(t *testing.T) *Sandbox {
	t.Helper()

	sbx := &Sandbox{
		Metadata: &Metadata{
			Config:  NewConfig(Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
		},
		Resources: &Resources{Slot: &network.Slot{HostIP: net.IPv4(127, 0, 0, 1)}},
	}
	sbx.SetChecks(NewChecks(sbx))

	return sbx
}

// The flag defaults to -1, which must leave the pause path exactly as it was:
// no probe, no refusal, no decision recorded, and crucially no dial of the
// guest.
func TestAwaitEnvdHealthy_DisabledByDefault(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	hits := envdAt(t, sbx, 0)

	outcome, _, err := sbx.AwaitEnvdAdmission(t.Context(), -1)
	require.NoError(t, err, "a disabled probe must never refuse a pause")
	assert.Equal(t, SnapshotAdmissionOutcome(""), outcome, "a disabled probe decides nothing")
	assert.Zero(t, hits.Load(), "a disabled probe must not dial the guest")
}

// A zero deadline expires before the request is sent, so 0 must disable the
// probe rather than refuse every pause.
func TestAwaitEnvdHealthy_ZeroTimeoutDisables(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	hits := envdAt(t, sbx, http.StatusNoContent)

	outcome, _, err := sbx.AwaitEnvdAdmission(t.Context(), 0)
	require.NoError(t, err, "a zero timeout must never refuse a pause")
	assert.Equal(t, SnapshotAdmissionOutcome(""), outcome)
	assert.Zero(t, hits.Load(), "a zero timeout must not dial the guest")
}

// The case this change exists for: envd does not answer, so the pause is
// refused BEFORE any destructive step rather than producing a snapshot that
// records a wedged agent and can never be resumed.
func TestAwaitEnvdHealthy_UnresponsiveEnvdRefusesRetryably(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	hits := envdAt(t, sbx, 0)

	outcome, _, err := sbx.AwaitEnvdAdmission(t.Context(), probeTimeout)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSnapshotAdmissionEnvdUnhealthy)
	assert.Equal(t, SnapshotAdmissionEnvdUnhealthy, outcome)
	assert.Equal(t, int32(1), hits.Load())
}

// A healthy envd must be admitted. envd answers /health with 204; anything else
// is a failure, so this also pins that a 200 is NOT mistaken for healthy.
func TestAwaitEnvdHealthy_HealthyEnvdAdmits(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	envdAt(t, sbx, http.StatusNoContent)

	outcome, _, err := sbx.AwaitEnvdAdmission(t.Context(), probeTimeout)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionEnvdHealthy, outcome)
}

// The sandbox HTTP client aborts at its own Timeout whatever the context says,
// so a longer flag value is capped to it rather than silently promised.
func TestCapEnvdProbeTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10*time.Second, capEnvdProbeTimeout(time.Hour, 10*time.Second))
	assert.Equal(t, probeTimeout, capEnvdProbeTimeout(probeTimeout, 10*time.Second))
	assert.Equal(t, time.Hour, capEnvdProbeTimeout(time.Hour, 0), "a client without a Timeout imposes no cap")
}

func TestAwaitEnvdHealthy_UnexpectedStatusRefuses(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	envdAt(t, sbx, http.StatusInternalServerError)

	outcome, _, err := sbx.AwaitEnvdAdmission(t.Context(), probeTimeout)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSnapshotAdmissionEnvdUnhealthy)
	assert.Equal(t, SnapshotAdmissionEnvdUnhealthy, outcome)
}

// A sandbox torn down concurrently can leave the checker nil. Missing evidence
// is not evidence of a wedged envd, so the pause proceeds undecided.
func TestAwaitEnvdHealthy_NilChecksAdmits(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	envdAt(t, sbx, 0)
	sbx.SetChecks(nil)

	outcome, _, err := sbx.AwaitEnvdAdmission(t.Context(), probeTimeout)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionOutcome(""), outcome)
}

// A context that ends mid-probe says nothing about envd. It must surface as the
// caller's context error with an EMPTY outcome, which the handler maps to
// "nothing was decided, the sandbox is untouched" — never as a refusal.
func TestAwaitEnvdHealthy_ContextCancelledIsNotARefusal(t *testing.T) {
	t.Parallel()

	sbx := newHealthProbeSandbox(t)
	envdAt(t, sbx, http.StatusNoContent)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	outcome, _, err := sbx.AwaitEnvdAdmission(ctx, probeTimeout)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSnapshotAdmissionEnvdUnhealthy,
		"a cancelled context must not be reported as an unhealthy envd")
	assert.Equal(t, SnapshotAdmissionOutcome(""), outcome,
		"an empty outcome is what tells the handler nothing was decided")
}

// The refusal must be its own sentinel and must NOT satisfy the pending one:
// pending and envd-unhealthy both return a retryable ResourceExhausted, while
// anything falling through to the latched path KILLS the sandbox.
func TestEnvdUnhealthySentinelIsDistinctAndNotLatched(t *testing.T) {
	t.Parallel()

	require.NotErrorIs(t, ErrSnapshotAdmissionEnvdUnhealthy, ErrSnapshotAdmissionPending)
	require.NotErrorIs(t, ErrSnapshotAdmissionPending, ErrSnapshotAdmissionEnvdUnhealthy)

	// Outcome labels are metric dimensions; keep them distinct and stable.
	assert.NotEqual(t, SnapshotAdmissionRefused, SnapshotAdmissionEnvdUnhealthy)
	assert.NotEqual(t, SnapshotAdmissionLatchedError, SnapshotAdmissionEnvdUnhealthy)
	assert.Equal(t, SnapshotAdmissionEnvdUnhealthy, SnapshotAdmissionOutcome("envd_unhealthy"))
	assert.Equal(t, SnapshotAdmissionEnvdHealthy, SnapshotAdmissionOutcome("envd_healthy"))
}
