//go:build linux

package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// envdProbeCheckpointServer is the admission metrics server with the start
// gate and the checkpoint counter a Checkpoint that passes the probe needs;
// the envd probe is enabled with envdMs, or left at its default when nil.
func envdProbeCheckpointServer(t *testing.T, envdMs *int) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	s, reader := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, envdMs)
	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	s.startingSandboxes = utils.Must(utils.NewAdjustableSemaphore(1))
	s.startGateWaitDuration = utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxStartGateWaitDurationName))
	s.sandboxCheckpointCounter = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorSandboxCheckpointCounterName))

	return s, reader
}

// awaitCheckpointCrossedMarkStopping waits for a checkpoint that was
// admitted: it has left the live map and set the checkpointing stop reason,
// then parks forever in the fake template's Metadata.
func awaitCheckpointCrossedMarkStopping(t *testing.T, s *Server, sbx *sandbox.Sandbox, msg string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)

		return !live
	}, 2*time.Second, time.Millisecond, msg)
	require.Eventually(t, func() bool {
		return sbx.GetStopReason() == sandbox.StopReasonCheckpointing
	}, 2*time.Second, time.Millisecond)
}

// An unanswered probe refuses the checkpoint retryably before any route runs,
// with the detail that lets the API name the cause; the decision is counted
// as envd_unhealthy under rpc=checkpoint. No claim was taken, so a second
// checkpoint gets the same answer, never "in progress".
func TestCheckpoint_EnvdUnresponsiveRefusesBeforeAnyRoute(t *testing.T) {
	t.Parallel()

	s, reader := envdProbeCheckpointServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-ckpt", 71)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt"})
	requireEnvdUnresponsive(t, ckptErr)

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-ckpt")
	assert.True(t, live, "a refused checkpoint must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason may be set by a refusal")
	assert.Nil(t, s.detachedCheckpointFor("sbx-envd-ckpt"), "a refused checkpoint holds no claim")
	assert.True(t, s.startingSandboxes.TryAcquire(1), "a refused checkpoint releases its start slot")
	s.startingSandboxes.Release(1)

	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	attrs := attrsAsMap(t, points[0].Attributes)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrs["outcome"])
	assert.Equal(t, "checkpoint", attrs["rpc"])
	waits := admissionWaitPoints(t, reader)
	require.Len(t, waits, 1)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrsAsMap(t, waits[0].Attributes)["outcome"])

	_, againErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt"})
	requireEnvdUnresponsive(t, againErr)
	assert.Nil(t, checkpointInProgressDetail(t, againErr), "nothing was claimed by the refusal, so the retry is not told in progress")
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-envd-ckpt", "lifecycle-1"),
		"a refused checkpoint must leave the sandbox unmarked")
}

// An envd that answers admits the checkpoint: it proceeds past the probe into
// today's resume-fresh order and the decision is counted as envd_healthy.
func TestCheckpoint_EnvdHealthyAdmits(t *testing.T) {
	t.Parallel()

	s, reader := envdProbeCheckpointServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-ckpt-healthy", 72)
	registerEnvd(t, sbx, &envdAnswer{status: http.StatusNoContent})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	go func() {
		_, _ = s.Checkpoint(context.WithoutCancel(t.Context()), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt-healthy"})
	}()

	awaitCheckpointCrossedMarkStopping(t, s, sbx, "a checkpoint whose envd answers must proceed to MarkStopping")
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	attrs := attrsAsMap(t, points[0].Attributes)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdHealthy), attrs["outcome"])
	assert.Equal(t, "checkpoint", attrs["rpc"])
}

// A filesystem-only checkpoint is probed like a memory one: its capture asks
// the guest to freeze the rootfs, which a hung agent never does. With envd
// unreachable it is refused retryably before its own flag is consulted and
// before any route runs, the sandbox stays live, and the decision is counted
// under rpc=checkpoint.
func TestCheckpoint_FilesystemOnlyIsProbed(t *testing.T) {
	t.Parallel()

	s, reader := envdProbeCheckpointServer(t, new(envdProbeMs))
	s.featureFlags = envdProbeFlagClientWith(t, new(envdProbeMs), true)
	sbx := envdProbeSandbox(t, "sbx-envd-ckpt-fs-only", 73)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt-fs-only", FilesystemOnly: true})
	requireEnvdUnresponsive(t, ckptErr)

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-ckpt-fs-only")
	assert.True(t, live, "a refused checkpoint must leave the sandbox live")
	assert.Nil(t, s.detachedCheckpointFor("sbx-envd-ckpt-fs-only"), "a refused checkpoint holds no claim")
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	attrs := attrsAsMap(t, points[0].Attributes)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthy), attrs["outcome"])
	assert.Equal(t, "checkpoint", attrs["rpc"])
}

// Flag absent, negative or zero means no probe: with envd unreachable the
// memory checkpoint proceeds straight into today's order and records no
// decision.
func TestCheckpoint_EnvdProbeFlagOffRunsTodaysOrder(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		flag *int
		slot int
	}{
		"absent":   {flag: nil, slot: 74},
		"negative": {flag: new(-1), slot: 75},
		"zero":     {flag: new(0), slot: 76},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, reader := envdProbeCheckpointServer(t, c.flag)
			sandboxID := "sbx-envd-ckpt-off-" + name
			sbx := envdProbeSandbox(t, sandboxID, c.slot)
			answer := &envdAnswer{unreachable: true, begun: make(chan struct{})}
			registerEnvd(t, sbx, answer)
			s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

			go func() {
				_, _ = s.Checkpoint(context.WithoutCancel(t.Context()), &orchestrator.SandboxCheckpointRequest{SandboxId: sandboxID})
			}()

			awaitCheckpointCrossedMarkStopping(t, s, sbx, "flag off must reach MarkStopping without probing")
			select {
			case <-answer.begun:
				t.Fatal("no probe runs with the flag off")
			default:
			}
			assert.Empty(t, admissionCounterPoints(t, reader), "no probe, no decision")
		})
	}
}

// A probe miss while an in-place checkpoint has the guest frozen is answered
// as the operation in progress, the permanent answer a concurrent checkpoint
// gets, never as a hung guest: the in-flight marker, not the live map, tells
// the two apart since an in-place checkpoint keeps the sandbox listed.
func TestCheckpoint_EnvdProbeMissDuringAnInPlaceCheckpointAnswersInProgress(t *testing.T) {
	t.Parallel()

	s, reader := envdProbeCheckpointServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-ckpt-inplace", 83)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)
	require.True(t, sbx.BeginCheckpoint(), "the test's in-place checkpoint holds the marker")
	defer sbx.EndCheckpoint()

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt-inplace"})
	require.Error(t, ckptErr)
	assert.Equal(t, codes.FailedPrecondition, status.Code(ckptErr), "the concurrent checkpoint's own answer")
	assert.NotNil(t, checkpointInProgressDetail(t, ckptErr))
	assert.Nil(t, envdUnresponsiveDetail(t, ckptErr), "never the guest-agent refusal")

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-ckpt-inplace")
	assert.True(t, live, "the sandbox is untouched")
	require.Len(t, admissionCounterPoints(t, reader), 1, "the probe decision is still counted")
}

// The probe runs after the start gate, so its answer is current when the
// snapshot begins: a checkpoint held at a saturated gate is not probed at all,
// and a guest that hangs while it waits is caught by the probe that follows.
func TestCheckpoint_EnvdProbeRunsAfterTheStartGate(t *testing.T) {
	t.Parallel()

	s, reader := envdProbeCheckpointServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-ckpt-gate", 77)
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	require.True(t, s.startingSandboxes.TryAcquire(1), "the test holds the node's only start slot")
	defer s.startingSandboxes.Release(1)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, ckptErr := s.Checkpoint(ctx, &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt-gate"})
	require.Error(t, ckptErr)
	assert.Nil(t, envdUnresponsiveDetail(t, ckptErr), "a checkpoint that never got a slot was not refused by the probe")
	assert.Empty(t, admissionCounterPoints(t, reader), "no probe ran while the gate held the checkpoint")

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-ckpt-gate")
	assert.True(t, live, "the sandbox is untouched")
}

// With filesystem-only checkpoints disabled the refusal is the flag's,
// permanent and deterministic, and comes before the probe: a hung guest does
// not turn it into a retryable guest-agent refusal, the guest is not asked and
// no decision is recorded.
func TestCheckpoint_FilesystemOnlyDisabledIsRefusedBeforeTheProbe(t *testing.T) {
	t.Parallel()

	s, reader := envdProbeCheckpointServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-envd-ckpt-fs-only-off", 81)
	answer := &envdAnswer{unreachable: true, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-envd-ckpt-fs-only-off", FilesystemOnly: true})
	require.Error(t, ckptErr)
	assert.Equal(t, codes.FailedPrecondition, status.Code(ckptErr), "the flag's own refusal, not the guest's")
	assert.Nil(t, envdUnresponsiveDetail(t, ckptErr))
	select {
	case <-answer.begun:
		t.Fatal("the guest must not be probed for a checkpoint the flag refuses")
	default:
	}
	assert.Empty(t, admissionCounterPoints(t, reader), "no decision is recorded")
	_, live := s.sandboxFactory.Sandboxes.Get("sbx-envd-ckpt-fs-only-off")
	assert.True(t, live)
}
