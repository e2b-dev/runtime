//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// A memory pause of a guest without a filesystem quiesce is probed like any
// other and, when the caller allows it, proceeds past a miss: the capture is
// a memory one, reported as taken over an unresponsive agent and not as
// disk-only, and counted as envd_unhealthy_proceeded.
func TestPause_UnquiescedGuestProceedsPastAMissWhenAllowed(t *testing.T) {
	t.Parallel()

	s, reader := envdProbePauseServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-unquiesced-envd-proceed", 94)
	sbx.Config.FirecrackerConfig.Guest = unquiescedGuest{guest.Default()}
	registerEnvd(t, sbx, &envdAnswer{unreachable: true})
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	steps := &fakePauseSteps{}
	resp, err := s.pauseWith(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID, BuildId: "build-1", AllowEnvdUnresponsive: true}, steps)
	require.NoError(t, err)
	assert.True(t, resp.GetEnvdUnresponsive(), "the response reports the miss the API persists")
	assert.False(t, resp.GetFilesystemOnly(), "a memory capture, not the disk-only one of a powered-off guest")
	assert.True(t, steps.envdUnresponsive.Load(), "the snapshot is stamped")
	assert.False(t, steps.filesystemOnly.Load())
	points := admissionCounterPoints(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, string(sandbox.SnapshotAdmissionEnvdUnhealthyProceeded), attrsAsMap(t, points[0].Attributes)["outcome"])
}

// The unsupported refusal of a filesystem-only pause comes before the probe,
// whatever the request allows: the guest is never asked, no admission
// decision is counted, and the answer is the permanent FailedPrecondition
// with its detail, not the retryable guest-agent refusal.
func TestPause_UnquiescedGuestFilesystemOnlyIsRefusedBeforeTheProbe(t *testing.T) {
	t.Parallel()

	s, reader := envdProbePauseServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-unquiesced-envd-fs", 95)
	sbx.Config.FirecrackerConfig.Guest = unquiescedGuest{guest.Default()}
	answer := &envdAnswer{unreachable: true, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	for _, allow := range []bool{false, true} {
		steps := &fakePauseSteps{}
		_, err := s.pauseWith(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID, FilesystemOnly: true, AllowEnvdUnresponsive: allow}, steps)
		st, ok := status.FromError(err)
		require.True(t, ok, "allow %t", allow)
		assert.Equal(t, codes.FailedPrecondition, st.Code(), "allow %t", allow)
		info := pauseRefusalInfo(st)
		require.NotNil(t, info, "allow %t: the API restores the record on this detail", allow)
		assert.Equal(t, sandboxtypes.PauseRefusedUnsupportedReason, info.GetReason())
		assert.Zero(t, steps.snapshots.Load(), "allow %t: nothing was captured", allow)
	}
	select {
	case <-answer.begun:
		t.Fatal("the guest must not be probed for a capture it cannot take")
	default:
	}
	assert.Empty(t, admissionCounterPoints(t, reader), "no admission decision was made")
	_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	assert.True(t, live, "the refused sandbox stays live")
}

// A guest that powered off before the pause has no agent to ask: the pause
// is not probed, whatever it allows, and captures the disk alone. The
// response says disk-only and reports no miss, since there is no memory for
// a mark to describe.
func TestPause_PoweredOffGuestIsCapturedDiskOnlyWithoutTheProbe(t *testing.T) {
	t.Parallel()

	s, reader := envdProbePauseServer(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-poweroff-envd-allowed", 96)
	answer := &envdAnswer{unreachable: true, begun: make(chan struct{})}
	registerEnvd(t, sbx, answer)
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))
	sbx.MarkExitedForTest(nil)
	sbx.HoldPoweredOff()

	steps := &fakePauseSteps{}
	resp, err := s.pauseWith(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID, BuildId: "build-1", AllowEnvdUnresponsive: true}, steps)
	require.NoError(t, err)
	select {
	case <-answer.begun:
		t.Fatal("a powered-off guest is not probed")
	default:
	}
	assert.True(t, resp.GetFilesystemOnly(), "the capture is the held disk")
	assert.False(t, resp.GetEnvdUnresponsive(), "no memory, no mark")
	assert.True(t, steps.filesystemOnly.Load())
	assert.False(t, steps.envdUnresponsive.Load())
	assert.Empty(t, admissionCounterPoints(t, reader), "no admission decision was made")
}
