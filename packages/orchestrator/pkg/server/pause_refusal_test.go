//go:build linux

package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// unquiescedGuest is a profile whose guest offers no filesystem quiesce.
type unquiescedGuest struct{ guest.Profile }

func (unquiescedGuest) Name() string  { return "unquiesced" }
func (unquiescedGuest) Label() string { return "unquiesced" }

func (unquiescedGuest) Capabilities() guest.Capabilities {
	return guest.Capabilities{NoFilesystemOnlyPause: true, NoQuiesce: true}
}

// unquiescedTestSandbox is the admission test sandbox of an unquiesced guest.
func unquiescedTestSandbox(t *testing.T, sandboxID string, slotIdx int) *sandbox.Sandbox {
	t.Helper()

	sbx := admissionTestSandbox(t, sandboxID, slotIdx, utils.NewSetOnce[*header.Header]())
	sbx.Config.FirecrackerConfig.Guest = unquiescedGuest{guest.Default()}

	return sbx
}

// boundedCall bounds a call into the admission test sandbox, whose template
// parks Metadata forever: a pause that crosses MarkStopping never returns, so
// a timeout reads as "the refusal came too late".
func boundedCall(t *testing.T, call func() error) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the call did not return: the refusal did not come before the destructive path")

		return nil
	}
}

func pauseRefusalInfo(st *status.Status) *errdetails.ErrorInfo {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == sandboxtypes.PauseRefusalDomain {
			return info
		}
	}

	return nil
}

// A filesystem-only pause of a guest without the capability is refused
// before MarkStopping: the sandbox stays live, keeps its stop reason, and can
// still be marked by a later legitimate stop.
func TestPause_UnsupportedGuestRefusesFilesystemOnlyBeforeMarkStopping(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, nil)
	sbx := unquiescedTestSandbox(t, "sbx-unquiesced-fs-pause", 21)
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	pauseErr := boundedCall(t, func() error {
		_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-unquiesced-fs-pause", FilesystemOnly: true})

		return err
	})
	require.Error(t, pauseErr)
	st, ok := status.FromError(pauseErr)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "unquiesced guest")
	info := pauseRefusalInfo(st)
	require.NotNil(t, info, "the API restores the record on this detail")
	assert.Equal(t, sandboxtypes.PauseRefusedUnsupportedReason, info.GetReason())
	assert.Equal(t, map[string]string{"guest_os": "unquiesced", "snapshot_kind": "filesystem_only"}, info.GetMetadata())

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-unquiesced-fs-pause")
	assert.True(t, live, "a refused pause must leave the sandbox live")
	assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason may be set by a refusal")
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-unquiesced-fs-pause", "lifecycle-1"),
		"a refused pause must leave the sandbox unmarked")
}

// The same refusal on a filesystem-only checkpoint, before admission and the
// start slot.
func TestCheckpoint_UnsupportedGuestRefusesFilesystemOnly(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, nil)
	sbx := unquiescedTestSandbox(t, "sbx-unquiesced-fs-checkpoint", 22)
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	cpErr := boundedCall(t, func() error {
		_, err := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-unquiesced-fs-checkpoint", FilesystemOnly: true})

		return err
	})
	require.Error(t, cpErr)
	st, ok := status.FromError(cpErr)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	require.NotNil(t, pauseRefusalInfo(st))
	assert.Equal(t, sandboxtypes.PauseRefusedUnsupportedReason, pauseRefusalInfo(st).GetReason())

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-unquiesced-fs-checkpoint")
	assert.True(t, live)
	assert.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sbx-unquiesced-fs-checkpoint", "lifecycle-1"))
}

// The preflight refuses only a filesystem-only capture of a guest without
// the capability: a memory pause passes, and the default guest is never refused.
func TestRefuseUnsupportedFilesystemOnly(t *testing.T) {
	t.Parallel()

	unquiesced := unquiescedTestSandbox(t, "sbx-unquiesced-memory-pause", 23)
	require.NoError(t, refuseUnsupportedFilesystemOnly(unquiesced, false))
	require.Error(t, refuseUnsupportedFilesystemOnly(unquiesced, true))

	linux := admissionTestSandbox(t, "sbx-default-fs-pause", 24, utils.NewSetOnce[*header.Header]())
	require.NoError(t, refuseUnsupportedFilesystemOnly(linux, true))
	require.NoError(t, refuseUnsupportedFilesystemOnly(linux, false))
}

// A sandbox whose timeout auto-pause is to persist only the filesystem is
// refused at create for a guest without the capability, a resume included,
// with the pause refusal's detail and the create-refused detail for the
// client's fault: the sweep could never take that capture, and a memory one
// in its place would keep what the policy drops. A memory policy, a resume
// that turns auto-pause off while its snapshot keeps the kind, and the
// default guest, pass.
func TestCreate_RefusesAFilesystemOnlyAutoPauseTheGuestCannotTake(t *testing.T) {
	t.Parallel()

	profile := unquiescedGuest{guest.Default()}
	for name, config := range map[string]*orchestrator.SandboxConfig{
		"create": {SandboxId: "sbx-unquiesced-create", AutoPause: true, AutoPauseFilesystemOnly: true},
		"resume": {SandboxId: "sbx-unquiesced-resume", AutoPause: true, AutoPauseFilesystemOnly: true, Snapshot: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := autoPauseRefusal(t.Context(), profile, config)
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, st.Code())
			assert.Contains(t, st.Message(), "unquiesced guest")
			assert.Contains(t, st.Message(), config.GetSandboxId())
			info := pauseRefusalInfo(st)
			require.NotNil(t, info, "the same detail as the pause path, so the API reads the same refusal")
			assert.Equal(t, sandboxtypes.PauseRefusedUnsupportedReason, info.GetReason())
			assert.Equal(t, map[string]string{"guest_os": "unquiesced", "snapshot_kind": "filesystem_only"}, info.GetMetadata())
			refusal := createRefusalDetail(t, st)
			assert.Equal(t, st.Message(), refusal.GetMessage())
			assert.EqualValues(t, http.StatusBadRequest, refusal.GetHttpStatus(), "the policy is a field of the request")
		})
	}

	require.NoError(t, autoPauseRefusal(t.Context(), profile, &orchestrator.SandboxConfig{AutoPause: true}), "a memory auto-pause is not refused")
	require.NoError(t, autoPauseRefusal(t.Context(), profile, &orchestrator.SandboxConfig{AutoPauseFilesystemOnly: true, Snapshot: true}), "a resume with auto-pause off is not refused for the kind its snapshot kept")
	require.NoError(t, autoPauseRefusal(t.Context(), guest.Default(), &orchestrator.SandboxConfig{AutoPause: true, AutoPauseFilesystemOnly: true}), "the default guest takes the capture")
	require.NoError(t, autoPauseRefusal(t.Context(), profile, nil), "no config, no refusal")
}
