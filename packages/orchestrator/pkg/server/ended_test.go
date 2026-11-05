//go:build linux

package server

import (
	"context"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func endedServerSandbox(config *orchestrator.SandboxConfig) *sandbox.Sandbox {
	sbx := &sandbox.Sandbox{
		APIStoredConfig: config,
		Metadata: &sandbox.Metadata{
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID:   listSandboxID,
				TeamID:      listTeamID,
				ExecutionID: listExecutionID,
			},
			Config: sandbox.NewConfig(sandbox.Config{Vcpu: 2, RamMB: 512}),
		},
	}
	sbx.SetStartedAt(startTime)
	sbx.SetEndAt(endTime)

	return sbx
}

// A sandbox that ended unrequested is listed as ended, with nothing kept, on
// every List until the API's kill drops it.
func TestListAndDeleteEndedSandbox(t *testing.T) {
	t.Parallel()

	sandboxes := sandbox.NewSandboxesMap()
	s := &Server{
		sandboxFactory: &sandbox.Factory{Sandboxes: sandboxes},
		info:           &service.ServiceInfo{},
	}
	sandboxes.AddEnded(endedServerSandbox(&orchestrator.SandboxConfig{TemplateId: "template-id"}), fc.GuestEventCrashNoRestart, time.Now())

	want := &orchestrator.RunningSandbox{
		Config:      &orchestrator.SandboxConfig{TemplateId: "template-id"},
		StartTime:   timestamppb.New(startTime),
		EndTime:     timestamppb.New(endTime),
		SandboxId:   listSandboxID,
		TeamId:      listTeamID,
		ExecutionId: listExecutionID,
		Vcpu:        2,
		RamMb:       512,
		Ended:       true,
		GuestEvent:  orchestrator.GuestEvent_GUEST_EVENT_CRASH_NO_RESTART,
	}
	for range 2 {
		got, err := s.List(t.Context(), &emptypb.Empty{})
		require.NoError(t, err)
		require.Len(t, got.GetSandboxes(), 1)
		assert.True(t, proto.Equal(want, got.GetSandboxes()[0]), "got %v", got.GetSandboxes()[0])
	}

	_, err := s.Delete(t.Context(), &orchestrator.SandboxDeleteRequest{SandboxId: listSandboxID})
	assert.Equal(t, codes.NotFound, status.Code(err))

	got, err := s.List(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	assert.Empty(t, got.GetSandboxes())
}

func TestEndsUnrequested(t *testing.T) {
	t.Parallel()

	config := &orchestrator.SandboxConfig{TemplateId: "template-id"}
	tests := []struct {
		name   string
		config *orchestrator.SandboxConfig
		reason sandbox.StopReason
		want   bool
	}{
		{name: "nothing asked for the end", config: config, want: true},
		{name: "killed", config: config, reason: sandbox.StopReasonKilled},
		{name: "paused", config: config, reason: sandbox.StopReasonPaused},
		{name: "checkpointing", config: config, reason: sandbox.StopReasonCheckpointing},
		{name: "registration failed", config: config, reason: sandbox.StopReasonRegistrationFailed},
		{name: "rebooted in place", config: config, reason: sandbox.StopReasonRebooted},
		{name: "a build sandbox, which is not the API's", config: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := endedServerSandbox(tt.config)
			if tt.reason != "" {
				sbx.SetStopReason(tt.reason)
			}

			assert.Equal(t, tt.want, endsUnrequested(sbx))
		})
	}
}

func TestGuestEventProto(t *testing.T) {
	t.Parallel()

	for event, want := range map[fc.GuestEvent]orchestrator.GuestEvent{
		fc.GuestEventPowerOff:         orchestrator.GuestEvent_GUEST_EVENT_POWER_OFF,
		fc.GuestEventRebootOrCrash:    orchestrator.GuestEvent_GUEST_EVENT_REBOOT_OR_CRASH,
		fc.GuestEventCrashWithRestart: orchestrator.GuestEvent_GUEST_EVENT_CRASH_WITH_RESTART,
		fc.GuestEventCrashNoRestart:   orchestrator.GuestEvent_GUEST_EVENT_CRASH_NO_RESTART,
		fc.GuestEventFailure:          orchestrator.GuestEvent_GUEST_EVENT_FAILURE,
		fc.GuestEventUnknown:          orchestrator.GuestEvent_GUEST_EVENT_UNKNOWN,
	} {
		assert.Equal(t, want, guestEventProto(event), event)
	}
}

// A crashed lifecycle is listed as ended only once it had gone live: a
// restore the migration never promoted is not, while the same lifecycle
// registered live is, under the flag that has the API end it.
func TestListsAsEnded_OnlyOnceLive(t *testing.T) {
	t.Parallel()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SandboxEndedOnNodeFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })
	sandboxes := sandbox.NewSandboxesMap()
	s := &Server{sandboxFactory: &sandbox.Factory{Sandboxes: sandboxes}, featureFlags: ff}

	crashed := func(t *testing.T, lifecycleID string) *sandbox.Sandbox {
		t.Helper()
		sbx := drainTestSandbox(t, lifecycleID)
		sbx.APIStoredConfig = &orchestrator.SandboxConfig{TemplateId: "template-id"}
		sbx.SetStopReason(sandbox.StopReasonCrashed)

		return sbx
	}

	restored := crashed(t, "lifecycle-restored")
	sandboxes.TrackLifecycle(t.Context(), restored)
	assert.False(t, s.listsAsEnded(t.Context(), restored), "a restore never promoted is not the API's to end")

	promoted := crashed(t, "lifecycle-promoted")
	require.NoError(t, sandboxes.MarkRunning(t.Context(), promoted))
	require.True(t, sandboxes.MarkStopping(t.Context(), promoted.Runtime.SandboxID, promoted.LifecycleID))
	assert.True(t, s.listsAsEnded(t.Context(), promoted), "one that went live is")
}
