//go:build linux

package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	internalevents "github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// These tests exercise the rollback path of Update: when egress application
// fails (no real network namespace), end_time and egress must not be changed.

func TestUpdate_EgressOnly_FailsAndDoesNotChangeEndTime(t *testing.T) {
	t.Parallel()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	sbx := &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id.Generate()},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sbx.SetStartedAt(time.Now())
	sbx.SetEndAt(time.Now().Add(time.Hour))
	originalEnd := sbx.GetEndAt()

	sandboxMap := sandbox.NewSandboxesMap()
	sandboxMap.AssignNetwork(t.Context(), sbx)
	sandboxMap.MarkRunning(t.Context(), sbx)

	s := &Server{
		sandboxFactory:   &sandbox.Factory{Sandboxes: sandboxMap},
		info:             &service.ServiceInfo{},
		sbxEventsService: internalevents.NewEventsService(nil),
	}

	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
		SandboxId: sbx.Runtime.SandboxID,
		Egress: &orchestrator.SandboxNetworkEgressConfig{
			DeniedCidrs: []string{"0.0.0.0/0"},
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Equal(t, originalEnd, sbx.GetEndAt())
}

func TestUpdate_EndTimeAndEgress_EgressFails_RevertsEndTime(t *testing.T) {
	t.Parallel()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	sbx := &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id.Generate()},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sbx.SetStartedAt(time.Now())
	sbx.SetEndAt(time.Now().Add(time.Hour))
	originalEnd := sbx.GetEndAt()

	sandboxMap := sandbox.NewSandboxesMap()
	sandboxMap.AssignNetwork(t.Context(), sbx)
	sandboxMap.MarkRunning(t.Context(), sbx)

	s := &Server{
		sandboxFactory:   &sandbox.Factory{Sandboxes: sandboxMap},
		info:             &service.ServiceInfo{},
		sbxEventsService: internalevents.NewEventsService(nil),
	}

	newEnd := time.Now().Add(5 * time.Hour)
	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
		SandboxId: sbx.Runtime.SandboxID,
		EndTime:   timestamppb.New(newEnd),
		Egress: &orchestrator.SandboxNetworkEgressConfig{
			DeniedCidrs: []string{"0.0.0.0/0"},
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	// end_time must be reverted to original since egress failed.
	assert.Equal(t, originalEnd, sbx.GetEndAt())
	// Network egress should not have been set.
	egress := sbx.Config.GetNetworkEgress()
	assert.Empty(t, egress.GetAllowedCidrs())
	assert.Empty(t, egress.GetDeniedCidrs())
	assert.Empty(t, egress.GetAllowedDomains())
}

// TestUpdate_NonBYOPEgress_FirewallFails_ConfigUntouched exercises the
// tighten-first (disable / no-BYOP) branch: UpdateInternet runs FIRST, so when
// the firewall apply fails (no real netns here) the in-memory egress config is
// never published. This is the kernel-before-userspace order that keeps the
// kernel from being relaxed ahead of userspace on the tighten direction.
func TestUpdate_NonBYOPEgress_FirewallFails_ConfigUntouched(t *testing.T) {
	t.Parallel()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	sbx := &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id.Generate()},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sbx.SetStartedAt(time.Now())
	sbx.SetEndAt(time.Now().Add(time.Hour))

	sandboxMap := sandbox.NewSandboxesMap()
	sandboxMap.AssignNetwork(t.Context(), sbx)
	sandboxMap.MarkRunning(t.Context(), sbx)

	s := &Server{
		sandboxFactory:   &sandbox.Factory{Sandboxes: sandboxMap},
		info:             &service.ServiceInfo{},
		sbxEventsService: internalevents.NewEventsService(nil),
	}

	// Sanity: no egress configured to start.
	require.Nil(t, sbx.Config.GetNetworkEgress())

	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
		SandboxId: sbx.Runtime.SandboxID,
		Egress: &orchestrator.SandboxNetworkEgressConfig{
			AllowedCidrs: []string{"1.2.3.4/32"},
		},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	// Firewall apply ran first and failed, so config must never have been set.
	assert.Nil(t, sbx.Config.GetNetworkEgress())
}

// TestTransitionEgress verifies the loosen-last / tighten-first ordering: the
// fake updateInternet records the config the proxy sees at the moment the
// kernel firewall would be mutated.
func TestTransitionEgress(t *testing.T) {
	t.Parallel()

	byop := &orchestrator.SandboxNetworkEgressConfig{EgressProxyAddress: "socks5://proxy.example:1080"}
	plain := &orchestrator.SandboxNetworkEgressConfig{AllowedCidrs: []string{"1.2.3.4/32"}}
	empty := &orchestrator.SandboxNetworkEgressConfig{}

	errFirewall := errors.New("firewall apply failed")

	tests := []struct {
		name        string
		from, to    *orchestrator.SandboxNetworkEgressConfig
		firewallErr error
		wantSeen    *orchestrator.SandboxNetworkEgressConfig // config published when the firewall is updated
		wantFinal   *orchestrator.SandboxNetworkEgressConfig // config published after the transition
		wantErr     bool
	}{
		{
			name: "enable BYOP publishes config before relaxing firewall",
			from: nil, to: byop,
			wantSeen: byop, wantFinal: byop,
		},
		{
			name: "enable BYOP failure unpublishes config",
			from: nil, to: byop, firewallErr: errFirewall,
			wantSeen: byop, wantFinal: nil, wantErr: true,
		},
		{
			name: "tighten updates firewall before publishing config",
			from: byop, to: plain,
			wantSeen: byop, wantFinal: plain,
		},
		{
			name: "tighten failure leaves config untouched",
			from: byop, to: plain, firewallErr: errFirewall,
			wantSeen: byop, wantFinal: byop, wantErr: true,
		},
		{
			name: "all-empty egress collapses to nil",
			from: plain, to: empty,
			wantSeen: plain, wantFinal: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := sandbox.NewConfig(sandbox.Config{})
			cfg.SetNetworkEgress(tt.from)

			var seen *orchestrator.SandboxNetworkEgressConfig
			updateInternet := func(context.Context, *orchestrator.SandboxNetworkEgressConfig) error {
				seen = cfg.GetNetworkEgress()

				return tt.firewallErr
			}

			err := transitionEgress(t.Context(), cfg, updateInternet, tt.from, tt.to)

			if tt.wantErr {
				require.ErrorIs(t, err, errFirewall)
			} else {
				require.NoError(t, err)
			}
			assertSameEgress(t, tt.wantSeen, seen)
			assertSameEgress(t, tt.wantFinal, cfg.GetNetworkEgress())
		})
	}
}

// assertSameEgress asserts pointer identity, tolerating nil.
func assertSameEgress(t *testing.T, want, got *orchestrator.SandboxNetworkEgressConfig) {
	t.Helper()

	if want == nil {
		assert.Nil(t, got)
	} else {
		assert.Same(t, want, got)
	}
}

func TestUpdate_SerializesUpdatesPerSandbox(t *testing.T) {
	t.Parallel()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	sbx := &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id.Generate()},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sbx.SetStartedAt(time.Now())
	sbx.SetEndAt(time.Now().Add(time.Hour))
	originalEnd := sbx.GetEndAt()

	sandboxMap := sandbox.NewSandboxesMap()
	sandboxMap.AssignNetwork(t.Context(), sbx)
	sandboxMap.MarkRunning(t.Context(), sbx)

	s := &Server{
		sandboxFactory:   &sandbox.Factory{Sandboxes: sandboxMap},
		info:             &service.ServiceInfo{},
		sbxEventsService: internalevents.NewEventsService(nil),
	}

	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- sbx.RunUpdate(func() error {
			close(updateStarted)
			<-releaseUpdate

			return nil
		})
	}()
	<-updateStarted

	newEnd := time.Now().Add(5 * time.Hour)
	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
			SandboxId: sbx.Runtime.SandboxID,
			EndTime:   timestamppb.New(newEnd),
		})
		updateDone <- updateErr
	}()

	var earlyErr error
	completedEarly := false
	select {
	case earlyErr = <-updateDone:
		completedEarly = true
	case <-time.After(100 * time.Millisecond):
	}

	assert.Equal(t, originalEnd, sbx.GetEndAt())
	close(releaseUpdate)
	require.NoError(t, <-holderDone)
	if completedEarly {
		require.Failf(t, "update completed before lock release", "error: %v", earlyErr)
	}

	require.NoError(t, <-updateDone)
	assert.True(t, newEnd.Equal(sbx.GetEndAt()))
}
