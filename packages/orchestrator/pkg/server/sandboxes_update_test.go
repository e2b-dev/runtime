//go:build linux

package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	internalevents "github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
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

// TestUpdate_EgressProxyFlagGate checks the egress proxy kill switch: an
// unresolved flag lets the request through to the build check, which the
// no-op proxy answers with Unimplemented, and only an explicit false refuses.
func TestUpdate_EgressProxyFlagGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		flag     *bool
		wantCode codes.Code
	}{
		{name: "flag unresolved falls back to enabled", flag: nil, wantCode: codes.Unimplemented},
		{name: "flag on", flag: new(true), wantCode: codes.Unimplemented},
		{name: "flag off refuses the team", flag: new(false), wantCode: codes.PermissionDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			if tt.flag != nil {
				td.Update(td.Flag(featureflags.BYOPProxyEnabledFlag.Key()).ValueForAll(ldvalue.Bool(*tt.flag)))
			}
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
			require.NoError(t, err)

			sbx := &sandbox.Sandbox{
				Metadata: &sandbox.Metadata{
					Config:  sandbox.NewConfig(sandbox.Config{}),
					Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id.Generate(), TeamID: id.Generate()},
				},
				Resources: &sandbox.Resources{Slot: slot},
			}

			sandboxMap := sandbox.NewSandboxesMap()
			sandboxMap.AssignNetwork(t.Context(), sbx)
			sandboxMap.MarkRunning(t.Context(), sbx)

			s := &Server{
				sandboxFactory: sandbox.NewFactory(
					t.Context(), cfg.BuilderConfig{},
					nil, nil, ff, nil, nil, network.NewNoopEgressProxy(), nil, sandboxMap,
				),
				featureFlags:     ff,
				info:             &service.ServiceInfo{},
				sbxEventsService: internalevents.NewEventsService(nil),
			}

			_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
				SandboxId: sbx.Runtime.SandboxID,
				Egress: &orchestrator.SandboxNetworkEgressConfig{
					EgressProxyAddress: "203.0.113.5:1080",
				},
			})

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.Nil(t, sbx.Config.GetNetworkEgress())
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

// ReadCommitted counts accepted updates only, and reads the state an update
// in progress commits.
func TestUpdate_ReadCommittedCountsAcceptedUpdates(t *testing.T) {
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
	sbx.SetEndAt(time.Now().Add(time.Hour))
	sandboxMap := sandbox.NewSandboxesMap()
	sandboxMap.MarkRunning(t.Context(), sbx)
	s := &Server{
		sandboxFactory:   &sandbox.Factory{Sandboxes: sandboxMap},
		info:             &service.ServiceInfo{},
		sbxEventsService: internalevents.NewEventsService(nil),
	}
	committed := func() (uint64, time.Time) {
		var updates uint64
		var end time.Time
		sbx.ReadCommitted(func(committedUpdates uint64) { updates, end = committedUpdates, sbx.GetEndAt() })

		return updates, end
	}

	accepted := time.Now().Add(2 * time.Hour)
	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{SandboxId: sbx.Runtime.SandboxID, EndTime: timestamppb.New(accepted)})
	require.NoError(t, err)
	updates, end := committed()
	assert.Equal(t, uint64(1), updates)
	assert.True(t, accepted.Equal(end))

	// The egress step fails without a network namespace and rolls the end
	// time back.
	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
		SandboxId: sbx.Runtime.SandboxID,
		EndTime:   timestamppb.New(time.Now().Add(3 * time.Hour)),
		Egress:    &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"0.0.0.0/0"}},
	})
	require.Error(t, err)
	updates, end = committed()
	assert.Equal(t, uint64(1), updates)
	assert.True(t, accepted.Equal(end))

	// A read that starts inside an update sees the state it commits.
	read := make(chan uint64, 1)
	require.NoError(t, sbx.RunUpdate(func() error {
		go func() { updates, _ := committed(); read <- updates }()

		return nil
	}))
	assert.Equal(t, uint64(2), <-read)
}

func TestMapSandboxUpdateError(t *testing.T) {
	t.Parallel()

	closedErr := mapSandboxUpdateError(sandbox.ErrSandboxUpdateClosed)
	require.Error(t, closedErr)
	assert.Equal(t, codes.NotFound, status.Code(closedErr))
	assert.Equal(t, "sandbox is closing or closed", status.Convert(closedErr).Message())

	assert.Equal(t, codes.FailedPrecondition, status.Code(mapSandboxUpdateError(sandbox.ErrEgressProxyUnsupported)), "a placed network refuses an egress proxy")

	unrelatedPrecondition := status.Error(codes.FailedPrecondition, "callback precondition")
	assert.Same(t, unrelatedPrecondition, mapSandboxUpdateError(unrelatedPrecondition))

	callbackErr := status.Error(codes.ResourceExhausted, "callback verdict")
	mappedCallbackErr := mapSandboxUpdateError(callbackErr)
	assert.Equal(t, callbackErr, mappedCallbackErr)
	assert.Equal(t, codes.ResourceExhausted, status.Code(mappedCallbackErr))
	assert.Equal(t, "callback verdict", status.Convert(mappedCallbackErr).Message())
}

func TestUpdate_ClosedSandboxReturnsNotFoundWithoutApplying(t *testing.T) {
	t.Parallel()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{}) //nolint:exhaustruct_v5 // only slot identity is used
	require.NoError(t, err)

	sandboxID := id.Generate()
	sbx := sandbox.NewBalloonTestSandbox(sandboxID, "", nil)
	sbx.Resources = &sandbox.Resources{Slot: slot}
	sbx.SetStartedAt(time.Now())
	originalEnd := time.Now().Add(time.Hour)
	sbx.SetEndAt(originalEnd)

	sandboxMap := sandbox.NewSandboxesMap()
	sandboxMap.AssignNetwork(t.Context(), sbx)
	require.NoError(t, sandboxMap.MarkRunning(t.Context(), sbx))

	s := &Server{ //nolint:exhaustruct_v5 // focused Update fixture
		sandboxFactory:   &sandbox.Factory{Sandboxes: sandboxMap},
		info:             &service.ServiceInfo{}, //nolint:exhaustruct_v5 // TrackWork needs only zero-value bookkeeping
		sbxEventsService: internalevents.NewEventsService(nil),
	}

	require.NoError(t, sbx.Close(t.Context()))
	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{ //nolint:exhaustruct_v5 // egress is intentionally absent
		SandboxId: sandboxID,
		EndTime:   timestamppb.New(time.Now().Add(5 * time.Hour)),
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, "sandbox is closing or closed", status.Convert(err).Message())
	assert.True(t, originalEnd.Equal(sbx.GetEndAt()), "closed update callback changed end time")
}

func TestUpdate_StoppingSandboxReturnsNotFoundWithoutApplying(t *testing.T) {
	t.Parallel()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)
	sbx := sandbox.NewBalloonTestSandbox(id.Generate(), "", nil)
	sbx.Resources = &sandbox.Resources{Slot: slot}
	sbx.LifecycleID = "update-rpc-owner"
	originalEnd := time.Now().Add(time.Hour)
	sbx.SetEndAt(originalEnd)
	m := sandbox.NewSandboxesMap()
	require.NoError(t, m.MarkRunning(t.Context(), sbx))
	require.True(t, m.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))
	s := &Server{
		sandboxFactory:   &sandbox.Factory{Sandboxes: m},
		info:             &service.ServiceInfo{},
		sbxEventsService: internalevents.NewEventsService(nil),
	}
	_, err = s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{
		SandboxId: sbx.Runtime.SandboxID,
		EndTime:   timestamppb.New(time.Now().Add(5 * time.Hour)),
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.True(t, originalEnd.Equal(sbx.GetEndAt()))
}
