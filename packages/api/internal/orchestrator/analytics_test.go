package orchestrator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/posthog/posthog-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

func TestSbxStopTime(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name      string
		startTime time.Time
		endTime   time.Time
		want      time.Time
	}{
		{
			// Normal kill/pause before timeout: StartRemoving already moved
			// EndTime to now for non-expired sandboxes; stop time is now.
			name:      "end time in the future",
			startTime: now.Add(-time.Hour),
			endTime:   now.Add(time.Hour),
			want:      now,
		},
		{
			// Late eviction of a stale record
			name:      "end time long past",
			startTime: now.Add(-30 * 24 * time.Hour),
			endTime:   now.Add(-30*24*time.Hour + time.Hour),
			want:      now.Add(-30*24*time.Hour + time.Hour),
		},
		{
			name:      "end time just passed",
			startTime: now.Add(-time.Hour),
			endTime:   now.Add(-time.Minute),
			want:      now.Add(-time.Minute),
		},
		{
			// Clock skew: record starts in the future.
			name:      "start time in the future",
			startTime: now.Add(time.Hour),
			endTime:   now.Add(2 * time.Hour),
			want:      now.Add(time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := sandbox.Sandbox{StartTime: tt.startTime, EndTime: tt.endTime}
			got := sbxStopTime(sbx, now)
			require.True(t, tt.want.Equal(got), "want %v, got %v", tt.want, got)
			require.GreaterOrEqual(t, got.Sub(tt.startTime), time.Duration(0), "duration must never be negative")
		})
	}
}

func TestSbxStopTime_CorruptEndTime(t *testing.T) {
	t.Parallel()

	now := time.Now()
	start := now.Add(-30 * 24 * time.Hour)

	tests := []struct {
		name    string
		endTime time.Time
	}{
		{
			name:    "end time before start time",
			endTime: start.Add(-55 * time.Second),
		},
		{
			name:    "end time equal to start time",
			endTime: start,
		},
		{
			name:    "zero end time",
			endTime: time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := sandbox.Sandbox{StartTime: start, EndTime: tt.endTime}
			got := sbxStopTime(sbx, now)

			// Corrupt record must be zero: stop time collapses to StartTime.
			require.True(t, start.Equal(got), "want StartTime %v, got %v", start, got)
			require.Zero(t, got.Sub(sbx.StartTime), "duration must be zero for corrupt records")
		})
	}
}

func TestCreatedInstancePropertiesEgressProxy(t *testing.T) {
	t.Parallel()

	const (
		proxyAddress  = "proxy.example.com:1080"
		proxyUsername = "proxy-user"
		proxyPassword = "proxy-secret"
	)

	tests := []struct {
		name        string
		network     *types.SandboxNetworkConfig
		wantAny     bool
		wantAuthSet bool
		wantAuth    bool
	}{
		{
			name: "no network config",
		},
		{
			name:    "no egress config",
			network: &types.SandboxNetworkConfig{},
		},
		{
			name:    "egress without proxy",
			network: &types.SandboxNetworkConfig{Egress: &types.SandboxNetworkEgressConfig{}},
		},
		{
			name: "proxy without credentials",
			network: &types.SandboxNetworkConfig{Egress: &types.SandboxNetworkEgressConfig{
				EgressProxyAddress: proxyAddress,
			}},
			wantAny:     true,
			wantAuthSet: true,
		},
		{
			name: "proxy with credentials",
			network: &types.SandboxNetworkConfig{Egress: &types.SandboxNetworkEgressConfig{
				EgressProxyAddress:  proxyAddress,
				EgressProxyUsername: proxyUsername,
				EgressProxyPassword: proxyPassword,
			}},
			wantAny:     true,
			wantAuthSet: true,
			wantAuth:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			props := createdInstanceProperties(
				posthog.NewProperties(),
				sandbox.Sandbox{Network: tt.network},
				sandbox.CreationMetadata{},
				0,
			)

			assert.Equal(t, tt.wantAny, props["egress_proxy"])

			if tt.wantAuthSet {
				assert.Equal(t, tt.wantAuth, props["egress_proxy_auth"])
			} else {
				assert.NotContains(t, props, "egress_proxy_auth")
			}

			// The endpoint and the credentials must never leave the cluster.
			for key, value := range props {
				assert.NotContains(t, fmt.Sprint(value), proxyAddress, "property %q leaks the proxy address", key)
				assert.NotContains(t, fmt.Sprint(value), proxyUsername, "property %q leaks the proxy username", key)
				assert.NotContains(t, fmt.Sprint(value), proxyPassword, "property %q leaks the proxy password", key)
			}
		})
	}
}

type memoryRecordingCollector struct {
	analyticscollector.AnalyticsCollectorClient

	started *analyticscollector.InstanceStartedEvent
	stopped *analyticscollector.InstanceStoppedEvent
}

func (r *memoryRecordingCollector) InstanceStarted(_ context.Context, in *analyticscollector.InstanceStartedEvent, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	r.started = in

	return &emptypb.Empty{}, nil
}

func (r *memoryRecordingCollector) InstanceStopped(_ context.Context, in *analyticscollector.InstanceStoppedEvent, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	r.stopped = in

	return &emptypb.Empty{}, nil
}

// Usage is billed from the stop event's ram_mb, so a sandbox that grew above
// boot RAM reports the whole memory it ran with.
func TestAnalyticsReportsTheWholeMemory(t *testing.T) {
	t.Parallel()

	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	rec := &memoryRecordingCollector{}
	o := &Orchestrator{posthogClient: posthog, analytics: analyticscollector.NewAnalyticsWithClient(rec)}

	start := time.Now().Add(-time.Minute)
	for _, tt := range []struct {
		memoryMB int64
		want     int64
	}{{memoryMB: 0, want: 1024}, {memoryMB: 3072, want: 3072}} {
		sbx := sandbox.Sandbox{SandboxID: "sbx-1", TeamID: uuid.New(), ExecutionID: uuid.NewString(), RamMB: 1024, MemoryMB: tt.memoryMB, StartTime: start, EndTime: start.Add(time.Hour)}

		o.analyticsInsert(t.Context(), sbx)
		o.analyticsRemove(t.Context(), sbx, sandbox.StateActionKill)

		require.NotNil(t, rec.started)
		require.NotNil(t, rec.stopped)
		assert.Equal(t, tt.want, rec.started.GetRamMb())
		assert.Equal(t, tt.want, rec.stopped.GetRamMb())
	}
}
