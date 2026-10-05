//go:build linux

package server

import (
	"errors"
	"net"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var (
	startTime = time.Now()
	endTime   = time.Now().Add(time.Hour)

	listSandboxID   = id.Generate()
	listTeamID      = "6c6f2ba0-9c62-4f2a-9ab8-0a0a9c6f3e11"
	listExecutionID = "8f7f6b3a-1a2b-4c3d-9e8f-7a6b5c4d3e2f"
)

func Test_server_List(t *testing.T) {
	t.Parallel()
	type args struct {
		in1 *emptypb.Empty
	}
	tests := []struct {
		name    string
		args    args
		want    *orchestrator.SandboxListResponse
		wantErr bool
		data    []*sandbox.Sandbox
		endAt   time.Time
	}{
		{
			name: "should return all sandboxes",

			args: args{
				in1: &emptypb.Empty{},
			},
			data: []*sandbox.Sandbox{
				{
					APIStoredConfig: &orchestrator.SandboxConfig{
						TemplateId: "template-id",
					},
					Metadata: &sandbox.Metadata{
						Runtime: sandboxtypes.RuntimeMetadata{
							SandboxID:   listSandboxID,
							TeamID:      listTeamID,
							ExecutionID: listExecutionID,
						},
						Config: sandbox.NewConfig(sandbox.Config{Vcpu: 2, RamMB: 512}),
					},
					Resources: &sandbox.Resources{
						Slot: &network.Slot{HostIP: net.IPv4(127, 0, 0, 1)},
					},
				},
			},
			endAt: endTime,
			want: &orchestrator.SandboxListResponse{
				Sandboxes: []*orchestrator.RunningSandbox{
					{
						Config: &orchestrator.SandboxConfig{TemplateId: "template-id"},
						// ClientId:  "client-id",
						StartTime:   timestamppb.New(startTime),
						EndTime:     timestamppb.New(endTime),
						SandboxId:   listSandboxID,
						TeamId:      listTeamID,
						ExecutionId: listExecutionID,
						Vcpu:        2,
						RamMb:       512,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sandboxes := sandbox.NewSandboxesMap()
			s := &Server{
				sandboxFactory: &sandbox.Factory{Sandboxes: sandboxes},
				info:           &service.ServiceInfo{},
			}
			for _, sbx := range tt.data {
				sbx.SetStartedAt(startTime)
				sbx.SetEndAt(tt.endAt)
				sandboxes.AssignNetwork(t.Context(), sbx)
				sandboxes.MarkRunning(t.Context(), sbx)
			}
			got, err := s.List(t.Context(), tt.args.in1)
			if (err != nil) != tt.wantErr {
				t.Errorf("server.List() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("server.List() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSandboxExecutionData(t *testing.T) {
	t.Parallel()

	sbxStartedAt := time.Now().Add(-5 * time.Minute)

	sbx := &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				Vcpu:  2,
				RamMB: 512,
			}),
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID: id.Generate(),
			},
		},
	}
	sbx.SetStartedAt(sbxStartedAt)

	s := &Server{}
	result := s.getSandboxExecutionData(sbx)

	assert.Equal(t, sbxStartedAt.UTC().Format(time.RFC3339), result["started_at"])
	assert.Equal(t, int64(2), result["vcpu_count"])
	assert.Equal(t, int64(512), result["memory_mb"])
	assert.IsType(t, int64(0), result["execution_time"])
	assert.Positive(t, result["execution_time"].(int64))
}

func TestAddKillReason(t *testing.T) {
	t.Parallel()

	t.Run("non-empty reason recorded", func(t *testing.T) {
		t.Parallel()

		eventData := map[string]any{}
		addKillReason(eventData, "request")

		assert.Equal(t, "request", eventData["kill_reason"])
	})

	t.Run("empty reason records unknown", func(t *testing.T) {
		t.Parallel()

		eventData := map[string]any{}
		addKillReason(eventData, "")

		assert.Equal(t, killReasonUnknown, eventData["kill_reason"])
	})
}

func TestRecordSandboxKill(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	counter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxKilledCounterName)
	require.NoError(t, err)

	recordSandboxKill(t.Context(), counter, "timeout")
	recordSandboxKill(t.Context(), counter, "")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxKilledCounterName) {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("kill_reason"))
				require.True(t, ok)
				got[v.AsString()] += dp.Value
			}
		}
	}

	assert.Equal(t, int64(1), got["timeout"])
	assert.Equal(t, int64(1), got[killReasonUnknown])
}

// The crash path runs for sandboxes that ended badly, including ones that
// never got a process: an absent exit must not panic and must not be guessed.
func TestRecordCrashWithoutFirecrackerProcess(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	counter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxCrashedCounterName)
	require.NoError(t, err)

	s := &Server{sandboxCrashedCounter: counter}

	require.NotPanics(t, func() {
		s.recordCrash(t.Context(), &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}, nil)
	})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxCrashedCounterName) {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("cause"))
				require.True(t, ok)
				got[v.AsString()] += dp.Value
			}
		}
	}

	assert.Equal(t, map[string]int64{string(fc.CrashCauseUnknown): 1}, got)
}

func TestIsCleanFirecrackerExit(t *testing.T) {
	t.Parallel()

	assert.True(t, isCleanFirecrackerExit(fc.CrashCauseCleanExit, nil))
	assert.False(t, isCleanFirecrackerExit(fc.CrashCauseCleanExit, errors.New("uffd exited")),
		"a clean exit that came with a wait error is still a crash")
	assert.False(t, isCleanFirecrackerExit(fc.CrashCauseExternalSignal, nil))
}

func TestCrashCause(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("uffdio copy: cannot allocate memory")
	ourSigterm := &fc.ExitInfo{Signaled: true, Signal: syscall.SIGTERM, SentByStop: true}
	foreignSigkill := &fc.ExitInfo{Signaled: true, Signal: syscall.SIGKILL}

	assert.Equal(t, fc.CrashCauseMemoryHandlerFailed, crashCause(ourSigterm, handlerErr),
		"teardown stopping Firecracker after the memory handler failed is that failure")
	assert.Equal(t, fc.CrashCauseRequestedSignal, crashCause(ourSigterm, nil),
		"without a handler failure our signal points at a missing stop reason")
	assert.Equal(t, fc.CrashCauseExternalSignal, crashCause(foreignSigkill, handlerErr),
		"a foreign kill can take the handler down with it and stays external")
}

func TestRecordExecutionDuration(t *testing.T) {
	t.Parallel()

	stoppedAt := time.Now()

	endedSandbox := func(reason sandbox.StopReason) *sandbox.Sandbox {
		sbx := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
		sbx.SetStartedAt(stoppedAt.Add(-time.Minute))
		sbx.SetStopReason(reason)
		sbx.SetStoppedAt(stoppedAt)

		return sbx
	}

	neverStarted := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	neverStarted.SetStopReason(sandbox.StopReasonKilled)
	neverStarted.SetStoppedAt(stoppedAt)

	stillRunning := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	stillRunning.SetStartedAt(stoppedAt.Add(-time.Minute))

	crashed := &sandbox.Sandbox{Metadata: &sandbox.Metadata{}}
	crashed.SetStartedAt(stoppedAt.Add(-time.Minute))
	crashed.SetStoppedAt(stoppedAt)

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	histogram, err := telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxExecutionDurationName)
	require.NoError(t, err)

	s := &Server{sandboxExecutionDuration: histogram}

	s.recordExecutionDuration(t.Context(), endedSandbox(sandbox.StopReasonKilled))
	s.recordExecutionDuration(t.Context(), endedSandbox(sandbox.StopReasonPaused))
	s.recordExecutionDuration(t.Context(), endedSandbox(sandbox.StopReasonCheckpointing))
	// An execution nobody asked to stop is a crash.
	s.recordExecutionDuration(t.Context(), crashed)
	// Neither of these ran a guest for a known span, so they record nothing.
	s.recordExecutionDuration(t.Context(), neverStarted)
	s.recordExecutionDuration(t.Context(), stillRunning)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	got := map[string]uint64{}
	sum := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxExecutionDurationName) {
				continue
			}

			hist, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)

			for _, dp := range hist.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("stop_reason"))
				require.True(t, ok)
				got[v.AsString()] += dp.Count
				sum[v.AsString()] += dp.Sum
			}
		}
	}

	assert.Equal(t, map[string]uint64{
		string(sandbox.StopReasonKilled):        1,
		string(sandbox.StopReasonPaused):        1,
		string(sandbox.StopReasonCheckpointing): 1,
		string(sandbox.StopReasonCrashed):       1,
	}, got)
	assert.Equal(t, time.Minute.Milliseconds(), sum[string(sandbox.StopReasonKilled)])
}
