//go:build linux

package service

import (
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	healthy      = orchestratorinfo.ServiceInfoStatus_Healthy
	draining     = orchestratorinfo.ServiceInfoStatus_Draining
	standby      = orchestratorinfo.ServiceInfoStatus_Standby
	shuttingDown = orchestratorinfo.ServiceInfoStatus_ShuttingDown
	notReady     = orchestratorinfo.ServiceInfoStatus_NotReady
)

// testInfo is a container whose status lives only in memory.
func testInfo() *ServiceInfo {
	return &ServiceInfo{machine: servicestatus.New(servicestatus.NopStore{}, time.Now)}
}

// enter moves a fresh container into target the way the process does: the
// startup hold and the shutdown set their own statuses, an operator overrides
// the rest.
func enter(t *testing.T, info *ServiceInfo, target orchestratorinfo.ServiceInfoStatus, owner string) {
	t.Helper()

	switch target {
	case notReady, shuttingDown:
		info.SetStatus(t.Context(), target)
	default:
		require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: target, Owner: owner}))
	}
}

func TestServiceStatusOverride(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		from, to  orchestratorinfo.ServiceInfoStatus
		fromOwner string
		owner     string
		force     bool
		wantCode  codes.Code
	}{
		{name: "DrainingToHealthy", from: draining, to: healthy, wantCode: codes.OK},
		{name: "DrainingToStandby", from: draining, to: standby, wantCode: codes.FailedPrecondition},
		{name: "DrainingToDraining", from: draining, to: draining, wantCode: codes.OK},
		{name: "HealthyToDraining", from: healthy, to: draining, wantCode: codes.OK},
		{name: "StandbyToHealthy", from: standby, to: healthy, wantCode: codes.OK},
		{name: "HealthyToStandby", from: healthy, to: standby, wantCode: codes.OK},
		{name: "HealthyToNotReady", from: healthy, to: notReady, wantCode: codes.FailedPrecondition},
		{name: "NotReadyToDraining", from: notReady, to: draining, wantCode: codes.OK},
		{name: "NotReadyToHealthy", from: notReady, to: healthy, wantCode: codes.OK},
		{name: "OwnedStandbyToHealthyByOwner", from: standby, fromOwner: "a", to: healthy, owner: "a", wantCode: codes.OK},
		{name: "OwnedStandbyToHealthyWithoutOwner", from: standby, fromOwner: "a", to: healthy, wantCode: codes.OK},
		{name: "OwnedStandbyToStandbyByAnother", from: standby, fromOwner: "a", to: standby, owner: "b", wantCode: codes.FailedPrecondition},
		{name: "OwnedStandbyToDraining", from: standby, fromOwner: "a", to: draining, wantCode: codes.OK},
		{name: "OwnedStandbyToHealthyForced", from: standby, fromOwner: "a", to: healthy, force: true, wantCode: codes.OK},
		{name: "HealthyToStandbyWithTooLongOwner", from: healthy, to: standby, owner: strings.Repeat("a", servicestatus.MaxOwnerLength+1), wantCode: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			info := testInfo()
			enter(t, info, tc.from, tc.fromOwner)
			before := info.Snapshot()
			server := &Server{info: info}

			response, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
				ServiceStatus: tc.to,
				Owner:         tc.owner,
				Force:         tc.force,
			})

			require.Equal(t, tc.wantCode, status.Code(err))
			if tc.wantCode != codes.OK {
				require.Nil(t, response)
				require.Equal(t, before, info.Snapshot())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, tc.to, info.Snapshot().Status)
		})
	}
}

func TestServiceStatusOverrideCountsRefusals(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/service")
	counter, err := telemetry.GetCounter(meter, telemetry.OrchestratorStatusOverrideRefusedCounterName)
	require.NoError(t, err)
	server := NewInfoService(testInfo(), sandbox.NewSandboxesMap(), metrics.NewHostMetrics(), counter)

	_, err = server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: standby, Owner: "a"})
	require.NoError(t, err)
	_, err = server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: standby, Owner: "b"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)
	refused := data.ScopeMetrics[0].Metrics[0]
	require.Equal(t, string(telemetry.OrchestratorStatusOverrideRefusedCounterName), refused.Name)
	sum, ok := refused.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(1), sum.DataPoints[0].Value)
	reason, ok := sum.DataPoints[0].Attributes.Value("reason")
	require.True(t, ok)
	require.Equal(t, "owner_mismatch", reason.AsString())
}

// The shutdown is terminal and keeps the owner and the Standby record, so the
// next process restores the park.
func TestServiceInfoShuttingDownIsTerminal(t *testing.T) {
	t.Parallel()

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			initial := orchestratorinfo.ServiceInfoStatus(value)
			owner := ""
			if initial == standby {
				owner = "x"
			}
			dir := t.TempDir()
			info := newContainer(t, dir)
			enter(t, info, initial, owner)

			info.SetStatus(t.Context(), shuttingDown)
			down := info.Snapshot()
			require.Equal(t, shuttingDown, down.Status)
			require.Equal(t, owner, down.Owner)
			if initial == standby {
				require.FileExists(t, filepath.Join(dir, servicestatus.FileName))
			}

			server := &Server{info: info}
			for target := range orchestratorinfo.ServiceInfoStatus_name {
				next := orchestratorinfo.ServiceInfoStatus(target)
				info.SetStatus(t.Context(), next)
				require.Equal(t, down, info.Snapshot())

				_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: next, Owner: owner, Force: true})
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Equal(t, down, info.Snapshot())
			}
		})
	}
}

func TestServiceStatusOverrideRejectsShuttingDown(t *testing.T) {
	t.Parallel()

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			info := testInfo()
			enter(t, info, orchestratorinfo.ServiceInfoStatus(value), "")
			before := info.Snapshot()
			server := &Server{info: info}
			_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
				ServiceStatus: shuttingDown,
			})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Equal(t, before, info.Snapshot())
		})
	}
}

// A starting node is NotReady until it first reports ready, and Healthy from
// then on even when it stops being ready. A status an operator or the
// shutdown sets while it waits ends the wait and stays.
func TestHoldUntilReady(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		interrupt  func(t *testing.T, info *ServiceInfo)
		wantStatus orchestratorinfo.ServiceInfoStatus
	}{
		{name: "ready", wantStatus: healthy},
		{
			name: "drained while waiting",
			interrupt: func(t *testing.T, info *ServiceInfo) {
				t.Helper()
				require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: draining}))
			},
			wantStatus: draining,
		},
		{
			name: "shut down while waiting",
			interrupt: func(t *testing.T, info *ServiceInfo) {
				t.Helper()
				info.SetStatus(t.Context(), shuttingDown)
			},
			wantStatus: shuttingDown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				info := testInfo()
				var ready atomic.Bool
				info.HoldUntilReady(t.Context(), ready.Load)
				require.Equal(t, notReady, info.Snapshot().Status)

				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, notReady, info.Snapshot().Status, "not ready yet")

				if tc.interrupt != nil {
					tc.interrupt(t, info)
				}
				ready.Store(true)
				time.Sleep(readinessPollInterval)
				synctest.Wait()
				require.Equal(t, tc.wantStatus, info.Snapshot().Status)

				ready.Store(false)
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, tc.wantStatus, info.Snapshot().Status, "the wait ended for good")
			})
		})
	}
}

func TestServiceInfoShutdownRacesWithOverrides(t *testing.T) {
	t.Parallel()

	info := testInfo()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		info.SetStatus(t.Context(), shuttingDown)
	})
	for range 32 {
		wg.Go(func() {
			<-start
			for value := range orchestratorinfo.ServiceInfoStatus_name {
				_ = info.OverrideStatus(t.Context(), servicestatus.Request{Status: orchestratorinfo.ServiceInfoStatus(value)})
			}
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, shuttingDown, info.Snapshot().Status)
}

// The base pool travels only when it was read, so the API can tell an unknown
// base from a pool that is all overcommit.
func TestHugePagesBaseIsUnsetUnlessRead(t *testing.T) {
	t.Parallel()

	assert.Nil(t, hugePagesBase(&metrics.MemoryMetrics{HugePagesBase: 1600}))

	base := hugePagesBase(&metrics.MemoryMetrics{HugePagesBase: 1600, HugePagesBaseRead: true})
	require.NotNil(t, base)
	assert.Equal(t, uint64(1600), *base)

	zero := hugePagesBase(&metrics.MemoryMetrics{HugePagesBaseRead: true})
	require.NotNil(t, zero)
	assert.Zero(t, *zero)
}

// A park survives the startup hold: a restored or newly recorded Standby keeps
// the wait alive, a release before the node is ready lands on NotReady, and a
// release after it goes straight to Healthy.
func TestHoldUntilReady_KeepsAPark(t *testing.T) {
	t.Parallel()

	t.Run("released before ready", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			info := testInfo()
			require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: standby, Owner: "x"}))
			var ready atomic.Bool
			info.HoldUntilReady(t.Context(), ready.Load)
			time.Sleep(time.Minute)
			synctest.Wait()
			require.Equal(t, standby, info.Snapshot().Status, "the park holds through the hold")

			require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: healthy, Owner: "x"}))
			require.Equal(t, notReady, info.Snapshot().Status, "released before ready")
			ready.Store(true)
			time.Sleep(readinessPollInterval)
			synctest.Wait()
			require.Equal(t, healthy, info.Snapshot().Status)
		})
	})

	t.Run("released after ready", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			info := testInfo()
			require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: standby, Owner: "x"}))
			var ready atomic.Bool
			info.HoldUntilReady(t.Context(), ready.Load)
			ready.Store(true)
			time.Sleep(readinessPollInterval)
			synctest.Wait()
			require.Equal(t, standby, info.Snapshot().Status, "ready does not release a park")

			require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: healthy, Owner: "x"}))
			require.Equal(t, healthy, info.Snapshot().Status)
		})
	})
}
