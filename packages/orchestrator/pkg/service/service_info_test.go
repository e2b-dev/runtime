//go:build linux

package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestServiceStatusOverride(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		from, to orchestratorinfo.ServiceInfoStatus
		wantCode codes.Code
	}{
		{orchestratorinfo.ServiceInfoStatus_Draining, orchestratorinfo.ServiceInfoStatus_Healthy, codes.OK},
		{orchestratorinfo.ServiceInfoStatus_Draining, orchestratorinfo.ServiceInfoStatus_Standby, codes.FailedPrecondition},
		{orchestratorinfo.ServiceInfoStatus_Draining, orchestratorinfo.ServiceInfoStatus_Draining, codes.OK},
		{orchestratorinfo.ServiceInfoStatus_Healthy, orchestratorinfo.ServiceInfoStatus_Draining, codes.OK},
		{orchestratorinfo.ServiceInfoStatus_Standby, orchestratorinfo.ServiceInfoStatus_Healthy, codes.OK},
		{orchestratorinfo.ServiceInfoStatus_Healthy, orchestratorinfo.ServiceInfoStatus_Standby, codes.OK},
		{orchestratorinfo.ServiceInfoStatus_Healthy, orchestratorinfo.ServiceInfoStatus_NotReady, codes.FailedPrecondition},
		{orchestratorinfo.ServiceInfoStatus_NotReady, orchestratorinfo.ServiceInfoStatus_Draining, codes.OK},
		{orchestratorinfo.ServiceInfoStatus_NotReady, orchestratorinfo.ServiceInfoStatus_Healthy, codes.OK},
	} {
		t.Run(tc.from.String()+"To"+tc.to.String(), func(t *testing.T) {
			t.Parallel()

			info := &ServiceInfo{}
			info.SetStatus(t.Context(), tc.from)
			before := info.GetStatus()
			server := &Server{info: info}

			response, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
				ServiceStatus: tc.to,
			})

			require.Equal(t, tc.wantCode, status.Code(err))
			if tc.wantCode != codes.OK {
				require.Nil(t, response)
				require.Equal(t, before, info.GetStatus())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, tc.to, info.GetStatus().Status)
		})
	}
}

func TestServiceInfoShuttingDownIsTerminal(t *testing.T) {
	t.Parallel()

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			initial := ServiceStatus{Status: orchestratorinfo.ServiceInfoStatus(value), ChangedAt: time.Unix(1, 0)}
			info := &ServiceInfo{status: initial}
			info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_ShuttingDown)
			shuttingDown := info.GetStatus()
			require.Equal(t, orchestratorinfo.ServiceInfoStatus_ShuttingDown, shuttingDown.Status)
			if initial.Status == shuttingDown.Status {
				require.Equal(t, initial.ChangedAt, shuttingDown.ChangedAt)
			} else {
				require.True(t, shuttingDown.ChangedAt.After(initial.ChangedAt))
			}

			server := &Server{info: info}
			for target := range orchestratorinfo.ServiceInfoStatus_name {
				next := orchestratorinfo.ServiceInfoStatus(target)
				info.SetStatus(t.Context(), next)
				require.Equal(t, shuttingDown, info.GetStatus())

				_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: next})
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Equal(t, shuttingDown, info.GetStatus())
			}
		})
	}
}

func TestServiceStatusOverrideRejectsShuttingDown(t *testing.T) {
	t.Parallel()

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			initial := ServiceStatus{Status: orchestratorinfo.ServiceInfoStatus(value), ChangedAt: time.Unix(1, 0)}
			info := &ServiceInfo{status: initial}
			server := &Server{info: info}
			_, err := server.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{
				ServiceStatus: orchestratorinfo.ServiceInfoStatus_ShuttingDown,
			})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Equal(t, initial, info.GetStatus())
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
		interrupt  func(context.Context, *ServiceInfo)
		wantStatus orchestratorinfo.ServiceInfoStatus
	}{
		{name: "ready", wantStatus: orchestratorinfo.ServiceInfoStatus_Healthy},
		{
			name: "drained while waiting",
			interrupt: func(ctx context.Context, info *ServiceInfo) {
				info.OverrideStatus(ctx, orchestratorinfo.ServiceInfoStatus_Draining)
			},
			wantStatus: orchestratorinfo.ServiceInfoStatus_Draining,
		},
		{
			name: "shut down while waiting",
			interrupt: func(ctx context.Context, info *ServiceInfo) {
				info.SetStatus(ctx, orchestratorinfo.ServiceInfoStatus_ShuttingDown)
			},
			wantStatus: orchestratorinfo.ServiceInfoStatus_ShuttingDown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				info := &ServiceInfo{}
				var ready atomic.Bool
				info.HoldUntilReady(t.Context(), ready.Load)
				require.Equal(t, orchestratorinfo.ServiceInfoStatus_NotReady, info.GetStatus().Status)

				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, orchestratorinfo.ServiceInfoStatus_NotReady, info.GetStatus().Status, "not ready yet")

				if tc.interrupt != nil {
					tc.interrupt(t.Context(), info)
				}
				ready.Store(true)
				time.Sleep(readinessPollInterval)
				synctest.Wait()
				require.Equal(t, tc.wantStatus, info.GetStatus().Status)

				ready.Store(false)
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, tc.wantStatus, info.GetStatus().Status, "the wait ended for good")
			})
		})
	}
}

func TestServiceInfoShutdownRacesWithOverrides(t *testing.T) {
	t.Parallel()

	info := &ServiceInfo{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_ShuttingDown)
	})
	for range 32 {
		wg.Go(func() {
			<-start
			for value := range orchestratorinfo.ServiceInfoStatus_name {
				info.OverrideStatus(t.Context(), orchestratorinfo.ServiceInfoStatus(value))
			}
		})
	}
	close(start)
	wg.Wait()
	require.Equal(t, orchestratorinfo.ServiceInfoStatus_ShuttingDown, info.GetStatus().Status)
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
