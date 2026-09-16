//go:build linux

package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	internalevents "github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type sandboxEventDeliveryFunc func(context.Context, string, events.SandboxEvent) error

func (f sandboxEventDeliveryFunc) Publish(ctx context.Context, key string, event events.SandboxEvent) error {
	return f(ctx, key, event)
}

func (sandboxEventDeliveryFunc) Close(context.Context) error { return nil }

func eventWorkSandbox() *sandbox.Sandbox {
	return &sandbox.Sandbox{
		LifecycleID: "lifecycle-1",
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{BaseTemplateID: "template-1", Vcpu: 2, RamMB: 512}),
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID: "sandbox-1", TeamID: uuid.NewString(), ExecutionID: "execution-1",
			},
		},
		Resources: &sandbox.Resources{Slot: &network.Slot{}},
		APIStoredConfig: &orchestrator.SandboxConfig{
			BuildId: "build-1", EventsTtlDays: 7, Metadata: map[string]string{"key": "value"},
		},
	}
}

func TestPublishEventAsyncTracksAllTargetAttempts(t *testing.T) {
	t.Parallel()

	for _, eventType := range []string{
		events.SandboxCreatedEvent, events.SandboxResumedEvent, events.SandboxUpdatedEvent,
		events.SandboxKilledEvent, events.SandboxPausedEvent, events.SandboxCheckpointedEvent,
	} {
		for _, outcome := range []string{"success-canceled", "error-deadline"} {
			t.Run(eventType+"/"+outcome, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					s := &Server{info: &service.ServiceInfo{}, sandboxKilledCounter: noop.Int64Counter{}}
					sbx := eventWorkSandbox()
					sbx.SetStartedAt(time.Now().Add(-time.Minute))
					teamID, buildID, ttl, data := s.prepareSandboxEventData(t.Context(), sbx)
					if eventType == events.SandboxKilledEvent {
						data[executionEventDataKey] = s.getSandboxExecutionData(sbx)
						data["kill_reason"] = killReasonUnknown
					}
					want := events.SandboxEvent{
						ID: uuid.New(), Version: events.StructureVersionV2, Type: eventType, Timestamp: time.Now().UTC(),
						SandboxID: sbx.Runtime.SandboxID, SandboxTeamID: teamID, SandboxExecutionID: sbx.Runtime.ExecutionID,
						SandboxTemplateID: sbx.Config.BaseTemplateID, SandboxBuildID: buildID, EventsTTLDays: ttl, EventData: data,
					}
					started := make(chan context.Context, 1)
					finish := make(chan struct{})
					deliver := sandboxEventDeliveryFunc(func(ctx context.Context, key string, got events.SandboxEvent) error {
						assert.Equal(t, events.DeliveryKey(teamID), key)
						assert.NotEqual(t, uuid.Nil, got.ID)
						if eventType == events.SandboxKilledEvent || eventType == events.SandboxCheckpointedEvent {
							got.ID = want.ID
						}
						assert.Equal(t, want, got)
						started <- ctx
						<-finish
						if outcome == "error-deadline" {
							return errors.New("delivery failed")
						}

						return nil
					})
					s.sbxEventsService = internalevents.NewEventsService([]events.Delivery[events.SandboxEvent]{deliver, deliver})
					type contextKey struct{}
					ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), contextKey{}, "request-value"), time.Second)
					defer cancel()
					releaseForeground, releaseLifecycle, releaseUpload := s.info.TrackWork(), s.info.TrackWork(), s.info.TrackWork()
					switch eventType {
					case events.SandboxKilledEvent:
						s.emitSandboxKilled(ctx, sbx, "")
					case events.SandboxCheckpointedEvent:
						s.publishSandboxEvent(ctx, sbx, eventType)
					default:
						s.publishEventAsync(ctx, teamID, want)
					}
					require.Equal(t, int64(4), s.info.OutstandingWork(), "publisher must overlap parent before launch returns")
					releaseForeground()
					releaseLifecycle()
					releaseUpload()
					for range 2 {
						publishCtx := <-started
						synctest.Wait()
						require.Equal(t, int64(1), s.info.OutstandingWork(), "publisher must own work through every target")
						if outcome == "success-canceled" {
							cancel()
						}
						<-ctx.Done()
						require.NoError(t, publishCtx.Err())
						require.Nil(t, publishCtx.Done())
						_, hasDeadline := publishCtx.Deadline()
						require.False(t, hasDeadline)
						require.Equal(t, "request-value", publishCtx.Value(contextKey{}))
						finish <- struct{}{}
					}
					synctest.Wait()
					require.Zero(t, s.info.OutstandingWork())
				})
			})
		}
	}
}

func TestPublishEventAsyncValidationReleasesWork(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		called := false
		deliver := sandboxEventDeliveryFunc(func(context.Context, string, events.SandboxEvent) error {
			called = true

			return nil
		})
		s := &Server{info: &service.ServiceInfo{}, sbxEventsService: internalevents.NewEventsService([]events.Delivery[events.SandboxEvent]{deliver})}
		s.publishEventAsync(t.Context(), uuid.New(), events.SandboxEvent{})
		synctest.Wait()
		require.False(t, called)
		require.Zero(t, s.info.OutstandingWork())
	})
}

func TestUpdateTracksWorkWhileLockedAndPublishing(t *testing.T) {
	t.Parallel()

	sbx := eventWorkSandbox()
	sbx.SetEndAt(time.Now().Add(time.Hour))
	originalEnd := sbx.GetEndAt()
	s := &Server{info: &service.ServiceInfo{}, sandboxFactory: &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()}}
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)
	started, unlock, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseUpdate, releasePublish := sync.OnceFunc(func() { close(unlock) }), sync.OnceFunc(func() { close(finish) })
	t.Cleanup(releaseUpdate)
	t.Cleanup(releasePublish)
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- sbx.RunUpdate(func() error {
			close(started)
			<-unlock

			return nil
		})
	}()
	<-started
	newEnd := time.Now().Add(2 * time.Hour)
	got := make(chan events.SandboxEvent, 1)
	deliver := sandboxEventDeliveryFunc(func(_ context.Context, _ string, event events.SandboxEvent) error {
		got <- event
		<-finish

		return nil
	})
	s.sbxEventsService = internalevents.NewEventsService([]events.Delivery[events.SandboxEvent]{deliver})
	releaseLifecycle := s.info.TrackWork()
	updateDone := make(chan error, 1)
	go func() {
		_, err := s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{SandboxId: sbx.Runtime.SandboxID, EndTime: timestamppb.New(newEnd)})
		updateDone <- err
	}()
	require.Eventually(t, func() bool { return s.info.OutstandingWork() == 2 }, time.Second, time.Millisecond)
	releaseLifecycle()
	require.Equal(t, int64(1), s.info.OutstandingWork(), "foreground work must survive lifecycle completion while Update waits")
	require.Equal(t, originalEnd, sbx.GetEndAt())
	releaseUpdate()
	require.NoError(t, <-holderDone)
	require.NoError(t, <-updateDone)
	event := <-got
	require.Equal(t, events.SandboxUpdatedEvent, event.Type)
	require.Equal(t, newEnd.Format(time.RFC3339), event.EventData["set_timeout"])
	require.True(t, newEnd.Equal(sbx.GetEndAt()))
	require.Equal(t, int64(1), s.info.OutstandingWork(), "publisher must survive Update return")
	releasePublish()
	require.Eventually(t, func() bool { return s.info.OutstandingWork() == 0 }, time.Second, time.Millisecond)
}

func TestUpdateDeleteNotFoundReleaseWork(t *testing.T) {
	t.Parallel()

	s := &Server{info: &service.ServiceInfo{}, sandboxFactory: &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()}}
	_, err := s.Update(t.Context(), &orchestrator.SandboxUpdateRequest{SandboxId: "missing"})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Zero(t, s.info.OutstandingWork())
	_, err = s.Delete(t.Context(), &orchestrator.SandboxDeleteRequest{SandboxId: "missing", ExecutionId: "execution-missing"})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Zero(t, s.info.OutstandingWork())
}

func TestDelete_StaleExecutionCannotStopReplacement(t *testing.T) {
	t.Parallel()

	sbx := eventWorkSandbox()
	s := &Server{info: &service.ServiceInfo{}, sandboxFactory: &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()}}
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	_, err := s.Delete(t.Context(), &orchestrator.SandboxDeleteRequest{
		SandboxId: sbx.Runtime.SandboxID, ExecutionId: "stale-execution", WaitForStop: true,
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	got, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, live)
	require.Same(t, sbx, got)
}
