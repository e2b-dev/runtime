package handlers

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	webhookevents "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/events"
)

func sampleSandboxEvent(t *testing.T, teamID uuid.UUID) *webhookevents.SandboxEvent {
	t.Helper()

	data, err := structpb.NewStruct(map[string]any{"exit_code": float64(0)})
	require.NoError(t, err)

	return webhookevents.SandboxEvent_builder{
		EventId:            uuid.NewString(),
		Version:            "v2",
		Type:               "sandbox.lifecycle.created",
		Timestamp:          timestamppb.New(time.Unix(1700000000, 0).UTC()),
		EventCategory:      new("lifecycle"),
		EventLabel:         new("create"),
		EventData:          data,
		SandboxId:          "isandbox",
		SandboxBuildId:     uuid.NewString(),
		SandboxExecutionId: uuid.NewString(),
		SandboxTemplateId:  uuid.NewString(),
		SandboxTeamId:      teamID.String(),
	}.Build()
}

func TestGetEventsSandboxesSendsTheCallersTeamAndPaging(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes", "")
	backend.events = []*webhookevents.SandboxEvent{sampleSandboxEvent(t, teamID)}

	types := []string{"sandbox.lifecycle.created"}
	store.GetEventsSandboxes(ginCtx, api.GetEventsSandboxesParams{
		Types:    &types,
		Limit:    new(int32(25)),
		Offset:   new(int32(50)),
		OrderAsc: new(true),
	})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Len(t, backend.teamEventsRequests, 1)

	sent := backend.teamEventsRequests[0]
	assert.Equal(t, teamID.String(), sent.GetTeamId())
	assert.Equal(t, types, sent.GetEventTypes())
	assert.True(t, sent.GetOrderAsc())
	assert.Equal(t, int32(25), sent.GetPagination().GetLimit())
	assert.Equal(t, int32(50), sent.GetPagination().GetOffset())
}

// Paging is the backend's to default, so an unset limit or offset must reach
// it unset rather than as a zero this side invented.
func TestGetEventsSandboxesLeavesPagingUnsetWhenNotGiven(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, _, _ := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes", "")

	store.GetEventsSandboxes(ginCtx, api.GetEventsSandboxesParams{})

	require.Len(t, backend.teamEventsRequests, 1)
	pagination := backend.teamEventsRequests[0].GetPagination()
	assert.False(t, pagination.HasLimit())
	assert.False(t, pagination.HasOffset())
}

func TestGetEventsSandboxesSandboxIDProjectsTheEvent(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes/isandbox", "")
	source := sampleSandboxEvent(t, teamID)
	backend.events = []*webhookevents.SandboxEvent{source}

	store.GetEventsSandboxesSandboxID(ginCtx, "isandbox", api.GetEventsSandboxesSandboxIDParams{})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Len(t, backend.eventsRequests, 1)
	assert.Equal(t, "isandbox", backend.eventsRequests[0].GetSandboxId())
	assert.Equal(t, teamID.String(), backend.eventsRequests[0].GetTeamId())

	var events []api.SandboxEvent
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &events))
	require.Len(t, events, 1)

	assert.Equal(t, source.GetType(), events[0].Type)
	assert.Equal(t, source.GetSandboxId(), events[0].SandboxId)
	assert.Equal(t, teamID, events[0].SandboxTeamId)
	require.NotNil(t, events[0].EventCategory)
	assert.Equal(t, "lifecycle", *events[0].EventCategory)
	require.NotNil(t, events[0].EventData)
	assert.InDelta(t, float64(0), (*events[0].EventData)["exit_code"], 0)
}

// The source may record no category, label or payload, and the response must
// omit them rather than publish an empty string the source never had.
func TestSandboxEventsOmitWhatTheSourceLeftUnset(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes", "")
	backend.events = []*webhookevents.SandboxEvent{
		webhookevents.SandboxEvent_builder{
			EventId:       uuid.NewString(),
			Type:          "sandbox.lifecycle.created",
			Timestamp:     timestamppb.New(time.Unix(1700000000, 0).UTC()),
			SandboxTeamId: teamID.String(),
		}.Build(),
	}

	store.GetEventsSandboxes(ginCtx, api.GetEventsSandboxesParams{})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var raw []map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))
	require.Len(t, raw, 1)
	assert.NotContains(t, raw[0], "eventCategory")
	assert.NotContains(t, raw[0], "eventLabel")
	assert.NotContains(t, raw[0], "eventData")
}

func TestSandboxEventsBackendFailuresMapToStatuses(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		err      error
		wantCode int
		wantBody string
	}{
		{
			name: "not found", err: status.Error(codes.NotFound, "sandbox events not found"),
			wantCode: http.StatusNotFound, wantBody: sandboxEventsNotFound,
		},
		{
			name: "invalid argument keeps its message", err: status.Error(codes.InvalidArgument, "Limit must not exceed 100"),
			wantCode: http.StatusBadRequest, wantBody: "Limit must not exceed 100",
		},
		{
			name: "internal is not passed through", err: status.Error(codes.Internal, "clickhouse: dial tcp refused"),
			wantCode: http.StatusInternalServerError, wantBody: sandboxEventsFallback,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeWebhooksBackend{err: tc.err}
			store := startWebhooksBackend(t, backend)
			ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes", "")

			store.GetEventsSandboxes(ginCtx, api.GetEventsSandboxesParams{})

			assert.Equal(t, tc.wantCode, recorder.Code)
			assert.Contains(t, recorder.Body.String(), tc.wantBody)
			assert.NotContains(t, recorder.Body.String(), "clickhouse")
		})
	}
}

func TestSandboxEventRoutesAnswerWhenNoBackendIsConfigured(t *testing.T) {
	t.Parallel()

	store := &APIStore{}
	ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes", "")

	store.GetEventsSandboxes(ginCtx, api.GetEventsSandboxesParams{})

	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Contains(t, recorder.Body.String(), sandboxEventsUnavailable)
}
