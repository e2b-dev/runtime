package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	webhookevents "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const sandboxEventsUnavailable = "Sandbox events are not available"

// sandboxEventsBackend resolves the caller's team and the backend together,
// since both routes need both and either one missing answers before any work.
func (a *APIStore) sandboxEventsBackend(c *gin.Context) (uuid.UUID, *string, bool) {
	if a.sandboxEvents == nil {
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, sandboxEventsUnavailable)

		return uuid.Nil, nil, false
	}

	team := auth.MustGetTeamInfo(c)
	telemetry.SetAttributes(c.Request.Context(), telemetry.WithTeamID(team.ID.String()))

	return team.ID, optionalClusterID(team.ClusterID), true
}

func (a *APIStore) GetEventsSandboxes(c *gin.Context, params api.GetEventsSandboxesParams) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.sandboxEventsBackend(c)
	if !ok {
		return
	}

	response, err := a.sandboxEvents.ListTeamSandboxEvents(ctx, webhookevents.ListTeamSandboxEventsRequest_builder{
		TeamId:     teamID.String(),
		ClusterId:  clusterID,
		EventTypes: eventTypes(params.Types),
		Pagination: webhookevents.Pagination_builder{Offset: params.Offset, Limit: params.Limit}.Build(),
		OrderAsc:   params.OrderAsc != nil && *params.OrderAsc,
	}.Build())
	if err != nil {
		a.sendSandboxEventsError(c, err)

		return
	}

	c.JSON(http.StatusOK, sandboxEvents(response.GetEvents()))
}

func (a *APIStore) GetEventsSandboxesSandboxID(
	c *gin.Context,
	sandboxID api.SandboxID,
	params api.GetEventsSandboxesSandboxIDParams,
) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.sandboxEventsBackend(c)
	if !ok {
		return
	}

	telemetry.SetAttributes(ctx, telemetry.WithSandboxID(sandboxID))

	response, err := a.sandboxEvents.ListSandboxEvents(ctx, webhookevents.ListSandboxEventsRequest_builder{
		TeamId:     teamID.String(),
		ClusterId:  clusterID,
		SandboxId:  sandboxID,
		EventTypes: eventTypes(params.Types),
		Pagination: webhookevents.Pagination_builder{Offset: params.Offset, Limit: params.Limit}.Build(),
		OrderAsc:   params.OrderAsc != nil && *params.OrderAsc,
	}.Build())
	if err != nil {
		a.sendSandboxEventsError(c, err)

		return
	}

	c.JSON(http.StatusOK, sandboxEvents(response.GetEvents()))
}

const (
	sandboxEventsNotFound = "Sandbox events not found"
	sandboxEventsFallback = "Error when querying sandbox events"
)

// sendSandboxEventsError answers a failed sandbox events call. Both routes read
// the same resource, so unlike the webhook ones they share a fallback too.
//
// The mapping is shared with webhook management; the not-found text is not, so
// a caller asking about events is not told a webhook was missing.
func (a *APIStore) sendSandboxEventsError(c *gin.Context, err error) {
	a.sendWebhooksBackendError(c, err, webhooksMessages{
		notFound: sandboxEventsNotFound,
		fallback: sandboxEventsFallback,
	})
}

func sandboxEvents(events []*webhookevents.SandboxEvent) []api.SandboxEvent {
	response := make([]api.SandboxEvent, 0, len(events))
	for _, event := range events {
		eventID, _ := uuid.Parse(event.GetEventId())
		sandboxTeamID, _ := uuid.Parse(event.GetSandboxTeamId())

		var eventData *map[string]any
		if event.HasEventData() {
			value := event.GetEventData().AsMap()
			eventData = &value
		}

		response = append(response, api.SandboxEvent{
			Id:        eventID,
			Version:   event.GetVersion(),
			Type:      event.GetType(),
			Timestamp: event.GetTimestamp().AsTime().UTC(),

			EventCategory: optionalString(event.HasEventCategory(), event.GetEventCategory()),
			EventLabel:    optionalString(event.HasEventLabel(), event.GetEventLabel()),
			EventData:     eventData,

			SandboxId:          event.GetSandboxId(),
			SandboxBuildId:     event.GetSandboxBuildId(),
			SandboxExecutionId: event.GetSandboxExecutionId(),
			SandboxTemplateId:  event.GetSandboxTemplateId(),
			SandboxTeamId:      sandboxTeamID,
		})
	}

	return response
}

// optionalString maps an opaque Has/Get pair onto the pointer the REST model
// uses, so a field the source left unset stays absent in the response.
func optionalString(has bool, value string) *string {
	if !has {
		return nil
	}

	return &value
}

func eventTypes(types *[]string) []string {
	if types == nil {
		return nil
	}

	return *types
}
