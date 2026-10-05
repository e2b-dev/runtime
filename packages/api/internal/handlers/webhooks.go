package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	webhookmanagement "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// webhooksUnavailable answers a route whose backend address is not configured.
// The routes stay registered, so the surface a caller sees does not depend on
// how a deployment is wired.
const webhooksUnavailable = "Webhooks are not available"

// webhooksBackend resolves the caller's team and the backend together, since
// every route needs both and either one missing answers before any work.
func (a *APIStore) webhooksBackend(c *gin.Context) (uuid.UUID, *string, bool) {
	if a.webhookManagement == nil {
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, webhooksUnavailable)

		return uuid.Nil, nil, false
	}

	team := auth.MustGetTeamInfo(c)
	telemetry.SetAttributes(c.Request.Context(), telemetry.WithTeamID(team.ID.String()))

	return team.ID, optionalClusterID(team.ClusterID), true
}

func (a *APIStore) PostEventsWebhooks(c *gin.Context) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	var body api.PostEventsWebhooksJSONRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid webhook creation request")

		return
	}

	response, err := a.webhookManagement.CreateWebhook(ctx, webhookmanagement.CreateWebhookRequest_builder{
		TeamId:          teamID.String(),
		ClusterId:       clusterID,
		Name:            body.Name,
		Url:             body.Url,
		Events:          body.Events,
		Enabled:         body.Enabled,
		SignatureSecret: body.SignatureSecret,
	}.Build())
	if err != nil {
		a.sendWebhooksError(c, err, "Failed to create webhook")

		return
	}

	webhook := response.GetWebhook()
	c.JSON(http.StatusCreated, api.WebhookCreation{
		Id:        webhook.GetWebhookId(),
		TeamId:    webhook.GetTeamId(),
		CreatedAt: webhook.GetCreatedAt().AsTime().UTC(),
		Name:      webhook.GetName(),
		Url:       webhook.GetUrl(),
		Enabled:   webhook.GetEnabled(),
		Events:    webhook.GetEvents(),
	})
}

func (a *APIStore) GetEventsWebhooks(c *gin.Context) {
	ctx := c.Request.Context()

	teamID, _, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	response, err := a.webhookManagement.ListWebhooks(ctx, webhookmanagement.ListWebhooksRequest_builder{
		TeamId: teamID.String(),
	}.Build())
	if err != nil {
		a.sendWebhooksError(c, err, "Failed to list webhooks")

		return
	}

	webhooks := make([]api.WebhookDetail, 0, len(response.GetWebhooks()))
	for _, webhook := range response.GetWebhooks() {
		webhooks = append(webhooks, api.WebhookDetail{
			Id:        webhook.GetWebhookId(),
			TeamId:    webhook.GetTeamId(),
			CreatedAt: webhook.GetCreatedAt().AsTime().UTC(),
			Name:      webhook.GetName(),
			Url:       webhook.GetUrl(),
			Enabled:   webhook.GetEnabled(),
			Events:    webhook.GetEvents(),
		})
	}

	c.JSON(http.StatusOK, webhooks)
}

func (a *APIStore) GetEventsWebhooksWebhookID(c *gin.Context, webhookID api.WebhookID) {
	ctx := c.Request.Context()

	teamID, _, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	response, err := a.webhookManagement.GetWebhook(ctx, webhookmanagement.GetWebhookRequest_builder{
		TeamId:    teamID.String(),
		WebhookId: webhookID.String(),
	}.Build())
	if err != nil {
		a.sendWebhooksError(c, err, "Failed to get webhook")

		return
	}

	webhook := response.GetWebhook()
	c.JSON(http.StatusOK, api.WebhookDetail{
		Id:        webhook.GetWebhookId(),
		TeamId:    webhook.GetTeamId(),
		CreatedAt: webhook.GetCreatedAt().AsTime().UTC(),
		Name:      webhook.GetName(),
		Url:       webhook.GetUrl(),
		Enabled:   webhook.GetEnabled(),
		Events:    webhook.GetEvents(),
	})
}

func (a *APIStore) DeleteEventsWebhooksWebhookID(c *gin.Context, webhookID api.WebhookID) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	_, err := a.webhookManagement.DeleteWebhook(ctx, webhookmanagement.DeleteWebhookRequest_builder{
		TeamId:    teamID.String(),
		ClusterId: clusterID,
		WebhookId: webhookID.String(),
	}.Build())
	if err != nil {
		a.sendWebhooksError(c, err, "Failed to delete webhook")

		return
	}

	// The service this replaces answers 200, and the spec documents it.
	c.Status(http.StatusOK)
}

func (a *APIStore) PatchEventsWebhooksWebhookID(c *gin.Context, webhookID api.WebhookID) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	var body api.PatchEventsWebhooksWebhookIDJSONRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid webhook update request")

		return
	}

	// The wrapper preserves the difference between an omitted update and an
	// explicitly empty list, which a repeated field cannot carry on its own.
	var events *webhookmanagement.WebhookEventsUpdate
	if body.Events != nil {
		events = webhookmanagement.WebhookEventsUpdate_builder{Values: *body.Events}.Build()
	}

	response, err := a.webhookManagement.UpdateWebhook(ctx, webhookmanagement.UpdateWebhookRequest_builder{
		TeamId:          teamID.String(),
		ClusterId:       clusterID,
		WebhookId:       webhookID.String(),
		Name:            body.Name,
		Url:             body.Url,
		Events:          events,
		Enabled:         body.Enabled,
		SignatureSecret: body.SignatureSecret,
	}.Build())
	if err != nil {
		a.sendWebhooksError(c, err, "Failed to update webhook")

		return
	}

	webhook := response.GetWebhook()
	c.JSON(http.StatusOK, api.WebhookDetail{
		Id:        webhook.GetWebhookId(),
		TeamId:    webhook.GetTeamId(),
		CreatedAt: webhook.GetCreatedAt().AsTime().UTC(),
		Name:      webhook.GetName(),
		Url:       webhook.GetUrl(),
		Enabled:   webhook.GetEnabled(),
		Events:    webhook.GetEvents(),
	})
}

const webhookNotFound = "Webhook not found"

// sendWebhooksError answers a failed webhook management call. The fallback
// varies by operation, so each call site names its own; the resource does not.
func (a *APIStore) sendWebhooksError(c *gin.Context, err error, fallback string) {
	a.sendWebhooksBackendError(c, err, webhooksMessages{notFound: webhookNotFound, fallback: fallback})
}
