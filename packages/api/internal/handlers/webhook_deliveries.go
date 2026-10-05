package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	webhookmanagement "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
)

const (
	webhookDeliveriesFallback = "Error when querying webhook deliveries"
	webhookStatsFallback      = "Error when querying webhook delivery stats"
)

// What a full deliveries page can weigh, which is what the connection to the
// backend has to accept.
const (
	// maxDeliveriesPageLimit is the ceiling the deliveries limit parameter
	// declares in spec/openapi.yml.
	maxDeliveriesPageLimit = 100
	// maxDeliveryBodyBytes is the cap the backend applies to each request and
	// response body it stores for a delivery.
	maxDeliveryBodyBytes = 64 << 10
	// maxDeliveryBytes budgets one delivery on the wire: both bodies at that
	// cap, plus the headers, the URL and protobuf framing.
	maxDeliveryBytes = 4 * maxDeliveryBodyBytes
	// maxDeliveriesPageBytes is the largest response these routes can produce.
	maxDeliveriesPageBytes = maxDeliveriesPageLimit * maxDeliveryBytes
)

func (a *APIStore) GetEventsWebhooksWebhookIDDeliveries(
	c *gin.Context,
	webhookID api.WebhookID,
	params api.GetEventsWebhooksWebhookIDDeliveriesParams,
) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	var cursor string
	if params.Cursor != nil {
		cursor = *params.Cursor
	}
	var deliveryStatuses []webhookmanagement.WebhookDeliveryStatus
	if params.DeliveryStatus != nil {
		deliveryStatuses = make([]webhookmanagement.WebhookDeliveryStatus, 0, len(*params.DeliveryStatus))
		for _, value := range *params.DeliveryStatus {
			deliveryStatuses = append(deliveryStatuses, deliveryStatus(value))
		}
	}
	var eventTypeFilter []string
	if params.EventType != nil {
		eventTypeFilter = *params.EventType
	}

	response, err := a.webhookManagement.ListWebhookDeliveries(
		ctx,
		webhookmanagement.ListWebhookDeliveriesRequest_builder{
			TeamId:           teamID.String(),
			ClusterId:        clusterID,
			WebhookId:        webhookID.String(),
			Cursor:           cursor,
			Limit:            params.Limit,
			OrderAsc:         params.OrderAsc != nil && *params.OrderAsc,
			Start:            optionalTimestamp(params.Start),
			End:              optionalTimestamp(params.End),
			DeliveryStatuses: deliveryStatuses,
			EventTypes:       eventTypeFilter,
		}.Build(),
	)
	if err != nil {
		a.sendWebhooksError(c, err, webhookDeliveriesFallback)

		return
	}

	groups := make([]api.WebhookDeliveryGroup, 0, len(response.GetGroups()))
	for _, group := range response.GetGroups() {
		groupEventID, _ := uuid.Parse(group.GetEventId())

		attempts := make([]api.WebhookDelivery, 0, len(group.GetAttempts()))
		for _, attempt := range group.GetAttempts() {
			deliveryID, _ := uuid.Parse(attempt.GetDeliveryId())
			attemptTeamID, _ := uuid.Parse(attempt.GetTeamId())
			attemptWebhookID, _ := uuid.Parse(attempt.GetWebhookId())
			eventID, _ := uuid.Parse(attempt.GetEventId())

			status := api.WebhookDeliveryStatusFailed
			if attempt.GetStatus() == webhookmanagement.WebhookDeliveryStatus_WEBHOOK_DELIVERY_STATUS_SUCCESS {
				status = api.WebhookDeliveryStatusSuccess
			}

			attempts = append(attempts, api.WebhookDelivery{
				Id:                     deliveryID,
				TeamId:                 attemptTeamID,
				WebhookId:              attemptWebhookID,
				EventId:                eventID,
				SandboxId:              attempt.GetSandboxId(),
				EventType:              attempt.GetEventType(),
				Status:                 status,
				DurationMs:             attempt.GetDurationMs(),
				RequestBody:            attempt.GetRequestBody(),
				RequestHeaders:         attempt.GetRequestHeaders(),
				RequestUrl:             attempt.GetRequestUrl(),
				ResponseBody:           optionalString(attempt.HasResponseBody(), attempt.GetResponseBody()),
				ResponseHeaders:        optionalString(attempt.HasResponseHeaders(), attempt.GetResponseHeaders()),
				ResponseHttpStatusCode: optionalInt32(attempt.HasResponseHttpStatusCode(), attempt.GetResponseHttpStatusCode()),
				ErrorClass:             errorClass(attempt),
				ErrorMessage:           optionalString(attempt.HasErrorMessage(), attempt.GetErrorMessage()),
				Timestamp:              attempt.GetTimestamp().AsTime().UTC(),
			})
		}

		groups = append(groups, api.WebhookDeliveryGroup{
			EventId:   groupEventID,
			EventType: group.GetEventType(),
			SandboxId: group.GetSandboxId(),
			Attempts:  attempts,
		})
	}

	c.JSON(http.StatusOK, api.WebhookDeliveriesListPayload{
		Data:       groups,
		NextCursor: optionalString(response.HasNextCursor(), response.GetNextCursor()),
	})
}

func (a *APIStore) GetEventsWebhooksWebhookIDStats(
	c *gin.Context,
	webhookID api.WebhookID,
	params api.GetEventsWebhooksWebhookIDStatsParams,
) {
	ctx := c.Request.Context()

	teamID, clusterID, ok := a.webhooksBackend(c)
	if !ok {
		return
	}

	response, err := a.webhookManagement.GetWebhookDeliveryStats(
		ctx,
		webhookmanagement.GetWebhookDeliveryStatsRequest_builder{
			TeamId:    teamID.String(),
			ClusterId: clusterID,
			WebhookId: webhookID.String(),
			Start:     optionalTimestamp(params.Start),
			End:       optionalTimestamp(params.End),
		}.Build(),
	)
	if err != nil {
		a.sendWebhooksError(c, err, webhookStatsFallback)

		return
	}

	stats := response.GetStats()
	buckets := make([]api.WebhookDeliveryStatsBucket, 0, len(stats.GetBuckets()))
	for _, bucket := range stats.GetBuckets() {
		buckets = append(buckets, api.WebhookDeliveryStatsBucket{
			Timestamp:  bucket.GetTimestamp().AsTime().UTC(),
			Total:      bucket.GetTotal(),
			Failed:     bucket.GetFailed(),
			DurationMs: durationStats(bucket.GetDurationMs()),
		})
	}

	c.JSON(http.StatusOK, api.WebhookDeliveryStats{
		Buckets:    buckets,
		Total:      stats.GetTotal(),
		Failed:     stats.GetFailed(),
		DurationMs: durationStats(stats.GetDurationMs()),
	})
}

func durationStats(d *webhookmanagement.WebhookDeliveryDurationStats) api.WebhookDeliveryDurationStats {
	return api.WebhookDeliveryDurationStats{
		Minimum: d.GetMinimum(),
		Average: d.GetAverage(),
		Maximum: d.GetMaximum(),
	}
}

func deliveryStatus(value api.GetEventsWebhooksWebhookIDDeliveriesParamsDeliveryStatus) webhookmanagement.WebhookDeliveryStatus {
	switch value {
	case api.GetEventsWebhooksWebhookIDDeliveriesParamsDeliveryStatusSuccess:
		return webhookmanagement.WebhookDeliveryStatus_WEBHOOK_DELIVERY_STATUS_SUCCESS
	case api.GetEventsWebhooksWebhookIDDeliveriesParamsDeliveryStatusFailed:
		return webhookmanagement.WebhookDeliveryStatus_WEBHOOK_DELIVERY_STATUS_FAILED
	default:
		return webhookmanagement.WebhookDeliveryStatus_WEBHOOK_DELIVERY_STATUS_UNSPECIFIED
	}
}

func errorClass(attempt *webhookmanagement.WebhookDelivery) *api.WebhookDeliveryErrorClass {
	if !attempt.HasErrorClass() {
		return nil
	}

	var value api.WebhookDeliveryErrorClass
	switch attempt.GetErrorClass() {
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_HTTP_ERROR:
		value = api.HttpError
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_DNS_ERROR:
		value = api.DnsError
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_TIMEOUT:
		value = api.Timeout
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_TRANSPORT_ERROR:
		value = api.TransportError
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_REQUEST_ERROR:
		value = api.RequestError
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_SIGNATURE_ERROR:
		value = api.SignatureError
	case webhookmanagement.WebhookDeliveryErrorClass_WEBHOOK_DELIVERY_ERROR_CLASS_CANCELED:
		value = api.Canceled
	default:
		return nil
	}

	return &value
}

func optionalTimestamp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}

	return timestamppb.New(*value)
}

func optionalInt32(has bool, value int32) *int32 {
	if !has {
		return nil
	}

	return &value
}
