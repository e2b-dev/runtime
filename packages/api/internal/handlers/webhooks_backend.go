package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	webhookmanagement "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// newWebhooksClient dials nothing: grpc.NewClient only prepares the connection,
// so a backend that is down or not yet deployed cannot keep the API from
// starting.
func newWebhooksClient(address string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, webhooksDialOptions()...)
	if err != nil {
		return nil, fmt.Errorf("creating the webhooks backend client: %w", err)
	}

	return conn, nil
}

// webhooksDialOptions configures the connection to the backend. The hop is
// private, in-cluster and plaintext by decision, it carries no credential, and
// a call that may have reached the backend is never replayed by the transport.
//
// The receive limit is raised because gRPC's own default is 4 MiB and a valid
// deliveries page exceeds it; the call would otherwise fail with
// ResourceExhausted for a request the spec allows.
func webhooksDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxDeliveriesPageBytes)),
	}
}

// optionalClusterID projects a team's cluster onto the contract's optional
// cluster_id, whose omission selects the local cluster.
func optionalClusterID(id *uuid.UUID) *string {
	if id == nil {
		return nil
	}

	value := id.String()

	return &value
}

// webhooksMessages are the texts one route family answers a failed call with.
type webhooksMessages struct {
	// notFound names the resource that family serves, so a caller is told
	// which one was missing rather than whichever the mapping was written for.
	notFound string
	// fallback answers every outcome the caller cannot act on, so a backend
	// failure describes itself to this service's logs and not to its caller.
	fallback string
}

// sendWebhooksBackendError answers a failed call on either family. A status the
// caller can act on is reported against its own code; a 5xx is this service's
// problem, so it is reported as critical with the message the caller was given.
func (a *APIStore) sendWebhooksBackendError(c *gin.Context, err error, messages webhooksMessages) {
	code, message := webhooksBackendStatus(err, messages)

	a.sendAPIStoreError(c, code, message)

	if code >= http.StatusInternalServerError {
		telemetry.ReportCriticalError(c.Request.Context(), messages.fallback, err)

		return
	}

	telemetry.ReportErrorByCode(c.Request.Context(), code, message, err)
}

// webhooksBackendStatus is the whole public error contract of the private hop.
// Only the outcomes a caller can act on keep their identity.
func webhooksBackendStatus(err error, messages webhooksMessages) (int, string) {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, messages.fallback
	}

	switch grpcStatus.Code() {
	case codes.InvalidArgument:
		// A contract rejection carries its violations as a typed detail, and
		// the status message renders them as a multi-line dump. The rendered
		// form names the field the caller sent and reads as one line.
		if rendered, found := validationMessage(grpcStatus); found {
			return http.StatusBadRequest, rendered
		}

		return http.StatusBadRequest, grpcStatus.Message()
	case codes.NotFound:
		return http.StatusNotFound, messages.notFound
	case codes.ResourceExhausted:
		if webhookLimitReached(grpcStatus) {
			return http.StatusTooManyRequests, grpcStatus.Message()
		}

		return http.StatusInternalServerError, messages.fallback
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, messages.fallback
	default:
		return http.StatusInternalServerError, messages.fallback
	}
}

// webhookLimitReached distinguishes the one exhaustion the caller can act on,
// which the contract carries as a typed detail rather than in the description.
func webhookLimitReached(grpcStatus *status.Status) bool {
	const limitReached = webhookmanagement.
		WebhookManagementErrorReason_WEBHOOK_MANAGEMENT_ERROR_REASON_WEBHOOK_LIMIT_REACHED

	for _, detail := range grpcStatus.Details() {
		reason, ok := detail.(*webhookmanagement.WebhookManagementErrorDetail)
		if ok && reason.GetReason() == limitReached {
			return true
		}
	}

	return false
}
