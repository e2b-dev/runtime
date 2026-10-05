package handlers

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	scpb "buf.build/gen/go/grpc/grpc/protocolbuffers/go/grpc/service_config"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health" // Registers the client side of healthCheckConfig.
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	managementv1 "github.com/e2b-dev/infra/packages/api/internal/secretsstore/management/v1"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/secretsstore"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// secretsBackendTimeout bounds one management call. A caller deadline that
	// is already shorter wins, since context.WithTimeout keeps the earlier one.
	secretsBackendTimeout  = 25 * time.Second
	secretLimitReachedCode = "secret_limit_reached"

	// secretsReadinessService is the health service a backend reports SERVING
	// only while it can take management calls.
	secretsReadinessService = "readiness"
)

// Client-facing messages. They are fixed and carry no request material: no
// selector, value, metadata, pagination token, or backend text ever reaches a
// client, a log, or a span through them.

// Internal errors recorded alongside those messages. They are constants for the
// same reason: whatever the backend said stays on the private hop.
var (
	errSecretsRequestInvalid  = errors.New("secrets request rejected before the backend call")
	errSecretsBackendCall     = errors.New("secrets backend call failed")
	errSecretsBackendResponse = errors.New("secrets backend returned an unusable response")
)

// newSecretsManagementClient dials nothing: grpc.NewClient only prepares the
// connection, so a backend that is down or not yet deployed cannot keep the API
// from starting. The hop is private, in-cluster and plaintext by decision, it
// carries no credential of any kind, and a call that may have reached the
// backend is never replayed by the transport. gRPC may still transparently
// retry a call that it knows never reached the server's application code.
//
// address is a gRPC target used as given. A plain "host:port" goes through
// the default DNS resolver. With a headless Service name every ready backend
// address becomes its own connection. The client resolves again whenever a
// connection closes, so a backend that rotates its connections by age lets it
// find replicas added later.
func newSecretsManagementClient(address string) (*grpc.ClientConn, error) {
	// Calls are spread over every resolved backend address, and only to a
	// backend whose readiness health service reports SERVING. A backend without
	// the health service is still picked: gRPC treats its UNIMPLEMENTED answer
	// as healthy. grpc-go takes a default service config only as JSON.
	serviceConfig, err := protojson.Marshal(&scpb.ServiceConfig{
		LoadBalancingConfig: []*scpb.LoadBalancingConfig{{Policy: &scpb.LoadBalancingConfig_RoundRobin{RoundRobin: &scpb.RoundRobinConfig{}}}},
		HealthCheckConfig:   &scpb.ServiceConfig_HealthCheckConfig{ServiceName: wrapperspb.String(secretsReadinessService)},
	})
	if err != nil {
		return nil, fmt.Errorf("encoding the secrets store management service config: %w", err)
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		// Turns off configured retries, whatever any service config says.
		grpc.WithDisableRetry(),
		// Keeps this caller's balancing and readiness policy: a resolver
		// cannot supply a service config that replaces it.
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(string(serviceConfig)),
	)
	if err != nil {
		return nil, fmt.Errorf("creating the secrets store management client: %w", err)
	}

	return conn, nil
}

// PostSecrets creates a secret with its first version.
func (a *APIStore) PostSecrets(c *gin.Context) {
	client, projectID, ok := a.secretsBackend(c)
	if !ok {
		return
	}

	var body api.PostSecretsJSONRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		a.rejectSecretsRequest(c, "secrets request body is not valid")

		return
	}

	name, err := secretsstore.NormalizeName(body.Name)
	if err != nil {
		a.rejectSecretsRequest(c, "secrets request carries an invalid name")

		return
	}

	value := []byte(body.Value)

	ctx, cancel := context.WithTimeout(c.Request.Context(), secretsBackendTimeout)
	defer cancel()

	response, err := client.CreateSecret(ctx, &managementv1.CreateSecretRequest{
		ProjectId: projectID,
		Name:      name,
		Value:     value,
		Metadata:  secretMetadataValues(body.Metadata),
	})
	if err != nil {
		a.sendSecretsBackendError(c, err)

		return
	}

	secret, ok := secretFromBackend(response.GetSecret())
	if !ok {
		a.sendSecretsBackendResponseError(c)

		return
	}

	c.JSON(http.StatusCreated, secret)
}

// GetSecrets lists the project's secrets.
func (a *APIStore) GetSecrets(c *gin.Context, params api.GetSecretsParams) {
	client, projectID, ok := a.secretsBackend(c)
	if !ok {
		return
	}

	// The backend owns the paging limits: it substitutes its default page
	// size for an absent limit and refuses an excessive one. The spec's
	// minimum keeps zero and negatives from ever reaching this handler.
	request := &managementv1.ListSecretsRequest{
		ProjectId: projectID,
	}

	if params.Limit != nil {
		request.Limit = *params.Limit
	}

	if params.NextToken != nil {
		request.NextToken = *params.NextToken
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), secretsBackendTimeout)
	defer cancel()

	response, err := client.ListSecrets(ctx, request)
	if err != nil {
		a.sendSecretsBackendError(c, err)

		return
	}

	secrets := make([]api.Secret, 0, len(response.GetSecrets()))
	for _, backendSecret := range response.GetSecrets() {
		secret, secretOK := secretFromBackend(backendSecret)
		if !secretOK {
			a.sendSecretsBackendResponseError(c)

			return
		}

		secrets = append(secrets, secret)
	}

	if nextToken := response.GetNextToken(); nextToken != "" {
		c.Header("X-Next-Token", nextToken)
	}

	c.JSON(http.StatusOK, secrets)
}

// GetSecretsSecretID returns one secret's metadata.
func (a *APIStore) GetSecretsSecretID(c *gin.Context, secretID api.SecretID) {
	client, projectID, ok := a.secretsBackend(c)
	if !ok {
		return
	}

	ref, ok := secretRef(secretID)
	if !ok {
		a.rejectSecretsRequest(c, "secrets request carries an invalid selector")

		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), secretsBackendTimeout)
	defer cancel()

	response, err := client.GetSecret(ctx, &managementv1.GetSecretRequest{
		ProjectId: projectID,
		Secret:    ref,
	})
	if err != nil {
		a.sendSecretsBackendError(c, err)

		return
	}

	secret, ok := secretFromBackend(response.GetSecret())
	if !ok {
		a.sendSecretsBackendResponseError(c)

		return
	}

	c.JSON(http.StatusOK, secret)
}

// PostSecretsSecretID replaces a secret's value by appending a version.
func (a *APIStore) PostSecretsSecretID(c *gin.Context, secretID api.SecretID) {
	client, projectID, ok := a.secretsBackend(c)
	if !ok {
		return
	}

	ref, ok := secretRef(secretID)
	if !ok {
		a.rejectSecretsRequest(c, "secrets request carries an invalid selector")

		return
	}

	var body api.PostSecretsSecretIDJSONRequestBody
	if err := c.ShouldBindJSON(&body); err != nil {
		a.rejectSecretsRequest(c, "secrets request body is not valid")

		return
	}

	value := []byte(body.Value)

	request := &managementv1.UpdateSecretRequest{
		ProjectId: projectID,
		Secret:    ref,
		Value:     value,
	}

	// An omitted metadata object preserves what the secret already carries; a
	// present one - including an empty object - replaces it.
	if body.Metadata != nil {
		request.Metadata = &managementv1.SecretMetadata{Values: secretMetadataValues(body.Metadata)}
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), secretsBackendTimeout)
	defer cancel()

	response, err := client.UpdateSecret(ctx, request)
	if err != nil {
		a.sendSecretsBackendError(c, err)

		return
	}

	secret, ok := secretFromBackend(response.GetSecret())
	if !ok {
		a.sendSecretsBackendResponseError(c)

		return
	}

	c.JSON(http.StatusOK, secret)
}

// DeleteSecretsSecretID revokes a secret.
func (a *APIStore) DeleteSecretsSecretID(c *gin.Context, secretID api.SecretID) {
	client, projectID, ok := a.secretsBackend(c)
	if !ok {
		return
	}

	ref, ok := secretRef(secretID)
	if !ok {
		a.rejectSecretsRequest(c, "secrets request carries an invalid selector")

		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), secretsBackendTimeout)
	defer cancel()

	if _, err := client.DeleteSecret(ctx, &managementv1.DeleteSecretRequest{
		ProjectId: projectID,
		Secret:    ref,
	}); err != nil {
		a.sendSecretsBackendError(c, err)

		return
	}

	c.Status(http.StatusNoContent)
}

// secretsBackend resolves the authenticated team into the trusted project the
// backend is asked about, and reports whether the routes are available at all.
// The gate is evaluated after authentication, and a missing backend address is
// indistinguishable from a closed gate, so an unauthenticated or ungated caller
// learns nothing about the rollout.
func (a *APIStore) secretsBackend(c *gin.Context) (managementv1.SecretManagementServiceClient, string, bool) {
	ctx := c.Request.Context()

	team, ok := auth.GetTeamInfo(c)
	if !ok || team == nil {
		a.sendAPIStoreError(c, http.StatusUnauthorized, middleware.SecretsUnauthorizedMessage)

		return nil, "", false
	}

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(team.ID.String()))

	if a.secretsManagement == nil || !a.featureFlags.BoolFlag(ctx, featureflags.CustomerSecretsFlag) {
		a.sendAPIStoreError(c, http.StatusForbidden, middleware.SecretsUnavailableMessage)

		return nil, "", false
	}

	// The project is the team under the name the backend uses. The UUID is
	// carried across unchanged; the public "prj_" spelling never goes on the
	// wire, and no tenant field a client sent is consulted.
	projectID := uuid.UUID(id.ConvertTeamIDToProjectID(team.ID)).String()

	return a.secretsManagement, projectID, true
}

// rejectSecretsRequest answers a request the API itself refused. reason is a
// fixed string chosen by the caller, never derived from the request.
func (a *APIStore) rejectSecretsRequest(c *gin.Context, reason string) {
	a.sendAPIStoreError(c, http.StatusBadRequest, middleware.SecretsInvalidRequestMessage)
	telemetry.ReportError(c.Request.Context(), reason, errSecretsRequestInvalid)
}

// sendSecretsBackendError maps a management failure onto the public status. The
// gRPC status description is never parsed, logged, or returned. Only its code
// and the typed quota detail determine the public response.
func (a *APIStore) sendSecretsBackendError(c *gin.Context, err error) {
	publicError := secretsBackendStatus(err)

	apierrors.SendAPIError(c, publicError)
	telemetry.ReportErrorByCode(c.Request.Context(), publicError.Code, "secrets backend call failed", errSecretsBackendCall,
		attribute.String("rpc.grpc.status_code", status.Code(err).String()),
	)
}

// sendSecretsBackendResponseError answers a response the API cannot trust. A
// partially converted secret is never returned.
func (a *APIStore) sendSecretsBackendResponseError(c *gin.Context) {
	a.sendAPIStoreError(c, http.StatusBadGateway, middleware.SecretsBackendMessage)
	telemetry.ReportCriticalError(c.Request.Context(), "secrets backend response was unusable", errSecretsBackendResponse)
}

// secretsBackendStatus is the whole public error contract of the private hop.
func secretsBackendStatus(err error) *apierrors.APIError {
	st, ok := status.FromError(err)
	if !ok {
		return &apierrors.APIError{Code: http.StatusBadGateway, ClientMsg: middleware.SecretsBackendMessage}
	}

	switch st.Code() {
	case codes.InvalidArgument:
		return &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: middleware.SecretsInvalidRequestMessage}
	case codes.NotFound:
		return &apierrors.APIError{Code: http.StatusNotFound, ClientMsg: middleware.SecretsNotFoundMessage}
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition:
		return &apierrors.APIError{Code: http.StatusConflict, ClientMsg: middleware.SecretsConflictMessage}
	case codes.ResourceExhausted:
		return secretsExhaustedStatus(st)
	case codes.DeadlineExceeded:
		return &apierrors.APIError{Code: http.StatusGatewayTimeout, ClientMsg: middleware.SecretsBackendTimeoutMessage}
	default:
		// Unavailable, Unimplemented, Internal, Unknown and everything else
		// the backend may grow are one outcome to a client: the backend did
		// not answer usefully.
		return &apierrors.APIError{Code: http.StatusBadGateway, ClientMsg: middleware.SecretsBackendMessage}
	}
}

// secretsExhaustedStatus splits RESOURCE_EXHAUSTED by its typed detail. The
// contract carries exactly one ManagementErrorDetail and nothing else, so a
// missing, repeated, foreign, unspecified or unrecognized detail is a response
// the API cannot act on: it is a malformed answer, not a lenient one. The
// status description stays private; only the typed, client-visible reason
// details for a secret limit error are returned.
func secretsExhaustedStatus(st *status.Status) *apierrors.APIError {
	details := st.Details()
	if len(details) != 1 {
		return &apierrors.APIError{Code: http.StatusBadGateway, ClientMsg: middleware.SecretsBackendMessage}
	}

	detail, ok := details[0].(*managementv1.ManagementErrorDetail)
	if !ok {
		return &apierrors.APIError{Code: http.StatusBadGateway, ClientMsg: middleware.SecretsBackendMessage}
	}

	switch detail.GetReason() {
	case managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED:
		publicError := &apierrors.APIError{
			Code:      http.StatusConflict,
			ClientMsg: "Project secret limit reached",
			ErrorCode: secretLimitReachedCode,
		}
		if reasonDetails := detail.GetReasonDetails(); reasonDetails != "" {
			publicError.ClientMsg = reasonDetails
		}

		return publicError
	case managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_VALUE_TOO_LARGE:
		return &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: middleware.SecretsInvalidRequestMessage}
	default:
		// Unspecified today, and whatever the contract grows tomorrow.
		return &apierrors.APIError{Code: http.StatusBadGateway, ClientMsg: middleware.SecretsBackendMessage}
	}
}

// secretRef builds the backend selector from one path segment. A selector that
// looks like an identifier is parsed as one and never falls back to a name, so
// "sec_" is reserved however it was written.
func secretRef(selector string) (*managementv1.SecretRef, bool) {
	// Classification is done on the trimmed, lowercased form, so " SEC_FOO "
	// is an identifier - an invalid one - rather than a name.
	classified := strings.ToLower(strings.TrimSpace(selector))

	// A canonical secret identifier starts with the secret kind's prefix and
	// its separator. The separator stays in the check: only "sec_" is
	// reserved, while plain names such as "secret-key" remain valid.
	if strings.HasPrefix(classified, id.SecretIDPrefix) {
		secretID, err := id.ParseSecretID(classified)
		if err != nil {
			return nil, false
		}

		return &managementv1.SecretRef{
			Ref: &managementv1.SecretRef_SecretId{SecretId: secretID.String()},
		}, true
	}

	name, err := secretsstore.NormalizeName(selector)
	if err != nil {
		return nil, false
	}

	return &managementv1.SecretRef{Ref: &managementv1.SecretRef_Name{Name: name}}, true
}

// secretMetadataValues copies an optional request metadata object into the map
// the backend expects. An omitted object is an empty map, which is what an
// omitted metadata means on create and what an explicit "{}" means on update.
func secretMetadataValues(metadata *api.SecretMetadata) map[string]string {
	values := make(map[string]string)
	if metadata == nil {
		return values
	}

	maps.Copy(values, *metadata)

	return values
}

// secretFromBackend converts a backend secret into the public one, defensively:
// anything missing, non-canonical or out of range makes the whole response
// unusable rather than a partially trusted answer.
func secretFromBackend(secret *managementv1.Secret) (api.Secret, bool) {
	if secret == nil {
		return api.Secret{}, false
	}

	secretID, err := id.ParseSecretID(secret.GetSecretId())
	if err != nil || secretID.String() != secret.GetSecretId() {
		return api.Secret{}, false
	}

	name, err := secretsstore.NormalizeName(secret.GetName())
	if err != nil || name != secret.GetName() {
		return api.Secret{}, false
	}

	if secret.GetCurrentVersion() <= 0 {
		return api.Secret{}, false
	}

	createdAt, ok := secretTimestamp(secret.GetCreatedAt())
	if !ok {
		return api.Secret{}, false
	}

	updatedAt, ok := secretTimestamp(secret.GetUpdatedAt())
	if !ok {
		return api.Secret{}, false
	}

	// Metadata is always present in a response, empty as "{}".
	metadata := make(api.SecretMetadata, len(secret.GetMetadata()))
	maps.Copy(metadata, secret.GetMetadata())

	return api.Secret{
		SecretID:       secret.GetSecretId(),
		Name:           name,
		CurrentVersion: secret.GetCurrentVersion(),
		Metadata:       metadata,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}, true
}

func secretTimestamp(timestamp *timestamppb.Timestamp) (time.Time, bool) {
	if !timestamp.IsValid() || timestamp.GetSeconds() <= 0 {
		return time.Time{}, false
	}

	return timestamp.AsTime().UTC(), true
}
