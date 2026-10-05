package handlers

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	managementv1 "github.com/e2b-dev/infra/packages/api/internal/secretsstore/management/v1"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

// Conspicuously fake stand-ins for customer material. Assertions never print
// them, so a failure cannot publish what it was checking for.
const (
	sentinelSecretValue    = "sentinel-value-DO-NOT-LOG-0000"
	sentinelMetadataValue  = "sentinel-metadata-DO-NOT-LOG-0000"
	sentinelNextToken      = "sentinel-next-token-DO-NOT-LOG-0000"
	sentinelAPIKey         = "e2b_sentinel-api-key-DO-NOT-LOG"
	sentinelBearerToken    = "sentinel-bearer-token-DO-NOT-LOG"
	sentinelAdminToken     = "sentinel-admin-token-DO-NOT-LOG"
	sentinelCookie         = "session=sentinel-cookie-DO-NOT-LOG"
	sentinelBackendMessage = "backend detail: sentinel-secret-name-DO-NOT-LOG"
)

// requireNoSentinel fails without echoing what it looked for.
func requireNoSentinel(t *testing.T, what, haystack string, needles ...string) {
	t.Helper()

	for _, needle := range needles {
		if needle != "" && strings.Contains(haystack, needle) {
			t.Fatalf("%s leaked confidential request material", what)
		}
	}
}

// fakeSecretsBackend is the management server the API talks to over a real
// gRPC connection: real status codes, real details, real metadata.
type fakeSecretsBackend struct {
	managementv1.UnimplementedSecretManagementServiceServer

	mu sync.Mutex

	createRequests []*managementv1.CreateSecretRequest
	listRequests   []*managementv1.ListSecretsRequest
	getRequests    []*managementv1.GetSecretRequest
	updateRequests []*managementv1.UpdateSecretRequest
	deleteRequests []*managementv1.DeleteSecretRequest
	incoming       []metadata.MD

	// secret is what create, get and update answer with when err is nil.
	secret *managementv1.Secret
	// listSecrets and listNextToken make up the list answer.
	listSecrets   []*managementv1.Secret
	listNextToken string
	// err, when set, is returned by every method.
	err error
}

func (f *fakeSecretsBackend) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.incoming = append(f.incoming, md)
}

func (f *fakeSecretsBackend) CreateSecret(ctx context.Context, request *managementv1.CreateSecretRequest) (*managementv1.CreateSecretResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.record(ctx)
	f.createRequests = append(f.createRequests, request)

	if f.err != nil {
		return nil, f.err
	}

	return &managementv1.CreateSecretResponse{Secret: f.secret}, nil
}

func (f *fakeSecretsBackend) ListSecrets(ctx context.Context, request *managementv1.ListSecretsRequest) (*managementv1.ListSecretsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.record(ctx)
	f.listRequests = append(f.listRequests, request)

	if f.err != nil {
		return nil, f.err
	}

	return &managementv1.ListSecretsResponse{Secrets: f.listSecrets, NextToken: f.listNextToken}, nil
}

func (f *fakeSecretsBackend) GetSecret(ctx context.Context, request *managementv1.GetSecretRequest) (*managementv1.GetSecretResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.record(ctx)
	f.getRequests = append(f.getRequests, request)

	if f.err != nil {
		return nil, f.err
	}

	return &managementv1.GetSecretResponse{Secret: f.secret}, nil
}

func (f *fakeSecretsBackend) UpdateSecret(ctx context.Context, request *managementv1.UpdateSecretRequest) (*managementv1.UpdateSecretResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.record(ctx)
	f.updateRequests = append(f.updateRequests, request)

	if f.err != nil {
		return nil, f.err
	}

	return &managementv1.UpdateSecretResponse{Secret: f.secret}, nil
}

func (f *fakeSecretsBackend) DeleteSecret(ctx context.Context, request *managementv1.DeleteSecretRequest) (*managementv1.DeleteSecretResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.record(ctx)
	f.deleteRequests = append(f.deleteRequests, request)

	if f.err != nil {
		return nil, f.err
	}

	return &managementv1.DeleteSecretResponse{}, nil
}

func (f *fakeSecretsBackend) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.createRequests) + len(f.listRequests) + len(f.getRequests) + len(f.updateRequests) + len(f.deleteRequests)
}

func (f *fakeSecretsBackend) allIncoming() []metadata.MD {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]metadata.MD(nil), f.incoming...)
}

// backendSecret is a well-formed backend answer.
func backendSecret() *managementv1.Secret {
	return &managementv1.Secret{
		SecretId:       id.SecretID(uuid.New()).String(),
		Name:           "sentinel-name",
		CurrentVersion: 3,
		Metadata:       map[string]string{"env": sentinelMetadataValue},
		CreatedAt:      timestamppb.New(time.Unix(1700000000, 0).UTC()),
		UpdatedAt:      timestamppb.New(time.Unix(1700000100, 0).UTC()),
	}
}

// startSecretsBackend serves the fake over an in-process connection and returns
// the generated client the store uses in production.
func startSecretsBackend(t *testing.T, backend *fakeSecretsBackend) managementv1.SecretManagementServiceClient {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	managementv1.RegisterSecretManagementServiceServer(server, backend)

	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///secrets-backend",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})

	return managementv1.NewSecretManagementServiceClient(conn)
}

// newSecretsStore builds the minimal store the secret handlers need.
func newSecretsStore(t *testing.T, client managementv1.SecretManagementServiceClient, gateOpen bool) *APIStore {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.CustomerSecretsFlag.Key()).VariationForAll(gateOpen))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return &APIStore{featureFlags: ff, secretsManagement: client}
}

// newSecretsRequest builds an authenticated gin context carrying credentials
// the handler must never forward.
func newSecretsRequest(t *testing.T, method, target, body string) (*gin.Context, *httptest.ResponseRecorder, uuid.UUID) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}

	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), method, target, reader)
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	ginCtx.Request.Header.Set(sharedauth.HeaderAPIKey, sentinelAPIKey)
	ginCtx.Request.Header.Set(sharedauth.HeaderAuthorization, "Bearer "+sentinelBearerToken)
	ginCtx.Request.Header.Set(sharedauth.HeaderAdminToken, sentinelAdminToken)
	ginCtx.Request.Header.Set("Cookie", sentinelCookie)

	teamID := uuid.New()
	ginCtx.Request.Header.Set(sharedauth.HeaderTeamID, teamID.String())
	sharedauth.SetTeamInfoForTest(t, ginCtx, &types.Team{Team: &authqueries.Team{ID: teamID}})

	return ginCtx, recorder, teamID
}

func TestPostSecretsSendsOnlyDerivedProjectAndValue(t *testing.T) {
	t.Parallel()

	backend := &fakeSecretsBackend{secret: backendSecret()}
	store := newSecretsStore(t, startSecretsBackend(t, backend), true)

	body, err := json.Marshal(api.NewSecret{Name: "  My-Secret  ", Value: sentinelSecretValue})
	require.NoError(t, err)

	ginCtx, recorder, teamID := newSecretsRequest(t, http.MethodPost, "/secrets", string(body))
	store.PostSecrets(ginCtx)

	require.Equal(t, http.StatusCreated, recorder.Code)
	require.Equal(t, 1, backend.callCount())

	request := backend.createRequests[0]
	require.Equal(t, teamID.String(), request.GetProjectId(), "the project is the team UUID, unchanged")
	require.NotEqual(t, id.ConvertTeamIDToProjectID(teamID).String(), request.GetProjectId(), "the public prj_ spelling never goes on the wire")
	require.Equal(t, "my-secret", request.GetName(), "the name is canonicalized before it is sent")
	require.Empty(t, request.GetMetadata(), "omitted metadata is sent as empty")

	if string(request.GetValue()) != sentinelSecretValue {
		t.Fatal("the backend did not receive the value the caller sent")
	}

	// No caller credential and no client-asserted tenant reaches the hop.
	for _, md := range backend.allIncoming() {
		for _, forbidden := range []string{"x-api-key", "authorization", "x-admin-token", "x-team-id", "cookie"} {
			require.Emptyf(t, md.Get(forbidden), "the backend must not receive %s", forbidden)
		}

		for _, values := range md {
			for _, value := range values {
				requireNoSentinel(t, "backend metadata", value,
					sentinelAPIKey, sentinelBearerToken, sentinelAdminToken, sentinelCookie, sentinelSecretValue)
			}
		}
	}

	// The response is metadata only.
	var response map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.NotContains(t, response, "value")
	requireNoSentinel(t, "create response", recorder.Body.String(), sentinelSecretValue)
}

func TestSecretsResponseAlwaysCarriesMetadata(t *testing.T) {
	t.Parallel()

	secret := backendSecret()
	secret.Metadata = nil

	backend := &fakeSecretsBackend{secret: secret}
	store := newSecretsStore(t, startSecretsBackend(t, backend), true)

	ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets/"+secret.GetName(), "")
	store.GetSecretsSecretID(ginCtx, secret.GetName())

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"metadata":{}`)
}

func TestSecretsSelectorClassification(t *testing.T) {
	t.Parallel()

	secretID := id.SecretID(uuid.New()).String()

	tests := []struct {
		name       string
		selector   string
		wantStatus int
		wantID     string
		wantName   string
	}{
		{name: "canonical identifier", selector: secretID, wantStatus: http.StatusOK, wantID: secretID},
		{name: "identifier with padding and capitals", selector: "  " + strings.ToUpper(secretID) + " ", wantStatus: http.StatusOK, wantID: secretID},
		{name: "reserved prefix is never a name", selector: " SEC_FOO ", wantStatus: http.StatusBadRequest},
		{name: "malformed identifier", selector: "sec_not-a-real-identifier", wantStatus: http.StatusBadRequest},
		{name: "name with padding and capitals", selector: "  My-Secret  ", wantStatus: http.StatusOK, wantName: "my-secret"},
		{name: "name with an illegal character", selector: "my secret", wantStatus: http.StatusBadRequest},
		{name: "empty name", selector: "   ", wantStatus: http.StatusBadRequest},
		{name: "name too long", selector: strings.Repeat("a", 129), wantStatus: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeSecretsBackend{secret: backendSecret()}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets/selector", "")
			store.GetSecretsSecretID(ginCtx, test.selector)

			require.Equal(t, test.wantStatus, recorder.Code)

			if test.wantStatus != http.StatusOK {
				require.Zero(t, backend.callCount(), "a rejected selector never reaches the backend")
				requireNoSentinel(t, "selector rejection", recorder.Body.String(), test.selector)

				return
			}

			require.Len(t, backend.getRequests, 1)
			ref := backend.getRequests[0].GetSecret()

			if test.wantID != "" {
				require.Equal(t, test.wantID, ref.GetSecretId())
				require.Empty(t, ref.GetName())
			} else {
				require.Equal(t, test.wantName, ref.GetName())
				require.Empty(t, ref.GetSecretId())
			}
		})
	}
}

func TestGetSecretsListPagination(t *testing.T) {
	t.Parallel()

	limit := int32(25)
	nextToken := sentinelNextToken

	tests := []struct {
		name          string
		params        api.GetSecretsParams
		backendToken  string
		wantStatus    int
		wantLimit     int32
		wantToken     string
		wantHeaderSet bool
	}{
		{
			name:       "defaults",
			params:     api.GetSecretsParams{},
			wantStatus: http.StatusOK,
			wantLimit:  0,
		},
		{
			name:          "explicit limit and token",
			params:        api.GetSecretsParams{Limit: &limit, NextToken: &nextToken},
			backendToken:  sentinelNextToken,
			wantStatus:    http.StatusOK,
			wantLimit:     limit,
			wantToken:     sentinelNextToken,
			wantHeaderSet: true,
		},
		{
			name:       "excessive limit is forwarded for the backend to refuse",
			params:     api.GetSecretsParams{Limit: new(int32(101))},
			wantStatus: http.StatusOK,
			wantLimit:  101,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeSecretsBackend{
				listSecrets:   []*managementv1.Secret{backendSecret()},
				listNextToken: test.backendToken,
			}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets", "")
			store.GetSecrets(ginCtx, test.params)

			require.Equal(t, test.wantStatus, recorder.Code)

			if test.wantStatus != http.StatusOK {
				require.Zero(t, backend.callCount())
				requireNoSentinel(t, "list rejection", recorder.Body.String(), sentinelNextToken)

				return
			}

			require.Len(t, backend.listRequests, 1)
			request := backend.listRequests[0]
			require.Equal(t, test.wantLimit, request.GetLimit())
			require.Equal(t, test.wantToken, request.GetNextToken())

			if test.wantHeaderSet {
				require.Equal(t, sentinelNextToken, recorder.Header().Get("X-Next-Token"))
			} else {
				require.Empty(t, recorder.Header().Get("X-Next-Token"), "the terminal page carries no cursor")
			}

			var secrets []api.Secret
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &secrets))
			require.Len(t, secrets, 1)
		})
	}
}

func TestGetSecretsEmptyListIsAnEmptyArray(t *testing.T) {
	t.Parallel()

	backend := &fakeSecretsBackend{}
	store := newSecretsStore(t, startSecretsBackend(t, backend), true)

	ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets", "")
	store.GetSecrets(ginCtx, api.GetSecretsParams{})

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `[]`, recorder.Body.String())
}

func TestPostSecretsSecretIDMetadataPresence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantWrapper  bool
		wantMetadata map[string]string
	}{
		{name: "omitted metadata is preserved", body: `{"value":"` + sentinelSecretValue + `"}`, wantWrapper: false},
		{name: "empty metadata clears", body: `{"value":"v","metadata":{}}`, wantWrapper: true},
		{name: "present metadata replaces", body: `{"value":"v","metadata":{"env":"prod"}}`, wantWrapper: true, wantMetadata: map[string]string{"env": "prod"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeSecretsBackend{secret: backendSecret()}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			ginCtx, recorder, _ := newSecretsRequest(t, http.MethodPost, "/secrets/my-secret", test.body)
			store.PostSecretsSecretID(ginCtx, "my-secret")

			require.Equal(t, http.StatusOK, recorder.Code)
			require.Len(t, backend.updateRequests, 1)

			metadataWrapper := backend.updateRequests[0].GetMetadata()
			if !test.wantWrapper {
				require.Nil(t, metadataWrapper, "an omitted metadata object preserves what the secret carries")

				return
			}

			// A present wrapper is what tells the backend to replace; an empty
			// map inside it clears. Proto does not encode an empty map, so the
			// wrapper - not the map - carries the presence.
			require.NotNil(t, metadataWrapper)
			require.Equal(t, test.wantMetadata, requireMetadataOrNil(metadataWrapper.GetValues()))
		})
	}
}

func TestDeleteSecretsSecretIDReturnsNoContent(t *testing.T) {
	t.Parallel()

	backend := &fakeSecretsBackend{}
	store := newSecretsStore(t, startSecretsBackend(t, backend), true)

	ginCtx, recorder, _ := newSecretsRequest(t, http.MethodDelete, "/secrets/my-secret", "")
	store.DeleteSecretsSecretID(ginCtx, "my-secret")

	// The engine flushes the status after the handler returns, so the status the
	// handler set is read off the writer here; the routed request in
	// secrets_confidentiality_test.go asserts the wire status.
	require.Equal(t, http.StatusNoContent, ginCtx.Writer.Status())
	require.Empty(t, recorder.Body.String())
	require.Len(t, backend.deleteRequests, 1)
}

func TestSecretsValueForwardedUntouched(t *testing.T) {
	t.Parallel()

	// The API is a transport: the decoded value crosses the wire unchanged,
	// whatever its size. The backend owns the size limit and its rejection is
	// mapped by TestSecretsBackendErrorMapping.
	for _, size := range []int{64 * 1024, 64*1024 + 1} {
		backend := &fakeSecretsBackend{secret: backendSecret()}
		store := newSecretsStore(t, startSecretsBackend(t, backend), true)

		body, err := json.Marshal(api.NewSecret{Name: "my-secret", Value: strings.Repeat("a", size)})
		require.NoError(t, err)

		ginCtx, recorder, _ := newSecretsRequest(t, http.MethodPost, "/secrets", string(body))
		store.PostSecrets(ginCtx)

		require.Equal(t, http.StatusCreated, recorder.Code)
		require.Len(t, backend.createRequests, 1)
		require.Len(t, backend.createRequests[0].GetValue(), size)
	}
}

func TestSecretsBackendErrorMapping(t *testing.T) {
	t.Parallel()

	// exhausted builds a RESOURCE_EXHAUSTED carrying exactly the details given.
	exhausted := func(details ...protoadapt.MessageV1) error {
		st := status.New(codes.ResourceExhausted, sentinelBackendMessage)

		if len(details) == 0 {
			return st.Err()
		}

		withDetails, err := st.WithDetails(details...)
		require.NoError(t, err)

		return withDetails.Err()
	}

	reason := func(reason managementv1.ManagementErrorReason) protoadapt.MessageV1 {
		return &managementv1.ManagementErrorDetail{Reason: reason}
	}

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, sentinelBackendMessage), wantStatus: http.StatusBadRequest},
		{name: "not found", err: status.Error(codes.NotFound, sentinelBackendMessage), wantStatus: http.StatusNotFound},
		{name: "already exists", err: status.Error(codes.AlreadyExists, sentinelBackendMessage), wantStatus: http.StatusConflict},
		{name: "aborted", err: status.Error(codes.Aborted, sentinelBackendMessage), wantStatus: http.StatusConflict},
		{name: "failed precondition", err: status.Error(codes.FailedPrecondition, sentinelBackendMessage), wantStatus: http.StatusConflict},
		// RESOURCE_EXHAUSTED is the one code whose meaning comes from a typed
		// detail. Exactly one ManagementErrorDetail and nothing else is a
		// usable answer; everything else is malformed.
		{name: "one secret limit reached detail", err: exhausted(reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED)), wantStatus: http.StatusConflict},
		{name: "one value too large detail", err: exhausted(reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_VALUE_TOO_LARGE)), wantStatus: http.StatusBadRequest},
		{name: "value too large ignores reason details", err: exhausted(&managementv1.ManagementErrorDetail{Reason: managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_VALUE_TOO_LARGE, ReasonDetails: sentinelBackendMessage}), wantStatus: http.StatusBadRequest},
		{name: "no detail", err: exhausted(), wantStatus: http.StatusBadGateway},
		{name: "unspecified reason", err: exhausted(reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_UNSPECIFIED)), wantStatus: http.StatusBadGateway},
		{name: "unknown future reason", err: exhausted(reason(managementv1.ManagementErrorReason(4242))), wantStatus: http.StatusBadGateway},
		{
			name: "duplicate detail with the same reason",
			err: exhausted(
				reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED),
				reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED),
			),
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "duplicate detail with conflicting reasons",
			err: exhausted(
				reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED),
				reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_VALUE_TOO_LARGE),
			),
			wantStatus: http.StatusBadGateway,
		},
		{name: "detail of an unknown type", err: exhausted(timestamppb.New(time.Unix(1700000000, 0))), wantStatus: http.StatusBadGateway},
		{
			name: "known detail accompanied by a foreign one",
			err: exhausted(
				reason(managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_VALUE_TOO_LARGE),
				timestamppb.New(time.Unix(1700000000, 0)),
			),
			wantStatus: http.StatusBadGateway,
		},
		{name: "unavailable", err: status.Error(codes.Unavailable, sentinelBackendMessage), wantStatus: http.StatusBadGateway},
		{name: "unimplemented", err: status.Error(codes.Unimplemented, sentinelBackendMessage), wantStatus: http.StatusBadGateway},
		{name: "internal", err: status.Error(codes.Internal, sentinelBackendMessage), wantStatus: http.StatusBadGateway},
		{name: "unknown", err: status.Error(codes.Unknown, sentinelBackendMessage), wantStatus: http.StatusBadGateway},
		{name: "permission denied", err: status.Error(codes.PermissionDenied, sentinelBackendMessage), wantStatus: http.StatusBadGateway},
		{name: "deadline exceeded", err: status.Error(codes.DeadlineExceeded, sentinelBackendMessage), wantStatus: http.StatusGatewayTimeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeSecretsBackend{err: test.err}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets/my-secret", "")
			store.GetSecretsSecretID(ginCtx, "my-secret")

			require.Equal(t, test.wantStatus, recorder.Code)
			requireNoSentinel(t, "backend failure response", recorder.Body.String(), sentinelBackendMessage)

			for _, ginErr := range ginCtx.Errors {
				requireNoSentinel(t, "gin error", ginErr.Error(), sentinelBackendMessage)
			}
		})
	}
}

func TestSecretsLimitErrorResponse(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		reasonDetails string
		message       string
	}{
		{name: "effective limit", reasonDetails: "Project has reached its limit of 50 live secrets", message: "Project has reached its limit of 50 live secrets"},
		{name: "older backend", message: "Project secret limit reached"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backendStatus, err := status.New(codes.ResourceExhausted, sentinelBackendMessage).WithDetails(
				&managementv1.ManagementErrorDetail{
					Reason:        managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED,
					ReasonDetails: test.reasonDetails,
				},
			)
			require.NoError(t, err)
			backend := &fakeSecretsBackend{err: backendStatus.Err()}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			requestBody, err := json.Marshal(api.NewSecret{Name: "my-secret", Value: sentinelSecretValue})
			require.NoError(t, err)
			ginCtx, recorder, _ := newSecretsRequest(t, http.MethodPost, "/secrets", string(requestBody))
			store.PostSecrets(ginCtx)

			require.Equal(t, http.StatusConflict, recorder.Code)
			requireNoSentinel(t, "quota error response", recorder.Body.String(), sentinelBackendMessage, sentinelSecretValue)
			var body map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			want := map[string]any{
				"code":       float64(http.StatusConflict),
				"message":    test.message,
				"error_code": secretLimitReachedCode,
			}
			require.Equal(t, want, body)
		})
	}
}

func TestSecretsMalformedBackendResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*managementv1.Secret) *managementv1.Secret
	}{
		{name: "missing secret", mutate: func(*managementv1.Secret) *managementv1.Secret { return nil }},
		{name: "empty identifier", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.SecretId = ""

			return s
		}},
		{name: "identifier of another kind", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.SecretId = id.ProjectID(uuid.New()).String()

			return s
		}},
		{name: "non-canonical name", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.Name = "Not Canonical"

			return s
		}},
		{name: "empty name", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.Name = ""

			return s
		}},
		{name: "zero version", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.CurrentVersion = 0

			return s
		}},
		{name: "negative version", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.CurrentVersion = -1

			return s
		}},
		{name: "missing created at", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.CreatedAt = nil

			return s
		}},
		{name: "missing updated at", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.UpdatedAt = nil

			return s
		}},
		{name: "invalid timestamp", mutate: func(s *managementv1.Secret) *managementv1.Secret {
			s.UpdatedAt = &timestamppb.Timestamp{Seconds: -1, Nanos: -1}

			return s
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeSecretsBackend{secret: test.mutate(backendSecret())}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets/my-secret", "")
			store.GetSecretsSecretID(ginCtx, "my-secret")

			require.Equal(t, http.StatusBadGateway, recorder.Code)
			require.NotContains(t, recorder.Body.String(), "secretID", "no part of an unusable response is returned")
		})
	}
}

func TestSecretsMutationsAreAttemptedOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "ambiguous unavailable", err: status.Error(codes.Unavailable, sentinelBackendMessage)},
		{name: "deadline exceeded", err: status.Error(codes.DeadlineExceeded, sentinelBackendMessage)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeSecretsBackend{err: test.err}
			store := newSecretsStore(t, startSecretsBackend(t, backend), true)

			createCtx, _, _ := newSecretsRequest(t, http.MethodPost, "/secrets", `{"name":"my-secret","value":"v"}`)
			store.PostSecrets(createCtx)
			require.Len(t, backend.createRequests, 1, "a create is attempted exactly once")

			updateCtx, _, _ := newSecretsRequest(t, http.MethodPost, "/secrets/my-secret", `{"value":"v"}`)
			store.PostSecretsSecretID(updateCtx, "my-secret")
			require.Len(t, backend.updateRequests, 1, "an update is attempted exactly once")
		})
	}

	// Through the production client, a mutation that ran and then failed or
	// lost its answer never runs again on the other ready replica.
	for index, test := range []struct {
		name         string
		update, lose bool
	}{
		{name: "create unavailable after it ran"},
		{name: "update unavailable after it ran", update: true},
		{name: "create answer lost after it ran", lose: true},
		{name: "update answer lost after it ran", update: true, lose: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			replicas := make([]*secretsReplica, 2)
			for i := range replicas {
				var server *grpc.Server
				server = grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					response, err := handler(ctx, req)
					if _, get := req.(*managementv1.GetSecretRequest); get {
						return response, err
					}
					if test.lose {
						server.Stop() // Closes the connection before any answer is sent.
					}

					return nil, status.Error(codes.Unavailable, sentinelBackendMessage)
				}))
				replicas[i] = startSecretsReplica(t, server, true)
			}
			mutationResolvers[index].UpdateState(secretsAddresses(replicas...))
			client := dialSecretsManagement(t, mutationResolvers[index].Scheme()+":///backend")
			requireEveryReplicaServes(t, client, replicas...)
			served := func() int { return replicas[0].backend.callCount() + replicas[1].backend.callCount() }
			before := served()
			var err error
			if test.update {
				_, err = client.UpdateSecret(t.Context(), &managementv1.UpdateSecretRequest{})
			} else {
				_, err = client.CreateSecret(t.Context(), &managementv1.CreateSecretRequest{})
			}
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Equal(t, before+1, served(), "the mutation ran exactly once")
		})
	}
}

func TestSecretsUnavailableWithoutBackendOrGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configured bool
		gateOpen   bool
	}{
		{name: "no backend address", configured: false, gateOpen: true},
		{name: "closed gate", configured: true, gateOpen: false},
		{name: "neither", configured: false, gateOpen: false},
	}

	var bodies []string

	for _, test := range tests {
		backend := &fakeSecretsBackend{secret: backendSecret()}

		var client managementv1.SecretManagementServiceClient
		if test.configured {
			client = startSecretsBackend(t, backend)
		}

		store := newSecretsStore(t, client, test.gateOpen)

		ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets", "")
		store.GetSecrets(ginCtx, api.GetSecretsParams{})

		require.Equalf(t, http.StatusForbidden, recorder.Code, "%s must be forbidden", test.name)
		require.Zerof(t, backend.callCount(), "%s must not reach the backend", test.name)

		bodies = append(bodies, recorder.Body.String())
	}

	for _, body := range bodies[1:] {
		require.Equal(t, bodies[0], body, "a missing address and a closed gate are indistinguishable")
	}
}

func TestSecretsRequireAuthenticatedTeam(t *testing.T) {
	t.Parallel()

	backend := &fakeSecretsBackend{secret: backendSecret()}
	store := newSecretsStore(t, startSecretsBackend(t, backend), true)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/secrets", strings.NewReader(""))

	store.GetSecrets(ginCtx, api.GetSecretsParams{})

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.Zero(t, backend.callCount())
}

// requireMetadataOrNil normalizes an empty proto map so table expectations can
// stay nil for "no filter sent".
func requireMetadataOrNil(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}

	return metadata
}

func TestSecretsManagementClientIsLazyAndFailsFast(t *testing.T) {
	t.Parallel()

	// Nothing listens on this address: construction must still succeed and
	// stay idle, and a call must fail without waiting for a connection.
	conn, err := newSecretsManagementClient("127.0.0.1:1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	require.Equal(t, connectivity.Idle, conn.GetState(), "construction must not dial")

	store := newSecretsStore(t, managementv1.NewSecretManagementServiceClient(conn), true)

	ginCtx, recorder, _ := newSecretsRequest(t, http.MethodPost, "/secrets", `{"name":"my-secret","value":"v"}`)

	start := time.Now()
	store.PostSecrets(ginCtx)
	elapsed := time.Since(start)

	require.Equal(t, http.StatusBadGateway, recorder.Code)
	require.Less(t, elapsed, 5*time.Second, "the call must not wait for the connection to become ready")
}

func TestSecretsManagementClientFollowsReadyReplicas(t *testing.T) {
	t.Parallel()

	a, b, c := startSecretsReplica(t, grpc.NewServer(), true), startSecretsReplica(t, grpc.NewServer(), true), startSecretsReplica(t, grpc.NewServer(), true)
	endpointsResolver.UpdateState(secretsAddresses(a, b, c))
	client := dialSecretsManagement(t, endpointsResolver.Scheme()+":///backend")
	require.NoError(t, getSecrets(t, client, 1))
	// Honouring the resolver's pick_first would send every call to one replica.
	state := secretsAddresses(a, b, c)
	state.ServiceConfig = endpointsResolver.CC().ParseServiceConfig(`{"loadBalancingConfig":[{"pick_first":{}}]}`)
	require.NoError(t, state.ServiceConfig.Err)
	endpointsResolver.UpdateState(state)
	requireEveryReplicaServes(t, client, a, b, c)
	before := []int{a.backend.callCount(), b.backend.callCount(), c.backend.callCount()}
	require.NoError(t, getSecrets(t, client, 30))
	for index, replica := range []*secretsReplica{a, b, c} {
		require.InDelta(t, 10, replica.backend.callCount()-before[index], 2, "round robin over three ready replicas")
	}
	// NOT_SERVING on a live connection keeps new calls off c until SERVING. A
	// call running through rotation: TestNewGRPCServerWithMaxConnectionAge.
	c.health.SetServingStatus(secretsReadinessService, healthv1.HealthCheckResponse_NOT_SERVING)
	require.Eventually(t, func() bool {
		served := c.backend.callCount()

		return getSecrets(t, client, 6) == nil && c.backend.callCount() == served
	}, 10*time.Second, 10*time.Millisecond, "the unready replica stops receiving calls")
	c.health.SetServingStatus(secretsReadinessService, healthv1.HealthCheckResponse_SERVING)
	requireEveryReplicaServes(t, client, c)
	// A stopped replica is routed around for a full three-call rotation before its address goes.
	c.server.Stop()
	require.Eventually(t, func() bool { return getSecrets(t, client, 3) == nil }, 10*time.Second, 10*time.Millisecond)
	endpointsResolver.UpdateState(secretsAddresses(a, b))
	require.NoError(t, getSecrets(t, client, 3))
	// An empty address set fails fast, never at the 5 second call deadline.
	endpointsResolver.UpdateState(resolver.State{})
	require.Eventually(t, func() bool { return status.Code(getSecrets(t, client, 1)) == codes.Unavailable }, 10*time.Second, 10*time.Millisecond)
	added := startSecretsReplica(t, grpc.NewServer(), true)
	endpointsResolver.UpdateState(secretsAddresses(a, b, added))
	requireEveryReplicaServes(t, client, a, b, added)
}

func TestSecretsManagementClientFindsAddedReplicaAfterRotation(t *testing.T) {
	t.Parallel()

	// The client resolves again when a connection closes. This manual resolver says nothing about DNS.
	resolutions := rotationResolutions.Load()
	watchEnded := make(chan struct{})
	endWatch := sync.OnceFunc(func() { close(watchEnded) })
	aging := startSecretsReplica(t, grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 200 * time.Millisecond, MaxConnectionAgeGrace: time.Minute}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			defer endWatch() // The only stream is the health watch.

			return handler(srv, stream)
		}),
	), true)
	rotationResolver.UpdateState(secretsAddresses(aging))
	client := dialSecretsManagement(t, rotationResolver.Scheme()+":///backend")
	require.NoError(t, getSecrets(t, client, 1))
	added := startSecretsReplica(t, grpc.NewServer(), true)
	// The client cancels its health watch on GOAWAY instead of holding the aged
	// connection for the whole grace.
	receiveWithin(t, watchEnded, "the health watch on the aged connection to end")
	require.Eventually(t, func() bool { return rotationResolutions.Load() > resolutions }, 10*time.Second, 10*time.Millisecond, "the client resolves again after rotation")
	rotationResolver.UpdateState(secretsAddresses(aging, added))
	requireEveryReplicaServes(t, client, aging, added)
}

func TestSecretsManagementClientTargetsDeadlinesAndCancellation(t *testing.T) {
	t.Parallel()

	deadlines, ended := make(chan time.Time, 2), make(chan error, 2)
	replica := startSecretsReplica(t, grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, list := req.(*managementv1.ListSecretsRequest); list {
			return handler(ctx, req)
		}
		deadline, _ := ctx.Deadline()
		deadlines <- deadline
		<-ctx.Done()
		ended <- ctx.Err()

		return nil, status.FromContextError(ctx.Err()).Err()
	})), false)
	// Without a health service, UNIMPLEMENTED counts as healthy. Plain and schemed targets work as
	// given and connect before the short deadline below. An absent backend fails fast.
	var client managementv1.SecretManagementServiceClient
	for _, target := range []string{replica.address, "dns:///" + replica.address} {
		client = dialSecretsManagement(t, target)
		_, err := client.ListSecrets(t.Context(), &managementv1.ListSecretsRequest{})
		require.NoError(t, err)
	}
	require.Equal(t, codes.Unavailable, status.Code(getSecrets(t, dialSecretsManagement(t, "dns:///127.0.0.1:1"), 1)))
	store := newSecretsStore(t, client, true)
	// A caller deadline shorter than the API's own timeout reaches the backend.
	deadlineCtx, cancelDeadline := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancelDeadline()
	callerDeadline, _ := deadlineCtx.Deadline()
	ginCtx, recorder, _ := newSecretsRequest(t, http.MethodGet, "/secrets/my-secret", "")
	ginCtx.Request = ginCtx.Request.WithContext(deadlineCtx)
	store.GetSecretsSecretID(ginCtx, "my-secret")
	require.Equal(t, http.StatusGatewayTimeout, recorder.Code)
	require.WithinDuration(t, callerDeadline, receiveWithin(t, deadlines, "the backend handler"), 100*time.Millisecond)
	receiveWithin(t, ended, "the backend call to end")
	// Without one the API's timeout applies. A caller that goes away cancels the call.
	cancelCtx, cancel := context.WithCancel(t.Context())
	ginCtx, recorder, _ = newSecretsRequest(t, http.MethodGet, "/secrets/my-secret", "")
	ginCtx.Request = ginCtx.Request.WithContext(cancelCtx)
	done := make(chan struct{})
	go func() { store.GetSecretsSecretID(ginCtx, "my-secret"); close(done) }()
	require.WithinDuration(t, time.Now().Add(secretsBackendTimeout), receiveWithin(t, deadlines, "the backend handler"), secretsBackendTimeout/5)
	cancel()
	require.ErrorIs(t, receiveWithin(t, ended, "the backend call to end"), context.Canceled)
	receiveWithin(t, done, "the canceled call to return")
	require.Equal(t, http.StatusBadGateway, recorder.Code)
}

// Manual resolvers stand in for DNS. resolver.Register must not race client
// construction, so each test owns a scheme registered at package init.
var (
	rotationResolutions atomic.Int32
	endpointsResolver   = registerSecretsTestResolver("secrets-test-endpoints", nil)
	rotationResolver    = registerSecretsTestResolver("secrets-test-rotation", func() { rotationResolutions.Add(1) })
	mutationResolvers   = []*manual.Resolver{
		registerSecretsTestResolver("secrets-test-mutation-0", nil), registerSecretsTestResolver("secrets-test-mutation-1", nil),
		registerSecretsTestResolver("secrets-test-mutation-2", nil), registerSecretsTestResolver("secrets-test-mutation-3", nil),
	}
)

// registerSecretsTestResolver calls resolveNow, if set, on each re-resolution.
func registerSecretsTestResolver(scheme string, resolveNow func()) *manual.Resolver {
	r := manual.NewBuilderWithScheme(scheme)
	if resolveNow != nil {
		r.ResolveNowCallback = func(resolver.ResolveNowOptions) { resolveNow() }
	}
	resolver.Register(r)

	return r
}

type secretsReplica struct {
	backend *fakeSecretsBackend
	health  *health.Server
	server  *grpc.Server
	address string
}

// startSecretsReplica serves a fake backend on local TCP, with SERVING readiness if withHealth.
func startSecretsReplica(t *testing.T, server *grpc.Server, withHealth bool) *secretsReplica {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	replica := &secretsReplica{backend: &fakeSecretsBackend{secret: backendSecret()}, server: server, address: listener.Addr().String()}
	managementv1.RegisterSecretManagementServiceServer(server, replica.backend)
	if withHealth {
		replica.health = health.NewServer()
		replica.health.SetServingStatus(secretsReadinessService, healthv1.HealthCheckResponse_SERVING)
		healthv1.RegisterHealthServer(server, replica.health)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return replica
}

func secretsAddresses(replicas ...*secretsReplica) resolver.State {
	var state resolver.State
	for _, replica := range replicas {
		state.Addresses = append(state.Addresses, resolver.Address{Addr: replica.address})
	}

	return state
}

// dialSecretsManagement builds the production client, which must not dial yet.
func dialSecretsManagement(t *testing.T, target string) managementv1.SecretManagementServiceClient {
	t.Helper()

	conn, err := newSecretsManagementClient(target)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.Equal(t, connectivity.Idle, conn.GetState())

	return managementv1.NewSecretManagementServiceClient(conn)
}

// getSecrets makes sequential calls within 5 seconds and returns the first error.
func getSecrets(t *testing.T, client managementv1.SecretManagementServiceClient, calls int) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range calls {
		if _, err := client.GetSecret(ctx, &managementv1.GetSecretRequest{}); err != nil {
			return err
		}
	}

	return nil
}

// requireEveryReplicaServes waits until each replica has served a new call.
func requireEveryReplicaServes(t *testing.T, client managementv1.SecretManagementServiceClient, replicas ...*secretsReplica) {
	t.Helper()

	before := map[*secretsReplica]int{}
	for _, replica := range replicas {
		before[replica] = replica.backend.callCount()
	}
	require.Eventually(t, func() bool {
		_ = getSecrets(t, client, 1)

		return !slices.ContainsFunc(replicas, func(r *secretsReplica) bool { return r.backend.callCount() == before[r] })
	}, 10*time.Second, 10*time.Millisecond, "every replica serves a call")
}

func receiveWithin[T any](t *testing.T, values <-chan T, what string) (value T) {
	t.Helper()

	select {
	case value = <-values:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}

	return value
}
