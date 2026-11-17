package handlers

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	custommiddleware "github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	managementv1 "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/secretsstore/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	secretsCreateTeamAKey = "e2b_secrets-create-team-a"
	secretsCreateTeamBKey = "e2b_secrets-create-team-b"
	secretsCreateRoute    = "POST /secrets"
	secretsCreateBody     = `{"name":"api-token","value":"` + sentinelSecretValue + `","metadata":{"env":"prod","owner":"ci"}}`
)

// secretsCreateIdempotencyBackend creates secrets the way the management
// backend does: a name is created once per project and a second create of it
// is a conflict. Failures can be injected before or after the create commits.
type secretsCreateIdempotencyBackend struct {
	managementv1.UnimplementedSecretManagementServiceServer

	mu      sync.Mutex
	creates int
	secrets map[string]*managementv1.Secret

	failBefore     error
	failAfter      error
	malformedAfter bool
	waitAfter      bool
	entered        chan struct{}
	release        chan struct{}
}

func (b *secretsCreateIdempotencyBackend) CreateSecret(ctx context.Context, request *managementv1.CreateSecretRequest) (*managementv1.CreateSecretResponse, error) {
	b.mu.Lock()
	b.creates++
	entered, release, waitAfter := b.entered, b.release, b.waitAfter
	secret, err := b.create(request)
	b.mu.Unlock()

	if entered != nil {
		entered <- struct{}{}
		<-release
	}
	if waitAfter && err == nil {
		<-ctx.Done()

		return nil, status.FromContextError(ctx.Err()).Err()
	}

	return secret, err
}

func (b *secretsCreateIdempotencyBackend) create(request *managementv1.CreateSecretRequest) (*managementv1.CreateSecretResponse, error) {
	if b.failBefore != nil {
		return nil, b.failBefore
	}

	key := request.GetProjectId() + "/" + request.GetName()
	if _, exists := b.secrets[key]; exists {
		return nil, status.Error(codes.AlreadyExists, sentinelBackendMessage)
	}

	now := timestamppb.New(time.Now().UTC().Truncate(time.Millisecond))
	secret := &managementv1.Secret{
		SecretId:       id.SecretID(uuid.New()).String(),
		Name:           request.GetName(),
		CurrentVersion: 1,
		Metadata:       maps.Clone(request.GetMetadata()),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	b.secrets[key] = secret

	if b.failAfter != nil {
		return nil, b.failAfter
	}
	if b.malformedAfter {
		return &managementv1.CreateSecretResponse{}, nil
	}

	return &managementv1.CreateSecretResponse{Secret: secret}, nil
}

func (b *secretsCreateIdempotencyBackend) createCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.creates
}

func (b *secretsCreateIdempotencyBackend) restore() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failBefore, b.failAfter, b.malformedAfter, b.waitAfter = nil, nil, false, false
}

func (b *secretsCreateIdempotencyBackend) deleteAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	clear(b.secrets)
}

type secretsCreateIdempotencyHarness struct {
	router  *gin.Engine
	redis   redis.UniversalClient
	backend *secretsCreateIdempotencyBackend
	flags   *ldtestdata.TestDataSource
	teamA   *types.Team
	teamB   *types.Team
	authErr *auth.APIError
}

// newSecretsCreateIdempotencyHarness serves the generated routes behind the
// production order of request deadline, schema and API key validation,
// blocked-team check and idempotency middleware, with real Redis and a real
// gRPC connection to the backend.
func newSecretsCreateIdempotencyHarness(t *testing.T, requestTimeout time.Duration) *secretsCreateIdempotencyHarness {
	t.Helper()

	h := &secretsCreateIdempotencyHarness{
		redis:   redisutils.SetupInstance(t),
		backend: &secretsCreateIdempotencyBackend{secrets: make(map[string]*managementv1.Secret)},
		flags:   ldtestdata.DataSource(),
		teamA:   newSecretsCreateTeam(),
		teamB:   newSecretsCreateTeam(),
	}
	h.flags.Update(h.flags.Flag(featureflags.CustomerSecretsFlag.Key()).VariationForAll(true))
	h.enableRoute(true)
	flags, err := featureflags.NewClientWithDatasource(h.flags)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil

	authenticate := auth.CreateAuthenticationFunc([]auth.Authenticator{
		auth.NewApiKeyAuthenticator(func(_ context.Context, _ *gin.Context, key string) (*types.Team, *auth.APIError) {
			if h.authErr != nil {
				return nil, h.authErr
			}
			switch key {
			case secretsCreateTeamAKey:
				return h.teamA, nil
			case secretsCreateTeamBKey:
				return h.teamB, nil
			default:
				return nil, &auth.APIError{Code: http.StatusUnauthorized, Err: errors.New("unknown key"), ClientMsg: "Invalid API key"}
			}
		}),
	}, nil)

	r := gin.New()
	r.Use(gin.Recovery(), sharedmiddleware.RequestTimeout(requestTimeout))
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(swagger, &ginmiddleware.Options{
		DiscardValidatedValue: true,
		ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), fallbackStatusCode))
		},
		MultiErrorHandler: utils.MultiErrorHandler,
		Options: openapi3filter.Options{
			AuthenticationFunc: custommiddleware.WithAPIGroup(authenticate),
			MultiError:         true,
		},
	}))
	r.Use(custommiddleware.EnforceBlockedTeam(), idempotency.Middleware(h.redis, flags))

	store := &APIStore{featureFlags: flags, secretsManagement: startSecretsCreateIdempotencyBackend(t, h.backend)}
	api.RegisterHandlersWithOptions(r, store, api.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, statusCode int) {
			utils.ErrorHandler(c, err.Error(), statusCode)
		},
	})
	h.router = r

	return h
}

func newSecretsCreateTeam() *types.Team {
	team := minimalTeamInfo()
	team.Limits = &types.TeamLimits{MaxSecrets: 10}

	return team
}

func startSecretsCreateIdempotencyBackend(t *testing.T, backend managementv1.SecretManagementServiceServer) managementv1.SecretManagementServiceClient {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	managementv1.RegisterSecretManagementServiceServer(server, backend)
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///secrets-backend",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
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

func (h *secretsCreateIdempotencyHarness) enableRoute(enabled bool) {
	h.flags.Update(h.flags.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{secretsCreateRoute: enabled})))
}

func (h *secretsCreateIdempotencyHarness) post(t *testing.T, target, body, apiKey string, keys ...string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.HeaderAPIKey, apiKey)
	for _, key := range keys {
		req.Header.Add("Idempotency-Key", key)
	}
	recorder := httptest.NewRecorder()
	h.router.ServeHTTP(recorder, req)

	return recorder
}

// record returns the idempotency hash stored for the team's key. The test
// client namespaces keyed commands only, so the record key is derived rather
// than scanned for.
func (h *secretsCreateIdempotencyHarness) record(t *testing.T, team *types.Team, key string) map[string]string {
	t.Helper()

	digest := sha256.Sum256([]byte(key))
	record, err := h.redis.HGetAll(t.Context(), "api:idempotency:"+team.ID.String()+":"+hex.EncodeToString(digest[:])).Result()
	require.NoError(t, err)

	return record
}

// requireSecretsCreateRecordHidesValue checks that the stored record carries
// the request only as a digest and the response only as metadata.
func requireSecretsCreateRecordHidesValue(t *testing.T, h *secretsCreateIdempotencyHarness, team *types.Team, key, value string) {
	t.Helper()

	record := h.record(t, team, key)
	require.NotEmpty(t, record["response"])
	require.Regexp(t, `^[0-9a-f]{64}$`, record["fingerprint"])
	for _, stored := range record {
		requireNoSentinel(t, "idempotency record", stored, value, base64.StdEncoding.EncodeToString([]byte(value)))
	}
	var response struct {
		Body []byte `json:"body"`
	}
	require.NoError(t, json.Unmarshal([]byte(record["response"]), &response))
	requireNoSentinel(t, "cached response", string(response.Body), value)
}

func requireSecretsCreateReplay(t *testing.T, original, replay *httptest.ResponseRecorder) {
	t.Helper()

	require.Equal(t, original.Code, replay.Code, replay.Body.String())
	require.Equal(t, original.Body.String(), replay.Body.String())
	assert.Equal(t, original.Header().Get("Content-Type"), replay.Header().Get("Content-Type"))
	assert.Equal(t, original.Header().Values(apierrors.ShouldRetryHeader), replay.Header().Values(apierrors.ShouldRetryHeader))
	assert.Equal(t, "no-store", replay.Header().Get("Cache-Control"))
}

func requireSecretsCreateIdempotencyError(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int, errorCode, shouldRetry string) {
	t.Helper()

	require.Equal(t, wantStatus, recorder.Code, recorder.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	assert.Equal(t, errorCode, body["error_code"])
	assert.Equal(t, shouldRetry, recorder.Header().Get(apierrors.ShouldRetryHeader))
}

func TestPostSecretsIdempotencyReplaysFirstVersion(t *testing.T) {
	t.Parallel()

	h := newSecretsCreateIdempotencyHarness(t, time.Minute)

	first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "create-once")
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	var created api.Secret
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &created))
	assert.Equal(t, int64(1), created.CurrentVersion)
	assert.Equal(t, api.SecretMetadata{"env": "prod", "owner": "ci"}, created.Metadata)
	assert.Empty(t, first.Header().Values(apierrors.ShouldRetryHeader))
	require.Equal(t, 1, h.backend.createCount())

	reordered := "{ \"metadata\" : {\"owner\":\"ci\", \"env\":\"prod\"},\n \"value\":\"" + sentinelSecretValue + "\", \"name\":\"api-token\" }"
	requireSecretsCreateReplay(t, first, h.post(t, "/secrets", reordered, secretsCreateTeamAKey, "create-once"))
	requireSecretsCreateReplay(t, first, h.post(t, "/secrets", `{"ignored":true,`+secretsCreateBody[1:], secretsCreateTeamAKey, "create-once"))
	require.Equal(t, 1, h.backend.createCount(), "a replay never reaches the backend")

	h.backend.deleteAll()
	requireSecretsCreateReplay(t, first, h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "create-once"))
	require.Equal(t, 1, h.backend.createCount(), "a replay outlives the secret it created")

	requireSecretsCreateRecordHidesValue(t, h, h.teamA, "create-once", sentinelSecretValue)
}

func TestPostSecretsIdempotencyReplaysInsteadOfNameConflict(t *testing.T) {
	t.Parallel()

	h := newSecretsCreateIdempotencyHarness(t, time.Minute)

	first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "create-once")
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	requireSecretsCreateReplay(t, first, h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "create-once"))

	fresh := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "another-key")
	require.Equal(t, http.StatusConflict, fresh.Code, "a fresh key runs the create again and meets the existing name")
	unkeyed := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey)
	require.Equal(t, http.StatusConflict, unkeyed.Code)
	require.Equal(t, 3, h.backend.createCount())
}

// The value is not part of the fingerprint, so a stored record cannot be used
// to test guesses of it and a changed value replays the first result.
func TestPostSecretsIdempotencyFingerprintExcludesValue(t *testing.T) {
	t.Parallel()

	h := newSecretsCreateIdempotencyHarness(t, time.Minute)
	changed := strings.Replace(secretsCreateBody, sentinelSecretValue, sentinelSecretValue+"-2", 1)

	first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "create-once")
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	requireSecretsCreateReplay(t, first, h.post(t, "/secrets", changed, secretsCreateTeamAKey, "create-once"))
	require.Equal(t, 1, h.backend.createCount(), "a changed value is replayed, not executed")

	other := h.post(t, "/secrets", changed, secretsCreateTeamAKey, "another-key")
	require.Equal(t, http.StatusConflict, other.Code, other.Body.String())
	require.Equal(t, 2, h.backend.createCount())

	assert.Equal(t, h.record(t, h.teamA, "create-once")["fingerprint"], h.record(t, h.teamA, "another-key")["fingerprint"],
		"requests that differ only in value share a fingerprint")
}

func TestPostSecretsIdempotencyMismatchedRequests(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		first, second string
		target        string
	}{
		{name: "changed name", first: secretsCreateBody, second: strings.Replace(secretsCreateBody, "api-token", "api-token-2", 1)},
		{name: "name in another case", first: secretsCreateBody, second: strings.Replace(secretsCreateBody, "api-token", "API-Token", 1)},
		{name: "changed metadata value", first: secretsCreateBody, second: strings.Replace(secretsCreateBody, `"prod"`, `"dev"`, 1)},
		{name: "added metadata entry", first: secretsCreateBody, second: strings.Replace(secretsCreateBody, `"owner":"ci"`, `"owner":"ci","team":"x"`, 1)},
		{name: "omitted metadata", first: secretsCreateBody, second: `{"name":"api-token","value":"` + sentinelSecretValue + `"}`},
		{name: "empty metadata after omitted", first: `{"name":"api-token","value":"v"}`, second: `{"name":"api-token","value":"v","metadata":{}}`},
		{name: "omitted metadata after empty", first: `{"name":"api-token","value":"v","metadata":{}}`, second: `{"name":"api-token","value":"v"}`},
		{name: "query string", first: secretsCreateBody, second: secretsCreateBody, target: "/secrets?attempt=2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := newSecretsCreateIdempotencyHarness(t, time.Minute)
			first := h.post(t, "/secrets", test.first, secretsCreateTeamAKey, "same-key")
			require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

			mismatch := h.post(t, cmp.Or(test.target, "/secrets"), test.second, secretsCreateTeamAKey, "same-key")
			requireSecretsCreateIdempotencyError(t, mismatch, http.StatusConflict, "idempotency_request_mismatch", "false")
			require.Equal(t, 1, h.backend.createCount(), "a mismatched request is not executed")

			requireSecretsCreateReplay(t, first, h.post(t, "/secrets", test.first, secretsCreateTeamAKey, "same-key"))
		})
	}
}

func TestPostSecretsIdempotencyRejectsMalformedKeys(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		keys []string
	}{
		{name: "uppercase", keys: []string{"Create-Once"}},
		{name: "underscore", keys: []string{"create_once"}},
		{name: "empty", keys: []string{""}},
		{name: "too long", keys: []string{strings.Repeat("a", 256)}},
		{name: "repeated", keys: []string{"create-once", "create-twice"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := newSecretsCreateIdempotencyHarness(t, time.Minute)
			rejected := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, test.keys...)
			require.Equal(t, http.StatusBadRequest, rejected.Code, rejected.Body.String())
			requireNoSentinel(t, "rejection", rejected.Body.String(), sentinelSecretValue)
			require.Zero(t, h.backend.createCount())
			require.Empty(t, h.record(t, h.teamA, test.keys[0]))
		})
	}
}

func TestPostSecretsIdempotencyConcurrentDuplicates(t *testing.T) {
	t.Parallel()

	h := newSecretsCreateIdempotencyHarness(t, time.Minute)
	h.backend.entered = make(chan struct{})
	h.backend.release = make(chan struct{})

	owner := make(chan *httptest.ResponseRecorder, 1)
	go func() { owner <- h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "concurrent") }()
	receiveWithin(t, h.backend.entered, "the owner reaching the backend")

	duplicates := make([]*httptest.ResponseRecorder, 4)
	var wg sync.WaitGroup
	for i := range duplicates {
		wg.Go(func() { duplicates[i] = h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "concurrent") })
	}
	wg.Wait()
	for _, duplicate := range duplicates {
		requireSecretsCreateIdempotencyError(t, duplicate, http.StatusConflict, "idempotency_in_progress", "true")
	}
	require.Equal(t, 1, h.backend.createCount())

	close(h.backend.release)
	first := receiveWithin(t, owner, "the owner's response")
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	requireSecretsCreateReplay(t, first, h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "concurrent"))
	require.Equal(t, 1, h.backend.createCount(), "five requests made one create")
}

func TestPostSecretsIdempotencyRetainsFailedAttempts(t *testing.T) {
	t.Parallel()

	limitReached, err := status.New(codes.ResourceExhausted, sentinelBackendMessage).WithDetails(&managementv1.ManagementErrorDetail{
		Reason: managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_SECRET_LIMIT_REACHED,
	})
	require.NoError(t, err)
	valueTooLarge, err := status.New(codes.ResourceExhausted, sentinelBackendMessage).WithDetails(&managementv1.ManagementErrorDetail{
		Reason: managementv1.ManagementErrorReason_MANAGEMENT_ERROR_REASON_VALUE_TOO_LARGE,
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name           string
		prepare        func(*secretsCreateIdempotencyBackend)
		wantStatus     int
		wantRetry      string
		committed      bool
		requestTimeout time.Duration
	}{
		{
			name: "unavailable after the create committed",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.Unavailable, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway, committed: true,
		},
		{
			name: "deadline exceeded after the create committed",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.DeadlineExceeded, sentinelBackendMessage)
			},
			wantStatus: http.StatusGatewayTimeout, committed: true,
		},
		{
			name:       "request deadline passed while the committed create was unanswered",
			prepare:    func(b *secretsCreateIdempotencyBackend) { b.waitAfter = true },
			wantStatus: http.StatusGatewayTimeout, committed: true, requestTimeout: time.Second,
		},
		{
			name: "conflict after the create committed",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.Aborted, sentinelBackendMessage)
			},
			wantStatus: http.StatusConflict, committed: true,
		},
		{
			name:       "unusable answer after the create committed",
			prepare:    func(b *secretsCreateIdempotencyBackend) { b.malformedAfter = true },
			wantStatus: http.StatusBadGateway, committed: true,
		},
		{
			name: "missing secret during activation",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.NotFound, sentinelBackendMessage)
			},
			wantStatus: http.StatusNotFound, committed: true,
		},
		{
			name: "corrupt projection after activation",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.FailedPrecondition, sentinelBackendMessage)
			},
			wantStatus: http.StatusConflict, committed: true,
		},
		{
			name: "internal error after activation",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.Internal, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway, committed: true,
		},
		{
			name: "untyped exhaustion after publication",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.ResourceExhausted, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway, committed: true,
		},
		{
			name: "unavailable without a write",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failBefore = status.Error(codes.Unavailable, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "invalid request",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failBefore = status.Error(codes.InvalidArgument, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadRequest, wantRetry: "false",
		},
		{
			name:       "secret limit reached",
			prepare:    func(b *secretsCreateIdempotencyBackend) { b.failBefore = limitReached.Err() },
			wantStatus: http.StatusConflict, wantRetry: "false",
		},
		{
			name:       "value too large",
			prepare:    func(b *secretsCreateIdempotencyBackend) { b.failBefore = valueTooLarge.Err() },
			wantStatus: http.StatusBadRequest, wantRetry: "false",
		},
		{
			name: "existing name",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failBefore = status.Error(codes.AlreadyExists, sentinelBackendMessage)
			},
			wantStatus: http.StatusConflict, wantRetry: "false",
		},
		{
			name: "backend authorization",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failBefore = status.Error(codes.PermissionDenied, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway, wantRetry: "false",
		},
		{
			name: "backend authentication",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failBefore = status.Error(codes.Unauthenticated, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway, wantRetry: "false",
		},
		{
			name: "unsupported response compression",
			prepare: func(b *secretsCreateIdempotencyBackend) {
				b.failAfter = status.Error(codes.Unimplemented, sentinelBackendMessage)
			},
			wantStatus: http.StatusBadGateway, committed: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := newSecretsCreateIdempotencyHarness(t, cmp.Or(test.requestTimeout, time.Minute))
			test.prepare(h.backend)

			first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "failed-once")
			require.Equal(t, test.wantStatus, first.Code, first.Body.String())
			assert.Equal(t, test.wantRetry, first.Header().Get(apierrors.ShouldRetryHeader))
			requireNoSentinel(t, "failure response", first.Body.String(), sentinelBackendMessage, sentinelSecretValue)

			h.backend.restore()
			requireSecretsCreateReplay(t, first, h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "failed-once"))
			require.Equal(t, 1, h.backend.createCount(), "the failed attempt is replayed, never repeated")

			changed := strings.Replace(secretsCreateBody, sentinelSecretValue, "other-value", 1)
			requireSecretsCreateReplay(t, first, h.post(t, "/secrets", changed, secretsCreateTeamAKey, "failed-once"))
			require.Equal(t, 1, h.backend.createCount(), "a changed value replays the failed attempt")
			requireSecretsCreateRecordHidesValue(t, h, h.teamA, "failed-once", sentinelSecretValue)

			fresh := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "next-key")
			if test.committed {
				require.Equal(t, http.StatusConflict, fresh.Code, "the first attempt did create the secret")
			} else {
				require.Equal(t, http.StatusCreated, fresh.Code, fresh.Body.String())
			}
			require.Equal(t, 2, h.backend.createCount())
		})
	}
}

func TestPostSecretsIdempotencyAdmissionPrecedesReplay(t *testing.T) {
	t.Parallel()

	t.Run("closed gate keeps the key unused", func(t *testing.T) {
		t.Parallel()

		h := newSecretsCreateIdempotencyHarness(t, time.Minute)
		h.flags.Update(h.flags.Flag(featureflags.CustomerSecretsFlag.Key()).VariationForAll(false))
		refused := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "gated")
		require.Equal(t, http.StatusForbidden, refused.Code, refused.Body.String())
		assert.Empty(t, refused.Header().Values(apierrors.ShouldRetryHeader))
		require.Zero(t, h.backend.createCount())
		require.Empty(t, h.record(t, h.teamA, "gated"))

		h.flags.Update(h.flags.Flag(featureflags.CustomerSecretsFlag.Key()).VariationForAll(true))
		created := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "gated")
		require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
		require.Equal(t, 1, h.backend.createCount())
	})

	t.Run("invalid credentials never replay", func(t *testing.T) {
		t.Parallel()

		h := newSecretsCreateIdempotencyHarness(t, time.Minute)
		first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "shared-key")
		require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

		unauthenticated := h.post(t, "/secrets", secretsCreateBody, "e2b_unknown", "shared-key")
		require.Equal(t, http.StatusUnauthorized, unauthenticated.Code, unauthenticated.Body.String())
		assert.Empty(t, unauthenticated.Header().Values(apierrors.ShouldRetryHeader))
		require.NotContains(t, unauthenticated.Body.String(), "secretID")
		require.Equal(t, 1, h.backend.createCount())
	})

	t.Run("another team's key is its own", func(t *testing.T) {
		t.Parallel()

		h := newSecretsCreateIdempotencyHarness(t, time.Minute)
		first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "shared-key")
		require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

		other := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamBKey, "shared-key")
		require.Equal(t, http.StatusCreated, other.Code, other.Body.String())
		require.NotEqual(t, first.Body.String(), other.Body.String())
		require.Equal(t, 2, h.backend.createCount())
		requireSecretsCreateReplay(t, first, h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "shared-key"))
		requireSecretsCreateReplay(t, other, h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamBKey, "shared-key"))
		require.Equal(t, 2, h.backend.createCount())
	})

	for _, test := range []struct {
		name     string
		disabled bool
		keys     []string
	}{
		{name: "disabled route keeps the original behavior", disabled: true, keys: []string{"unused-key"}},
		{name: "missing key keeps the original behavior"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			h := newSecretsCreateIdempotencyHarness(t, time.Minute)
			h.enableRoute(!test.disabled)
			first := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, test.keys...)
			require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
			second := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, test.keys...)
			require.Equal(t, http.StatusConflict, second.Code, second.Body.String())
			assert.Equal(t, "false", second.Header().Get(apierrors.ShouldRetryHeader))
			require.Equal(t, 2, h.backend.createCount())
			require.Empty(t, h.record(t, h.teamA, "unused-key"))
		})
	}
}

func TestPostSecretsAdmissionRetryRecovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		status  int
		blocked bool
	}{
		{name: "auth lookup failed", status: http.StatusInternalServerError},
		{name: "auth lookup timed out", status: http.StatusRequestTimeout},
		{name: "blocked team", status: http.StatusForbidden, blocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newSecretsCreateIdempotencyHarness(t, time.Minute)
			if tc.blocked {
				h.teamA.IsBlocked = true
			} else {
				h.authErr = &auth.APIError{Code: tc.status, ClientMsg: "Authentication unavailable", Err: errors.New(sentinelBackendMessage)}
			}
			refused := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "admission")
			require.Equal(t, tc.status, refused.Code, refused.Body.String())
			assert.Empty(t, refused.Header().Values(apierrors.ShouldRetryHeader))
			requireNoSentinel(t, "admission", refused.Body.String(), sentinelSecretValue, sentinelBackendMessage)
			require.Zero(t, h.backend.createCount())
			require.Empty(t, h.record(t, h.teamA, "admission"))

			h.authErr = nil
			h.teamA.IsBlocked = false
			created := h.post(t, "/secrets", secretsCreateBody, secretsCreateTeamAKey, "admission")
			require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
			assert.Empty(t, created.Header().Values(apierrors.ShouldRetryHeader))
			require.Equal(t, 1, h.backend.createCount())
		})
	}
}

func TestPostSecretsCancellationBeforeDispatch(t *testing.T) {
	t.Parallel()
	backend := &fakeSecretsBackend{secret: backendSecret()}
	store := newSecretsStore(t, startSecretsBackend(t, backend), true)
	c, recorder, _ := newSecretsRequest(t, http.MethodPost, "/secrets", secretsCreateBody)
	ctx, cancel := context.WithCancel(c.Request.Context())
	cancel()
	c.Request = c.Request.WithContext(ctx)
	store.PostSecrets(c, api.PostSecretsParams{})
	require.Equal(t, http.StatusBadGateway, recorder.Code, recorder.Body.String())
	assert.Empty(t, recorder.Header().Values(apierrors.ShouldRetryHeader))
	assert.Zero(t, backend.callCount())
}
