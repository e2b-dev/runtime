package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const v3TemplatesAdminToken = "v3-templates-admin-token"

type v3TemplatesIdempotencyOptions struct {
	routeDisabled bool
	// injectTeam skips credential checks and serves every request as one team
	// whose limits the test can change; otherwise API keys and admin tokens
	// are checked by the production authenticators.
	injectTeam bool
	cacheRedis redis.UniversalClient
}

type v3TemplatesIdempotency struct {
	db     *testutils.Database
	store  *APIStore
	cache  *templatecache.TemplateCache
	router *gin.Engine
	team   *types.Team
}

func newV3TemplatesIdempotency(t *testing.T, opts v3TemplatesIdempotencyOptions) *v3TemplatesIdempotency {
	t.Helper()
	db := testutils.SetupDatabase(t)
	cacheRedis := opts.cacheRedis
	if cacheRedis == nil {
		cacheRedis = redisutils.SetupInstance(t)
	}
	source := ldtestdata.DataSource()
	if !opts.routeDisabled {
		source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST /v3/templates": true})))
	}
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	authService, err := auth.NewAuthService(t.Context(), redisutils.SetupInstance(t), db.AuthDB, auth.ProviderConfig{}, http.DefaultClient)
	require.NoError(t, err)
	cache := templatecache.NewTemplateCache(db.SqlcClient, cacheRedis)
	store := &APIStore{
		sqlcDB:        db.SqlcClient,
		authDB:        db.AuthDB,
		authService:   authService,
		templateCache: cache,
		featureFlags:  flags,
		posthog:       posthog,
	}
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		require.NoError(t, cache.Close(ctx))
		require.NoError(t, authService.Close(ctx))
		require.NoError(t, flags.Close(ctx))
	})

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	authenticate := auth.CreateAuthenticationFunc([]auth.Authenticator{
		auth.NewApiKeyAuthenticator(store.GetTeamFromAPIKey),
		auth.NewAuthProviderBearerAuthenticator(store.GetUserIDFromAuthProviderToken),
		auth.NewAuthProviderTeamAuthenticator(store.GetTeamFromAuthProviderToken),
		auth.NewAdminApiKeyAuthenticator(v3TemplatesAdminToken),
		auth.NewAdminJWTAuthenticator(nil),
		auth.NewAdminTeamAuthenticator(store.GetTeamFromAdminToken),
	}, nil)
	env := &v3TemplatesIdempotency{db: db, store: store, cache: cache}
	if opts.injectTeam {
		authenticate = func(context.Context, *openapi3filter.AuthenticationInput) error { return nil }
		teamID := testutils.CreateTestTeam(t, db)
		env.team = minimalTeamInfo()
		env.team.ID = teamID
		env.team.Slug = testutils.GetTeamSlug(t, t.Context(), db, teamID)
		env.team.Limits = &types.TeamLimits{BuildConcurrency: 32, MaxVcpu: 8, MaxRamMb: 8192, DefaultFreeDiskSizeMb: 10240, MaxFreeDiskSizeMb: 25600}
	}

	r := gin.New()
	r.Use(sharedmiddleware.RequestTimeout(time.Minute))
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(swagger, &ginmiddleware.Options{
		ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), fallbackStatusCode))
		},
		MultiErrorHandler: utils.MultiErrorHandler,
		Options:           openapi3filter.Options{AuthenticationFunc: authenticate, MultiError: true},
	}))
	if env.team != nil {
		r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, env.team) })
	}
	r.Use(idempotency.Middleware(redisutils.SetupInstance(t), flags))
	api.RegisterHandlersWithOptions(r, store, api.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, statusCode int) {
			utils.ErrorHandler(c, err.Error(), statusCode)
		},
	})
	env.router = r

	return env
}

func (e *v3TemplatesIdempotency) post(t *testing.T, target, body string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(body))
	req.Header = header.Clone()
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	e.router.ServeHTTP(w, req)

	return w
}

// apiKeyTeam creates a team and returns its ID and the headers authenticating as it with an Idempotency-Key.
func (e *v3TemplatesIdempotency) apiKeyTeam(t *testing.T, idempotencyKey string) (uuid.UUID, http.Header) {
	t.Helper()
	teamID := testutils.CreateTestTeam(t, e.db)
	key := addProjectAPIKey(t, e.db, teamID)

	return teamID, v3TemplatesHeader(idempotencyKey, "X-API-Key", key.PrefixedRawValue)
}

func v3TemplatesHeader(idempotencyKey string, pairs ...string) http.Header {
	header := http.Header{}
	if idempotencyKey != "" {
		header.Set("Idempotency-Key", idempotencyKey)
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		header.Set(pairs[i], pairs[i+1])
	}

	return header
}

func countV3TemplateBuilds(t *testing.T, db *testutils.Database, teamID uuid.UUID) int64 {
	t.Helper()
	var count int64
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `
		SELECT count(DISTINCT eb.id)
		FROM public.envs e
		JOIN public.env_build_assignments eba ON eba.env_id = e.id
		JOIN public.env_builds eb ON eb.id = eba.build_id
		WHERE e.team_id = $1`, func(rows pgx.Rows) error {
		require.True(t, rows.Next())

		return rows.Scan(&count)
	}, teamID)
	require.NoError(t, err)

	return count
}

func decodeV3TemplateBuild(t *testing.T, w *httptest.ResponseRecorder) api.TemplateRequestResponseV3 {
	t.Helper()
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	var response api.TemplateRequestResponseV3
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.NotEmpty(t, response.TemplateID)
	require.NotEmpty(t, response.BuildID)

	return response
}

func requireV3TemplatesReplay(t *testing.T, original, replay *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, original.Code, replay.Code, replay.Body.String())
	assert.Equal(t, original.Body.String(), replay.Body.String())
	assert.Equal(t, original.Header().Get("Content-Type"), replay.Header().Get("Content-Type"))
	assert.Equal(t, original.Header().Values(apierrors.ShouldRetryHeader), replay.Header().Values(apierrors.ShouldRetryHeader))
}

func requireV3TemplatesConflict(t *testing.T, w *httptest.ResponseRecorder, errorCode, shouldRetry string) {
	t.Helper()
	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `"error_code":"`+errorCode+`"`)
	assert.Equal(t, shouldRetry, w.Header().Get(apierrors.ShouldRetryHeader))
}

func TestPostV3TemplatesIdempotencyReplaysOneRegisteredBuild(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{})
	teamID, header := env.apiKeyTeam(t, "build-once")

	first := env.post(t, "/v3/templates", `{"name":"replayed","tags":["v1"],"cpuCount":2,"memoryMB":1024}`, header)
	created := decodeV3TemplateBuild(t, first)
	assert.Empty(t, first.Header().Values(apierrors.ShouldRetryHeader))

	reordered := " {\n \"memoryMB\": 1024, \"tags\": [\"v1\"],\"ignored\":true,\n\"cpuCount\":2, \"name\":\"replayed\"}"
	otherAgent := header.Clone()
	otherAgent.Set("User-Agent", "another-client/1.0")
	requireV3TemplatesReplay(t, first, env.post(t, "/v3/templates", reordered, otherAgent))
	require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, teamID))

	// A later build of the same name and a deleted template do not change the stored result.
	rebuilt := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", `{"name":"replayed","tags":["v1"],"cpuCount":2,"memoryMB":1024}`, v3TemplatesHeader("", "X-API-Key", header.Get("X-API-Key"))))
	require.Equal(t, created.TemplateID, rebuilt.TemplateID)
	require.NotEqual(t, created.BuildID, rebuilt.BuildID)
	testutils.SoftDeleteEnvUnstamped(t, t.Context(), env.db, created.TemplateID)
	requireV3TemplatesReplay(t, first, env.post(t, "/v3/templates", `{"name":"replayed","tags":["v1"],"cpuCount":2,"memoryMB":1024}`, header))
	require.EqualValues(t, 2, countV3TemplateBuilds(t, env.db, teamID))
}

func TestPostV3TemplatesIdempotencyFingerprintsEveryInput(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{})
	teamID, header := env.apiKeyTeam(t, "fingerprint")
	first := env.post(t, "/v3/templates", `{"name":"fingerprinted","cpuCount":2,"memoryMB":1024}`, header)
	decodeV3TemplateBuild(t, first)

	for name, request := range map[string]struct{ target, body string }{
		"name":             {"/v3/templates", `{"name":"renamed","cpuCount":2,"memoryMB":1024}`},
		"deprecated alias": {"/v3/templates", `{"alias":"fingerprinted","cpuCount":2,"memoryMB":1024}`},
		"tag in name":      {"/v3/templates", `{"name":"fingerprinted:default","cpuCount":2,"memoryMB":1024}`},
		"tags":             {"/v3/templates", `{"name":"fingerprinted","tags":["v2"],"cpuCount":2,"memoryMB":1024}`},
		"empty tags":       {"/v3/templates", `{"name":"fingerprinted","tags":[],"cpuCount":2,"memoryMB":1024}`},
		"cpu":              {"/v3/templates", `{"name":"fingerprinted","cpuCount":4,"memoryMB":1024}`},
		"memory":           {"/v3/templates", `{"name":"fingerprinted","cpuCount":2,"memoryMB":2048}`},
		"omitted resource": {"/v3/templates", `{"name":"fingerprinted","cpuCount":2}`},
		"free disk":        {"/v3/templates", `{"name":"fingerprinted","cpuCount":2,"memoryMB":1024,"minFreeDiskMb":0}`},
		"team selector":    {"/v3/templates", `{"name":"fingerprinted","cpuCount":2,"memoryMB":1024,"teamID":"` + teamID.String() + `"}`},
		"query":            {"/v3/templates?retry=1", `{"name":"fingerprinted","cpuCount":2,"memoryMB":1024}`},
	} {
		requireV3TemplatesConflict(t, env.post(t, request.target, request.body, header), "idempotency_request_mismatch", "false")
		require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, teamID), name)
	}
	requireV3TemplatesReplay(t, first, env.post(t, "/v3/templates", `{"name":"fingerprinted","cpuCount":2,"memoryMB":1024}`, header))
}

func TestPostV3TemplatesIdempotencyRejectsMalformedKeyBeforeTheHandler(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{})
	teamID, header := env.apiKeyTeam(t, "Not_A_Key")

	w := env.post(t, "/v3/templates", `{"name":"malformed-key"}`, header)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "Idempotency-Key")
	require.Zero(t, countV3TemplateBuilds(t, env.db, teamID))
}

func TestPostV3TemplatesIdempotencyIsScopedToTheAuthenticatedTeam(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{})
	firstTeam, firstHeader := env.apiKeyTeam(t, "shared-key")
	secondTeam, secondHeader := env.apiKeyTeam(t, "shared-key")
	body := `{"name":"scoped"}`

	first := env.post(t, "/v3/templates", body, firstHeader)
	firstBuild := decodeV3TemplateBuild(t, first)
	secondBuild := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", body, secondHeader))
	require.NotEqual(t, firstBuild.TemplateID, secondBuild.TemplateID)
	require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, firstTeam))
	require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, secondTeam))

	// An admin token acting for the first team reaches the same record.
	admin := v3TemplatesHeader("shared-key", "X-Admin-Token", v3TemplatesAdminToken, "X-Team-ID", firstTeam.String())
	requireV3TemplatesReplay(t, first, env.post(t, "/v3/templates", body, admin))

	invalid := v3TemplatesHeader("shared-key", "X-API-Key", "e2b_"+strings.Repeat("0", 40))
	rejected := env.post(t, "/v3/templates", body, invalid)
	require.Equal(t, http.StatusUnauthorized, rejected.Code, rejected.Body.String())
	assert.NotEqual(t, first.Body.String(), rejected.Body.String())

	// The deprecated body selector does not override the authenticated team.
	otherTeamBody := `{"name":"selected","teamID":"` + secondTeam.String() + `"}`
	selected := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", otherTeamBody, v3TemplatesHeader("selector", "X-API-Key", firstHeader.Get("X-API-Key"))))
	require.Equal(t, []string{testutils.GetTeamSlug(t, t.Context(), env.db, firstTeam) + "/selected"}, selected.Names)
	require.EqualValues(t, 2, countV3TemplateBuilds(t, env.db, firstTeam))
	require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, secondTeam))
}

func TestPostV3TemplatesIdempotencyWithoutKeyOrFlagRegistersEachRequest(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		routeDisabled bool
		key           string
	}{
		"no key":         {},
		"disabled route": {routeDisabled: true, key: "ignored-key"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{routeDisabled: test.routeDisabled})
			teamID, header := env.apiKeyTeam(t, test.key)
			first := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", `{"name":"unkeyed"}`, header))
			second := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", `{"name":"unkeyed"}`, header))
			require.Equal(t, first.TemplateID, second.TemplateID)
			require.NotEqual(t, first.BuildID, second.BuildID)
			require.EqualValues(t, 2, countV3TemplateBuilds(t, env.db, teamID))
		})
	}
}

// v3TemplatesRedisGate blocks the first template-cache Redis command issued after it is armed.
type v3TemplatesRedisGate struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *v3TemplatesRedisGate) DialHook(next redis.DialHook) redis.DialHook { return next }

func (g *v3TemplatesRedisGate) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		g.wait()

		return next(ctx, cmd)
	}
}

func (g *v3TemplatesRedisGate) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		g.wait()

		return next(ctx, cmds)
	}
}

func (g *v3TemplatesRedisGate) wait() {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
}

func TestPostV3TemplatesIdempotencyRunsConcurrentDuplicatesOnce(t *testing.T) {
	t.Parallel()
	cacheRedis := redisutils.SetupInstance(t)
	gate := &v3TemplatesRedisGate{entered: make(chan struct{}), release: make(chan struct{})}
	cacheRedis.AddHook(gate)
	release := sync.OnceFunc(func() { close(gate.release) })
	t.Cleanup(release)
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true, cacheRedis: cacheRedis})
	header := v3TemplatesHeader("concurrent")
	body := `{"name":"concurrent","cpuCount":2,"memoryMB":1024}`

	gate.armed.Store(true)
	owner := make(chan *httptest.ResponseRecorder, 1)
	go func() { owner <- env.post(t, "/v3/templates", body, header) }()
	<-gate.entered

	var duplicates sync.WaitGroup
	responses := make([]*httptest.ResponseRecorder, 4)
	for i := range responses {
		duplicates.Go(func() { responses[i] = env.post(t, "/v3/templates", body, header) })
	}
	duplicates.Wait()
	for _, w := range responses {
		requireV3TemplatesConflict(t, w, "idempotency_in_progress", "true")
	}
	require.Zero(t, countV3TemplateBuilds(t, env.db, env.team.ID))

	release()
	first := <-owner
	decodeV3TemplateBuild(t, first)
	requireV3TemplatesReplay(t, first, env.post(t, "/v3/templates", body, header))
	require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, env.team.ID))
}

func TestPostV3TemplatesIdempotencyRetriesOnlyUncommittedFailures(t *testing.T) {
	t.Parallel()
	body := `{"name":"recovering","cpuCount":2,"memoryMB":1024}`

	for name, test := range map[string]struct {
		fail, restore func(*testing.T, *v3TemplatesIdempotency)
		wantStatus    int
		wantMessage   string
	}{
		"team cluster read": {
			fail: func(t *testing.T, env *v3TemplatesIdempotency) {
				t.Helper()
				closed := testutils.SetupDatabase(t)
				require.NoError(t, closed.SqlcClient.Close())
				env.store.sqlcDB = closed.SqlcClient
			},
			restore:     func(_ *testing.T, env *v3TemplatesIdempotency) { env.store.sqlcDB = env.db.SqlcClient },
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "Error when checking the team's cluster",
		},
		"template alias read": {
			fail: func(t *testing.T, env *v3TemplatesIdempotency) {
				t.Helper()
				closed := testutils.SetupDatabase(t)
				failing := templatecache.NewTemplateCache(closed.SqlcClient, redisutils.SetupInstance(t))
				t.Cleanup(func() { require.NoError(t, failing.Close(context.WithoutCancel(t.Context()))) })
				require.NoError(t, closed.SqlcClient.Close())
				env.store.templateCache = failing
			},
			restore:     func(_ *testing.T, env *v3TemplatesIdempotency) { env.store.templateCache = env.cache },
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "error resolving template",
		},
		"concurrent build count read": {
			fail: func(t *testing.T, env *v3TemplatesIdempotency) {
				t.Helper()
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `ALTER TABLE public.active_template_builds RENAME TO active_template_builds_hidden`))
			},
			restore: func(t *testing.T, env *v3TemplatesIdempotency) {
				t.Helper()
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `ALTER TABLE public.active_template_builds_hidden RENAME TO active_template_builds`))
			},
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "Error when checking concurrent builds",
		},
		"concurrent build limit": {
			fail:        func(_ *testing.T, env *v3TemplatesIdempotency) { env.team.Limits.BuildConcurrency = 0 },
			restore:     func(_ *testing.T, env *v3TemplatesIdempotency) { env.team.Limits.BuildConcurrency = 32 },
			wantStatus:  http.StatusTooManyRequests,
			wantMessage: "maximum number of concurrent template builds",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true})
			header := v3TemplatesHeader("recovering")

			test.fail(t, env)
			failed := env.post(t, "/v3/templates", body, header)
			require.Equal(t, test.wantStatus, failed.Code, failed.Body.String())
			assert.Contains(t, failed.Body.String(), test.wantMessage)
			assert.Equal(t, "true", failed.Header().Get(apierrors.ShouldRetryHeader))
			require.Zero(t, countV3TemplateBuilds(t, env.db, env.team.ID))

			test.restore(t, env)
			retried := env.post(t, "/v3/templates", body, header)
			decodeV3TemplateBuild(t, retried)
			test.fail(t, env)
			requireV3TemplatesReplay(t, retried, env.post(t, "/v3/templates", body, header))
			require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, env.team.ID))
		})
	}
}

func TestPostV3TemplatesIdempotencyReplaysDeterministicRefusal(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true})
	header := v3TemplatesHeader("too-many-cpus")
	body := `{"name":"refused","cpuCount":16}`

	refused := env.post(t, "/v3/templates", body, header)
	require.Equal(t, http.StatusBadRequest, refused.Code, refused.Body.String())
	assert.NotEqual(t, "true", refused.Header().Get(apierrors.ShouldRetryHeader))

	env.team.Limits.MaxVcpu = 32
	requireV3TemplatesReplay(t, refused, env.post(t, "/v3/templates", body, header))
	requireV3TemplatesConflict(t, env.post(t, "/v3/templates", `{"name":"refused","cpuCount":8}`, header), "idempotency_request_mismatch", "false")
	require.Zero(t, countV3TemplateBuilds(t, env.db, env.team.ID))
}

func TestPostV3TemplatesIdempotencyKeepsUncertainCommit(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true})
	header := v3TemplatesHeader("uncertain-commit")
	body := `{"name":"uncertain","cpuCount":2,"memoryMB":1024}`
	require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `
		CREATE FUNCTION public.v3_templates_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'commit refused by test'; END $$`))
	require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `
		CREATE CONSTRAINT TRIGGER v3_templates_fail_commit AFTER INSERT ON public.env_builds
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.v3_templates_fail_commit()`))

	failed := env.post(t, "/v3/templates", body, header)
	require.Equal(t, http.StatusInternalServerError, failed.Code, failed.Body.String())
	assert.Contains(t, failed.Body.String(), "Error when committing transaction")
	assert.NotContains(t, failed.Result().Header, apierrors.ShouldRetryHeader)

	require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `DROP TRIGGER v3_templates_fail_commit ON public.env_builds`))
	replayed := env.post(t, "/v3/templates", body, header)
	requireV3TemplatesReplay(t, failed, replayed)
	assert.NotContains(t, replayed.Result().Header, apierrors.ShouldRetryHeader)
	requireV3TemplatesConflict(t, env.post(t, "/v3/templates", `{"name":"uncertain"}`, header), "idempotency_request_mismatch", "false")
	require.Zero(t, countV3TemplateBuilds(t, env.db, env.team.ID))

	decodeV3TemplateBuild(t, env.post(t, "/v3/templates", body, v3TemplatesHeader("after-uncertain-commit")))
	require.EqualValues(t, 1, countV3TemplateBuilds(t, env.db, env.team.ID))
}

type v3TemplatesRegistrationCounts struct {
	envs, builds, assignments, aliases, activeBuilds int64
}

func countV3TemplatesRegistrationRows(t *testing.T, db *testutils.Database) v3TemplatesRegistrationCounts {
	t.Helper()
	var counts v3TemplatesRegistrationCounts
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `
		SELECT (SELECT count(*) FROM public.envs), (SELECT count(*) FROM public.env_builds),
			(SELECT count(*) FROM public.env_build_assignments), (SELECT count(*) FROM public.env_aliases),
			(SELECT count(*) FROM public.active_template_builds)`, func(rows pgx.Rows) error {
		require.True(t, rows.Next())

		return rows.Scan(&counts.envs, &counts.builds, &counts.assignments, &counts.aliases, &counts.activeBuilds)
	})
	require.NoError(t, err)

	return counts
}

// v3TemplatesStaleRebuild registers "rebuilt" on clusterA, moves the team to clusterB and caches
// the alias so the next request plans to replace that template.
func v3TemplatesStaleRebuild(t *testing.T, env *v3TemplatesIdempotency, clusterA, clusterB uuid.UUID) string {
	t.Helper()
	assignTemplateClusterTeam(t, env.db, env.team, &clusterA)
	previous := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", `{"name":"rebuilt"}`, v3TemplatesHeader("")))
	assignTemplateClusterTeam(t, env.db, env.team, &clusterB)
	aliasInfo, metadata, err := env.store.templateCache.ResolveAliasWithMetadata(t.Context(), "rebuilt", env.team.Slug)
	require.NoError(t, err)
	require.Equal(t, previous.TemplateID, aliasInfo.TemplateID)
	require.Equal(t, clusterA, metadata.ClusterID)

	return previous.TemplateID
}

func v3TemplatesEnvCluster(t *testing.T, db *testutils.Database, templateID string) *uuid.UUID {
	t.Helper()
	var clusterID *uuid.UUID
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT cluster_id FROM public.envs WHERE id = $1`, func(rows pgx.Rows) error {
		require.True(t, rows.Next())

		return rows.Scan(&clusterID)
	}, templateID)
	require.NoError(t, err)

	return clusterID
}

func TestPostV3TemplatesIdempotencyRetriesRolledBackRegistrationConflicts(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		body string
		// conflict prepares state and sends the keyed request that must hit the conflict.
		conflict func(t *testing.T, env *v3TemplatesIdempotency, gate *v3TemplatesRedisGate, release func(), send func() *httptest.ResponseRecorder) (*httptest.ResponseRecorder, string)
		check    func(t *testing.T, env *v3TemplatesIdempotency, previous string, retried api.TemplateRequestResponseV3)
	}{
		"team cluster changed": {
			body: `{"name":"moving"}`,
			conflict: func(t *testing.T, env *v3TemplatesIdempotency, gate *v3TemplatesRedisGate, release func(), send func() *httptest.ResponseRecorder) (*httptest.ResponseRecorder, string) {
				t.Helper()
				target := uuid.New()
				assignTemplateClusterTeam(t, env.db, env.team, &target)
				assignTemplateClusterTeam(t, env.db, env.team, nil)
				// The handler has read the team's cluster; move the team before registration re-reads it.
				gate.armed.Store(true)
				response := make(chan *httptest.ResponseRecorder, 1)
				go func() { response <- send() }()
				<-gate.entered
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, env.team.ID, target))
				release()

				return <-response, target.String()
			},
			check: func(t *testing.T, env *v3TemplatesIdempotency, target string, retried api.TemplateRequestResponseV3) {
				t.Helper()
				cluster := v3TemplatesEnvCluster(t, env.db, retried.TemplateID)
				require.NotNil(t, cluster)
				require.Equal(t, target, cluster.String(), "the retry registers on the team's new cluster")
			},
		},
		"replaced template deleted": {
			body: `{"name":"rebuilt"}`,
			conflict: func(t *testing.T, env *v3TemplatesIdempotency, _ *v3TemplatesRedisGate, _ func(), send func() *httptest.ResponseRecorder) (*httptest.ResponseRecorder, string) {
				t.Helper()
				previous := v3TemplatesStaleRebuild(t, env, uuid.New(), uuid.New())
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `DELETE FROM public.env_aliases WHERE env_id = $1`, previous))
				testutils.SoftDeleteEnvUnstamped(t, t.Context(), env.db, previous)

				return send(), previous
			},
			check: func(t *testing.T, env *v3TemplatesIdempotency, previous string, retried api.TemplateRequestResponseV3) {
				t.Helper()
				require.NotEqual(t, previous, retried.TemplateID)
				require.NoError(t, env.db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT 1 FROM public.envs WHERE id = $1 AND deleted_at IS NOT NULL`, func(rows pgx.Rows) error {
					require.True(t, rows.Next(), "the deleted template stays deleted")

					return nil
				}, previous))
			},
		},
		"replaced alias removed": {
			body: `{"name":"rebuilt"}`,
			conflict: func(t *testing.T, env *v3TemplatesIdempotency, _ *v3TemplatesRedisGate, _ func(), send func() *httptest.ResponseRecorder) (*httptest.ResponseRecorder, string) {
				t.Helper()
				previous := v3TemplatesStaleRebuild(t, env, uuid.New(), uuid.New())
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `DELETE FROM public.env_aliases WHERE env_id = $1`, previous))

				return send(), previous
			},
			check: func(t *testing.T, env *v3TemplatesIdempotency, previous string, retried api.TemplateRequestResponseV3) {
				t.Helper()
				require.NotEqual(t, previous, retried.TemplateID)
				require.Equal(t, []string{env.team.Slug + "/rebuilt"}, retried.Names)
			},
		},
		"replaced alias moved": {
			body: `{"name":"rebuilt"}`,
			conflict: func(t *testing.T, env *v3TemplatesIdempotency, _ *v3TemplatesRedisGate, _ func(), send func() *httptest.ResponseRecorder) (*httptest.ResponseRecorder, string) {
				t.Helper()
				previous := v3TemplatesStaleRebuild(t, env, uuid.New(), uuid.New())
				other := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", `{"name":"other"}`, v3TemplatesHeader("")))
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.env_aliases SET env_id = $2 WHERE alias = 'rebuilt' AND env_id = $1`, previous, other.TemplateID))

				return send(), other.TemplateID
			},
			check: func(t *testing.T, _ *v3TemplatesIdempotency, other string, retried api.TemplateRequestResponseV3) {
				t.Helper()
				require.Equal(t, other, retried.TemplateID, "the retry rebuilds the template the alias now names")
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cacheRedis := redisutils.SetupInstance(t)
			gate := &v3TemplatesRedisGate{entered: make(chan struct{}), release: make(chan struct{})}
			cacheRedis.AddHook(gate)
			release := sync.OnceFunc(func() { close(gate.release) })
			t.Cleanup(release)
			env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true, cacheRedis: cacheRedis})
			header := v3TemplatesHeader("rolled-back")
			send := func() *httptest.ResponseRecorder { return env.post(t, "/v3/templates", test.body, header) }

			// Setup registers its own builds; the conflict is measured against the state just before the keyed request.
			var baseline v3TemplatesRegistrationCounts
			wrapped := func() *httptest.ResponseRecorder {
				baseline = countV3TemplatesRegistrationRows(t, env.db)

				return send()
			}
			conflict, expected := test.conflict(t, env, gate, release, wrapped)
			require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
			assert.Contains(t, conflict.Body.String(), "retry the template build request")
			assert.Equal(t, "true", conflict.Header().Get(apierrors.ShouldRetryHeader))
			require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db), "the conflicting registration is rolled back")

			requireV3TemplatesConflict(t, env.post(t, "/v3/templates", `{"name":"changed"}`, header), "idempotency_request_mismatch", "false")
			require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db))

			first := send()
			retried := decodeV3TemplateBuild(t, first)
			afterRetry := countV3TemplatesRegistrationRows(t, env.db)
			require.Equal(t, baseline.builds+1, afterRetry.builds, "the retry registers exactly one build")
			test.check(t, env, expected, retried)

			requireV3TemplatesReplay(t, first, send())
			require.Equal(t, afterRetry, countV3TemplatesRegistrationRows(t, env.db))
		})
	}
}

func TestPostV3TemplatesIdempotencyMarksDeterministicRefusalsFinal(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		body, changed string
		status        int
		withoutKey    bool
		// setup returns the body to send; clear removes the refusal's cause to show the retry replays.
		setup func(t *testing.T, env *v3TemplatesIdempotency) string
		clear func(t *testing.T, env *v3TemplatesIdempotency)
	}{
		"missing name":               {body: `{"cpuCount":2}`, changed: `{"cpuCount":4}`, status: http.StatusBadRequest},
		"invalid name":               {body: `{"name":"Not A Name"}`, changed: `{"name":"valid"}`, status: http.StatusBadRequest},
		"invalid name without a key": {body: `{"name":"Not A Name"}`, status: http.StatusBadRequest, withoutKey: true},
		"foreign namespace":          {body: `{"name":"someone-else/base"}`, changed: `{"name":"base"}`, status: http.StatusBadRequest},
		"invalid tag":                {body: `{"name":"tagged","tags":["Not A Tag"]}`, changed: `{"name":"tagged"}`, status: http.StatusBadRequest},
		"cpu above allowance": {
			body: `{"name":"large","cpuCount":16}`, changed: `{"name":"large","cpuCount":8}`, status: http.StatusBadRequest,
			clear: func(_ *testing.T, env *v3TemplatesIdempotency) { env.team.Limits.MaxVcpu = 32 },
		},
		"memory above allowance": {
			body: `{"name":"large","memoryMB":16384}`, changed: `{"name":"large","memoryMB":8192}`, status: http.StatusBadRequest,
			clear: func(_ *testing.T, env *v3TemplatesIdempotency) { env.team.Limits.MaxRamMb = 32768 },
		},
		"free disk above allowance": {
			body: `{"name":"large","minFreeDiskMb":30000}`, changed: `{"name":"large","minFreeDiskMb":20000}`, status: http.StatusBadRequest,
			clear: func(_ *testing.T, env *v3TemplatesIdempotency) { env.team.Limits.MaxFreeDiskSizeMb = 40000 },
		},
		"name reserved by a template ID": {
			changed: `{"name":"unreserved"}`, status: http.StatusConflict,
			setup: func(t *testing.T, env *v3TemplatesIdempotency) string {
				t.Helper()

				return `{"name":"` + testutils.CreateTestTemplate(t, env.db, testutils.CreateTestTeam(t, env.db)) + `"}`
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true})
			body := test.body
			if test.setup != nil {
				body = test.setup(t, env)
			}
			header := v3TemplatesHeader("final-refusal")
			if test.withoutKey {
				header = v3TemplatesHeader("")
			}
			baseline := countV3TemplatesRegistrationRows(t, env.db)

			refused := env.post(t, "/v3/templates", body, header)
			require.Equal(t, test.status, refused.Code, refused.Body.String())
			assert.Equal(t, "false", refused.Result().Header.Get(apierrors.ShouldRetryHeader))
			require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db))
			if test.withoutKey {
				return
			}

			if test.clear != nil {
				test.clear(t, env)
			}
			requireV3TemplatesReplay(t, refused, env.post(t, "/v3/templates", body, header))
			requireV3TemplatesConflict(t, env.post(t, "/v3/templates", test.changed, header), "idempotency_request_mismatch", "false")
			require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db))
		})
	}
}

func TestPostV3TemplatesIdempotencyMalformedBodyLeavesKeyUnclaimed(t *testing.T) {
	t.Parallel()
	env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true})
	// The generated validator rejects malformed JSON first; serve the handler directly to reach its own parse.
	r := gin.New()
	r.Use(sharedmiddleware.RequestTimeout(time.Minute))
	r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, env.team) })
	r.Use(idempotency.Middleware(redisutils.SetupInstance(t), env.store.featureFlags))
	r.POST("/v3/templates", func(c *gin.Context) { env.store.PostV3Templates(c, api.PostV3TemplatesParams{}) })
	env.router = r
	header := v3TemplatesHeader("malformed-body")
	baseline := countV3TemplatesRegistrationRows(t, env.db)

	malformed := env.post(t, "/v3/templates", `{"name":`, header)
	require.Equal(t, http.StatusBadRequest, malformed.Code, malformed.Body.String())
	assert.Contains(t, malformed.Body.String(), "Invalid request body")
	// Result reports the headers as sent, so a header set after the status was written is missed.
	assert.Equal(t, "false", malformed.Result().Header.Get(apierrors.ShouldRetryHeader))
	require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db))

	first := env.post(t, "/v3/templates", `{"name":"parsed"}`, header)
	decodeV3TemplateBuild(t, first)
	requireV3TemplatesReplay(t, first, env.post(t, "/v3/templates", `{"name":"parsed"}`, header))
	require.Equal(t, baseline.builds+1, countV3TemplatesRegistrationRows(t, env.db).builds)
}

func TestPostV3TemplatesIdempotencyKeepsStaleCacheRefusalsFinal(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		alias  string
		status int
		// stale leaves the alias cache disagreeing with the database and returns the template a refreshed request should reach.
		stale func(t *testing.T, env *v3TemplatesIdempotency) string
		check func(t *testing.T, expected string, recovered api.TemplateRequestResponseV3)
	}{
		"alias taken after a cached miss": {
			alias: "shared", status: http.StatusForbidden,
			stale: func(t *testing.T, env *v3TemplatesIdempotency) string {
				t.Helper()
				_, _, err := env.store.templateCache.ResolveAliasWithMetadata(t.Context(), "shared", env.team.Slug)
				require.ErrorIs(t, err, templatecache.ErrTemplateNotFound)
				owner := testutils.CreateTestTemplate(t, env.db, env.team.ID)
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(),
					`INSERT INTO public.env_aliases (alias, env_id, namespace, is_renamable) VALUES ('shared', $1, $2, true)`, owner, env.team.Slug))

				return owner
			},
			check: func(t *testing.T, owner string, recovered api.TemplateRequestResponseV3) {
				t.Helper()
				require.Equal(t, owner, recovered.TemplateID, "a refreshed request rebuilds the alias's template")
			},
		},
		"template deleted after a cached hit": {
			alias: "gone", status: http.StatusNotFound,
			stale: func(t *testing.T, env *v3TemplatesIdempotency) string {
				t.Helper()
				gone := decodeV3TemplateBuild(t, env.post(t, "/v3/templates", `{"name":"gone"}`, v3TemplatesHeader("")))
				_, _, err := env.store.templateCache.ResolveAliasWithMetadata(t.Context(), "gone", env.team.Slug)
				require.NoError(t, err)
				require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(), `DELETE FROM public.env_aliases WHERE env_id = $1`, gone.TemplateID))
				testutils.SoftDeleteEnvUnstamped(t, t.Context(), env.db, gone.TemplateID)

				return gone.TemplateID
			},
			check: func(t *testing.T, gone string, recovered api.TemplateRequestResponseV3) {
				t.Helper()
				require.NotEqual(t, gone, recovered.TemplateID, "a refreshed request registers a new template")
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newV3TemplatesIdempotency(t, v3TemplatesIdempotencyOptions{injectTeam: true})
			expected := test.stale(t, env)
			body := `{"name":"` + test.alias + `"}`
			header := v3TemplatesHeader("stale-cache")
			baseline := countV3TemplatesRegistrationRows(t, env.db)

			refused := env.post(t, "/v3/templates", body, header)
			require.Equal(t, test.status, refused.Code, refused.Body.String())
			assert.Equal(t, "false", refused.Result().Header.Get(apierrors.ShouldRetryHeader))
			require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db))

			// The key stays final even once a refreshed lookup would succeed.
			env.store.templateCache.InvalidateAlias(t.Context(), &env.team.Slug, test.alias)
			requireV3TemplatesReplay(t, refused, env.post(t, "/v3/templates", body, header))
			requireV3TemplatesConflict(t, env.post(t, "/v3/templates", `{"name":"other"}`, header), "idempotency_request_mismatch", "false")
			require.Equal(t, baseline, countV3TemplatesRegistrationRows(t, env.db))

			test.check(t, expected, decodeV3TemplateBuild(t, env.post(t, "/v3/templates", body, v3TemplatesHeader("fresh-key"))))
			require.Equal(t, baseline.builds+1, countV3TemplatesRegistrationRows(t, env.db).builds)
		})
	}
}
