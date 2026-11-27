package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
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
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

type templateTagsIdemTenant struct {
	apiKey     string
	templateID string
	name       string
	buildID    uuid.UUID
}

type templateTagsIdemFixture struct {
	db     *testutils.Database
	router *gin.Engine
	a, b   templateTagsIdemTenant
}

// newTemplateTagsIdemFixture serves the generated route through API key authentication, the
// request validator and the idempotency middleware, backed by real PostgreSQL and Redis.
func newTemplateTagsIdemFixture(t *testing.T, routeEnabled bool) *templateTagsIdemFixture {
	t.Helper()

	return newTemplateTagsIdemFixtureWithStore(t, routeEnabled, nil)
}

func newTemplateTagsIdemFixtureWithStore(t *testing.T, routeEnabled bool, idempotencyRedis redis.UniversalClient) *templateTagsIdemFixture {
	t.Helper()
	db := testutils.SetupDatabase(t)
	cacheRedis := redisutils.SetupInstance(t)
	if idempotencyRedis == nil {
		idempotencyRedis = cacheRedis
	}
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST /templates/tags": routeEnabled})))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
	cache := templatecache.NewTemplateCache(db.SqlcClient, cacheRedis)
	t.Cleanup(func() { require.NoError(t, cache.Close(context.WithoutCancel(t.Context()))) })
	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)

	f := &templateTagsIdemFixture{db: db}
	teams := map[string]*types.Team{}
	f.a = newTemplateTagsIdemTenant(t, db, teams)
	f.b = newTemplateTagsIdemTenant(t, db, teams)

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	authenticate := auth.CreateAuthenticationFunc([]auth.Authenticator{
		auth.NewApiKeyAuthenticator(func(_ context.Context, _ *gin.Context, key string) (*types.Team, *auth.APIError) {
			if team, ok := teams[key]; ok {
				return team, nil
			}
			if key == auth.PrefixAPIKey+"unavailable" {
				return nil, &auth.APIError{Code: http.StatusInternalServerError, Err: errors.New("auth unavailable"), ClientMsg: "Authentication unavailable"}
			}

			return nil, &auth.APIError{Code: http.StatusUnauthorized, Err: errors.New("unknown key"), ClientMsg: "Invalid API key"}
		}),
	}, nil)

	r := gin.New()
	r.Use(sharedmiddleware.RequestTimeout(time.Minute))
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(swagger, &ginmiddleware.Options{
		ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), fallbackStatusCode))
		},
		MultiErrorHandler:     utils.MultiErrorHandler,
		Options:               openapi3filter.Options{AuthenticationFunc: authenticate, MultiError: true},
		SilenceServersWarning: true,
	}))
	r.Use(middleware.EnforceBlockedTeam())
	r.Use(idempotency.Middleware(idempotencyRedis, flags))
	api.RegisterHandlersWithOptions(r, &APIStore{sqlcDB: db.SqlcClient, templateCache: cache, posthog: posthog}, api.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, statusCode int) { utils.ErrorHandler(c, err.Error(), statusCode) },
	})
	f.router = r

	return f
}

func newTemplateTagsIdemTenant(t *testing.T, db *testutils.Database, teams map[string]*types.Team) templateTagsIdemTenant {
	t.Helper()
	teamID := testutils.CreateTestTeam(t, db)
	slug := testutils.GetTeamSlug(t, t.Context(), db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &slug)
	buildID := testutils.CreateTestBuild(t, t.Context(), db, templateID, "ready")
	testutils.CreateTestBuildAssignment(t, t.Context(), db, templateID, buildID, "source")
	apiKey := auth.PrefixAPIKey + teamID.String()
	team := minimalTeamInfo()
	team.ID, team.Slug, team.Name = teamID, slug, slug
	teams[apiKey] = team

	return templateTagsIdemTenant{apiKey: apiKey, templateID: templateID, name: slug + "/" + alias, buildID: buildID}
}

func (f *templateTagsIdemFixture) post(t *testing.T, apiKey, path, body, key string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.HeaderAPIKey, apiKey)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, req)

	return w
}

func (f *templateTagsIdemFixture) tagBuilds(t *testing.T, templateID, tag string) []uuid.UUID {
	t.Helper()
	var builds []uuid.UUID
	for _, assignment := range testutils.GetBuildAssignments(t, t.Context(), f.db, templateID) {
		if assignment.Tag == tag {
			builds = append(builds, assignment.BuildID)
		}
	}

	return builds
}

func (f *templateTagsIdemFixture) assignmentCount(t *testing.T, templateID string) int {
	t.Helper()

	return len(testutils.GetBuildAssignments(t, t.Context(), f.db, templateID))
}

func (f *templateTagsIdemFixture) exec(t *testing.T, sql string) {
	t.Helper()
	require.NoError(t, f.db.SqlcClient.TestsRawSQL(t.Context(), sql))
}

func templateTagsIdemBody(t *testing.T, target string, tags ...string) string {
	t.Helper()
	encoded, err := json.Marshal(api.AssignTemplateTagsRequest{Target: target, Tags: tags})
	require.NoError(t, err)

	return string(encoded)
}

func requireTemplateTagsIdemReplay(t *testing.T, original, replayed *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, original.Code, replayed.Code, replayed.Body.String())
	require.Equal(t, original.Body.Bytes(), replayed.Body.Bytes())
	for _, header := range []string{"Content-Type", apierrors.ShouldRetryHeader} {
		require.Equal(t, original.Header().Values(header), replayed.Header().Values(header), header)
	}
}

func requireTemplateTagsIdemError(t *testing.T, w *httptest.ResponseRecorder, status int, errorCode, shouldRetry string) {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	var body api.Error
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	if errorCode != "" {
		require.NotNil(t, body.ErrorCode, w.Body.String())
		require.Equal(t, errorCode, *body.ErrorCode)
	}
	require.Equal(t, shouldRetry, w.Result().Header.Get(apierrors.ShouldRetryHeader))
}

func requireTemplateTagsIdemAssigned(t *testing.T, w *httptest.ResponseRecorder, buildID uuid.UUID, tags ...string) {
	t.Helper()
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var body api.AssignedTemplateTags
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, buildID, body.BuildID)
	require.ElementsMatch(t, tags, body.Tags)
	require.Empty(t, w.Header().Values(apierrors.ShouldRetryHeader))
}

func TestPostTemplatesTagsIdempotentReplayKeepsBuildAfterTargetMoves(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	target := f.a.name + ":source"

	first := f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, target, "prod", " PROD "), "move")
	requireTemplateTagsIdemAssigned(t, first, f.a.buildID, "prod")
	require.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "prod"))

	newer := testutils.CreateTestBuild(t, t.Context(), f.db, f.a.templateID, "ready")
	testutils.CreateTestBuildAssignment(t, t.Context(), f.db, f.a.templateID, newer, "source")
	before := f.assignmentCount(t, f.a.templateID)

	reordered := fmt.Sprintf(` { "tags" : [ "prod", " PROD " ],
		"target" : %q } `, target)
	replayed := f.post(t, f.a.apiKey, "/templates/tags", reordered, "move")
	requireTemplateTagsIdemReplay(t, first, replayed)
	require.Equal(t, before, f.assignmentCount(t, f.a.templateID))
	require.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "prod"))

	fresh := f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, target, "prod", " PROD "), "after-move")
	requireTemplateTagsIdemAssigned(t, fresh, newer, "prod")
	require.Equal(t, []uuid.UUID{newer, f.a.buildID}, f.tagBuilds(t, f.a.templateID, "prod"))
}

func TestPostTemplatesTagsIdempotentReplayIgnoresIrrelevantInputs(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	target := f.a.name + ":source"
	first := f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, target, "prod"), "irrelevant", "User-Agent", "e2b-python-sdk/1.0.0")
	requireTemplateTagsIdemAssigned(t, first, f.a.buildID, "prod")

	for name, retry := range map[string]func() *httptest.ResponseRecorder{
		"unknown body field": func() *httptest.ResponseRecorder {
			return f.post(t, f.a.apiKey, "/templates/tags", fmt.Sprintf(`{"target":%q,"tags":["prod"],"ignored":true}`, target), "irrelevant")
		},
		"analytics headers": func() *httptest.ResponseRecorder {
			return f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, target, "prod"), "irrelevant", "User-Agent", "e2b-js-sdk/2.0.0", "lang", "js")
		},
	} {
		requireTemplateTagsIdemReplay(t, first, retry())
		require.Len(t, f.tagBuilds(t, f.a.templateID, "prod"), 1, name)
	}
}

func TestPostTemplatesTagsIdempotentMismatchOnChangedInputs(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	testutils.CreateTestBuildAssignment(t, t.Context(), f.db, f.a.templateID, f.a.buildID, "default")
	for _, test := range []struct {
		name, firstTarget, retryTarget, retryPath string
		firstTags, retryTags                      []string
	}{
		{name: "target tag", firstTarget: ":source", retryTarget: ":default", firstTags: []string{"prod"}, retryTags: []string{"prod"}},
		{name: "omitted default tag", firstTarget: "", retryTarget: ":default", firstTags: []string{"prod"}, retryTags: []string{"prod"}},
		{name: "assigned tags", firstTarget: ":source", retryTarget: ":source", firstTags: []string{"prod"}, retryTags: []string{"stable"}},
		{name: "duplicate tag", firstTarget: ":source", retryTarget: ":source", firstTags: []string{"prod"}, retryTags: []string{"prod", "prod"}},
		{name: "query", firstTarget: ":source", retryTarget: ":source", retryPath: "?dry=true", firstTags: []string{"prod"}, retryTags: []string{"prod"}},
	} {
		key := "mismatch-" + strings.ReplaceAll(test.name, " ", "-")
		first := f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, f.a.name+test.firstTarget, test.firstTags...), key)
		requireTemplateTagsIdemAssigned(t, first, f.a.buildID, test.firstTags[0])
		before := f.assignmentCount(t, f.a.templateID)

		retry := f.post(t, f.a.apiKey, "/templates/tags"+test.retryPath, templateTagsIdemBody(t, f.a.name+test.retryTarget, test.retryTags...), key)
		requireTemplateTagsIdemError(t, retry, http.StatusConflict, "idempotency_request_mismatch", "false")
		require.Equal(t, before, f.assignmentCount(t, f.a.templateID), test.name)
	}
}

func TestPostTemplatesTagsIdempotentConcurrentDuplicatesWaitForOwner(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	body := templateTagsIdemBody(t, f.a.name+":source", "prod", "stable")

	// Holding the template row blocks the owner's assignment insert inside its transaction.
	conn, err := pgx.Connect(t.Context(), f.db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	lock, err := conn.Begin(t.Context())
	require.NoError(t, err)
	_, err = lock.Exec(t.Context(), "SELECT 1 FROM public.envs WHERE id = $1 FOR UPDATE", f.a.templateID)
	require.NoError(t, err)

	owner := make(chan *httptest.ResponseRecorder, 1)
	go func() { owner <- f.post(t, f.a.apiKey, "/templates/tags", body, "concurrent") }()
	require.Eventually(t, func() bool {
		waiting := 0
		err := f.db.SqlcClient.TestsRawSQLQuery(t.Context(),
			"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'",
			func(rows pgx.Rows) error {
				rows.Next()

				return rows.Scan(&waiting)
			})

		return err == nil && waiting == 1
	}, 30*time.Second, 10*time.Millisecond)

	var wg sync.WaitGroup
	duplicates := make([]*httptest.ResponseRecorder, 4)
	for i := range duplicates {
		wg.Go(func() { duplicates[i] = f.post(t, f.a.apiKey, "/templates/tags", body, "concurrent") })
	}
	wg.Wait()
	for _, duplicate := range duplicates {
		requireTemplateTagsIdemError(t, duplicate, http.StatusConflict, "idempotency_in_progress", "true")
	}
	require.Equal(t, 1, f.assignmentCount(t, f.a.templateID))

	require.NoError(t, lock.Rollback(t.Context()))
	completed := <-owner
	requireTemplateTagsIdemAssigned(t, completed, f.a.buildID, "prod", "stable")
	requireTemplateTagsIdemReplay(t, completed, f.post(t, f.a.apiKey, "/templates/tags", body, "concurrent"))
	assert.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "prod"))
	assert.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "stable"))
}

func TestPostTemplatesTagsIdempotentRetriesRolledBackAssignment(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	body := templateTagsIdemBody(t, f.a.name+":source", "first", "second")
	f.exec(t, `CREATE FUNCTION fail_second_tag_assignment() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM public.env_build_assignments WHERE env_id = NEW.env_id AND tag IN ('first', 'second')) THEN
		RAISE EXCEPTION 'injected assignment failure';
	END IF;
	RETURN NEW;
END $$`)
	f.exec(t, `CREATE TRIGGER fail_second_tag_assignment BEFORE INSERT ON public.env_build_assignments
FOR EACH ROW EXECUTE FUNCTION fail_second_tag_assignment()`)

	failed := f.post(t, f.a.apiKey, "/templates/tags", body, "rollback")
	requireTemplateTagsIdemError(t, failed, http.StatusInternalServerError, "", "true")
	require.Empty(t, f.tagBuilds(t, f.a.templateID, "first"))
	require.Empty(t, f.tagBuilds(t, f.a.templateID, "second"))

	f.exec(t, "DROP TRIGGER fail_second_tag_assignment ON public.env_build_assignments")
	retried := f.post(t, f.a.apiKey, "/templates/tags", body, "rollback")
	requireTemplateTagsIdemAssigned(t, retried, f.a.buildID, "first", "second")
	requireTemplateTagsIdemReplay(t, retried, f.post(t, f.a.apiKey, "/templates/tags", body, "rollback"))
	assert.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "first"))
	assert.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "second"))
}

func TestPostTemplatesTagsIdempotentKeepsFailedCommitOutcome(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	body := templateTagsIdemBody(t, f.a.name+":source", "prod")
	f.exec(t, `CREATE FUNCTION fail_tag_assignment_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'injected commit failure';
END $$`)
	f.exec(t, `CREATE CONSTRAINT TRIGGER fail_tag_assignment_commit AFTER INSERT ON public.env_build_assignments
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_tag_assignment_commit()`)

	failed := f.post(t, f.a.apiKey, "/templates/tags", body, "commit")
	requireTemplateTagsIdemError(t, failed, http.StatusInternalServerError, "", "")
	require.Contains(t, failed.Body.String(), "Error committing transaction")

	f.exec(t, "DROP TRIGGER fail_tag_assignment_commit ON public.env_build_assignments")
	before := f.assignmentCount(t, f.a.templateID)
	requireTemplateTagsIdemReplay(t, failed, f.post(t, f.a.apiKey, "/templates/tags", body, "commit"))
	require.Equal(t, before, f.assignmentCount(t, f.a.templateID))

	requireTemplateTagsIdemAssigned(t, f.post(t, f.a.apiKey, "/templates/tags", body, "after-commit"), f.a.buildID, "prod")
}

func TestPostTemplatesTagsIdempotentReplaysDeterministicError(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	body := templateTagsIdemBody(t, f.a.name+":later", "prod")
	missing := f.post(t, f.a.apiKey, "/templates/tags", body, "missing")
	requireTemplateTagsIdemError(t, missing, http.StatusNotFound, "", "false")

	testutils.CreateTestBuildAssignment(t, t.Context(), f.db, f.a.templateID, f.a.buildID, "later")
	before := f.assignmentCount(t, f.a.templateID)
	requireTemplateTagsIdemReplay(t, missing, f.post(t, f.a.apiKey, "/templates/tags", body, "missing"))
	requireTemplateTagsIdemError(t, f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, f.a.name+":later", "stable"), "missing"),
		http.StatusConflict, "idempotency_request_mismatch", "false")
	require.Equal(t, before, f.assignmentCount(t, f.a.templateID))
	require.Empty(t, f.tagBuilds(t, f.a.templateID, "prod"))
}

func TestPostTemplatesTagsWithoutIdempotencyExecutesEachRequest(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		enabled bool
		key     string
	}{
		{name: "missing key", enabled: true},
		{name: "disabled route", enabled: false, key: "disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newTemplateTagsIdemFixture(t, test.enabled)
			body := templateTagsIdemBody(t, f.a.name+":source", "prod")
			for range 2 {
				requireTemplateTagsIdemAssigned(t, f.post(t, f.a.apiKey, "/templates/tags", body, test.key), f.a.buildID, "prod")
			}
			require.Equal(t, []uuid.UUID{f.a.buildID, f.a.buildID}, f.tagBuilds(t, f.a.templateID, "prod"))
		})
	}
}

func TestPostTemplatesTagsIdempotencyIsTeamScoped(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	first := f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, f.a.name+":source", "prod"), "shared")
	requireTemplateTagsIdemAssigned(t, first, f.a.buildID, "prod")

	other := f.post(t, f.b.apiKey, "/templates/tags", templateTagsIdemBody(t, f.b.name+":source", "prod"), "shared")
	requireTemplateTagsIdemAssigned(t, other, f.b.buildID, "prod")
	foreign := f.post(t, f.b.apiKey, "/templates/tags", templateTagsIdemBody(t, f.a.name+":source", "prod"), "shared-foreign")
	requireTemplateTagsIdemError(t, foreign, http.StatusBadRequest, "", "false")

	unauthenticated := f.post(t, auth.PrefixAPIKey+"unknown", "/templates/tags", templateTagsIdemBody(t, f.a.name+":source", "prod"), "shared")
	requireTemplateTagsIdemError(t, unauthenticated, http.StatusUnauthorized, "", "")
	malformed := f.post(t, f.a.apiKey, "/templates/tags", templateTagsIdemBody(t, f.a.name+":source", "stable"), "Not_Valid")
	requireTemplateTagsIdemError(t, malformed, http.StatusBadRequest, "", "")

	assert.Equal(t, []uuid.UUID{f.a.buildID}, f.tagBuilds(t, f.a.templateID, "prod"))
	assert.Empty(t, f.tagBuilds(t, f.a.templateID, "stable"))
	assert.Equal(t, []uuid.UUID{f.b.buildID}, f.tagBuilds(t, f.b.templateID, "prod"))
}

func TestPostTemplatesTagsIdempotencyUnavailableMatchesSpec(t *testing.T) {
	t.Parallel()
	unreachable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = unreachable.Close() })
	f := newTemplateTagsIdemFixtureWithStore(t, true, unreachable)
	body := templateTagsIdemBody(t, f.a.name+":source", "prod")

	w := f.post(t, f.a.apiKey, "/templates/tags", body, "unavailable")
	requireTemplateTagsIdemError(t, w, http.StatusServiceUnavailable, "", "true")
	require.Equal(t, 1, f.assignmentCount(t, f.a.templateID))

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	router, err := gorillamux.NewRouter(swagger)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/templates/tags", strings.NewReader(body))
	route, pathParams, err := router.FindRoute(req)
	require.NoError(t, err)
	require.NoError(t, openapi3filter.ValidateResponse(t.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: pathParams, Route: route},
		Status:                 w.Code,
		Header:                 w.Header(),
		Body:                   io.NopCloser(bytes.NewReader(w.Body.Bytes())),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true},
	}))

	parsed, err := api.ParsePostTemplatesTagsResponse(w.Result())
	require.NoError(t, err)
	require.NotNil(t, parsed.JSON503)
	require.NotNil(t, parsed.Headers503)
	require.NotNil(t, parsed.Headers503.ShouldRetry)
	require.True(t, *parsed.Headers503.ShouldRetry)
}

func TestPostTemplatesTagsRetryRefusals(t *testing.T) {
	t.Parallel()
	f := newTemplateTagsIdemFixture(t, true)
	before := f.assignmentCount(t, f.a.templateID)
	for _, test := range []struct {
		name, body  string
		status      int
		shouldRetry string
	}{
		{name: "malformed body", body: `{`, status: http.StatusBadRequest},
		{name: "missing tags", body: templateTagsIdemBody(t, f.a.name+":source"), status: http.StatusBadRequest},
		{name: "invalid target", body: templateTagsIdemBody(t, "invalid:target:tag", "prod"), status: http.StatusBadRequest, shouldRetry: "false"},
		{name: "invalid tag", body: templateTagsIdemBody(t, f.a.name+":source", "invalid/tag"), status: http.StatusBadRequest, shouldRetry: "false"},
		{name: "missing alias", body: templateTagsIdemBody(t, "missing-template", "prod"), status: http.StatusNotFound, shouldRetry: "false"},
		{name: "foreign template", body: templateTagsIdemBody(t, f.b.templateID+":source", "prod"), status: http.StatusForbidden, shouldRetry: "false"},
	} {
		key := strings.ReplaceAll(test.name, " ", "-")
		for _, requestKey := range []string{"", key} {
			w := f.post(t, f.a.apiKey, "/templates/tags", test.body, requestKey)
			requireTemplateTagsIdemError(t, w, test.status, "", test.shouldRetry)
		}
	}
	require.Equal(t, before, f.assignmentCount(t, f.a.templateID))

	body := templateTagsIdemBody(t, f.a.name+":source", "prod")
	unavailable := f.post(t, auth.PrefixAPIKey+"unavailable", "/templates/tags", body, "auth")
	requireTemplateTagsIdemError(t, unavailable, http.StatusInternalServerError, "", "")
	requireTemplateTagsIdemAssigned(t, f.post(t, f.a.apiKey, "/templates/tags", body, "auth"), f.a.buildID, "prod")
}
