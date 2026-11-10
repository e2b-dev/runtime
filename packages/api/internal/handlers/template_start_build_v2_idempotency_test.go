package handlers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	templatemanager "github.com/e2b-dev/infra/packages/api/internal/template-manager"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	buildStartV2Route     = "POST /v2/templates/:templateID/builds/:buildID"
	buildStartV2Node      = "build-start-v2-node"
	buildStartV2Body      = `{"fromImage":"alpine","startCmd":"start","readyCmd":"ready"}`
	buildStartV2UserAgent = "e2b-python-sdk/2.31.0"
)

// buildStartV2Builder stands in for the template builder behind the real
// template manager and counts the RPCs that cancel and dispatch builds.
type buildStartV2Builder struct {
	templatemanagergrpc.UnimplementedTemplateServiceServer

	creates atomic.Int32
	deletes atomic.Int32

	mu            sync.Mutex
	createErr     error
	deleteErr     error
	createEntered chan struct{}
	createRelease chan struct{}

	polled     chan struct{}
	pollOnce   sync.Once
	stopStatus chan struct{}
}

func (b *buildStartV2Builder) TemplateCreate(context.Context, *templatemanagergrpc.TemplateCreateRequest) (*emptypb.Empty, error) {
	b.creates.Add(1)
	b.mu.Lock()
	entered, release, err := b.createEntered, b.createRelease, b.createErr
	b.mu.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}

	return &emptypb.Empty{}, err
}

func (b *buildStartV2Builder) TemplateBuildDelete(context.Context, *templatemanagergrpc.TemplateBuildDeleteRequest) (*emptypb.Empty, error) {
	b.deletes.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()

	return &emptypb.Empty{}, b.deleteErr
}

// TemplateBuildStatus holds the dispatched build in progress for the rest of the test.
func (b *buildStartV2Builder) TemplateBuildStatus(ctx context.Context, _ *templatemanagergrpc.TemplateStatusRequest) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	b.pollOnce.Do(func() { close(b.polled) })
	select {
	case <-b.stopStatus:
	case <-ctx.Done():
	}

	return nil, status.Error(codes.Unavailable, "builder stopped")
}

func (b *buildStartV2Builder) failCreate(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createErr = err
}

func (b *buildStartV2Builder) failDelete(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteErr = err
}

func (b *buildStartV2Builder) blockCreate() (entered, release chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createEntered, b.createRelease = make(chan struct{}), make(chan struct{})

	return b.createEntered, b.createRelease
}

type buildStartV2Env struct {
	store   *APIStore
	db      *testutils.Database
	redis   redis.UniversalClient
	flags   *featureflags.Client
	builder *buildStartV2Builder
	conn    *grpc.ClientConn
	team    *types.Team
}

func newBuildStartV2Env(t *testing.T, routeEnabled bool, builderStatus infogrpc.ServiceInfoStatus) *buildStartV2Env {
	t.Helper()
	db := testutils.SetupDatabase(t)
	redisClient := redisutils.SetupInstance(t)
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{buildStartV2Route: routeEnabled})))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)

	builder := &buildStartV2Builder{polled: make(chan struct{}), stopStatus: make(chan struct{})}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	templatemanagergrpc.RegisterTemplateServiceServer(server, builder)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///build-start-v2-builder",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	)
	require.NoError(t, err)

	env := &buildStartV2Env{
		store: &APIStore{
			sqlcDB:              db.SqlcClient,
			templateCache:       templatecache.NewTemplateCache(db.SqlcClient, redisClient),
			templateBuildsCache: templatecache.NewTemplateBuildCache(db.SqlcClient, redisClient),
			featureFlags:        flags,
			posthog:             posthog,
		},
		db:      db,
		redis:   redisClient,
		flags:   flags,
		builder: builder,
		conn:    conn,
	}
	env.useBuilder(t, builderStatus)
	env.team = templateClusterTeam(t, db, nil)
	t.Cleanup(func() {
		// A dispatched build keeps polling a builder that now reports itself unavailable, which
		// touches neither the database nor the caches; the server and connection stay open for it.
		close(builder.stopStatus)
		ctx := context.WithoutCancel(t.Context())
		require.NoError(t, env.store.templateCache.Close(ctx))
		require.NoError(t, env.store.templateBuildsCache.Close(ctx))
		require.NoError(t, flags.Close(ctx))
	})

	return env
}

func (e *buildStartV2Env) useBuilder(t *testing.T, builderStatus infogrpc.ServiceInfoStatus) {
	t.Helper()
	pool := clusters.NewTestPool(clusters.NewTestBuilderCluster(consts.LocalClusterID, clusters.TestBuilder{
		NodeID: buildStartV2Node,
		Status: builderStatus,
		Client: clusters.NewTestSharedGRPCClient(e.conn),
	}))
	manager, err := templatemanager.New(e.db.SqlcClient, pool, e.store.templateBuildsCache, e.store.templateCache, e.flags)
	require.NoError(t, err)
	e.store.templateManager = manager
}

// registerBuild registers a pending build; a second call for the same team rebuilds the same template.
func (e *buildStartV2Env) registerBuild(t *testing.T) *api.TemplateRequestResponseV3 {
	t.Helper()
	registered := requestClusterTemplate(t, e.store, e.team)
	require.Equal(t, http.StatusAccepted, registered.StatusCode(), string(registered.Body))
	require.NotNil(t, registered.JSON202)

	return registered.JSON202
}

// registerRunningBuild leaves a build of the same template running on the builder,
// so starting the next build must cancel it.
func (e *buildStartV2Env) registerRunningBuild(t *testing.T) {
	t.Helper()
	running := e.registerBuild(t)
	require.NoError(t, e.db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.env_builds SET status = 'building', cluster_node_id = $2 WHERE id = $1`, uuid.MustParse(running.BuildID), buildStartV2Node))
}

func buildStartV2Path(build *api.TemplateRequestResponseV3) string {
	return fmt.Sprintf("/v2/templates/%s/builds/%s", build.TemplateID, build.BuildID)
}

type buildStartV2Request struct {
	team      *types.Team
	path      string
	body      string
	key       string
	userAgent string
}

func (e *buildStartV2Env) start(t *testing.T, r buildStartV2Request) *httptest.ResponseRecorder {
	t.Helper()
	team := r.team
	if team == nil {
		team = e.team
	}
	if r.body == "" {
		r.body = buildStartV2Body
	}
	if r.userAgent == "" {
		r.userAgent = buildStartV2UserAgent
	}
	router := gin.New()
	router.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, team) })
	router.Use(sharedmiddleware.RequestTimeout(time.Minute), idempotency.Middleware(e.redis, e.flags))
	router.POST("/v2/templates/:templateID/builds/:buildID", func(c *gin.Context) {
		e.store.PostV2TemplatesTemplateIDBuildsBuildID(c, c.Param("templateID"), c.Param("buildID"), api.PostV2TemplatesTemplateIDBuildsBuildIDParams{})
	})
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, r.path, strings.NewReader(r.body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", r.userAgent)
	if r.key != "" {
		req.Header.Set("Idempotency-Key", r.key)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	return w
}

func (e *buildStartV2Env) buildStatus(t *testing.T, build *api.TemplateRequestResponseV3) dbtypes.BuildStatusGroup {
	t.Helper()
	row, err := e.db.SqlcClient.GetTemplateBuildWithTemplate(t.Context(), queries.GetTemplateBuildWithTemplateParams{
		TemplateID: build.TemplateID, BuildID: uuid.MustParse(build.BuildID),
	})
	require.NoError(t, err)

	return row.EnvBuild.StatusGroup
}

// waitForPoll returns once the dispatched build is held by the builder, so no
// background database work outlives the test.
func (e *buildStartV2Env) waitForPoll(t *testing.T) {
	t.Helper()
	select {
	case <-e.builder.polled:
	case <-time.After(30 * time.Second):
		t.Fatal("dispatched build was never polled")
	}
}

func (e *buildStartV2Env) requireCounts(t *testing.T, creates, deletes int32) {
	t.Helper()
	require.Equal(t, creates, e.builder.creates.Load(), "TemplateCreate calls")
	require.Equal(t, deletes, e.builder.deletes.Load(), "TemplateBuildDelete calls")
}

func requireBuildStartV2Replay(t *testing.T, want, got *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, want.Code, got.Code, got.Body.String())
	require.Equal(t, want.Body.Bytes(), got.Body.Bytes())
	for _, name := range []string{"Content-Type", apierrors.ShouldRetryHeader} {
		require.Equal(t, want.Result().Header.Values(name), got.Result().Header.Values(name), name)
	}
}

func requireBuildStartV2Error(t *testing.T, w *httptest.ResponseRecorder, code int, text, shouldRetry string) {
	t.Helper()
	require.Equal(t, code, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), text)
	require.Equal(t, shouldRetry, w.Header().Get(apierrors.ShouldRetryHeader))
}

func TestPostV2TemplateBuildStartReplaysAfterTheBuildStarts(t *testing.T) {
	t.Parallel()
	env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
	build := env.registerBuild(t)
	path := buildStartV2Path(build)

	first := env.start(t, buildStartV2Request{path: path, key: "start-once"})
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Empty(t, first.Body.Bytes())
	require.Empty(t, first.Header().Get(apierrors.ShouldRetryHeader))
	env.requireCounts(t, 1, 0)
	require.Equal(t, dbtypes.BuildStatusGroupInProgress, env.buildStatus(t, build))
	env.waitForPoll(t)

	for name, r := range map[string]buildStartV2Request{
		"identical":                 {},
		"reordered with whitespace": {body: "{ \"readyCmd\" : \"ready\",\n\t\"startCmd\":\"start\",  \"fromImage\":\"alpine\" }"},
		"unknown field":             {body: `{"fromImage":"alpine","startCmd":"start","readyCmd":"ready","unknown":true}`},
		"another agent token":       {userAgent: buildStartV2UserAgent + " custom-client/1.0.0"},
		"unrecognized agent":        {userAgent: "custom-client/1.0.0"},
		"older SDK version":         {userAgent: "e2b-js-sdk/2.2.0"},
		"invalid SDK version":       {userAgent: "e2b-js-sdk/not-a-version"},
	} {
		r.path, r.key = path, "start-once"
		requireBuildStartV2Replay(t, first, env.start(t, r))
		env.requireCounts(t, 1, 0)
		require.Equal(t, dbtypes.BuildStatusGroupInProgress, env.buildStatus(t, build), name)
	}

	// Without the recorded result, the same request is refused because the build already started.
	for _, key := range []string{"", "another-key"} {
		requireBuildStartV2Error(t, env.start(t, buildStartV2Request{path: path, key: key}), http.StatusBadRequest, "build is not in waiting state", "false")
	}

	for name, r := range map[string]buildStartV2Request{
		"fromImage":        {body: `{"fromImage":"ubuntu","startCmd":"start","readyCmd":"ready"}`},
		"startCmd":         {body: `{"fromImage":"alpine","startCmd":"other","readyCmd":"ready"}`},
		"omitted readyCmd": {body: `{"fromImage":"alpine","startCmd":"start"}`},
		"force":            {body: `{"fromImage":"alpine","startCmd":"start","readyCmd":"ready","force":true}`},
		"empty steps":      {body: `{"fromImage":"alpine","startCmd":"start","readyCmd":"ready","steps":[]}`},
		"registry":         {body: `{"fromImage":"alpine","startCmd":"start","readyCmd":"ready","fromImageRegistry":{"type":"registry","username":"u","password":"p"}}`},
		"fromTemplate":     {body: `{"fromTemplate":"base","startCmd":"start","readyCmd":"ready"}`},
		"another build":    {path: fmt.Sprintf("/v2/templates/%s/builds/%s", build.TemplateID, uuid.NewString())},
		"query string":     {path: path + "?force=true"},
	} {
		if r.path == "" {
			r.path = path
		}
		r.key = "start-once"
		w := env.start(t, r)
		requireBuildStartV2Error(t, w, http.StatusConflict, "idempotency_request_mismatch", "false")
		env.requireCounts(t, 1, 0)
		require.Equal(t, dbtypes.BuildStatusGroupInProgress, env.buildStatus(t, build), name)
	}

	otherTeam := templateClusterTeam(t, env.db, nil)
	requireBuildStartV2Error(t, env.start(t, buildStartV2Request{team: otherTeam, path: path, key: "start-once"}), http.StatusForbidden, "User does not have access to the template", "false")
	env.requireCounts(t, 1, 0)
}

func TestPostV2TemplateBuildStartFingerprintsTheRawRegistry(t *testing.T) {
	t.Parallel()
	env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
	build := env.registerBuild(t)
	path := buildStartV2Path(build)
	withRegistry := func(registry string) string {
		return `{"fromImage":"alpine","startCmd":"start","readyCmd":"ready","fromImageRegistry":` + registry + `}`
	}

	first := env.start(t, buildStartV2Request{path: path, key: "start-registry", body: withRegistry(`{"type":"registry","username":"u","password":"p"}`)})
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	env.waitForPoll(t)

	requireBuildStartV2Replay(t, first, env.start(t, buildStartV2Request{path: path, key: "start-registry", body: withRegistry("{ \"type\" : \"registry\",\n\t\"username\":\"u\", \"password\":\"p\" }")}))
	// The registry is a union kept as raw JSON, so its key order and unknown fields are part of the request.
	for _, registry := range []string{
		`{"password":"p","username":"u","type":"registry"}`,
		`{"type":"registry","username":"u","password":"p","unknown":true}`,
		`{"type":"registry","username":"u","password":"other"}`,
		`{"type":"registry","username":"other","password":"p"}`,
		`{"type":"aws","awsAccessKeyId":"u","awsSecretAccessKey":"p","awsRegion":"us-east-1"}`,
		`{"type":"gcp","serviceAccountJson":"{}"}`,
	} {
		requireBuildStartV2Error(t, env.start(t, buildStartV2Request{path: path, key: "start-registry", body: withRegistry(registry)}), http.StatusConflict, "idempotency_request_mismatch", "false")
	}
	requireBuildStartV2Error(t, env.start(t, buildStartV2Request{path: path, key: "start-registry"}), http.StatusConflict, "idempotency_request_mismatch", "false")
	env.requireCounts(t, 1, 0)
}

func TestPostV2TemplateBuildStartRunsOnceForConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
	env.registerRunningBuild(t)
	build := env.registerBuild(t)
	request := buildStartV2Request{path: buildStartV2Path(build), key: "start-concurrently"}
	entered, release := env.builder.blockCreate()

	var owner *httptest.ResponseRecorder
	var wg sync.WaitGroup
	wg.Go(func() { owner = env.start(t, request) })
	<-entered
	duplicates := make([]*httptest.ResponseRecorder, 4)
	var duplicatesWG sync.WaitGroup
	for i := range duplicates {
		duplicatesWG.Go(func() { duplicates[i] = env.start(t, request) })
	}
	duplicatesWG.Wait()
	for _, duplicate := range duplicates {
		requireBuildStartV2Error(t, duplicate, http.StatusConflict, "idempotency_in_progress", "true")
	}
	close(release)
	wg.Wait()

	require.Equal(t, http.StatusAccepted, owner.Code, owner.Body.String())
	requireBuildStartV2Replay(t, owner, env.start(t, request))
	env.requireCounts(t, 1, 1)
	require.Equal(t, dbtypes.BuildStatusGroupInProgress, env.buildStatus(t, build))
	env.waitForPoll(t)
}

func TestPostV2TemplateBuildStartRetriesOnlyFailuresWithoutSideEffects(t *testing.T) {
	t.Parallel()

	t.Run("no builder before any cancellation", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Draining)
		build := env.registerBuild(t)
		request := buildStartV2Request{path: buildStartV2Path(build), key: "retry-capacity"}

		requireBuildStartV2Error(t, env.start(t, request), http.StatusServiceUnavailable, "Error when getting available build client", "true")
		env.requireCounts(t, 0, 0)
		require.Equal(t, dbtypes.BuildStatusGroupPending, env.buildStatus(t, build))

		env.useBuilder(t, infogrpc.ServiceInfoStatus_Healthy)
		started := env.start(t, request)
		require.Equal(t, http.StatusAccepted, started.Code, started.Body.String())
		requireBuildStartV2Replay(t, started, env.start(t, request))
		env.requireCounts(t, 1, 0)
		env.waitForPoll(t)
	})

	t.Run("lookup failure", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
		build := env.registerBuild(t)
		request := buildStartV2Request{path: buildStartV2Path(build), key: "retry-lookup"}
		closedDB := testutils.SetupDatabase(t)
		require.NoError(t, closedDB.SqlcClient.Close())

		env.store.sqlcDB = closedDB.SqlcClient
		requireBuildStartV2Error(t, env.start(t, request), http.StatusNotFound, "Error when getting template", "true")
		env.store.sqlcDB = env.db.SqlcClient
		started := env.start(t, request)
		require.Equal(t, http.StatusAccepted, started.Code, started.Body.String())
		env.requireCounts(t, 1, 0)
		env.waitForPoll(t)
	})

	t.Run("concurrent build read failure", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
		concurrent := env.registerBuild(t)
		build := env.registerBuild(t)
		request := buildStartV2Request{path: buildStartV2Path(build), key: "retry-concurrent-read"}
		// Only the concurrent-build query reads the other build's row; an array reason cannot be scanned,
		// and without a node the running build is never cancelled.
		setReason := func(reason string) {
			require.NoError(t, env.db.SqlcClient.TestsRawSQL(t.Context(),
				`UPDATE public.env_builds SET status = 'building', cluster_node_id = NULL, reason = $2::jsonb WHERE id = $1`, uuid.MustParse(concurrent.BuildID), reason))
		}

		setReason(`[]`)
		requireBuildStartV2Error(t, env.start(t, request), http.StatusInternalServerError, "Error during template build request", "true")
		env.requireCounts(t, 0, 0)
		require.Equal(t, dbtypes.BuildStatusGroupPending, env.buildStatus(t, build))

		setReason(`{}`)
		started := env.start(t, request)
		require.Equal(t, http.StatusAccepted, started.Code, started.Body.String())
		env.requireCounts(t, 1, 0)
		env.waitForPoll(t)
	})

	t.Run("no builder after a cancellation", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Draining)
		env.registerRunningBuild(t)
		build := env.registerBuild(t)
		request := buildStartV2Request{path: buildStartV2Path(build), key: "replay-capacity"}

		failed := env.start(t, request)
		requireBuildStartV2Error(t, failed, http.StatusServiceUnavailable, "Error when getting available build client", "false")
		env.requireCounts(t, 0, 1)

		env.useBuilder(t, infogrpc.ServiceInfoStatus_Healthy)
		requireBuildStartV2Replay(t, failed, env.start(t, request))
		env.requireCounts(t, 0, 1)
		require.Equal(t, dbtypes.BuildStatusGroupPending, env.buildStatus(t, build))
	})

	t.Run("failed cancellation", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
		env.registerRunningBuild(t)
		build := env.registerBuild(t)
		request := buildStartV2Request{path: buildStartV2Path(build), key: "replay-cancel"}
		env.builder.failDelete(status.Error(codes.Unavailable, "builder unavailable"))

		failed := env.start(t, request)
		requireBuildStartV2Error(t, failed, http.StatusInternalServerError, "Error during template build request", "false")
		env.builder.failDelete(nil)
		requireBuildStartV2Replay(t, failed, env.start(t, request))
		env.requireCounts(t, 0, 1)
	})

	t.Run("unavailable dispatch", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
		build := env.registerBuild(t)
		request := buildStartV2Request{path: buildStartV2Path(build), key: "replay-dispatch"}
		env.builder.failCreate(status.Error(codes.Unavailable, "builder unavailable"))

		failed := env.start(t, request)
		requireBuildStartV2Error(t, failed, http.StatusInternalServerError, "Error when starting template build", "false")
		env.builder.failCreate(nil)
		requireBuildStartV2Replay(t, failed, env.start(t, request))
		env.requireCounts(t, 1, 0)
		require.Equal(t, dbtypes.BuildStatusGroupFailed, env.buildStatus(t, build))
	})

	t.Run("deterministic rejections", func(t *testing.T) {
		t.Parallel()
		env := newBuildStartV2Env(t, true, infogrpc.ServiceInfoStatus_Healthy)
		build := env.registerBuild(t)
		path := buildStartV2Path(build)

		noSource := buildStartV2Request{path: path, key: "replay-source", body: `{"steps":[]}`}
		rejected := env.start(t, noSource)
		requireBuildStartV2Error(t, rejected, http.StatusBadRequest, "must specify either fromImage or fromTemplate", "false")
		requireBuildStartV2Replay(t, rejected, env.start(t, noSource))
		requireBuildStartV2Error(t, env.start(t, buildStartV2Request{path: path, key: "replay-source"}), http.StatusConflict, "idempotency_request_mismatch", "false")

		badAgent := buildStartV2Request{path: path, key: "replay-agent", userAgent: "e2b-js-sdk/not-a-version"}
		rejected = env.start(t, badAgent)
		requireBuildStartV2Error(t, rejected, http.StatusBadRequest, "Error when parsing user agent", "false")
		requireBuildStartV2Replay(t, rejected, env.start(t, badAgent))
		requireBuildStartV2Replay(t, rejected, env.start(t, buildStartV2Request{path: path, key: "replay-agent"}))

		env.requireCounts(t, 0, 0)
		require.Equal(t, dbtypes.BuildStatusGroupPending, env.buildStatus(t, build))
	})
}

func TestPostV2TemplateBuildStartWithoutIdempotencyRunsEveryRequest(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		routeEnabled bool
		key          string
	}{
		"no key":         {routeEnabled: true},
		"route disabled": {key: "ignored-key"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newBuildStartV2Env(t, test.routeEnabled, infogrpc.ServiceInfoStatus_Healthy)
			build := env.registerBuild(t)
			request := buildStartV2Request{path: buildStartV2Path(build), key: test.key}

			started := env.start(t, request)
			require.Equal(t, http.StatusAccepted, started.Code, started.Body.String())
			require.Empty(t, started.Body.Bytes())
			requireBuildStartV2Error(t, env.start(t, request), http.StatusBadRequest, "build is not in waiting state", "false")
			env.requireCounts(t, 1, 0)
			env.waitForPoll(t)
		})
	}
}
