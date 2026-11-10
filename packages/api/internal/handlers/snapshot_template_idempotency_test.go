package handlers

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	custommiddleware "github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const snapshotIdempotencyTeamSlug = "test-team"

type snapshotIdempotencyBackend struct {
	teamID    uuid.UUID
	sandboxID string
	gone      atomic.Bool
	reads     atomic.Int64
	checks    atomic.Int64
	creates   atomic.Int64
	// Set only while no request is in flight.
	getErr      error
	supportErr  error
	createErr   error
	envdVersion string
	started     chan struct{}
	release     chan struct{}
	lastOpts    atomic.Pointer[orchestrator.SnapshotTemplateOpts]
}

func (s *snapshotIdempotencyBackend) GetSandbox(_ context.Context, teamID uuid.UUID, sandboxID string) (sandbox.Sandbox, error) {
	s.reads.Add(1)
	if s.getErr != nil {
		return sandbox.Sandbox{}, s.getErr
	}
	if teamID != s.teamID || sandboxID != s.sandboxID || s.gone.Load() {
		return sandbox.Sandbox{}, sandbox.ErrNotFound
	}

	return sandbox.Sandbox{SandboxID: sandboxID, TeamID: teamID, State: sandbox.StateRunning, EnvdVersion: cmp.Or(s.envdVersion, sharedutils.MinEnvdVersionForSnapshot)}, nil
}

func (s *snapshotIdempotencyBackend) EnsureSnapshotSupport(context.Context, sandbox.Sandbox, orchestrator.SnapshotTemplateOpts) error {
	s.checks.Add(1)

	return s.supportErr
}

func (s *snapshotIdempotencyBackend) CreateSnapshotTemplate(_ context.Context, _ uuid.UUID, _ string, opts orchestrator.SnapshotTemplateOpts) (orchestrator.SnapshotTemplateResult, error) {
	call := s.creates.Add(1)
	s.lastOpts.Store(&opts)
	if call == 1 && s.started != nil {
		close(s.started)
		<-s.release
	}
	if s.createErr != nil {
		return orchestrator.SnapshotTemplateResult{}, s.createErr
	}

	return orchestrator.SnapshotTemplateResult{TemplateID: fmt.Sprintf("snapshot%d", call)}, nil
}

type snapshotIdempotencyFixture struct {
	backend   *snapshotIdempotencyBackend
	store     *APIStore
	router    *gin.Engine
	cache     *templatecache.TemplateCache
	otherTeam uuid.UUID
	blocked   atomic.Bool
}

func newSnapshotIdempotencyFixture(t *testing.T, enabled bool) *snapshotIdempotencyFixture {
	t.Helper()
	client := redisutils.SetupInstance(t)
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{
		"POST /sandboxes/:sandboxID/snapshots": enabled,
	})))
	source.Update(source.Flag(featureflags.FilesystemOnlyCheckpointFlag.Key()).VariationForAll(true))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
	db := testutils.SetupDatabase(t)
	cache := templatecache.NewTemplateCache(db.SqlcClient, client)
	t.Cleanup(func() { require.NoError(t, cache.Close(context.WithoutCancel(t.Context()))) })

	f := &snapshotIdempotencyFixture{
		backend:   &snapshotIdempotencyBackend{teamID: uuid.New(), sandboxID: "i" + id.Generate()},
		cache:     cache,
		otherTeam: uuid.New(),
	}
	f.store = &APIStore{snapshotBackendOverride: f.backend, featureFlags: flags, templateCache: cache}

	spec, err := api.GetSwagger()
	require.NoError(t, err)
	spec.Servers = nil
	r := gin.New()
	r.Use(sharedmiddleware.RequestTimeout(time.Minute))
	r.Use(func(c *gin.Context) {
		teamID := f.backend.teamID
		switch c.GetHeader("X-API-Key") {
		case "team":
		case "other-team":
			teamID = f.otherTeam
		default:
			c.AbortWithStatus(http.StatusUnauthorized)

			return
		}
		team := minimalTeamInfo()
		team.ID = teamID
		team.Slug = snapshotIdempotencyTeamSlug
		team.IsBlocked = f.blocked.Load()
		auth.SetTeamInfoForTest(t, c, team)
	})
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(spec, &ginmiddleware.Options{
		Options: openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
	}))
	r.Use(custommiddleware.InitLaunchDarklyContext, custommiddleware.EnforceBlockedTeam())
	r.Use(idempotency.Middleware(client, flags))
	api.RegisterHandlers(r, f.store)
	f.router = r

	return f
}

type snapshotIdempotencyCall struct {
	target string
	body   string
	key    string
	apiKey string
	agent  string
}

func (f *snapshotIdempotencyFixture) post(t *testing.T, call snapshotIdempotencyCall) *httptest.ResponseRecorder {
	t.Helper()
	target := call.target
	if target == "" {
		target = "/sandboxes/" + f.backend.sandboxID + "/snapshots"
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(call.body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", cmp.Or(call.apiKey, "team"))
	if call.agent != "" {
		req.Header.Set("User-Agent", call.agent)
	}
	if call.key != "" {
		req.Header.Set("Idempotency-Key", call.key)
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, req)

	return w
}

func requireSnapshotReplay(t *testing.T, original, replay *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, original.Code, replay.Code, replay.Body.String())
	require.Equal(t, original.Body.String(), replay.Body.String())
	require.Equal(t, original.Header().Get("Content-Type"), replay.Header().Get("Content-Type"))
	require.Equal(t, original.Header().Values(apierrors.ShouldRetryHeader), replay.Header().Values(apierrors.ShouldRetryHeader))
}

func requireSnapshotIdempotencyError(t *testing.T, w *httptest.ResponseRecorder, status int, errorCode, shouldRetry string) {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	var body struct {
		ErrorCode string `json:"error_code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, errorCode, body.ErrorCode)
	assert.Equal(t, shouldRetry, w.Header().Get(apierrors.ShouldRetryHeader))
}

func TestSnapshotIdempotencyReplaysAfterSourceIsGone(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	first := f.post(t, snapshotIdempotencyCall{body: `{"memory":true,"name":"replayed-snapshot"}`, key: "replay-key"})
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	require.JSONEq(t, `{"snapshotID":"test-team/replayed-snapshot:default","names":["test-team/replayed-snapshot"]}`, first.Body.String())
	require.Empty(t, first.Header().Values(apierrors.ShouldRetryHeader))

	f.backend.gone.Store(true)
	for _, body := range []string{
		`{"memory":true,"name":"replayed-snapshot"}`,
		` { "name" : "replayed-snapshot", "memory" : true, "ignored": [1] } `,
		`{"name":"replayed-snapshot"}`,
	} {
		requireSnapshotReplay(t, first, f.post(t, snapshotIdempotencyCall{body: body, key: "replay-key", agent: "another-client/1.0"}))
	}
	assert.EqualValues(t, 1, f.backend.creates.Load())
	assert.EqualValues(t, 1, f.backend.reads.Load())
}

func TestSnapshotIdempotencyRejectsChangedRequests(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	first := f.post(t, snapshotIdempotencyCall{body: `{}`, key: "changed-key"})
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	path := "/sandboxes/" + f.backend.sandboxID + "/snapshots"
	for _, call := range []snapshotIdempotencyCall{
		{body: `{"memory":false}`},
		{body: `{"name":"another-snapshot"}`},
		{target: "/sandboxes/i" + id.Generate() + "/snapshots", body: `{}`},
		{target: "/sandboxes/" + f.backend.sandboxID + "-client/snapshots", body: `{}`},
		{target: path + "?retry=1", body: `{}`},
	} {
		call.key = "changed-key"
		requireSnapshotIdempotencyError(t, f.post(t, call), http.StatusConflict, "idempotency_request_mismatch", "false")
	}
	assert.EqualValues(t, 1, f.backend.creates.Load())
	assert.EqualValues(t, 1, f.backend.reads.Load())
	requireSnapshotReplay(t, first, f.post(t, snapshotIdempotencyCall{body: `{"memory":true}`, key: "changed-key"}))
}

func TestSnapshotIdempotencyReportsConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	f.backend.started = make(chan struct{})
	f.backend.release = make(chan struct{})
	release := sync.OnceFunc(func() { close(f.backend.release) })
	t.Cleanup(release)
	owner := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		owner <- f.post(t, snapshotIdempotencyCall{body: `{"memory":false}`, key: "concurrent-key"})
	}()
	select {
	case <-f.backend.started:
	case w := <-owner:
		t.Fatalf("snapshot finished before it was blocked: %d %s", w.Code, w.Body.String())
	}

	duplicates := make(chan *httptest.ResponseRecorder, 4)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			duplicates <- f.post(t, snapshotIdempotencyCall{body: `{"memory":false}`, key: "concurrent-key"})
		})
	}
	wg.Wait()
	close(duplicates)
	for w := range duplicates {
		requireSnapshotIdempotencyError(t, w, http.StatusConflict, "idempotency_in_progress", "true")
	}
	assert.EqualValues(t, 1, f.backend.creates.Load())

	release()
	first := <-owner
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	requireSnapshotReplay(t, first, f.post(t, snapshotIdempotencyCall{body: `{"memory":false}`, key: "concurrent-key"}))
	assert.EqualValues(t, 1, f.backend.creates.Load())
	assert.True(t, f.backend.lastOpts.Load().FilesystemOnly)
}

func TestSnapshotIdempotencyRetriesFailuresWithoutSnapshot(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		body        string
		getErr      error
		supportErr  error
		createErr   error
		status      int
		wantCreates int64
	}{
		"sandbox lookup failure": {getErr: errors.New("store unavailable"), status: http.StatusInternalServerError, wantCreates: 1},
		"api draining":           {createErr: orchestrator.ErrDraining, status: http.StatusServiceUnavailable, wantCreates: 2},
		"source in another transition": {
			createErr: &sandbox.InvalidStateTransitionError{CurrentState: sandbox.StatePausing}, status: http.StatusConflict, wantCreates: 2,
		},
		"node below version floor": {
			body: `{"memory":false}`, supportErr: orchestrator.NodeFeatureMissingError{NodeID: "old"}, status: http.StatusConflict, wantCreates: 1,
		},
		"source lifecycle held": {
			createErr: fmt.Errorf("failed to start snapshotting: %w", sandbox.ErrLifecycleHeld), status: http.StatusConflict, wantCreates: 2,
		},
		"source lock lost": {
			createErr: fmt.Errorf("failed to start snapshotting: %w", sandbox.ErrLockLost), status: http.StatusConflict, wantCreates: 2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			body := cmp.Or(tc.body, `{}`)
			f := newSnapshotIdempotencyFixture(t, true)
			f.backend.getErr, f.backend.supportErr, f.backend.createErr = tc.getErr, tc.supportErr, tc.createErr
			failed := f.post(t, snapshotIdempotencyCall{body: body, key: "retry-key"})
			require.Equal(t, tc.status, failed.Code, failed.Body.String())
			assert.Equal(t, "true", failed.Header().Get(apierrors.ShouldRetryHeader))

			f.backend.getErr, f.backend.supportErr, f.backend.createErr = nil, nil, nil
			retried := f.post(t, snapshotIdempotencyCall{body: body, key: "retry-key"})
			require.Equal(t, http.StatusCreated, retried.Code, retried.Body.String())
			assert.Equal(t, tc.wantCreates, f.backend.creates.Load())
			requireSnapshotReplay(t, retried, f.post(t, snapshotIdempotencyCall{body: body, key: "retry-key"}))
			assert.Equal(t, tc.wantCreates, f.backend.creates.Load())
		})
	}
}

func TestSnapshotIdempotencyRetriesAliasResolutionFailure(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	closedDB := testutils.SetupDatabase(t)
	failedCache := templatecache.NewTemplateCache(closedDB.SqlcClient, redisutils.SetupInstance(t))
	t.Cleanup(func() { require.NoError(t, failedCache.Close(context.WithoutCancel(t.Context()))) })
	require.NoError(t, closedDB.SqlcClient.Close())
	f.store.templateCache = failedCache

	failed := f.post(t, snapshotIdempotencyCall{body: `{"name":"retried-alias"}`, key: "alias-key"})
	require.Equal(t, http.StatusInternalServerError, failed.Code, failed.Body.String())
	assert.Equal(t, "true", failed.Header().Get(apierrors.ShouldRetryHeader))
	assert.Zero(t, f.backend.reads.Load())

	f.store.templateCache = f.cache
	retried := f.post(t, snapshotIdempotencyCall{body: `{"name":"retried-alias"}`, key: "alias-key"})
	require.Equal(t, http.StatusCreated, retried.Code, retried.Body.String())
	assert.EqualValues(t, 1, f.backend.creates.Load())
}

func TestSnapshotIdempotencyKeepsOutcomesThatMayHaveSnapshotted(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		body        string
		createErr   error
		status      int
		creates     int64
		shouldRetry string
	}{
		"checkpoint failure": {body: `{}`, createErr: errors.New("checkpoint failed"), status: http.StatusInternalServerError, creates: 1, shouldRetry: "false"},
		"checkpoint in progress": {
			body: `{}`, createErr: orchestrator.PauseRefusedCheckpointInProgressError{}, status: http.StatusServiceUnavailable, creates: 1,
		},
		"guest agent unresponsive": {
			body: `{}`, createErr: orchestrator.PauseRefusedEnvdUnresponsiveError{}, status: http.StatusServiceUnavailable, creates: 1,
		},
		"node queue exhausted": {
			body: `{}`, createErr: orchestrator.PauseQueueExhaustedError{}, status: http.StatusServiceUnavailable, creates: 1,
		},
		"source lost during snapshot": {
			body: `{}`, createErr: fmt.Errorf("failed to start snapshotting: %w", sandbox.ErrNotFound), status: http.StatusNotFound, creates: 1, shouldRetry: "false",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotIdempotencyFixture(t, true)
			f.backend.createErr = tc.createErr
			first := f.post(t, snapshotIdempotencyCall{body: tc.body, key: "kept-key"})
			require.Equal(t, tc.status, first.Code, first.Body.String())
			assert.Equal(t, tc.shouldRetry, first.Header().Get(apierrors.ShouldRetryHeader))

			f.backend.createErr = nil
			requireSnapshotReplay(t, first, f.post(t, snapshotIdempotencyCall{body: tc.body, key: "kept-key"}))
			requireSnapshotIdempotencyError(t, f.post(t, snapshotIdempotencyCall{body: `{"name":"changed"}`, key: "kept-key"}),
				http.StatusConflict, "idempotency_request_mismatch", "false")
			assert.Equal(t, tc.creates, f.backend.creates.Load())
			assert.EqualValues(t, 1, f.backend.checks.Load())
		})
	}
}

func TestSnapshotIdempotencyReplaysPermanentFailures(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		body        string
		getErr      error
		envdVersion string
		createErr   error
		status      int
		creates     int64
	}{
		"invalid name":             {body: `{"name":"invalid name!"}`, status: http.StatusBadRequest},
		"another team's namespace": {body: `{"name":"other-team/snapshot"}`, status: http.StatusBadRequest},
		"sandbox not found":        {body: `{}`, getErr: sandbox.ErrNotFound, status: http.StatusNotFound},
		"guest agent too old":      {body: `{}`, envdVersion: "0.0.1", status: http.StatusBadRequest},
		"guest cannot skip memory": {body: `{"memory":false}`, createErr: orchestrator.PauseUnsupportedError{}, status: http.StatusConflict, creates: 1},
		"node has memory-free off": {body: `{"memory":false}`, createErr: orchestrator.FilesystemOnlyDisabledError{}, status: http.StatusBadRequest, creates: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotIdempotencyFixture(t, true)
			f.backend.getErr, f.backend.envdVersion, f.backend.createErr = tc.getErr, tc.envdVersion, tc.createErr
			first := f.post(t, snapshotIdempotencyCall{body: tc.body, key: "permanent-key"})
			require.Equal(t, tc.status, first.Code, first.Body.String())
			assert.Equal(t, "false", first.Header().Get(apierrors.ShouldRetryHeader))

			f.backend.getErr, f.backend.envdVersion, f.backend.createErr = nil, "", nil
			requireSnapshotReplay(t, first, f.post(t, snapshotIdempotencyCall{body: tc.body, key: "permanent-key"}))
			assert.Equal(t, tc.creates, f.backend.creates.Load())
		})
	}
}

func TestSnapshotIdempotencyBypassesWithoutKeyOrRoute(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		enabled bool
		keys    [2]string
	}{
		"route disabled": {enabled: false, keys: [2]string{"same-key", "same-key"}},
		"no key":         {enabled: true},
		"fresh key":      {enabled: true, keys: [2]string{"first-key", "second-key"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotIdempotencyFixture(t, tc.enabled)
			for i, key := range tc.keys {
				w := f.post(t, snapshotIdempotencyCall{body: `{}`, key: key})
				require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
				require.JSONEq(t, fmt.Sprintf(`{"snapshotID":"snapshot%d:default","names":[]}`, i+1), w.Body.String())
			}
			assert.EqualValues(t, 2, f.backend.creates.Load())
		})
	}
}

func TestSnapshotIdempotencyIsScopedToAuthenticatedTeam(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	first := f.post(t, snapshotIdempotencyCall{body: `{}`, key: "team-key"})
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	other := f.post(t, snapshotIdempotencyCall{body: `{}`, key: "team-key", apiKey: "other-team"})
	require.Equal(t, http.StatusNotFound, other.Code, other.Body.String())
	unauthenticated := f.post(t, snapshotIdempotencyCall{body: `{}`, key: "team-key", apiKey: "revoked"})
	require.Equal(t, http.StatusUnauthorized, unauthenticated.Code)
	require.Empty(t, unauthenticated.Body.String())
	f.blocked.Store(true)
	blocked := f.post(t, snapshotIdempotencyCall{body: `{}`, key: "team-key"})
	require.Equal(t, http.StatusForbidden, blocked.Code, blocked.Body.String())
	f.blocked.Store(false)

	requireSnapshotReplay(t, first, f.post(t, snapshotIdempotencyCall{body: `{}`, key: "team-key"}))
	assert.EqualValues(t, 1, f.backend.creates.Load())
	assert.EqualValues(t, 2, f.backend.reads.Load())
}

func TestSnapshotIdempotencyRejectsInvalidRequestsBeforeReserving(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	for _, call := range []snapshotIdempotencyCall{
		{body: `{}`, key: "Upper-Case-Key"},
		{body: `{}`, key: strings.Repeat("k", 256)},
		{body: `{"memory":"yes"}`, key: "valid-key"},
	} {
		w := f.post(t, call)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	}
	assert.Zero(t, f.backend.reads.Load())

	w := f.post(t, snapshotIdempotencyCall{body: `{}`, key: "valid-key"})
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.EqualValues(t, 1, f.backend.creates.Load())
}

func TestSnapshotRejectsInvalidSandboxIDWithoutRetry(t *testing.T) {
	t.Parallel()
	f := newSnapshotIdempotencyFixture(t, true)
	w := f.post(t, snapshotIdempotencyCall{target: "/sandboxes/a-b-c/snapshots", body: `{}`, key: "invalid-id-key"})
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Equal(t, "false", w.Header().Get(apierrors.ShouldRetryHeader))
	assert.Zero(t, f.backend.reads.Load())
}
