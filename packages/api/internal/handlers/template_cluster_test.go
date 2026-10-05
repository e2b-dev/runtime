package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	templateregistration "github.com/e2b-dev/infra/packages/api/internal/template"
	templatemanager "github.com/e2b-dev/infra/packages/api/internal/template-manager"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	sharedclusters "github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

func templateClusterStore(t *testing.T) (*APIStore, *testutils.Database) {
	t.Helper()
	db := testutils.SetupDatabase(t)
	redis := redisutils.SetupInstance(t)
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	store := &APIStore{
		sqlcDB:              db.SqlcClient,
		templateCache:       templatecache.NewTemplateCache(db.SqlcClient, redis),
		templateBuildsCache: templatecache.NewTemplateBuildCache(db.SqlcClient, redis),
		featureFlags:        flags,
		posthog:             posthog,
	}
	store.templateManager, err = templatemanager.New(db.SqlcClient, clusters.NewTestPool(), nil, store.templateCache, flags)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		require.NoError(t, store.templateCache.Close(ctx))
		require.NoError(t, store.templateBuildsCache.Close(ctx))
		require.NoError(t, flags.Close(ctx))
	})

	return store, db
}

func templateClusterTeam(t *testing.T, db *testutils.Database, clusterID *uuid.UUID) *types.Team {
	t.Helper()
	teamID := testutils.CreateTestTeam(t, db)
	team := &types.Team{
		Team:   &authqueries.Team{ID: teamID, Slug: testutils.GetTeamSlug(t, t.Context(), db, teamID)},
		Limits: &types.TeamLimits{BuildConcurrency: 32, MaxVcpu: 8, MaxRamMb: 8192, DefaultFreeDiskSizeMb: 10240, MaxFreeDiskSizeMb: 25600},
	}
	assignTemplateClusterTeam(t, db, team, clusterID)

	return team
}

func assignTemplateClusterTeam(t *testing.T, db *testutils.Database, team *types.Team, clusterID *uuid.UUID) {
	t.Helper()
	if clusterID != nil {
		require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
			`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token)
			 VALUES ($1::uuid, $1::uuid::text, $1::uuid::text, true, 'test-token') ON CONFLICT (id) DO NOTHING`, clusterID))
	}
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, team.ID, clusterID))
	team.ClusterID = clusterID
}

func requestClusterTemplate(t *testing.T, store *APIStore, team *types.Team) *api.PostV3TemplatesResponse {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v3/templates", strings.NewReader(`{"name":"base"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, c, team)
	store.PostV3Templates(c)
	response, err := api.ParsePostV3TemplatesResponse(w.Result())
	require.NoError(t, err)

	return response
}

func TestTemplateRebuildCluster(t *testing.T) {
	t.Parallel()
	store, db := templateClusterStore(t)
	first, second, local := uuid.New(), uuid.New(), consts.LocalClusterID
	for _, tt := range []struct {
		name              string
		original, current *uuid.UUID
		promoted          bool
		status            int
	}{
		{"default", nil, nil, false, http.StatusAccepted},
		{"explicit-default", nil, &local, false, http.StatusAccepted},
		{"same-cluster", &first, &first, false, http.StatusAccepted},
		{"default-to-custom", nil, &first, false, http.StatusAccepted},
		{"custom-to-default", &first, nil, false, http.StatusAccepted},
		{"different-cluster", &first, &second, false, http.StatusAccepted},
		{"promoted-template", nil, &first, true, http.StatusAccepted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			team := templateClusterTeam(t, db, tt.original)
			initial := requestClusterTemplate(t, store, team)
			require.Equal(t, http.StatusAccepted, initial.StatusCode(), string(initial.Body))
			require.NotNil(t, initial.JSON202)
			before, err := db.SqlcClient.GetTemplateBuildWithTemplate(t.Context(), queries.GetTemplateBuildWithTemplateParams{
				TemplateID: initial.JSON202.TemplateID, BuildID: uuid.MustParse(initial.JSON202.BuildID),
			})
			require.NoError(t, err)
			if tt.promoted {
				require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.env_aliases SET namespace = NULL WHERE env_id = $1`, initial.JSON202.TemplateID))
				require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET public = true WHERE id = $1`, initial.JSON202.TemplateID))
				team = templateClusterTeam(t, db, tt.current)
			} else {
				assignTemplateClusterTeam(t, db, team, tt.current)
			}
			result := requestClusterTemplate(t, store, team)
			require.Equal(t, tt.status, result.StatusCode(), string(result.Body))
			after, err := db.SqlcClient.GetTemplateBuildWithTemplate(t.Context(), queries.GetTemplateBuildWithTemplateParams{
				TemplateID: initial.JSON202.TemplateID, BuildID: uuid.MustParse(initial.JSON202.BuildID),
			})
			require.NoError(t, err)
			require.Equal(t, before.ActiveEnv.ClusterID, after.ActiveEnv.ClusterID, "rebuilding must not move the template")
			require.NotNil(t, result.JSON202)
			moved := sharedclusters.WithClusterFallback(tt.original) != sharedclusters.WithClusterFallback(tt.current)
			if moved && !tt.promoted {
				require.Equal(t, before, after, "cross-cluster rebuild must preserve old build and template")
			}
			if tt.promoted || moved {
				require.NotEqual(t, initial.JSON202.TemplateID, result.JSON202.TemplateID)
			} else {
				require.Equal(t, initial.JSON202.TemplateID, result.JSON202.TemplateID)
			}
			template, err := db.SqlcClient.GetTemplateById(t.Context(), result.JSON202.TemplateID)
			require.NoError(t, err)
			require.Equal(t, team.ID, template.TeamID)
			require.Equal(t, sharedclusters.WithClusterFallback(tt.current), sharedclusters.WithClusterFallback(template.ClusterID))
		})
	}
}

func TestTemplateClusterRebuildReadyLookup(t *testing.T) {
	t.Parallel()
	store, db := templateClusterStore(t)
	team := templateClusterTeam(t, db, nil)
	initial := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, initial.StatusCode(), string(initial.Body))
	require.NotNil(t, initial.JSON202)
	old := initial.JSON202
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET public = true WHERE id = $1`, old.TemplateID))
	require.NoError(t, db.SqlcClient.FinishTemplateBuild(t.Context(), queries.FinishTemplateBuildParams{
		BuildID: uuid.MustParse(old.BuildID), Status: dbtypes.BuildStatusUploaded,
	}))
	alias, err := store.templateCache.ResolveAlias(t.Context(), "base", team.Slug)
	require.NoError(t, err)
	_, _, err = store.templateCache.Get(t.Context(), alias.TemplateID, nil, team.ID, consts.LocalClusterID)
	require.NoError(t, err)
	clusterID := uuid.New()
	assignTemplateClusterTeam(t, db, team, &clusterID)
	rebuilt := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, rebuilt.StatusCode(), string(rebuilt.Body))
	require.NotNil(t, rebuilt.JSON202)
	require.NotEqual(t, old.TemplateID, rebuilt.JSON202.TemplateID)
	require.True(t, rebuilt.JSON202.Public)
	replacement, err := db.SqlcClient.GetTemplateById(t.Context(), rebuilt.JSON202.TemplateID)
	require.NoError(t, err)
	require.True(t, replacement.Public, "cross-cluster rebuild preserves the visibility setting")
	alias, err = store.templateCache.ResolveAlias(t.Context(), "base", team.Slug)
	require.NoError(t, err)
	require.Equal(t, rebuilt.JSON202.TemplateID, alias.TemplateID, "cached alias must resolve to the new build's template")
	_, _, err = store.templateCache.Get(t.Context(), alias.TemplateID, nil, team.ID, clusterID)
	require.ErrorIs(t, err, templatecache.ErrTemplateNotFound, "pending rebuild must not expose the old ready artifact")
	require.NoError(t, db.SqlcClient.FinishTemplateBuild(t.Context(), queries.FinishTemplateBuildParams{
		BuildID: uuid.MustParse(rebuilt.JSON202.BuildID), Status: dbtypes.BuildStatusFailed,
	}))
	_, _, err = store.templateCache.Get(t.Context(), alias.TemplateID, nil, team.ID, clusterID)
	require.ErrorIs(t, err, templatecache.ErrTemplateNotFound, "failed rebuild must not fall back to the old cluster")
	_, oldBuild, err := store.templateCache.Get(t.Context(), old.TemplateID, nil, team.ID, consts.LocalClusterID)
	require.NoError(t, err)
	require.Equal(t, old.BuildID, oldBuild.ID.String())
	_, _, err = store.templateCache.Get(t.Context(), old.TemplateID, nil, team.ID, clusterID)
	require.ErrorIs(t, err, templatecache.ErrClusterMismatch)
	retried := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, retried.StatusCode(), string(retried.Body))
	require.NotNil(t, retried.JSON202)
	require.Equal(t, rebuilt.JSON202.TemplateID, retried.JSON202.TemplateID)
	require.NoError(t, db.SqlcClient.FinishTemplateBuild(t.Context(), queries.FinishTemplateBuildParams{
		BuildID: uuid.MustParse(retried.JSON202.BuildID), Status: dbtypes.BuildStatusUploaded,
	}))
	_, readyBuild, err := store.templateCache.Get(t.Context(), alias.TemplateID, nil, team.ID, clusterID)
	require.NoError(t, err)
	require.Equal(t, retried.JSON202.BuildID, readyBuild.ID.String())
	reader := &originalClusterLogs{}
	store.clusters = clusters.NewTestPool(clusters.NewCluster(consts.LocalClusterID, nil, "", nil, nil, reader))
	for _, read := range []func(*gin.Context){
		func(c *gin.Context) {
			store.GetTemplatesTemplateIDBuildsBuildIDLogs(c, old.TemplateID, old.BuildID, api.GetTemplatesTemplateIDBuildsBuildIDLogsParams{})
		},
		func(c *gin.Context) {
			store.GetTemplatesTemplateIDBuildsBuildIDStatus(c, old.TemplateID, old.BuildID, api.GetTemplatesTemplateIDBuildsBuildIDStatusParams{})
		},
	} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/templates/builds", nil)
		auth.SetTeamInfoForTest(t, c, team)
		read(c)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "original cluster")
	}
	require.Equal(t, 2, reader.calls)
}

type originalClusterLogs struct {
	clusters.ClusterResource

	calls int
}

func (r *originalClusterLogs) GetBuildLogs(context.Context, *string, string, string, int32, int32, *logs.LogLevel, *time.Time, api.LogsDirection, *api.LogsSource) ([]logs.LogEntry, *api.APIError) {
	r.calls++

	return []logs.LogEntry{{Message: "original cluster", Timestamp: time.Now()}}, nil
}

func TestTemplateClusterConcurrentAliasHandoff(t *testing.T) {
	t.Parallel()
	store, db := templateClusterStore(t)
	team := templateClusterTeam(t, db, nil)
	initial := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, initial.StatusCode(), string(initial.Body))
	require.NotNil(t, initial.JSON202)
	clusterID := uuid.New()
	assignTemplateClusterTeam(t, db, team, &clusterID)
	type result struct {
		templateID string
		err        *api.APIError
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			templateID := id.Generate()
			alias := "base"
			_, apiErr := templateregistration.RegisterBuild(t.Context(), store.templateCache, db.SqlcClient, templateregistration.RegisterBuildData{
				ReplacesTemplateID: initial.JSON202.TemplateID, TemplateID: templateID,
				ClusterID: clusterID, Team: team, Alias: &alias, Version: templates.TemplateV2LatestVersion,
			})
			results <- result{templateID: templateID, err: apiErr}
		}()
	}
	var winner string
	conflicts := 0
	for range 2 {
		got := <-results
		if got.err == nil {
			require.Empty(t, winner, "only one request can replace the old alias")
			winner = got.templateID
		} else {
			require.Equal(t, http.StatusConflict, got.err.Code, got.err)
			conflicts++
			_, err := db.SqlcClient.GetTemplateById(t.Context(), got.templateID)
			require.Error(t, err, "losing registration must roll back its template")
		}
	}
	require.Equal(t, 1, conflicts)
	alias, err := store.templateCache.ResolveAlias(t.Context(), "base", team.Slug)
	require.NoError(t, err)
	require.Equal(t, winner, alias.TemplateID)
	old, err := db.SqlcClient.GetTemplateById(t.Context(), initial.JSON202.TemplateID)
	require.NoError(t, err)
	require.Nil(t, old.ClusterID)
}

func TestTemplateClusterUsesCurrentAssignment(t *testing.T) {
	t.Parallel()
	store, db := templateClusterStore(t)
	team := templateClusterTeam(t, db, nil)
	initial := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, initial.StatusCode(), string(initial.Body))
	require.NotNil(t, initial.JSON202)
	clusterID := uuid.New()
	assignTemplateClusterTeam(t, db, team, &clusterID)
	team.ClusterID = nil
	rebuilt := requestClusterTemplate(t, store, team)
	require.Equal(t, http.StatusAccepted, rebuilt.StatusCode(), string(rebuilt.Body))
	require.NotNil(t, rebuilt.JSON202)
	current, err := db.SqlcClient.GetTemplateById(t.Context(), rebuilt.JSON202.TemplateID)
	require.NoError(t, err)
	require.Equal(t, &clusterID, current.ClusterID, "registration must use DB assignment despite cached auth")
	assignTemplateClusterTeam(t, db, team, nil)
	team.ClusterID = &clusterID
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/templates/builds", strings.NewReader(`{"fromImage":"alpine"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, c, team)
	store.PostV2TemplatesTemplateIDBuildsBuildID(c, rebuilt.JSON202.TemplateID, rebuilt.JSON202.BuildID)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "is not available in the requested cluster")
	alias := "base"
	uncommittedID := id.Generate()
	_, apiErr := templateregistration.RegisterBuild(t.Context(), store.templateCache, db.SqlcClient, templateregistration.RegisterBuildData{
		ReplacesTemplateID: initial.JSON202.TemplateID, TemplateID: uncommittedID,
		ClusterID: clusterID, Team: team, Alias: &alias, Version: templates.TemplateV2LatestVersion,
	})
	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusConflict, apiErr.Code)
	_, err = db.SqlcClient.GetTemplateById(t.Context(), uncommittedID)
	require.Error(t, err, "assignment change during registration must roll back")
}

func TestTemplateStartCluster(t *testing.T) {
	t.Parallel()
	store, db := templateClusterStore(t)
	first, second, local := uuid.New(), uuid.New(), consts.LocalClusterID
	for _, tt := range []struct {
		name              string
		original, current *uuid.UUID
		status            int
	}{
		{"default", nil, nil, http.StatusServiceUnavailable},
		{"explicit-default", nil, &local, http.StatusServiceUnavailable},
		{"same-cluster", &first, &first, http.StatusServiceUnavailable},
		{"default-to-custom", nil, &first, http.StatusBadRequest},
		{"custom-to-default", &first, nil, http.StatusBadRequest},
		{"different-cluster", &first, &second, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			team := templateClusterTeam(t, db, tt.original)
			registered := requestClusterTemplate(t, store, team)
			require.Equal(t, http.StatusAccepted, registered.StatusCode(), string(registered.Body))
			require.NotNil(t, registered.JSON202)
			build := registered.JSON202
			params := queries.GetTemplateBuildWithTemplateParams{TemplateID: build.TemplateID, BuildID: uuid.MustParse(build.BuildID)}
			before, err := db.SqlcClient.GetTemplateBuildWithTemplate(t.Context(), params)
			require.NoError(t, err)
			assignTemplateClusterTeam(t, db, team, tt.current)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost,
				fmt.Sprintf("/v2/templates/%s/builds/%s", build.TemplateID, build.BuildID), strings.NewReader(`{"fromImage":"alpine"}`))
			c.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, c, team)
			store.PostV2TemplatesTemplateIDBuildsBuildID(c, build.TemplateID, build.BuildID)
			require.Equal(t, tt.status, w.Code, w.Body.String())
			if tt.status == http.StatusBadRequest {
				require.Contains(t, w.Body.String(), "is not available in the requested cluster")
			} else {
				require.Contains(t, w.Body.String(), "Error when getting available build client", "matching clusters must reach builder selection")
			}
			after, err := db.SqlcClient.GetTemplateBuildWithTemplate(t.Context(), params)
			require.NoError(t, err)
			require.Equal(t, before, after, "rejected start must not alter the template or build")
		})
	}
}
