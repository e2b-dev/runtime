package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestClusterDestroyReadinessRefusesOnlySharedClustersWithoutMutation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	owner := createClusterAssignmentTestTeam(t, db)
	other := testutils.CreateTestTeam(t, db)
	foreignOwner := testutils.CreateTestTeam(t, db)
	sharedA, sharedB := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	historyOwner := testutils.CreateTestTeam(t, db)
	ownCluster, foreignEnvCluster, sharedCluster, orphanCluster, historyCluster := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, clusterID := range []uuid.UUID{ownCluster, foreignEnvCluster, sharedCluster, orphanCluster, historyCluster} {
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection) VALUES ($1::uuid, $1::uuid::text, $1::uuid::text, true, 'secret-token', false)`, clusterID))
	}
	// Created without a protection value, so it takes the column default.
	protectedCluster := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token) VALUES ($1::uuid, $1::uuid::text, $1::uuid::text, true, 'secret-token')`, protectedCluster))
	for _, assignment := range []struct {
		teamID, clusterID uuid.UUID
	}{
		{owner, ownCluster},
		{foreignOwner, foreignEnvCluster},
		{sharedA, sharedCluster},
		{sharedB, sharedCluster},
		{historyOwner, historyCluster},
	} {
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, assignment.teamID, assignment.clusterID))
	}
	for _, fixture := range []struct {
		teamID, clusterID uuid.UUID
		source            string
		deleted           bool
	}{
		{owner, ownCluster, "template", false},
		{owner, ownCluster, "snapshot", false},
		{other, foreignEnvCluster, "template", false},
		{other, orphanCluster, "snapshot_template", false},
		{other, historyCluster, "template", true},
	} {
		templateID := testutils.CreateTestTemplate(t, db, fixture.teamID)
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2, source = $3 WHERE id = $1`, templateID, fixture.clusterID, fixture.source))
		if fixture.deleted {
			_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: templateID, TeamID: fixture.teamID})
			require.NoError(t, err)
		}
	}
	readState := func(t *testing.T) string {
		t.Helper()
		var state string
		require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT jsonb_build_object(
			'clusters', (SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM public.clusters c),
			'teams', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM public.teams t),
			'envs', (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM public.envs e))::text`,
			func(rows pgx.Rows) error {
				require.True(t, rows.Next())

				return rows.Scan(&state)
			}))

		return state
	}
	before := readState(t)
	const sharedClusterMessage = "Cluster is used by more than one team. Move the other teams and their templates off it before destroying the deployment."
	for _, tc := range []struct {
		name      string
		clusterID uuid.UUID
		want      int
		message   string
	}{
		{"absent cluster", uuid.New(), http.StatusNoContent, ""},
		{"assigned team's own template and snapshot", ownCluster, http.StatusNoContent, ""},
		{"another team's active template", foreignEnvCluster, http.StatusConflict, sharedClusterMessage},
		{"two assigned teams", sharedCluster, http.StatusConflict, sharedClusterMessage},
		{"one unassigned team's snapshot template", orphanCluster, http.StatusNoContent, ""},
		{"another team's soft deleted history", historyCluster, http.StatusNoContent, ""},
		{"deletion protection", protectedCluster, http.StatusConflict, "Cluster has deletion protection turned on."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			router := gin.New()
			api.RegisterHandlers(router, &APIStore{db: db.SqlcClient})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
				"/v1/management/clusters/"+tc.clusterID.String()+"/destroy-readiness", nil))
			require.Equal(t, tc.want, response.Code, response.Body.String())
			if tc.want == http.StatusConflict {
				want, err := json.Marshal(map[string]any{"code": http.StatusConflict, "message": tc.message})
				require.NoError(t, err)
				require.JSONEq(t, string(want), response.Body.String())
			} else {
				require.Empty(t, response.Body.String())
			}
			require.JSONEq(t, before, readState(t))
		})
	}
}

func TestClusterDestroyReadinessRequiresAdminAuthentication(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	path := swagger.Paths.Value("/v1/management/clusters/{clusterID}/destroy-readiness")
	require.NotNil(t, path)
	require.NotNil(t, path.Get)
	require.NotNil(t, path.Get.Security)
	require.Equal(t, openapi3.SecurityRequirements{{"AdminJWTAuth": {}}}, *path.Get.Security)
	require.Nil(t, path.Get.Responses.Status(http.StatusNotFound))
}
