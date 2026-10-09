package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestPostAdminClustersCreatesImmutableCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	authOrgID := "org_test"
	sandboxDomain := "sandbox.example.test"
	request := api.AdminClusterCreateRequest{
		Name:               "AutoBYOC",
		Endpoint:           "api.example.test:5008",
		EndpointTls:        true,
		Token:              "cluster-token",
		SandboxProxyDomain: &sandboxDomain,
		AuthOrgId:          &authOrgID,
	}
	store := &APIStore{db: db.SqlcClient}

	response := callCreateCluster(t, store, request)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())

	var created api.AdminClusterCreateResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
	require.NotEqual(t, uuid.Nil, created.ClusterId)

	var count int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT count(*) FROM public.clusters WHERE id = $1 AND name = $2 AND endpoint = $3 AND token = $4`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&count)
		},
		created.ClusterId,
		request.Name,
		request.Endpoint,
		request.Token,
	))
	require.Equal(t, 1, count)

	conflict := callCreateCluster(t, store, request)
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	requireClusterErrorCode(t, conflict, api.ClusterRegistrationConflict)
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT count(*) FROM public.clusters WHERE id = $1 AND name = $2`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&count)
		},
		created.ClusterId,
		request.Name,
	))
	require.Equal(t, 1, count)
}

func TestPostAdminClustersReusesStableIDOnlyForIdenticalConfiguration(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	request := api.AdminClusterCreateRequest{
		ClusterId:   &clusterID,
		Name:        "Managed cluster",
		Endpoint:    "api.example.test:5008",
		EndpointTls: true,
		Token:       "cluster-token",
	}
	store := &APIStore{db: db.SqlcClient}

	created := callCreateCluster(t, store, request)
	replayed := callCreateCluster(t, store, request)
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	require.Equal(t, http.StatusCreated, replayed.Code, replayed.Body.String())

	request.Token = "different-token"
	conflict := callCreateCluster(t, store, request)
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	requireClusterErrorCode(t, conflict, api.ClusterRegistrationConflict)

	var storedToken string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT token FROM public.clusters WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&storedToken)
		},
		clusterID,
	))
	require.Equal(t, "cluster-token", storedToken)
}

func TestDeleteAdminClustersClusterIDDeletesIdempotently(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection) VALUES ($1, 'managed', 'api.example.test:5008', true, 'token', false)`,
		clusterID,
	))

	store := &APIStore{db: db.SqlcClient}
	response := callDeleteCluster(t, store, clusterID)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())

	replayed := callDeleteCluster(t, store, clusterID)
	require.Equal(t, http.StatusNoContent, replayed.Code, replayed.Body.String())

	var count int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT count(*) FROM public.clusters WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&count)
		},
		clusterID,
	))
	require.Zero(t, count)
}

func TestDeleteAdminClustersClusterIDWaitsForConcurrentTeamReference(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := createClusterAssignmentTestTeam(t, db)
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection) VALUES ($1, 'managed', 'api.example.test:5008', true, 'token', false)`,
		clusterID,
	))

	referenceClient, referenceTx, err := db.SqlcClient.WithTx(ctx)
	require.NoError(t, err)
	defer func() {
		_ = referenceTx.Rollback(t.Context())
	}()

	result, err := referenceClient.Dashboard.AssignTeamCluster(ctx, dashboardqueries.AssignTeamClusterParams{
		TeamID:    teamID,
		ClusterID: clusterID,
	})
	require.NoError(t, err)
	require.True(t, result.Assigned)

	store := &APIStore{db: db.SqlcClient}
	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responseCh <- callDeleteCluster(t, store, clusterID)
	}()

	require.Eventually(t, func() bool {
		var blocked bool
		err := db.SqlcClient.TestsRawSQLQuery(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%DELETE FROM public.clusters%'
			)`, func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&blocked)
		})

		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)

	select {
	case response := <-responseCh:
		require.FailNow(t, "cluster deletion returned before the team reference committed", response.Body.String())
	default:
	}

	require.NoError(t, referenceTx.Commit(ctx))

	select {
	case response := <-responseCh:
		require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	case <-time.After(5 * time.Second):
		require.FailNow(t, "cluster deletion did not finish after the team reference committed")
	}

	var assignedClusterID uuid.UUID
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT cluster_id FROM public.teams WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&assignedClusterID)
		},
		teamID,
	))
	require.Equal(t, clusterID, assignedClusterID)
}

func TestDeleteClusterSoftDeletesItsEnvironmentsWhenNoTeamIsAssigned(t *testing.T) {
	t.Parallel()

	for _, teamAssigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("team assigned %t", teamAssigned), func(t *testing.T) {
			t.Parallel()
			db := testutils.SetupDatabase(t)
			ctx := t.Context()
			teamID := createClusterAssignmentTestTeam(t, db)
			clusterID, otherClusterID := uuid.New(), uuid.New()
			for _, id := range []uuid.UUID{clusterID, otherClusterID} {
				require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
					`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection) VALUES ($1::uuid, $1::uuid::text, $1::uuid::text, true, 'token', false)`, id))
			}
			deletedID := testutils.CreateTestTemplate(t, db, teamID)
			activeID, _ := testutils.CreateTestTemplateWithAlias(t, db, teamID)
			snapshotID := testutils.CreateTestTemplate(t, db, teamID)
			otherID := testutils.CreateTestTemplate(t, db, teamID)
			localID := testutils.CreateTestTemplate(t, db, teamID)
			require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
				`UPDATE public.envs SET cluster_id = CASE id WHEN $4 THEN $6::uuid ELSE $5::uuid END,
					source = CASE id WHEN $3 THEN 'snapshot' ELSE source END
				WHERE id IN ($1, $2, $3, $4)`,
				deletedID, activeID, snapshotID, otherID, clusterID, otherClusterID))
			buildID := testutils.CreateTestBuild(t, ctx, db, activeID, "building")
			require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
				`INSERT INTO public.active_template_builds (build_id, team_id, template_id, tags) VALUES ($1, $2, $3, '{default}')`,
				buildID, teamID, activeID))
			_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: deletedID, TeamID: teamID})
			require.NoError(t, err)
			if teamAssigned {
				_, err := db.SqlcClient.Dashboard.AssignTeamCluster(ctx, dashboardqueries.AssignTeamClusterParams{TeamID: teamID, ClusterID: clusterID})
				require.NoError(t, err)
			}

			store := &APIStore{db: db.SqlcClient}
			response := callManagementDeleteCluster(t, store, clusterID)

			type envState struct {
				Cluster *uuid.UUID `json:"cluster"`
				Deleted bool       `json:"deleted"`
			}
			wantEnvs := map[string]envState{
				deletedID:  {Deleted: true},
				activeID:   {Deleted: true},
				snapshotID: {Deleted: true},
				otherID:    {Cluster: &otherClusterID},
				localID:    {},
			}
			wantReferences := 0
			if teamAssigned {
				require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
				wantEnvs = map[string]envState{
					deletedID:  {Cluster: &clusterID, Deleted: true},
					activeID:   {Cluster: &clusterID},
					snapshotID: {Cluster: &clusterID},
					otherID:    {Cluster: &otherClusterID},
					localID:    {},
				}
				wantReferences = 1
			} else {
				require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
				require.Equal(t, http.StatusNoContent, callManagementDeleteCluster(t, store, clusterID).Code)
			}
			want := map[string]any{
				"cluster_exists": teamAssigned,
				"envs":           wantEnvs,
				"aliases":        wantReferences,
				"active_builds":  wantReferences,
			}
			wantJSON, err := json.Marshal(want)
			require.NoError(t, err)
			var state string
			require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT jsonb_build_object(
				'cluster_exists', EXISTS (SELECT FROM public.clusters WHERE id = $1),
				'envs', (SELECT jsonb_object_agg(id, jsonb_build_object('cluster', cluster_id, 'deleted', deleted_at IS NOT NULL))
					FROM public.envs WHERE id = ANY($2::text[])),
				'aliases', (SELECT count(*) FROM public.env_aliases WHERE env_id = $3),
				'active_builds', (SELECT count(*) FROM public.active_template_builds WHERE template_id = $3))::text`,
				func(rows pgx.Rows) error {
					require.True(t, rows.Next())

					return rows.Scan(&state)
				}, clusterID, []string{deletedID, activeID, snapshotID, otherID, localID}, activeID))
			require.JSONEq(t, string(wantJSON), state)
			require.True(t, testutils.GetEnvBuildByID(t, ctx, db, buildID), "build history must survive")
		})
	}
}

func TestDeleteClusterReleasesRowsARacingRegistrationCommits(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection) VALUES ($1::uuid, $1::uuid::text, $1::uuid::text, true, 'token', false)`, clusterID))
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "building")

	// A build registration holds the env row lock while it writes the alias and
	// the active-build row, then commits.
	_, registrationTx, err := db.SqlcClient.WithTx(ctx)
	require.NoError(t, err)
	defer func() { _ = registrationTx.Rollback(context.WithoutCancel(ctx)) }()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE public.envs SET updated_at = NOW() WHERE id = $1`, []any{templateID}},
		{`INSERT INTO public.env_aliases (alias, env_id, is_renamable) VALUES ($1, $2, true)`, []any{"racing-" + templateID, templateID}},
		{`INSERT INTO public.active_template_builds (build_id, team_id, template_id, tags) VALUES ($1, $2, $3, '{default}')`, []any{buildID, teamID, templateID}},
	} {
		_, err := registrationTx.Exec(ctx, statement.sql, statement.args...)
		require.NoError(t, err)
	}

	store := &APIStore{db: db.SqlcClient}
	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responseCh <- callManagementDeleteCluster(t, store, clusterID)
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := db.SqlcClient.TestsRawSQLQuery(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%SoftDeleteClusterEnvironments%'
			)`, func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&blocked)
		})

		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, registrationTx.Commit(ctx))

	select {
	case response := <-responseCh:
		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	case <-time.After(5 * time.Second):
		require.FailNow(t, "cluster deletion did not finish after the registration committed")
	}
	var state string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT jsonb_build_object(
		'deleted', (SELECT deleted_at IS NOT NULL FROM public.envs WHERE id = $1),
		'aliases', (SELECT count(*) FROM public.env_aliases WHERE env_id = $1),
		'active_builds', (SELECT count(*) FROM public.active_template_builds WHERE template_id = $1))::text`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&state)
		}, templateID))
	require.JSONEq(t, `{"deleted": true, "aliases": 0, "active_builds": 0}`, state)
}

func TestDeleteClusterRefusesTheLocalCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := createClusterAssignmentTestTeam(t, db)
	// The local cluster's environments store a NULL cluster_id; one stored
	// under the nil ID would match by equality, so both must survive.
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection) VALUES ($1, 'local', 'local', true, 'token', false)`, uuid.Nil))
	localID := testutils.CreateTestTemplate(t, db, teamID)
	nilClusterID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, nilClusterID, uuid.Nil))

	store := &APIStore{db: db.SqlcClient}
	for _, response := range []*httptest.ResponseRecorder{
		callDeleteCluster(t, store, uuid.Nil),
		callManagementDeleteCluster(t, store, uuid.Nil),
	} {
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	}

	var anyDeleted, clusterExists bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT
		EXISTS (SELECT FROM public.envs WHERE id IN ($1, $2) AND deleted_at IS NOT NULL),
		EXISTS (SELECT FROM public.clusters WHERE id = $3)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&anyDeleted, &clusterExists)
		}, localID, nilClusterID, uuid.Nil))
	require.False(t, anyDeleted)
	require.True(t, clusterExists)
}

func TestGetAdminTeamsTeamIDClusterReturnsOnlyAssignment(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := createClusterAssignmentTestTeam(t, db)
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token) VALUES ($1, 'managed', 'api.example.test:5008', true, 'secret-token')`,
		clusterID,
	))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET cluster_id = $1 WHERE id = $2`,
		clusterID,
		teamID,
	))

	store := &APIStore{db: db.SqlcClient}
	response := callGetClusterAssignment(t, store, teamID)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var assignment api.AdminTeamClusterAssignmentResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &assignment))
	require.Equal(t, clusterID, assignment.ClusterId)
	require.NotContains(t, response.Body.String(), "secret-token")

	missing := callGetClusterAssignment(t, store, uuid.New())
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
}

func TestDeleteClusterRefusesAProtectedCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	store := &APIStore{db: db.SqlcClient}
	clusterID := uuid.New()
	registration := managementClusterRegistration()
	created := callCreateCluster(t, store, api.AdminClusterCreateRequest{
		ClusterId:          &clusterID,
		Name:               registration.Name,
		Endpoint:           registration.Endpoint,
		EndpointTls:        registration.EndpointTls,
		Token:              registration.Token,
		SandboxProxyDomain: registration.SandboxProxyDomain,
		AuthOrgId:          registration.AuthOrgId,
	})
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	teamID := createClusterAssignmentTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	// Registering the same cluster through the management API, which creates
	// new clusters unprotected, must keep the stored protection.
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)

	for _, response := range []*httptest.ResponseRecorder{
		callDeleteCluster(t, store, clusterID),
		callManagementDeleteCluster(t, store, clusterID),
	} {
		require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
		require.JSONEq(t, `{"code":409,"message":"Cluster has deletion protection turned on"}`, response.Body.String())
	}

	var clusterExists, templateIntact bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT
		EXISTS (SELECT FROM public.clusters WHERE id = $1 AND deletion_protection),
		EXISTS (SELECT FROM public.envs WHERE id = $2 AND cluster_id = $1 AND deleted_at IS NULL)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&clusterExists, &templateIntact)
		}, clusterID, templateID))
	require.True(t, clusterExists)
	require.True(t, templateIntact)
}

func callCreateCluster(t *testing.T, store *APIStore, request api.AdminClusterCreateRequest) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(request)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/clusters", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	store.PostAdminClusters(ginCtx)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func callDeleteCluster(t *testing.T, store *APIStore, clusterID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(
		t.Context(),
		http.MethodDelete,
		"/admin/clusters/"+clusterID.String(),
		nil,
	)
	store.DeleteAdminClustersClusterID(ginCtx, clusterID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func callGetClusterAssignment(
	t *testing.T,
	store *APIStore,
	teamID uuid.UUID,
) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		"/admin/teams/"+teamID.String()+"/cluster",
		nil,
	)
	store.GetAdminTeamsTeamIDCluster(ginCtx, teamID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

type configurableCacheAuthService struct {
	noopAuthService

	invalidated   []uuid.UUID
	invalidateErr error
}

func (s *configurableCacheAuthService) InvalidateTeamCache(_ context.Context, teamID uuid.UUID) error {
	s.invalidated = append(s.invalidated, teamID)

	return s.invalidateErr
}

func createClusterAssignmentTestTeam(t *testing.T, db *testutils.Database) uuid.UUID {
	t.Helper()

	teamID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `
		INSERT INTO public.tiers (
			id,
			name,
			disk_mb,
			concurrent_instances,
			max_length_hours,
			max_vcpu,
			max_ram_mb,
			concurrent_template_builds,
			events_ttl_days,
			default_free_disk_size_mb,
			max_disk_size_mb
		)
		VALUES
			('Enterprise_cluster_assignment_test', 'Enterprise cluster assignment test', 512, 20, 1, 8, 8096, 20, 7, 512, 25512),
			('cluster_assignment_test', 'Cluster assignment test', 512, 20, 1, 8, 8096, 20, 7, 512, 25512)
	`))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.teams (id, name, tier, email, slug) VALUES ($1, $2, 'Enterprise_cluster_assignment_test', $3, $4)`,
		teamID,
		"Cluster assignment test team",
		"cluster-"+teamID.String()+"@example.com",
		"cluster-"+teamID.String()[:8],
	))

	return teamID
}
