package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

func TestManagementClusterLifecycleReplaysAndEndsAbsent(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	store, _ := newManagementStore(t, db)
	registration := managementClusterRegistration()

	for range 2 {
		require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	}
	for range 2 {
		require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	}
	for range 2 {
		require.Equal(t, http.StatusNoContent, callManagementDetachProjectCluster(t, store, projectID, clusterID).Code)
	}
	for range 2 {
		require.Equal(t, http.StatusNoContent, callManagementDeleteCluster(t, store, clusterID).Code)
	}

	require.False(t, clusterExists(t, db, clusterID))
}

func TestManagementRegisterClusterRejectsIdentityChange(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	store, _ := newManagementStore(t, db)
	registration := managementClusterRegistration()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)

	for name, tc := range map[string]struct {
		change func(*api.ManagementClusterRegistrationRequest)
	}{
		"auth organization":         {func(r *api.ManagementClusterRegistrationRequest) { r.AuthOrgId = new("changed-org") }},
		"missing auth organization": {func(r *api.ManagementClusterRegistrationRequest) { r.AuthOrgId = nil }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			changed := registration
			changed.Token = "replacement-test-token"
			tc.change(&changed)
			response := callManagementRegisterCluster(t, store, clusterID, changed)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			requireClusterErrorCode(t, response, clusterRegistrationConflictCode)
		})
	}
}

// A cluster that moves to a new domain registers its new endpoint and sandbox
// domain under the same identity; its assignments and protection stay.
func TestManagementRegisterClusterMovesConnectionDetails(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	store, _ := newManagementStore(t, db)
	registration := managementClusterRegistration()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.clusters SET deletion_protection = true WHERE id = $1`, clusterID))

	registration.Endpoint = "api.moved.example.test:443"
	registration.SandboxProxyDomain = new("moved.example.test")
	registration.EndpointTls = !registration.EndpointTls
	for range 2 {
		require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	}

	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT c.endpoint, c.endpoint_tls, c.sandbox_proxy_domain, c.token, c.deletion_protection, t.cluster_id
		FROM public.clusters c JOIN public.teams t ON t.cluster_id = c.id WHERE c.id = $1 AND t.id = $2`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())
			var endpoint, proxy, token string
			var tls, protected bool
			var assigned uuid.UUID
			require.NoError(t, rows.Scan(&endpoint, &tls, &proxy, &token, &protected, &assigned))
			require.Equal(t, registration.Endpoint, endpoint)
			require.Equal(t, registration.EndpointTls, tls)
			require.Equal(t, *registration.SandboxProxyDomain, proxy)
			require.Equal(t, registration.Token, token)
			require.True(t, protected)
			require.Equal(t, clusterID, assigned)

			return nil
		}, clusterID, projectID))
}

func TestManagementRegisterClusterRefreshesOnlyToken(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	store, _ := newManagementStore(t, db)
	registration := managementClusterRegistration()
	registration.AuthOrgId = nil
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.clusters SET deletion_protection = true WHERE id = $1`, clusterID))

	registration.Token = "replacement-test-token"
	for range 2 {
		require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	}

	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT c.token, c.deletion_protection, t.cluster_id
		FROM public.clusters c JOIN public.teams t ON t.cluster_id = c.id WHERE c.id = $1 AND t.id = $2`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())
			var token string
			var protected bool
			var assigned uuid.UUID
			require.NoError(t, rows.Scan(&token, &protected, &assigned))
			require.Equal(t, registration.Token, token)
			require.True(t, protected)
			require.Equal(t, clusterID, assigned)

			return nil
		}, clusterID, projectID))

	// Creation without a token refresh still rejects a credential change.
	_, err := store.createCluster(t.Context(), clusterRegistration{
		ClusterID: &clusterID, Name: registration.Name, Endpoint: registration.Endpoint,
		EndpointTLS: registration.EndpointTls, Token: "another-test-token",
		SandboxProxyDomain: registration.SandboxProxyDomain, AuthOrgID: registration.AuthOrgId,
	})
	require.Error(t, err)
}

func TestManagementRegisterClusterStartsUnprotected(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	store, _ := newManagementStore(t, db)

	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)

	protected, err := db.SqlcClient.ClusterDeletionProtected(t.Context(), clusterID)
	require.NoError(t, err)
	require.False(t, protected)
}

func TestManagementRegisterClusterRejectsAnInvalidRegistration(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)

	for name, tc := range map[string]struct {
		clusterID uuid.UUID
		body      string
	}{
		"the local cluster's nil ID": {uuid.Nil, mustJSON(t, managementClusterRegistration())},
		"a blank name":               {uuid.New(), `{"name":"  ","endpoint":"api.example.test:443","endpoint_tls":true,"token":"token"}`},
		"a malformed body":           {uuid.New(), `{"name":`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := callManagement(t, store, http.MethodPut, "/unstable/management/clusters/"+tc.clusterID.String(), tc.body)
			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			requireClusterErrorCode(t, response, clusterRegistrationInvalidCode)
			require.False(t, clusterExists(t, db, tc.clusterID))
		})
	}
}

func TestManagementAssignProjectClusterRejectsTheLocalCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	store, auth := newManagementStore(t, db)

	response := callManagementAssignProjectCluster(t, store, projectID, uuid.Nil)

	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	requireClusterErrorCode(t, response, clusterAssignmentInvalidCode)
	require.Empty(t, *auth.invalidatedTeamIDs)
}

func TestManagementAssignProjectClusterRejectsDifferentAssignment(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	firstID, secondID := uuid.New(), uuid.New()
	store, auth := newManagementStore(t, db)
	first := managementClusterRegistration()
	second := managementClusterRegistration()
	second.AuthOrgId = new("org_other")
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, firstID, first).Code)
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, secondID, second).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, firstID).Code)

	response := callManagementAssignProjectCluster(t, store, projectID, secondID)

	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	requireClusterErrorCode(t, response, clusterAssignmentAlreadyAssignedCode)
	require.Equal(t, []uuid.UUID{projectID}, *auth.invalidatedTeamIDs)
	require.Equal(t, firstID, assignedProjectCluster(t, db, projectID))
}

func TestManagementAssignProjectClusterAssignsAndEvictsTheProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	store, auth := newManagementStore(t, db)
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)

	response := callManagementAssignProjectCluster(t, store, projectID, clusterID)

	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.Equal(t, []uuid.UUID{projectID}, *auth.invalidatedTeamIDs)
	require.Equal(t, clusterID, assignedProjectCluster(t, db, projectID))
}

func TestManagementAssignmentMissingResourcesHaveStableCodes(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	for name, tc := range map[string]struct {
		projectID uuid.UUID
		code      string
	}{
		"project": {uuid.New(), clusterAssignmentProjectNotFoundCode},
		"cluster": {projectID, clusterAssignmentClusterNotFoundCode},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := callManagementAssignProjectCluster(t, store, tc.projectID, uuid.New())
			require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
			requireClusterErrorCode(t, response, tc.code)
		})
	}
}

func TestManagementDetachProjectClusterRefusesADifferentCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	store, auth := newManagementStore(t, db)
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)

	response := callManagementDetachProjectCluster(t, store, projectID, uuid.New())

	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Equal(t, clusterID, assignedProjectCluster(t, db, projectID))
	require.Equal(t, []uuid.UUID{projectID}, *auth.invalidatedTeamIDs, "only the assignment evicted the team")
}

func TestManagementDetachProjectClusterAnswersNotFoundForAMissingProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, auth := newManagementStore(t, db)

	response := callManagementDetachProjectCluster(t, store, uuid.New(), uuid.New())

	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
	require.Empty(t, *auth.invalidatedTeamIDs)
}

// The assignment is stored before the eviction, so a failed eviction answers
// 500 and the caller's retry, which replays the identical write, evicts again.
func TestManagementClusterAssignmentReportsAFailedCacheEviction(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	setup, _ := newManagementStore(t, db)
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, setup, clusterID, managementClusterRegistration()).Code)

	for name, tc := range map[string]struct {
		call         func(*testing.T, *APIStore, uuid.UUID, uuid.UUID) *httptest.ResponseRecorder
		wantAssigned bool
	}{
		"assignment": {callManagementAssignProjectCluster, true},
		"detachment": {callManagementDetachProjectCluster, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			projectID := testutils.CreateTestTeam(t, db)
			require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $1 WHERE id = $2 AND $3`,
				clusterID, projectID, !tc.wantAssigned))
			store, auth := newManagementStore(t, db)
			auth.invalidateTeamErr = errors.New("cache unavailable")

			response := tc.call(t, store, projectID, clusterID)

			require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
			require.Equal(t, []uuid.UUID{projectID}, *auth.invalidatedTeamIDs)
			var assigned *uuid.UUID
			require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT cluster_id FROM public.teams WHERE id = $1`,
				func(rows pgx.Rows) error {
					require.True(t, rows.Next())

					return rows.Scan(&assigned)
				}, projectID))
			require.Equal(t, tc.wantAssigned, assigned != nil, "the write is kept")
		})
	}
}

func TestManagementDeleteClusterSoftDeletesItsEnvironmentsWhenNoTeamIsAssigned(t *testing.T) {
	t.Parallel()

	for _, teamAssigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("team assigned %t", teamAssigned), func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			ctx := t.Context()
			teamID := testutils.CreateTestTeam(t, db)
			clusterID, otherClusterID := uuid.New(), uuid.New()
			for _, id := range []uuid.UUID{clusterID, otherClusterID} {
				insertCluster(t, db, id, false)
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
			buildID := testutils.CreateTestBuild(t, ctx, db, activeID, dbtypes.BuildStatusGroupInProgress)
			require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
				`INSERT INTO public.active_template_builds (build_id, team_id, template_id, tags) VALUES ($1, $2, $3, '{default}')`,
				buildID, teamID, activeID))
			_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: deletedID, TeamID: teamID})
			require.NoError(t, err)
			if teamAssigned {
				_, err := db.SqlcClient.AssignTeamCluster(ctx, queries.AssignTeamClusterParams{TeamID: teamID, ClusterID: clusterID})
				require.NoError(t, err)
			}
			store, _ := newManagementStore(t, db)

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
			wantJSON, err := json.Marshal(map[string]any{
				"cluster_exists": teamAssigned,
				"envs":           wantEnvs,
				"aliases":        wantReferences,
				"active_builds":  wantReferences,
			})
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

func TestManagementDeleteClusterWaitsForAConcurrentTeamReference(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)

	referenceClient, referenceTx, err := db.SqlcClient.WithTx(ctx)
	require.NoError(t, err)
	defer func() { _ = referenceTx.Rollback(context.WithoutCancel(ctx)) }()
	result, err := referenceClient.AssignTeamCluster(ctx, queries.AssignTeamClusterParams{TeamID: teamID, ClusterID: clusterID})
	require.NoError(t, err)
	require.True(t, result.Assigned)

	store, _ := newManagementStore(t, db)
	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { responseCh <- callManagementDeleteCluster(t, store, clusterID) }()
	waitForBlockedQuery(t, db, "%DELETE FROM public.clusters%")

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
	require.Equal(t, clusterID, assignedProjectCluster(t, db, teamID))
}

func TestManagementDeleteClusterReleasesRowsARacingRegistrationCommits(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, dbtypes.BuildStatusGroupInProgress)

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

	store, _ := newManagementStore(t, db)
	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() { responseCh <- callManagementDeleteCluster(t, store, clusterID) }()
	waitForBlockedQuery(t, db, "%SoftDeleteClusterEnvironments%")
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

func TestManagementDeleteClusterRefusesTheLocalCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, db)
	// The local cluster's environments store a NULL cluster_id; one stored
	// under the nil ID would match by equality, so both must survive.
	insertCluster(t, db, uuid.Nil, false)
	localID := testutils.CreateTestTemplate(t, db, teamID)
	nilClusterID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, nilClusterID, uuid.Nil))
	store, _ := newManagementStore(t, db)

	response := callManagementDeleteCluster(t, store, uuid.Nil)

	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	var anyDeleted bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT EXISTS (SELECT FROM public.envs WHERE id IN ($1, $2) AND deleted_at IS NOT NULL)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&anyDeleted)
		}, localID, nilClusterID))
	require.False(t, anyDeleted)
	require.True(t, clusterExists(t, db, uuid.Nil))
}

func TestManagementDeleteClusterRefusesAProtectedCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	clusterID := uuid.New()
	registration := managementClusterRegistration()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, sandbox_proxy_domain, auth_org_id, deletion_protection)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true)`,
		clusterID, registration.Name, registration.Endpoint, registration.EndpointTls, registration.Token,
		*registration.SandboxProxyDomain, *registration.AuthOrgId))
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	store, _ := newManagementStore(t, db)
	// Registering the same cluster through the management API, which creates
	// new clusters unprotected, must keep the stored protection.
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)

	response := callManagementDeleteCluster(t, store, clusterID)

	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.JSONEq(t, `{"code":409,"message":"Cluster has deletion protection turned on"}`, response.Body.String())
	var protectedAndIntact bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT
		EXISTS (SELECT FROM public.clusters WHERE id = $1 AND deletion_protection)
		AND EXISTS (SELECT FROM public.envs WHERE id = $2 AND cluster_id = $1 AND deleted_at IS NULL)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&protectedAndIntact)
		}, clusterID, templateID))
	require.True(t, protectedAndIntact)
}

func TestManagementClusterDeletionRequestKeepsProtectionOff(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(ctx)) })
	unprotected, protected := uuid.New(), uuid.New()
	insertCluster(t, db, unprotected, false)
	insertCluster(t, db, protected, true)

	require.Equal(t, http.StatusNoContent, callManagementRequestClusterDeletion(t, store, unprotected))
	require.Equal(t, http.StatusNoContent, callManagementRequestClusterDeletion(t, store, unprotected), "a repeat succeeds")
	var requested bool
	require.NoError(t, conn.QueryRow(ctx, `SELECT deletion_requested_at IS NOT NULL FROM public.clusters WHERE id = $1`, unprotected).Scan(&requested))
	require.True(t, requested)
	_, err = conn.Exec(ctx, `UPDATE public.clusters SET deletion_protection = true WHERE id = $1`, unprotected)
	require.ErrorContains(t, err, "clusters_deletion_requested_unprotected", "a requested deletion keeps protection off")

	require.Equal(t, http.StatusConflict, callManagementRequestClusterDeletion(t, store, protected))
	require.NoError(t, conn.QueryRow(ctx, `SELECT deletion_requested_at IS NOT NULL FROM public.clusters WHERE id = $1`, protected).Scan(&requested))
	require.False(t, requested)

	require.Equal(t, http.StatusNoContent, callManagementRequestClusterDeletion(t, store, uuid.New()), "an absent cluster has nothing to protect")
	require.Equal(t, http.StatusBadRequest, callManagementRequestClusterDeletion(t, store, consts.LocalClusterID))
}

func TestManagementClusterDeletionRequestWaitsForAProtectionChangeInFlight(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	ctx := t.Context()
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)

	protecting, err := pgx.Connect(ctx, db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = protecting.Close(context.WithoutCancel(ctx)) })
	tx, err := protecting.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `UPDATE public.clusters SET deletion_protection = true WHERE id = $1`, clusterID)
	require.NoError(t, err)
	var protectingPID int
	require.NoError(t, tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&protectingPID))

	status := make(chan int, 1)
	go func() { status <- callManagementRequestClusterDeletion(t, store, clusterID) }()
	waitForBlockedBy(t, db, protectingPID)
	require.NoError(t, tx.Commit(ctx))

	require.Equal(t, http.StatusConflict, <-status)
	var requested bool
	require.NoError(t, protecting.QueryRow(ctx, `SELECT deletion_requested_at IS NOT NULL FROM public.clusters WHERE id = $1`, clusterID).Scan(&requested))
	require.False(t, requested)
}

func TestManagementListClusterProjectsListsLiveAssignments(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := uuid.New()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)
	assigned, detached := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	testutils.CreateTestTeam(t, db)
	for _, projectID := range []uuid.UUID{assigned, detached} {
		require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, clusterID).Code)
	}
	require.Equal(t, http.StatusNoContent, callManagementDetachProjectCluster(t, store, detached, clusterID).Code)

	require.Equal(t, []uuid.UUID{assigned}, listClusterProjects(t, store, clusterID))
	absent := listClusterProjects(t, store, uuid.New())
	require.NotNil(t, absent, "an absent cluster lists no projects, not null")
	require.Empty(t, absent)
}

func TestNoTeamIsNewlyAssignedToAClusterBeingDeleted(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	ctx := t.Context()
	clusterID := uuid.New()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)
	assigned, late, raw := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, assigned, clusterID).Code)
	require.Equal(t, http.StatusNoContent, callManagementRequestClusterDeletion(t, store, clusterID))

	refused := callManagementAssignProjectCluster(t, store, late, clusterID)
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	requireClusterErrorCode(t, refused, clusterAssignmentClusterDeletingCode)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, assigned, clusterID).Code, "an existing assignment is not a new one")

	err := db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.teams SET cluster_id = $1 WHERE id = $2`, clusterID, raw)
	require.Equal(t, clusterDeletingConstraint, dberrors.ConstraintName(err), "every writer of a team's cluster is refused")
	require.Equal(t, []uuid.UUID{assigned}, listClusterProjects(t, store, clusterID))
}

func TestAnAssignmentWaitsForADeletionRequestInFlight(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	ctx := t.Context()
	clusterID := uuid.New()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)
	team := testutils.CreateTestTeam(t, db)

	deleting, err := pgx.Connect(ctx, db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = deleting.Close(context.WithoutCancel(ctx)) })
	tx, err := deleting.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `SELECT deletion_protection FROM public.clusters WHERE id = $1 FOR UPDATE`, clusterID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `UPDATE public.clusters SET deletion_requested_at = now() WHERE id = $1`, clusterID)
	require.NoError(t, err)
	var deletingPID int
	require.NoError(t, tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&deletingPID))

	status := make(chan int, 1)
	go func() { status <- callManagementAssignProjectCluster(t, store, team, clusterID).Code }()
	waitForBlockedBy(t, db, deletingPID)
	require.NoError(t, tx.Commit(ctx))

	require.Equal(t, http.StatusConflict, <-status)
	var assigned *uuid.UUID
	require.NoError(t, deleting.QueryRow(ctx, `SELECT cluster_id FROM public.teams WHERE id = $1`, team).Scan(&assigned))
	require.Nil(t, assigned)
}

func TestATeamDeletedBeforeDeletionClearedItsClusterNeitherListsNorBlocks(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	ctx := t.Context()
	clusterID := uuid.New()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, managementClusterRegistration()).Code)
	deleted := testutils.CreateTestTeam(t, db)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, deleted, clusterID).Code)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.teams SET deleted_at = now(), is_blocked = true, blocked_reason = 'Project deletion' WHERE id = $1`, deleted))

	require.Empty(t, listClusterProjects(t, store, clusterID))
	require.Equal(t, http.StatusNoContent, callManagementDeleteCluster(t, store, clusterID).Code)
	var cluster *uuid.UUID
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT cluster_id FROM public.teams WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&cluster)
		}, deleted))
	require.Nil(t, cluster)
}

func TestClusterDestroyReadinessRefusesOnlySharedClustersWithoutMutation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	owner, other, foreignOwner := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	sharedA, sharedB := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	historyOwner := testutils.CreateTestTeam(t, db)
	ownCluster, foreignEnvCluster, sharedCluster, orphanCluster, historyCluster := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, clusterID := range []uuid.UUID{ownCluster, foreignEnvCluster, sharedCluster, orphanCluster, historyCluster} {
		insertCluster(t, db, clusterID, false)
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
	store, _ := newManagementStore(t, db)
	const sharedClusterMessage = "Cluster is used by more than one team. Move the other teams and their templates off it before destroying the deployment."

	for name, tc := range map[string]struct {
		clusterID uuid.UUID
		want      int
		message   string
	}{
		"absent cluster": {uuid.New(), http.StatusNoContent, ""},
		"assigned team's own template and snapshot": {ownCluster, http.StatusNoContent, ""},
		"another team's active template":            {foreignEnvCluster, http.StatusConflict, sharedClusterMessage},
		"two assigned teams":                        {sharedCluster, http.StatusConflict, sharedClusterMessage},
		"one unassigned team's snapshot template":   {orphanCluster, http.StatusNoContent, ""},
		"another team's soft deleted history":       {historyCluster, http.StatusNoContent, ""},
		"deletion protection":                       {protectedCluster, http.StatusConflict, "Cluster has deletion protection turned on."},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			response := callManagement(t, store, http.MethodGet, "/unstable/management/clusters/"+tc.clusterID.String()+"/destroy-readiness", "")

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

func managementClusterRegistration() api.ManagementClusterRegistrationRequest {
	return api.ManagementClusterRegistrationRequest{
		Name:               "Managed cluster",
		Endpoint:           "api.example.test:443",
		EndpointTls:        true,
		Token:              "cluster-token",
		SandboxProxyDomain: new("sandbox.example.test"),
		AuthOrgId:          new("org_example"),
	}
}

func callManagementRegisterCluster(t *testing.T, store *APIStore, clusterID uuid.UUID, registration api.ManagementClusterRegistrationRequest) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/clusters/"+clusterID.String(), mustJSON(t, registration))
}

func callManagementDeleteCluster(t *testing.T, store *APIStore, clusterID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodDelete, "/unstable/management/clusters/"+clusterID.String(), "")
}

func callManagementRequestClusterDeletion(t *testing.T, store *APIStore, clusterID uuid.UUID) int {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/clusters/"+clusterID.String()+"/deletion", "").Code
}

func callManagementAssignProjectCluster(t *testing.T, store *APIStore, projectID, clusterID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/projects/"+projectID.String()+"/cluster/"+clusterID.String(), "")
}

func callManagementDetachProjectCluster(t *testing.T, store *APIStore, projectID, clusterID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodDelete, "/unstable/management/projects/"+projectID.String()+"/cluster/"+clusterID.String(), "")
}

func listClusterProjects(t *testing.T, store *APIStore, clusterID uuid.UUID) []uuid.UUID {
	t.Helper()

	response := callManagement(t, store, http.MethodGet, "/unstable/management/clusters/"+clusterID.String()+"/projects", "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var projects api.ManagementClusterProjects
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &projects))

	return projects.ProjectIds
}

func requireClusterErrorCode(t *testing.T, response *httptest.ResponseRecorder, code string) {
	t.Helper()

	var body api.Error
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.NotNil(t, body.ErrorCode)
	require.Equal(t, code, *body.ErrorCode)
	require.EqualValues(t, response.Code, body.Code)
}

func insertCluster(t *testing.T, db *testutils.Database, clusterID uuid.UUID, protected bool) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, deletion_protection)
		VALUES ($1::uuid, $1::uuid::text, $1::uuid::text, true, 'token', $2)`, clusterID, protected))
}

func clusterExists(t *testing.T, db *testutils.Database, clusterID uuid.UUID) bool {
	t.Helper()

	var exists bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT EXISTS (SELECT FROM public.clusters WHERE id = $1)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&exists)
		}, clusterID))

	return exists
}

func assignedProjectCluster(t *testing.T, db *testutils.Database, projectID uuid.UUID) uuid.UUID {
	t.Helper()

	var clusterID uuid.UUID
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT cluster_id FROM public.teams WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&clusterID)
		}, projectID))

	return clusterID
}

// waitForBlockedQuery waits until a statement matching the LIKE pattern waits
// on a lock.
func waitForBlockedQuery(t *testing.T, db *testutils.Database, pattern string) {
	t.Helper()

	require.Eventually(t, func() bool {
		var blocked bool
		err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND state = 'active' AND wait_event_type = 'Lock' AND query LIKE $1)`,
			func(rows pgx.Rows) error {
				if !rows.Next() {
					return rows.Err()
				}

				return rows.Scan(&blocked)
			}, pattern)

		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
}

// waitForBlockedBy waits until some backend waits on a lock the given backend
// holds.
func waitForBlockedBy(t *testing.T, db *testutils.Database, pid int) {
	t.Helper()

	require.Eventually(t, func() bool {
		var blocked bool
		err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
			`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1::int = ANY (pg_blocking_pids(pid)))`,
			func(rows pgx.Rows) error {
				if !rows.Next() {
					return rows.Err()
				}

				return rows.Scan(&blocked)
			}, pid)

		return err == nil && blocked
	}, 10*time.Second, 10*time.Millisecond)
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()

	body, err := json.Marshal(value)
	require.NoError(t, err)

	return string(body)
}

func TestRegistrationProjectsUniqueTelemetryClientIdentity(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	store := newAdminClusterStore(db)
	clusterID := uuid.New()
	registration := managementClusterRegistration()
	registration.TelemetryClientId = new("client_first")
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	rows, err := db.SqlcClient.ListTelemetryNames(t.Context())
	require.NoError(t, err)
	require.Contains(t, rows, queries.ListTelemetryNamesRow{Kind: "cluster", ID: clusterID.String(), Name: registration.Name, TelemetryClientID: "client_first"})
	require.Equal(t, http.StatusConflict, callManagementRegisterCluster(t, store, uuid.New(), registration).Code)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.clusters SET name='Renamed cluster' WHERE id=$1`, clusterID))
	registration.TelemetryClientId = new("client_replacement")
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	rows, err = db.SqlcClient.ListTelemetryNames(t.Context())
	require.NoError(t, err)
	require.Contains(t, rows, queries.ListTelemetryNamesRow{Kind: "cluster", ID: clusterID.String(), Name: "Renamed cluster", TelemetryClientID: "client_replacement"})
	registration.TelemetryClientId = nil
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	rows, err = db.SqlcClient.ListTelemetryNames(t.Context())
	require.NoError(t, err)
	require.Contains(t, rows, queries.ListTelemetryNamesRow{Kind: "cluster", ID: clusterID.String(), Name: "Renamed cluster", TelemetryClientID: "client_replacement"})
}
