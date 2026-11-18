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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

// Registration replays are the cluster service's; this covers the routes the
// API serves from its own tables.
func TestManagementClusterLifecycleReplaysAndEndsAbsent(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	store, _ := newManagementStore(t, db)

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
	insertCluster(t, db, firstID, false)
	insertCluster(t, db, secondID, false)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, projectID, firstID).Code)

	response := callManagementAssignProjectCluster(t, store, projectID, secondID)

	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	requireClusterErrorCode(t, response, clusterAssignmentAlreadyAssignedCode)
	require.Equal(t, []uuid.UUID{projectID}, *auth.invalidatedTeamIDs)
	require.Equal(t, firstID, assignedProjectCluster(t, db, projectID))

	versioned := newProjectFixture().request()
	versioned.ClusterId, versioned.ClusterRevision = &firstID, new(int64(1))
	require.Equal(t, http.StatusOK, callUpsertProject(t, store, projectID, versioned).Code)
	response = callManagementAssignProjectCluster(t, store, projectID, secondID)
	require.Equal(t, http.StatusConflict, response.Code)
	requireClusterErrorCode(t, response, clusterAssignmentAlreadyAssignedCode)
}

func TestManagementAssignProjectClusterAssignsAndEvictsTheProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	store, auth := newManagementStore(t, db)
	insertCluster(t, db, clusterID, false)

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
	insertCluster(t, db, clusterID, false)
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
	insertCluster(t, db, clusterID, false)

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
				"envs":          wantEnvs,
				"aliases":       wantReferences,
				"active_builds": wantReferences,
			})
			require.NoError(t, err)
			var state string
			require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT jsonb_build_object(
				'envs', (SELECT jsonb_object_agg(id, jsonb_build_object('cluster', cluster_id, 'deleted', deleted_at IS NOT NULL))
					FROM public.envs WHERE id = ANY($1::text[])),
				'aliases', (SELECT count(*) FROM public.env_aliases WHERE env_id = $2),
				'active_builds', (SELECT count(*) FROM public.active_template_builds WHERE template_id = $2))::text`,
				func(rows pgx.Rows) error {
					require.True(t, rows.Next())

					return rows.Scan(&state)
				}, []string{deletedID, activeID, snapshotID, otherID, localID}, activeID))
			require.JSONEq(t, string(wantJSON), state)
			require.Equal(t, teamAssigned, clusterExists(t, db, clusterID))
			require.True(t, testutils.GetEnvBuildByID(t, ctx, db, buildID), "build history must survive")
		})
	}
}

// The cluster service records the deletion before it refuses or fails the
// removal, so the cluster stays refused to new teams after the API releases
// its lock; the templates are restored for the retry.
func TestAClusterDeleteTheServiceRefusesKeepsTheTemplatesAndTheRecordedDeletion(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID, late := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	store, _ := newManagementStore(t, db)
	clusterService := newSingleClusterService(t, clusterID, clusterPresent)
	clusterService.EXPECT().DeleteCluster(mock.Anything, mock.Anything).Return(nil, clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_IN_USE)).Once()
	withClusterService(t, db, store, clusterService)

	response := callManagementDeleteCluster(t, store, clusterID)

	// The API found no live team on the cluster, so the refusal means the
	// cluster service's copy is behind, and the caller is told to retry.
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	var deleted bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx, `SELECT deleted_at IS NOT NULL FROM public.envs WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&deleted)
		}, templateID))
	require.False(t, deleted)
	refused := callManagementAssignProjectCluster(t, store, late, clusterID)
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	requireClusterErrorCode(t, refused, clusterAssignmentClusterDeletingCode)
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
		require.Fail(t, "cluster deletion did not finish after the registration committed")
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
	insertCluster(t, db, clusterID, true)
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	store, _ := newManagementStore(t, db)

	response := callManagementDeleteCluster(t, store, clusterID)

	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.JSONEq(t, `{"code":409,"message":"Cluster has deletion protection turned on"}`, response.Body.String())
	stored, ok := recordsOf(db).get(clusterID)
	require.True(t, ok)
	require.True(t, stored.GetDeletionProtection())
	var intact bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT EXISTS (SELECT FROM public.envs WHERE id = $1 AND cluster_id = $2 AND deleted_at IS NULL)`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&intact)
		}, templateID, clusterID))
	require.True(t, intact)
}

func TestManagementRequestClusterDeletion(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	for name, tc := range map[string]struct {
		answer error
		want   int
	}{
		"an unprotected cluster":                   {nil, http.StatusNoContent},
		"an absent cluster has nothing to protect": {status.Error(codes.NotFound, "cluster not found"), http.StatusNoContent},
		"a protected cluster": {
			clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_PROTECTED), http.StatusConflict,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clusterID := uuid.New()
			want := clustersgrpc.UpdateClusterRequest_builder{ClusterId: clusterID.String(), RequestDeletion: true}.Build()
			store, clusterService := newClusterServiceStore(t)
			store.sqlcDB = db.SqlcClient
			var answer *clustersgrpc.UpdateClusterResponse
			if tc.answer == nil {
				answer = clustersgrpc.UpdateClusterResponse_builder{Cluster: clustersgrpc.Cluster_builder{ClusterId: clusterID.String()}.Build()}.Build()
			}
			clusterService.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.UpdateClusterRequest) bool {
				return proto.Equal(want, req)
			})).Return(answer, tc.answer).Once()

			require.Equal(t, tc.want, callManagementRequestClusterDeletion(t, store, clusterID))
		})
	}

	t.Run("the local cluster", func(t *testing.T) {
		t.Parallel()

		store, _ := newClusterServiceStore(t)
		require.Equal(t, http.StatusBadRequest, callManagementRequestClusterDeletion(t, store, consts.LocalClusterID))
	})
}

func TestManagementListClusterProjectsListsLiveAssignments(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
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
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	assigned, late := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, assigned, clusterID).Code)
	markClusterDeleting(t, db, clusterID)

	refused := callManagementAssignProjectCluster(t, store, late, clusterID)
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	requireClusterErrorCode(t, refused, clusterAssignmentClusterDeletingCode)
	require.Equal(t, http.StatusNoContent, callManagementAssignProjectCluster(t, store, assigned, clusterID).Code, "an existing assignment is not a new one")
	require.Equal(t, []uuid.UUID{assigned}, listClusterProjects(t, store, clusterID))
}

func TestATeamDeletedBeforeDeletionClearedItsClusterNeitherListsNorBlocks(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	ctx := t.Context()
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
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
	protectedCluster := uuid.New()
	insertCluster(t, db, protectedCluster, true)
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
			'clusters', $1::jsonb,
			'teams', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM public.teams t),
			'envs', (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM public.envs e))::text`,
			func(rows pgx.Rows) error {
				require.True(t, rows.Next())

				return rows.Scan(&state)
			}, recordedClustersJSON(t, db)))

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
