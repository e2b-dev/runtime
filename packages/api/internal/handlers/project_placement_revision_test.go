package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestProjectPlacementAcknowledgesOnlyTheAppliedTarget(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, cache := newManagementStore(t, db)
	project := newProjectFixture()
	target := uuid.New()
	insertCluster(t, db, target, false)
	push := func(revision int64, cluster *uuid.UUID) *httptest.ResponseRecorder {
		t.Helper()
		body := project.request()
		body.ClusterId, body.ClusterRevision = cluster, &revision

		return callUpsertProject(t, store, project.id, body)
	}
	ack := func(w *httptest.ResponseRecorder, code int, revision int64, cluster *uuid.UUID) {
		t.Helper()
		require.Equal(t, code, w.Code, w.Body.String())
		var stored api.ManagementProject
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &stored))
		assert.Equal(t, cluster, stored.ClusterId)
		assert.Equal(t, &revision, stored.ClusterRevision)
		assert.Equal(t, cluster, teamCluster(t, db, project.id))
	}
	ack(push(1, &target), http.StatusCreated, 1, &target)
	assert.Equal(t, http.StatusConflict, callManagementDetachProjectCluster(t, store, project.id, target).Code)
	cache.invalidateTeamErr = errors.New("eviction failed")
	assert.Equal(t, http.StatusInternalServerError, push(2, nil).Code)
	assert.Nil(t, teamCluster(t, db, project.id))
	assert.Equal(t, http.StatusInternalServerError, push(2, nil).Code)
	metadata := project.request()
	metadata.Name = "Renamed"
	assert.Equal(t, http.StatusInternalServerError, callUpsertProject(t, store, project.id, metadata).Code)
	cache.invalidateTeamErr = nil
	ack(callUpsertProject(t, store, project.id, metadata), http.StatusOK, 2, nil)
	ack(push(2, nil), http.StatusOK, 2, nil)
	ack(push(1, &target), http.StatusOK, 2, nil)
	assert.Equal(t, http.StatusConflict, push(2, &target).Code)
	old := project.request()
	old.ClusterId = &target
	assert.Equal(t, http.StatusConflict, callUpsertProject(t, store, project.id, old).Code)
	assert.Nil(t, teamCluster(t, db, project.id))
	require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, project.id).Code)
	assert.Equal(t, http.StatusNotFound, push(3, &target).Code)
	assert.Nil(t, teamCluster(t, db, project.id))
}

func TestWaitingPlacementWritesObserveThePreviousWritersRevision(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"stale upsert", "unversioned detach"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			db := testutils.SetupDatabase(t)
			store, _ := newManagementStore(t, db)
			project := newProjectFixture()
			target := createCluster(t, db)
			require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)
			var committedTarget *uuid.UUID
			projectPath := "/unstable/management/projects/" + project.id.String()
			method, path, body := http.MethodDelete, projectPath+"/cluster/"+target.String(), ""
			wantStatus := http.StatusConflict
			switch operation {
			case "stale upsert":
				request := project.request()
				request.ClusterId, request.ClusterRevision = &target, new(int64(1))
				method, path, body, wantStatus = http.MethodPut, projectPath, mustJSON(t, request), http.StatusOK
			case "unversioned detach":
				committedTarget = &target
			}

			tx, err := db.SqlcClient.Pool().Begin(t.Context())
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
			_, err = tx.Exec(t.Context(), "UPDATE public.teams SET cluster_id = $2 WHERE id = $1", project.id, committedTarget)
			require.NoError(t, err)
			_, err = tx.Exec(t.Context(), "INSERT INTO projection.project_placements (project_id, revision) VALUES ($1, 2)", project.id)
			require.NoError(t, err)

			reply := startPlacementWrite(t, store, method, path, body)
			waitForBlockedBy(t, db, int(tx.Conn().PgConn().PID()))
			require.NoError(t, tx.Commit(t.Context()))

			res := <-reply
			require.Equal(t, wantStatus, res.Code, res.Body.String())
			if wantStatus == http.StatusOK {
				var stored api.ManagementProject
				require.NoError(t, json.Unmarshal(res.Body.Bytes(), &stored))
				assert.Equal(t, new(int64(2)), stored.ClusterRevision)
				assert.Nil(t, stored.ClusterId)
			} else {
				requireClusterErrorCode(t, res, clusterAssignmentRevisionRequiredCode)
			}
			assert.Equal(t, committedTarget, teamCluster(t, db, project.id))
		})
	}
}

func TestPlacementTargetIsNotVisibleBeforeItsRevisionCommits(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	target := createCluster(t, db)
	request := project.request()
	request.ClusterId, request.ClusterRevision = &target, new(int64(1))
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, request).Code)

	tx, err := db.SqlcClient.Pool().Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	var revision int64
	require.NoError(t, tx.QueryRow(t.Context(), "SELECT revision FROM projection.project_placements WHERE project_id = $1 FOR UPDATE", project.id).Scan(&revision))
	require.EqualValues(t, 1, revision)

	request.ClusterId, request.ClusterRevision = nil, new(int64(2))
	reply := startPlacementWrite(t, store, http.MethodPut, "/unstable/management/projects/"+project.id.String(), mustJSON(t, request))
	waitForBlockedBy(t, db, int(tx.Conn().PgConn().PID()))
	assert.Equal(t, &target, teamCluster(t, db, project.id))
	require.NoError(t, tx.Commit(t.Context()))

	require.Equal(t, http.StatusOK, (<-reply).Code)
	assert.Nil(t, teamCluster(t, db, project.id))
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(), "SELECT revision FROM projection.project_placements WHERE project_id = $1", project.id).Scan(&revision))
	assert.EqualValues(t, 2, revision)
}

func startPlacementWrite(t *testing.T, store *APIStore, method, path, body string) <-chan *httptest.ResponseRecorder {
	t.Helper()
	reply := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reply <- callManagement(t, store, method, path, body)
	}()

	return reply
}
