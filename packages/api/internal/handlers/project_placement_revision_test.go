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

func TestUpsertCannotChangeAProjectsCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	a, b := createCluster(t, db), createCluster(t, db)
	for name, tc := range map[string]struct {
		original, requested *uuid.UUID
		revision, next      *int64
	}{
		"unversioned local to remote":  {nil, &a, nil, nil},
		"unversioned remote to remote": {&a, &b, nil, nil},
		"first revision changes home":  {nil, &a, nil, new(int64(1))},
		"higher revision changes home": {&a, &b, new(int64(1)), new(int64(2))},
		"versioned return to local":    {&a, nil, new(int64(1)), new(int64(2))},
		"stale conflicting revision":   {&a, &b, new(int64(2)), new(int64(1))},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, _ := newManagementStore(t, db)
			project := newProjectFixture()
			body := project.request()
			body.ClusterId, body.ClusterRevision = tc.original, tc.revision
			require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, body).Code)

			body.ClusterId, body.ClusterRevision = tc.requested, tc.next
			body.Name = "Rejected rename"
			response := callUpsertProject(t, store, project.id, body)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			assert.Equal(t, tc.original, teamCluster(t, db, project.id))
			assert.Equal(t, project.request().Name, teamColumn(t, db, project.id, "name"))
			var revision *int64
			queryRow(t, db, `SELECT (SELECT revision FROM projection.project_placements WHERE project_id = $1)`, []any{project.id}, &revision)
			assert.Equal(t, tc.revision, revision)
		})
	}
}

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
	cache.invalidateTeamErr = errors.New("eviction failed")
	assert.Equal(t, http.StatusInternalServerError, push(2, &target).Code)
	assert.Equal(t, http.StatusInternalServerError, push(2, &target).Code)
	metadata := project.request()
	metadata.Name = "Renamed"
	assert.Equal(t, http.StatusInternalServerError, callUpsertProject(t, store, project.id, metadata).Code)
	cache.invalidateTeamErr = nil
	ack(callUpsertProject(t, store, project.id, metadata), http.StatusOK, 2, &target)
	ack(push(2, &target), http.StatusOK, 2, &target)
	ack(push(1, &target), http.StatusOK, 2, &target)
	require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, project.id).Code)
	assert.Equal(t, http.StatusNotFound, push(3, &target).Code)
	assert.Nil(t, teamCluster(t, db, project.id))
}

func TestAWaitingPlacementWriteObservesThePreviousWritersRevision(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)
	stale := project.request()
	stale.ClusterRevision = new(int64(1))

	tx, err := db.SqlcClient.Pool().Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), "SELECT id FROM public.teams WHERE id = $1 FOR UPDATE", project.id)
	require.NoError(t, err)
	_, err = tx.Exec(t.Context(), "INSERT INTO projection.project_placements (project_id, revision) VALUES ($1, 2)", project.id)
	require.NoError(t, err)

	reply := startPlacementWrite(t, store, http.MethodPut, "/unstable/management/projects/"+project.id.String(), mustJSON(t, stale))
	waitForBlockedBy(t, db, int(tx.Conn().PgConn().PID()))
	require.NoError(t, tx.Commit(t.Context()))

	res := <-reply
	require.Equal(t, http.StatusOK, res.Code, res.Body.String())
	var stored api.ManagementProject
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &stored))
	assert.Equal(t, new(int64(2)), stored.ClusterRevision)
	assert.Nil(t, stored.ClusterId)
	assert.Nil(t, teamCluster(t, db, project.id))
}

func TestConcurrentProjectCreationCannotReplaceTheFirstCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	target := createCluster(t, db)
	request := project.request()
	request.ClusterId = &target

	tx, err := db.SqlcClient.Pool().Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), `INSERT INTO public.teams (id, name, slug, email, is_blocked) VALUES ($1,$2,$3,$4,false)`,
		project.id, request.Name, request.Slug, request.Email)
	require.NoError(t, err)

	reply := startPlacementWrite(t, store, http.MethodPut, "/unstable/management/projects/"+project.id.String(), mustJSON(t, request))
	waitForBlockedBy(t, db, int(tx.Conn().PgConn().PID()))
	require.NoError(t, tx.Commit(t.Context()))

	require.Equal(t, http.StatusConflict, (<-reply).Code)
	require.Equal(t, http.StatusConflict, callUpsertProject(t, store, project.id, request).Code)
	assert.Nil(t, teamCluster(t, db, project.id))
}

func startPlacementWrite(t *testing.T, store *APIStore, method, path, body string) <-chan *httptest.ResponseRecorder {
	t.Helper()
	reply := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reply <- callManagement(t, store, method, path, body)
	}()

	return reply
}
