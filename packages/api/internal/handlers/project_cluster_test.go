package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

func TestUpsertCannotChangeAProjectsCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	a, b := createCluster(t, db), createCluster(t, db)
	for name, tc := range map[string]struct {
		original, requested *uuid.UUID
	}{
		"local to remote":  {nil, &a},
		"remote to remote": {&a, &b},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, _ := newManagementStore(t, db)
			project := newProjectFixture()
			body := project.request()
			body.ClusterId = tc.original
			require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, body).Code)

			body.ClusterId = tc.requested
			body.Name = "Rejected rename"
			response := callUpsertProject(t, store, project.id, body)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			assert.Equal(t, tc.original, teamCluster(t, db, project.id))
			assert.Equal(t, project.request().Name, teamColumn(t, db, project.id, "name"))
		})
	}
}

func TestPlacementRetryWaitingOnDeletionCannotReviveTheProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	target := createCluster(t, db)
	request := project.request()
	request.ClusterId = &target
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, request).Code)

	tx, err := db.SqlcClient.Pool().Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), "UPDATE public.teams SET deleted_at = now(), is_blocked = true, cluster_id = NULL WHERE id = $1", project.id)
	require.NoError(t, err)

	reply := startPlacementWrite(t, store, http.MethodPut, "/unstable/management/projects/"+project.id.String(), mustJSON(t, request))
	waitForBlockedBy(t, db, int(tx.Conn().PgConn().PID()))
	require.NoError(t, tx.Commit(t.Context()))

	res := <-reply
	require.Equal(t, http.StatusNotFound, res.Code, res.Body.String())
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
