package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	dbmodule "github.com/e2b-dev/infra/packages/db"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// The caller cannot tell a create from a reconcile, so one request serves both
// and the status carries the answer.
func TestUpsertProjectCreatesThenReconciles(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, auth := newManagementStore(t, db)
	project := newProjectFixture()

	created := callUpsertProject(t, store, project.id, project.request())
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	require.Empty(t, *auth.invalidatedTeamIDs, "a project that did not exist has nothing cached")

	renamed := project.request()
	renamed.Name = "Acme Renamed"

	reconciled := callUpsertProject(t, store, project.id, renamed)
	require.Equal(t, http.StatusOK, reconciled.Code, reconciled.Body.String())

	require.Equal(t, "Acme Renamed", teamColumn(t, db, project.id, "name"))
	// The team changed, so every cached copy of it is stale.
	require.Equal(t, []uuid.UUID{project.id}, *auth.invalidatedTeamIDs)
}

// A retry after a response the caller never saw has to land on the same state.
func TestUpsertProjectIsIdempotent(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	repeated := callUpsertProject(t, store, project.id, project.request())
	require.Equal(t, http.StatusOK, repeated.Code, repeated.Body.String())
	require.Equal(t, "Acme", teamColumn(t, db, project.id, "name"))
}

// The slug is unique cluster-wide on the way in and on the way out, so a rename
// can collide just as a create can.
func TestUpsertProjectRenameRejectsATakenSlug(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	incumbent, mover := newProjectFixture(), newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, incumbent.id, incumbent.request()).Code)
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, mover.id, mover.request()).Code)

	collide := mover.request()
	collide.Slug = incumbent.slug

	response := callUpsertProject(t, store, mover.id, collide)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Equal(t, mover.slug, teamColumn(t, db, mover.id, "slug"))
}

// Slugs are unique cluster-wide, so the collision may be with a project the
// caller has never heard of.
func TestUpsertProjectRejectsASlugTakenByAnotherProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	incumbent := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, incumbent.id, incumbent.request()).Code)

	challenger := newProjectFixture()
	duplicate := challenger.request()
	duplicate.Slug = incumbent.slug

	collision := callUpsertProject(t, store, challenger.id, duplicate)
	require.Equal(t, http.StatusConflict, collision.Code, collision.Body.String())
}

// A routine name push must not undo an operator's decision.
func TestUpsertProjectPreservesBlockedState(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
		"UPDATE public.teams SET is_blocked = true WHERE id = $1", project.id))

	reconcile := project.request()
	reconcile.Name = "Acme Renamed"

	renamed := callUpsertProject(t, store, project.id, reconcile)
	require.Equal(t, http.StatusOK, renamed.Code, renamed.Body.String())

	require.Equal(t, "true", teamColumn(t, db, project.id, "is_blocked::text"))
}

// The team lookups read limits from project_limits or default_limits, so a
// project created without a tier still resolves.
func TestUpsertProjectCreatesWithoutATier(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	team, err := db.AuthDB.GetTeamByTeamID(t.Context(), project.id)
	require.NoError(t, err)
	require.Nil(t, team.Team.Tier)
}

// Every property in the contract is synchronized by the caller and sent on
// every push, so a reconcile is a complete statement rather than a patch.
func TestUpsertProjectReconcilesSynchronizedProperties(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)
	rebilled := project.request()
	rebilled.Name = "Acme Renamed"
	rebilled.Email = "billing@acme.test"

	require.Equal(t, http.StatusOK, callUpsertProject(t, store, project.id, rebilled).Code)

	require.Equal(t, "Acme Renamed", teamColumn(t, db, project.id, "name"))
	require.Equal(t, "billing@acme.test", teamColumn(t, db, project.id, "email"))
}

func TestUpsertProjectEchoesTheStoredProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()

	recorder := callUpsertProject(t, store, project.id, project.request())
	require.Equal(t, http.StatusCreated, recorder.Code)

	var decoded api.ManagementProject
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &decoded))
	require.Equal(t, project.id, decoded.Id)
	require.Equal(t, "Acme", decoded.Name)
	require.Equal(t, project.slug, decoded.Slug)
	require.Equal(t, "ops@acme.test", decoded.Email)
}

// projectFixture is a valid project with a slug unique to its test, so cases
// that care about one field can change that one and send the rest.
type projectFixture struct {
	id   uuid.UUID
	slug string
}

func newProjectFixture() projectFixture {
	id := uuid.New()

	return projectFixture{id: id, slug: "acme-" + id.String()[:8]}
}

func (p projectFixture) request() api.ManagementProjectUpsertRequest {
	return api.ManagementProjectUpsertRequest{
		Name:  "Acme",
		Slug:  p.slug,
		Email: "ops@acme.test",
	}
}

// newManagementStore serves the management routes on db, recording the cache
// evictions they make.
func newManagementStore(t *testing.T, db *testutils.Database) (*APIStore, *fakeAPIKeyAuthService) {
	t.Helper()

	var invalidatedTeams []uuid.UUID
	var invalidatedKeys []string
	auth := &fakeAPIKeyAuthService{invalidatedTeamIDs: &invalidatedTeams, invalidatedAPIKeyHashes: &invalidatedKeys}
	riverClient, err := river.NewClient(riverpgxv5.New(db.SqlcClient.Pool()), &river.Config{Schema: dbmodule.RiverSchema})
	require.NoError(t, err)

	return &APIStore{
		sqlcDB:            db.SqlcClient,
		authDB:            db.AuthDB,
		authService:       auth,
		riverClient:       riverClient,
		managementService: management.NewService(db.AuthDB, db.SqlcClient, auth, noop.NewMeterProvider()),
	}, auth
}

// callManagement serves one request the way the server does after
// authentication: behind the deleted-project check.
func callManagement(t *testing.T, store *APIStore, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	router := gin.New()
	router.Use(middleware.RefuseDeletedProject(store.IsProjectDeleted))
	api.RegisterHandlers(router, store)
	request := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

func callUpsertProject(t *testing.T, store *APIStore, projectID uuid.UUID, request api.ManagementProjectUpsertRequest) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(request)
	require.NoError(t, err)

	return callManagement(t, store, http.MethodPut, "/unstable/management/projects/"+projectID.String(), string(body))
}

func teamColumn(t *testing.T, db *testutils.Database, teamID uuid.UUID, column string) string {
	t.Helper()

	var value string
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT "+column+" FROM public.teams WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&value)
		}, teamID)
	require.NoError(t, err)

	return value
}

func TestUpsertProjectSynchronizesOptionalCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := createCluster(t, db)
	for range 2 {
		project := newProjectFixture()
		request := project.request()
		request.ClusterId = &clusterID
		require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, request).Code)
		require.Equal(t, http.StatusOK, callUpsertProject(t, store, project.id, request).Code)
		require.Equal(t, http.StatusOK, callUpsertProject(t, store, project.id, project.request()).Code)
		require.Equal(t, &clusterID, teamCluster(t, db, project.id))
	}
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)
	require.Nil(t, teamCluster(t, db, project.id))
}

func TestUpsertProjectRefusesAClusterBeingDeleted(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := createCluster(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.clusters SET deletion_requested_at = now() WHERE id = $1", clusterID))
	project := newProjectFixture()
	request := project.request()
	request.ClusterId = &clusterID

	response := callUpsertProject(t, store, project.id, request)

	require.Equal(t, http.StatusConflict, response.Code)
	require.Contains(t, response.Body.String(), "Cluster is being deleted")
}

func createCluster(t *testing.T, db *testutils.Database) uuid.UUID {
	t.Helper()

	clusterID, err := db.SqlcClient.Dashboard.CreateCluster(t.Context(), dashboardqueries.CreateClusterParams{
		Name: "project-cluster-" + uuid.NewString()[:8], Endpoint: "https://cluster.example.test", Token: "test-token",
	})
	require.NoError(t, err)

	return clusterID
}

func teamCluster(t *testing.T, db *testutils.Database, teamID uuid.UUID) *uuid.UUID {
	t.Helper()

	var clusterID *uuid.UUID
	require.NoError(t, db.AuthDB.TestsRawSQLQuery(t.Context(), "SELECT cluster_id FROM public.teams WHERE id = $1", func(rows pgx.Rows) error {
		require.True(t, rows.Next())

		return rows.Scan(&clusterID)
	}, teamID))

	return clusterID
}
