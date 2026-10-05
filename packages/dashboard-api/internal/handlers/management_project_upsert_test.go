package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// The caller cannot tell a create from a reconcile, so one request serves both
// and the status carries the answer.
func TestUpsertProjectCreatesThenReconciles(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, cache := newUpsertStore(db)
	project := newProjectFixture()

	created := callUpsertProject(t, store, project.id, project.request())
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	require.Empty(t, cache.invalidated, "a project that did not exist has nothing cached")

	renamed := project.request()
	renamed.Name = "Acme Renamed"

	reconciled := callUpsertProject(t, store, project.id, renamed)
	require.Equal(t, http.StatusOK, reconciled.Code, reconciled.Body.String())

	require.Equal(t, "Acme Renamed", teamColumn(t, db, project.id, "name"))
	// The team changed, so every cached copy of it is stale.
	require.Equal(t, []uuid.UUID{project.id}, cache.invalidated)
}

// A retry after a response the caller never saw has to land on the same state.
func TestUpsertProjectIsIdempotent(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newUpsertStore(db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	repeated := callUpsertProject(t, store, project.id, project.request())
	require.Equal(t, http.StatusOK, repeated.Code, repeated.Body.String())
	require.Equal(t, "Acme", teamColumn(t, db, project.id, "name"))
}

// A rename moves the slug and nothing else: template names embed the slug they
// were built under, and rewriting them would break references users already
// have.
func TestUpsertProjectRenameLeavesTemplateNames(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newUpsertStore(db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	templateID := testutils.CreateTestTemplate(t, db, project.id)
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &project.slug)

	moved := project.request()
	moved.Slug += "-moved"

	response := callUpsertProject(t, store, project.id, moved)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	require.Equal(t, moved.Slug, teamColumn(t, db, project.id, "slug"))
	require.Equal(t, project.slug, aliasNamespace(t, db, alias))
}

// The slug is unique cluster-wide on the way in and on the way out, so a rename
// can collide just as a create can.
func TestUpsertProjectRenameRejectsATakenSlug(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newUpsertStore(db)
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
	store, _ := newUpsertStore(db)
	incumbent := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, incumbent.id, incumbent.request()).Code)

	challenger := newProjectFixture()
	duplicate := challenger.request()
	duplicate.Slug = incumbent.slug

	collision := callUpsertProject(t, store, challenger.id, duplicate)
	require.Equal(t, http.StatusConflict, collision.Code, collision.Body.String())
}

// A routine name push must not undo an operator's decision, nor move a project
// off limits that arrived through their own route.
func TestUpsertProjectPreservesBlockedStateAndTier(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newUpsertStore(db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
		`INSERT INTO public.tiers (id, name, disk_mb, concurrent_instances, max_length_hours,
			max_vcpu, max_ram_mb, concurrent_template_builds, events_ttl_days,
			default_free_disk_size_mb, max_disk_size_mb)
		 VALUES ('pro_v1', 'Pro', 20480, 100, 24, 8, 8192, 20, 30, 10240, 51200)
		 ON CONFLICT (id) DO NOTHING`))
	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(),
		"UPDATE public.teams SET is_blocked = true, tier = 'pro_v1' WHERE id = $1", project.id))

	reconcile := project.request()
	reconcile.Name = "Acme Renamed"

	renamed := callUpsertProject(t, store, project.id, reconcile)
	require.Equal(t, http.StatusOK, renamed.Code, renamed.Body.String())

	require.Equal(t, "true", teamColumn(t, db, project.id, "is_blocked::text"))
	// The tier is this side's to assign, once, at creation. No push moves it.
	require.Equal(t, "pro_v1", teamColumn(t, db, project.id, "tier"))
}

// Assigned once, at creation, and never moved by a push. The caller names no
// tier at all, so there is nothing here that could move it.
func TestUpsertProjectCreatesOnTheDefaultTier(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newUpsertStore(db)
	project := newProjectFixture()

	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	require.Equal(t, "base_v1", teamColumn(t, db, project.id, "tier"))
}

// Every property in the contract is synchronized by the caller and sent on
// every push, so a reconcile is a complete statement rather than a patch.
func TestUpsertProjectReconcilesSynchronizedProperties(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newUpsertStore(db)
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
	store, _ := newUpsertStore(db)
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

// Deleting a project needs teardown this process cannot reach. If you are
// removing this test, read the handler comment first.
func TestDeleteProjectIsDeliberatelyNotImplemented(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodDelete,
		"/v1/management/projects/"+uuid.NewString(), nil)

	(&APIStore{}).ManagementDeleteProject(ginCtx, uuid.New())

	require.Equal(t, http.StatusNotImplemented, recorder.Code)
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

func newUpsertStore(db *testutils.Database) (*APIStore, *recordingCacheAuthService) {
	auth := &recordingCacheAuthService{}

	return &APIStore{managementService: management.NewService(db.AuthDB, db.SqlcClient, auth, noop.NewMeterProvider())}, auth
}

func callUpsertProject(t *testing.T, store *APIStore, projectID uuid.UUID, request api.ManagementProjectUpsertRequest) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(request)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPut,
		"/v1/management/projects/"+projectID.String(), bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store.ManagementUpsertProject(ginCtx, projectID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func aliasNamespace(t *testing.T, db *testutils.Database, alias string) string {
	t.Helper()

	var namespace *string
	require.NoError(t, db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT namespace FROM public.env_aliases WHERE alias = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&namespace)
		}, alias))

	if namespace == nil {
		return ""
	}

	return *namespace
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
