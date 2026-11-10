package handlers

import (
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestUpsertProjectSlugChangeGivesEveryNamespacedAliasUnderTheNewSlug(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	templateID := testutils.CreateTestTemplate(t, db, project.id)
	historical := "acme-historical-" + project.id.String()[:8]
	current := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &project.slug)
	earlier := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &historical)
	global := testutils.CreateTestTemplateAlias(t, db, templateID)

	moved := renamed(project)
	response := callUpsertProject(t, store, project.id, moved)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	require.Equal(t, map[string]string{project.slug: templateID, moved.Slug: templateID}, aliasTemplates(t, db, current))
	require.Equal(t, map[string]string{historical: templateID, moved.Slug: templateID}, aliasTemplates(t, db, earlier))
	require.Equal(t, map[string]string{"": templateID}, aliasTemplates(t, db, global))
}

func TestUpsertProjectSlugChangeKeepsAnAliasTheNewSlugAlreadyHolds(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		heldByAnotherTemplate bool
	}{
		"for the same template": {heldByAnotherTemplate: false},
		"for another template":  {heldByAnotherTemplate: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			store, _ := newManagementStore(t, db)
			project := newProjectFixture()
			require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

			moved := renamed(project)
			templateID := testutils.CreateTestTemplate(t, db, project.id)
			alias := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &project.slug)
			sibling := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &project.slug)

			holderTemplateID := templateID
			if tc.heldByAnotherTemplate {
				holderTemplateID = testutils.CreateTestTemplate(t, db, testutils.CreateTestTeam(t, db))
			}
			testutils.CreateTestTemplateAliasWithName(t, db, holderTemplateID, alias, &moved.Slug)

			response := callUpsertProject(t, store, project.id, moved)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())

			require.Equal(t, moved.Slug, teamColumn(t, db, project.id, "slug"))
			require.Equal(t, map[string]string{project.slug: templateID, moved.Slug: holderTemplateID}, aliasTemplates(t, db, alias))
			require.Equal(t, map[string]string{project.slug: templateID, moved.Slug: templateID}, aliasTemplates(t, db, sibling))
		})
	}
}

func TestUpsertProjectSlugChangePrefersTheReplacedSlugWhenEarlierNamespacesDisagree(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	currentTemplateID := testutils.CreateTestTemplate(t, db, project.id)
	historicalTemplateID := testutils.CreateTestTemplate(t, db, project.id)
	// Sorts ahead of the replaced slug, so ordering by name alone would pick it.
	historical := "a-historical-" + project.id.String()[:8]
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, db, currentTemplateID, &project.slug)
	testutils.CreateTestTemplateAliasWithName(t, db, historicalTemplateID, alias, &historical)

	moved := renamed(project)
	response := callUpsertProject(t, store, project.id, moved)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	require.Equal(t, currentTemplateID, aliasTemplates(t, db, alias)[moved.Slug])
}

func TestUpsertProjectWithAnUnchangedSlugCopiesNoAliases(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	templateID := testutils.CreateTestTemplate(t, db, project.id)
	historical := "acme-historical-" + project.id.String()[:8]
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &historical)

	reconcile := project.request()
	reconcile.Name = "Acme Renamed"
	response := callUpsertProject(t, store, project.id, reconcile)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	require.Equal(t, map[string]string{historical: templateID}, aliasTemplates(t, db, alias))
}

func TestUpsertProjectSlugChangeLeavesOtherProjectsAliasesAlone(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project, other := newProjectFixture(), newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, other.id, other.request()).Code)

	otherTemplateID := testutils.CreateTestTemplate(t, db, other.id)
	otherAlias := testutils.CreateTestTemplateAliasWithNamespace(t, db, otherTemplateID, &other.slug)

	moved := renamed(project)
	response := callUpsertProject(t, store, project.id, moved)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	require.Equal(t, map[string]string{other.slug: otherTemplateID}, aliasTemplates(t, db, otherAlias))
}

func TestUpsertProjectSlugChangeSkipsDeletedTemplates(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	templateID := testutils.CreateTestTemplate(t, db, project.id)
	alias := testutils.CreateTestTemplateAliasWithNamespace(t, db, templateID, &project.slug)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.envs SET deleted_at = NOW() WHERE id = $1", templateID))

	response := callUpsertProject(t, store, project.id, renamed(project))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	require.Equal(t, map[string]string{project.slug: templateID}, aliasTemplates(t, db, alias))
}

// A delete releases the aliases it can see, so a copy it cannot see yet would
// keep the deleted template's name taken. The copy waits for the delete instead.
func TestUpsertProjectSlugChangeWaitsForATemplateDeleteInProgress(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	project := newProjectFixture()
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)

	deletedID := testutils.CreateTestTemplate(t, db, project.id)
	deletedAlias := testutils.CreateTestTemplateAliasWithNamespace(t, db, deletedID, &project.slug)
	liveID := testutils.CreateTestTemplate(t, db, project.id)
	liveAlias := testutils.CreateTestTemplateAliasWithNamespace(t, db, liveID, &project.slug)

	txDB, tx, err := db.SqlcClient.WithTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(t.Context()) }()
	_, err = txDB.SoftDeleteTemplate(t.Context(), queries.SoftDeleteTemplateParams{TemplateID: deletedID, TeamID: project.id})
	require.NoError(t, err)

	moved := renamed(project)
	upserted := make(chan error, 1)
	go func() {
		_, _, err := store.managementService.UpsertProject(t.Context(), management.Project{
			ID: project.id, Name: moved.Name, Slug: moved.Slug, Email: moved.Email,
		})
		upserted <- err
	}()

	require.Eventually(t, func() bool { return waitingOnALock(t, db) }, 10*time.Second, 20*time.Millisecond)

	_, err = txDB.ReleaseTemplateAliases(t.Context(), deletedID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(t.Context()))
	require.NoError(t, <-upserted)

	require.Empty(t, aliasTemplates(t, db, deletedAlias))
	require.Equal(t, map[string]string{project.slug: liveID, moved.Slug: liveID}, aliasTemplates(t, db, liveAlias))
}

func renamed(project projectFixture) api.ManagementProjectUpsertRequest {
	moved := project.request()
	moved.Slug += "-moved"

	return moved
}

// aliasTemplates maps each namespace holding the alias to its template; the
// empty key stands for an alias without a namespace.
func aliasTemplates(t *testing.T, db *testutils.Database, alias string) map[string]string {
	t.Helper()

	templates := map[string]string{}
	require.NoError(t, db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT COALESCE(namespace, ''), env_id FROM public.env_aliases WHERE alias = $1",
		func(rows pgx.Rows) error {
			for rows.Next() {
				var namespace, templateID string
				if err := rows.Scan(&namespace, &templateID); err != nil {
					return err
				}
				templates[namespace] = templateID
			}

			return rows.Err()
		}, alias))

	return templates
}

func waitingOnALock(t *testing.T, db *testutils.Database) bool {
	t.Helper()

	var waiting bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock')",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&waiting)
		}))

	return waiting
}
