package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/management"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestDeleteProjectSoftDeletesItAndEnqueuesItsTeardown(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	store, _ := newManagementStore(t, db)
	projectID := testutils.CreateTestTeam(t, db)
	clusterID, err := db.SqlcClient.Dashboard.CreateCluster(ctx, dashboardqueries.CreateClusterParams{
		Name: "project-cluster", Endpoint: "https://cluster.example.test", Token: "test-token",
	})
	require.NoError(t, err)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, projectID, clusterID))
	addProjectMember(t, db, projectID)
	addProjectAPIKey(t, db, projectID)
	publicTemplate := testutils.CreateTestTemplate(t, db, projectID)
	privateTemplate := testutils.CreateTestTemplate(t, db, projectID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET public = false WHERE id = $1`, privateTemplate))
	testutils.CreateTestTemplateAlias(t, db, privateTemplate)
	snapshot := testutils.UpsertTestSnapshot(t, ctx, db, "snapshot-"+uuid.NewString(), "sandbox-"+uuid.NewString(), projectID, publicTemplate)

	response := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	var blocked, deleted bool
	var reason string
	var cluster *uuid.UUID
	queryRow(t, db, `SELECT is_blocked, deleted_at IS NOT NULL, blocked_reason, cluster_id FROM public.teams WHERE id = $1`,
		[]any{projectID}, &blocked, &deleted, &reason, &cluster)
	require.True(t, blocked)
	require.True(t, deleted)
	require.Equal(t, projectDeletionReason, reason)
	requireProjectScrubbed(t, db, projectID)
	require.Nil(t, cluster)
	for templateID, wantDeleted := range map[string]bool{publicTemplate: true, privateTemplate: true, snapshot.TemplateID: true} {
		var envDeleted bool
		queryRow(t, db, `SELECT deleted_at IS NOT NULL FROM public.envs WHERE id = $1`, []any{templateID}, &envDeleted)
		require.Equal(t, wantDeleted, envDeleted, templateID)
	}
	var aliases int
	queryRow(t, db, `SELECT count(*) FROM public.env_aliases WHERE env_id = $1`, []any{privateTemplate}, &aliases)
	require.Zero(t, aliases, "a deleted template releases its aliases")
	hashes, err := db.AuthDB.GetTeamAPIKeyHashes(ctx, projectID)
	require.NoError(t, err)
	require.Empty(t, hashes)
	members, err := db.AuthDB.GetTeamMemberIDs(ctx, projectID)
	require.NoError(t, err)
	require.Empty(t, members)
	require.Equal(t, 1, teardownJobs(t, db, projectID))
}

// A retry answers 204 again. While the first teardown is open the enqueue is a
// no-op; once it finished, a new teardown catches anything created since.
func TestDeleteProjectRetryEnqueuesTeardownOnlyWhenNoneIsOpen(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	projectID := testutils.CreateTestTeam(t, db)
	require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, projectID).Code)

	retry := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusNoContent, retry.Code, retry.Body.String())
	require.Equal(t, 1, teardownJobs(t, db, projectID))

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE river.river_job SET state = 'completed', finalized_at = now() WHERE args->>'team_id' = $1`, projectID.String()))

	again := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusNoContent, again.Code, again.Body.String())
	require.Equal(t, 2, teardownJobs(t, db, projectID))
}

// Writes can land just after the deletion commits: a sandbox that pauses before
// its teardown leaves a snapshot, and a delivery that passed its deleted-project
// check before the commit can add a member, rename the project, assign a
// cluster or rewrite the block reason. A retry repairs all of it, scrubbing the
// project again, without moving the deletion's time.
func TestDeleteProjectRetryRepairsWhatRacedTheDeletion(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	store, _ := newManagementStore(t, db)
	projectID := testutils.CreateTestTeam(t, db)
	baseTemplate := testutils.CreateTestTemplate(t, db, projectID)
	require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, projectID).Code)
	var firstDeletedAt time.Time
	queryRow(t, db, `SELECT deleted_at FROM public.teams WHERE id = $1`, []any{projectID}, &firstDeletedAt)
	lateSnapshot := testutils.UpsertTestSnapshot(t, ctx, db, "snapshot-"+uuid.NewString(), "sandbox-"+uuid.NewString(), projectID, baseTemplate)
	addProjectAPIKey(t, db, projectID)
	addProjectMember(t, db, projectID)
	clusterID, err := db.SqlcClient.Dashboard.CreateCluster(ctx, dashboardqueries.CreateClusterParams{
		Name: "raced-cluster", Endpoint: "https://cluster.example.test", Token: "test-token",
	})
	require.NoError(t, err)
	// An upsert that passed its deleted-project check before the deletion
	// committed reaches the service and assigns its cluster.
	_, _, err = store.managementService.UpsertProject(ctx, management.Project{
		ClusterID: &clusterID, ID: projectID, Name: "raced", Slug: "raced-" + projectID.String()[:8], Email: "raced@example.test",
	})
	require.NoError(t, err)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.teams SET blocked_reason = 'raced block' WHERE id = $1`, projectID))

	retry := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusNoContent, retry.Code, retry.Body.String())
	var snapshotDeleted bool
	queryRow(t, db, `SELECT deleted_at IS NOT NULL FROM public.envs WHERE id = $1`, []any{lateSnapshot.TemplateID}, &snapshotDeleted)
	require.True(t, snapshotDeleted)
	hashes, err := db.AuthDB.GetTeamAPIKeyHashes(ctx, projectID)
	require.NoError(t, err)
	require.Empty(t, hashes)
	members, err := db.AuthDB.GetTeamMemberIDs(ctx, projectID)
	require.NoError(t, err)
	require.Empty(t, members)
	var deletedAt time.Time
	var reason string
	var cluster *uuid.UUID
	queryRow(t, db, `SELECT deleted_at, blocked_reason, cluster_id FROM public.teams WHERE id = $1`,
		[]any{projectID}, &deletedAt, &reason, &cluster)
	require.Equal(t, firstDeletedAt, deletedAt)
	require.Equal(t, projectDeletionReason, reason)
	requireProjectScrubbed(t, db, projectID)
	require.Nil(t, cluster)
}

// A key created while the deletion was in flight survives its key cleanup until
// the next retry; API-key auth refuses it meanwhile instead of authorizing a
// merely blocked team.
func TestDeleteProjectLeavesALateKeyRefusedByAuth(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	auth, err := sharedauth.NewAuthService(ctx, redisutils.SetupInstance(t), db.AuthDB, sharedauth.ProviderConfig{}, http.DefaultClient)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, auth.Close(context.WithoutCancel(ctx))) })
	store, _ := newManagementStore(t, db)
	store.authService = auth
	projectID := testutils.CreateTestTeam(t, db)
	require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, projectID).Code)
	lateKey := addProjectAPIKey(t, db, projectID)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)

	_, apiErr := auth.ValidateAPIKey(ctx, ginCtx, lateKey.PrefixedRawValue)

	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusForbidden, apiErr.Code)
	require.Equal(t, "team is deleted", apiErr.ClientMsg)
}

// Deletion releases the slug, so a new project can take it, and that project
// can be deleted in turn without its scrubbed slug colliding.
func TestDeleteProjectReleasesTheSlugForReuse(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	slug := "reused-" + uuid.NewString()[:8]
	for range 2 {
		projectID := testutils.CreateTestTeam(t, db)
		require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET slug = $2 WHERE id = $1`, projectID, slug))

		response := callDeleteProject(t, store, projectID)

		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
		requireProjectScrubbed(t, db, projectID)
	}
}

func TestDeleteProjectAnswersNotFoundForAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := uuid.New()

	store, _ := newManagementStore(t, db)

	response := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
	require.Zero(t, teardownJobs(t, db, projectID))
}

// The teardown is enqueued in the deletion's transaction, so a deletion that
// fails leaves no job behind and changes nothing.
func TestDeleteProjectThatFailsLeavesNoTeardownJob(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	store, _ := newManagementStore(t, db)
	projectID := testutils.CreateTestTeam(t, db)
	addProjectAPIKey(t, db, projectID)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `CREATE FUNCTION refuse_api_key_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'deletion unavailable'; END $$;
		CREATE TRIGGER refuse_api_key_delete BEFORE DELETE ON public.team_api_keys FOR EACH ROW EXECUTE FUNCTION refuse_api_key_delete();`))

	response := callDeleteProject(t, store, projectID)

	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.Zero(t, teardownJobs(t, db, projectID))
	var blocked, deleted bool
	queryRow(t, db, `SELECT is_blocked, deleted_at IS NOT NULL FROM public.teams WHERE id = $1`, []any{projectID}, &blocked, &deleted)
	require.False(t, blocked)
	require.False(t, deleted)
	hashes, err := db.AuthDB.GetTeamAPIKeyHashes(ctx, projectID)
	require.NoError(t, err)
	require.Len(t, hashes, 1)
}

// The deleted keys' cached entries are evicted, so a key that authorized just
// before the deletion stops authorizing right after it.
func TestDeleteProjectEvictsTheDeletedKeysCachedAuth(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	auth, err := sharedauth.NewAuthService(ctx, redisutils.SetupInstance(t), db.AuthDB, sharedauth.ProviderConfig{}, http.DefaultClient)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, auth.Close(context.WithoutCancel(ctx))) })
	store, _ := newManagementStore(t, db)
	store.authService = auth
	projectID := testutils.CreateTestTeam(t, db)
	key := addProjectAPIKey(t, db, projectID)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	_, apiErr := auth.ValidateAPIKey(ctx, ginCtx, key.PrefixedRawValue)
	require.Nil(t, apiErr)

	require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, projectID).Code)

	_, apiErr = auth.ValidateAPIKey(ctx, ginCtx, key.PrefixedRawValue)
	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusUnauthorized, apiErr.Code)
}

func callDeleteProject(t *testing.T, store *APIStore, projectID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodDelete, "/unstable/management/projects/"+projectID.String(), "")
}

func addProjectMember(t *testing.T, db *testutils.Database, projectID uuid.UUID) {
	t.Helper()

	userID := uuid.New()
	require.NoError(t, db.AuthDB.UpsertPublicUser(t.Context(), userID))
	require.NoError(t, db.AuthDB.CreateTeamMembership(t.Context(), authqueries.CreateTeamMembershipParams{
		UserID: userID, TeamID: projectID,
	}))
}

func addProjectAPIKey(t *testing.T, db *testutils.Database, projectID uuid.UUID) keys.Key {
	t.Helper()

	key, err := keys.GenerateKey(keys.ApiKeyPrefix)
	require.NoError(t, err)
	_, err = db.AuthDB.CreateTeamAPIKey(t.Context(), authqueries.CreateTeamAPIKeyParams{
		TeamID: projectID, ApiKeyHash: key.HashedValue, ApiKeyPrefix: key.Masked.Prefix,
		ApiKeyLength: int32(key.Masked.ValueLength), ApiKeyMaskPrefix: key.Masked.MaskedValuePrefix,
		ApiKeyMaskSuffix: key.Masked.MaskedValueSuffix, Name: "project key",
	})
	require.NoError(t, err)

	return key
}

// requireProjectScrubbed checks that a deleted project no longer carries its
// name, email or slug.
func requireProjectScrubbed(t *testing.T, db *testutils.Database, projectID uuid.UUID) {
	t.Helper()

	var name, email, slug string
	queryRow(t, db, `SELECT name, email, slug FROM public.teams WHERE id = $1`, []any{projectID}, &name, &email, &slug)
	require.Equal(t, "DELETED", name)
	require.Equal(t, "DELETED", email)
	require.Equal(t, "deleted-"+projectID.String(), slug)
}

func teardownJobs(t *testing.T, db *testutils.Database, projectID uuid.UUID) int {
	t.Helper()

	var jobs int
	queryRow(t, db, `SELECT count(*) FROM river.river_job WHERE kind = 'teardown_team_resources' AND args->>'team_id' = $1`,
		[]any{projectID.String()}, &jobs)

	return jobs
}

func queryRow(t *testing.T, db *testutils.Database, query string, args []any, dest ...any) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), query, func(rows pgx.Rows) error {
		require.True(t, rows.Next())

		return rows.Scan(dest...)
	}, args...))
}
