package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// insertAdminTestSnapshot creates a paused sandbox whose snapshot template has
// no builds yet.
func insertAdminTestSnapshot(t *testing.T, db *testutils.Database) (sandboxID, templateID string) {
	t.Helper()

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID = "i" + id.Generate()
	templateID = id.Generate()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.envs (id, team_id, public, updated_at, source) VALUES ($1, $2, false, NOW(), 'snapshot')`,
		templateID, teamID,
	))
	testutils.CreateSnapshotRecord(t, t.Context(), db, templateID, sandboxID, teamID, baseTemplateID)

	return sandboxID, templateID
}

// insertAdminTestBuild creates a build of the template with the given status
// and assigns it under tag. A nil assignedAt leaves the assignment undated.
func insertAdminTestBuild(t *testing.T, db *testutils.Database, templateID, status, tag string, assignedAt *time.Time) uuid.UUID {
	t.Helper()

	buildID := testutils.CreateTestBuild(t, t.Context(), db, templateID, status)
	assignAdminTestBuild(t, db, templateID, buildID, tag, assignedAt)

	return buildID
}

func assignAdminTestBuild(t *testing.T, db *testutils.Database, templateID string, buildID uuid.UUID, tag string, assignedAt *time.Time) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag, created_at) VALUES ($1, $2, $3, $4)`,
		templateID, buildID, tag, assignedAt,
	))
}

func getAdminSandboxBuilds(t *testing.T, store *APIStore, sandboxID string, params api.GetUnstableAdminSandboxesSandboxIDBuildsParams) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/admin/sandboxes/"+sandboxID+"/builds", nil)

	store.GetUnstableAdminSandboxesSandboxIDBuilds(ginCtx, sandboxID, params)

	return recorder
}

func postAdminSandboxBuildRestore(t *testing.T, store *APIStore, sandboxID string, buildID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/unstable/admin/sandboxes/"+sandboxID+"/builds/restore",
		strings.NewReader(`{"buildID":"`+buildID.String()+`"}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store.PostUnstableAdminSandboxesSandboxIDBuildsRestore(ginCtx, sandboxID)

	return recorder
}

type adminTestBuildEntry struct {
	buildID    uuid.UUID
	isCurrent  bool
	restorable bool
}

func adminTestBuildEntries(t *testing.T, recorder *httptest.ResponseRecorder) ([]adminTestBuildEntry, int64) {
	t.Helper()

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var list api.AdminSandboxBuildList
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &list))

	entries := make([]adminTestBuildEntry, 0, len(list.Items))
	for _, item := range list.Items {
		entries = append(entries, adminTestBuildEntry{buildID: item.BuildID, isCurrent: item.IsCurrent, restorable: item.Restorable})
	}

	return entries, list.Total
}

func TestGetUnstableAdminSandboxesSandboxIDBuildsListsTheSnapshotTemplateHistory(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(hours int) *time.Time { return new(start.Add(time.Duration(hours) * time.Hour)) }

	oldest := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, at(1))
	failed := insertAdminTestBuild(t, testDB, templateID, "failed", id.DefaultTag, at(2))
	ready := insertAdminTestBuild(t, testDB, templateID, "uploaded", id.DefaultTag, at(3))
	building := insertAdminTestBuild(t, testDB, templateID, "building", id.DefaultTag, at(4))
	insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, nil)
	assignAdminTestBuild(t, testDB, templateID, oldest, "stable", at(5))

	store := &APIStore{sqlcDB: testDB.SqlcClient}

	tests := map[string]struct {
		params    api.GetUnstableAdminSandboxesSandboxIDBuildsParams
		want      []adminTestBuildEntry
		wantTotal int64
	}{
		"dated default assignments are listed newest first with the newest ready one current": {
			want: []adminTestBuildEntry{
				{buildID: building},
				{buildID: ready, isCurrent: true},
				{buildID: failed},
				{buildID: oldest, restorable: true},
			},
			wantTotal: 4,
		},
		"ascending order lists the oldest first": {
			params: api.GetUnstableAdminSandboxesSandboxIDBuildsParams{Order: new(api.Asc)},
			want: []adminTestBuildEntry{
				{buildID: oldest, restorable: true},
				{buildID: failed},
				{buildID: ready, isCurrent: true},
				{buildID: building},
			},
			wantTotal: 4,
		},
		"a status filter narrows the builds and the total but not which build is current": {
			params: api.GetUnstableAdminSandboxesSandboxIDBuildsParams{StatusGroup: &[]api.AdminSandboxBuildStatusGroup{api.AdminSandboxBuildStatusGroupReady, api.AdminSandboxBuildStatusGroupFailed}},
			want: []adminTestBuildEntry{
				{buildID: ready, isCurrent: true},
				{buildID: failed},
				{buildID: oldest, restorable: true},
			},
			wantTotal: 3,
		},
		"limit and offset page through the history while the total counts all of it": {
			params:    api.GetUnstableAdminSandboxesSandboxIDBuildsParams{Limit: new(int32(2)), Offset: new(int32(1))},
			want:      []adminTestBuildEntry{{buildID: ready, isCurrent: true}, {buildID: failed}},
			wantTotal: 4,
		},
		"an offset past the end lists no builds": {
			params:    api.GetUnstableAdminSandboxesSandboxIDBuildsParams{Offset: new(int32(10))},
			want:      []adminTestBuildEntry{},
			wantTotal: 4,
		},
		"another tag lists its own assignments, which restore does not accept": {
			params:    api.GetUnstableAdminSandboxesSandboxIDBuildsParams{Tag: new("stable")},
			want:      []adminTestBuildEntry{{buildID: oldest, isCurrent: true}},
			wantTotal: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			entries, total := adminTestBuildEntries(t, getAdminSandboxBuilds(t, store, sandboxID, tc.params))
			require.Equal(t, tc.want, entries)
			require.Equal(t, tc.wantTotal, total)
		})
	}
}

func TestGetUnstableAdminSandboxesSandboxIDBuildsReturnsEveryBuildField(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
	buildID := insertAdminTestBuild(t, testDB, templateID, "snapshotting", id.DefaultTag, new(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)))

	recorder := getAdminSandboxBuilds(t, &APIStore{sqlcDB: testDB.SqlcClient}, sandboxID, api.GetUnstableAdminSandboxesSandboxIDBuildsParams{})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{
		"snapshotTemplateID": "`+templateID+`",
		"total": 1,
		"items": [{
			"buildID": "`+buildID.String()+`",
			"templateID": "`+templateID+`",
			"tag": "default",
			"status": "in_progress",
			"statusGroup": "in_progress",
			"clusterNodeID": "test-node",
			"assignedAt": "2026-01-01T12:00:00Z",
			"isCurrent": false,
			"restorable": false
		}]
	}`, recorder.Body.String())
}

func TestGetUnstableAdminSandboxesSandboxIDBuildsReturnsANullTemplateForASandboxWithoutASnapshot(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)

	recorder := getAdminSandboxBuilds(t, &APIStore{sqlcDB: testDB.SqlcClient}, "i"+id.Generate(), api.GetUnstableAdminSandboxesSandboxIDBuildsParams{})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, `{"items":[],"total":0,"snapshotTemplateID":null}`, recorder.Body.String())
}

func TestPostUnstableAdminSandboxesSandboxIDBuildsRestoreMakesTheBuildCurrentForTheNextResume(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
	older := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-2*time.Hour)))
	current := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-time.Hour)))
	pausing := insertAdminTestBuild(t, testDB, templateID, "snapshotting", id.DefaultTag, new(time.Now().Add(-time.Minute)))

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		snapshotCache: snapshotcache.NewSnapshotCache(testDB.SqlcClient, redisClient),
	}

	cached, err := store.snapshotCache.Get(t.Context(), sandboxID)
	require.NoError(t, err)
	require.Equal(t, current, cached.EnvBuild.ID)

	recorder := postAdminSandboxBuildRestore(t, store, sandboxID, older)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var result api.AdminSandboxBuildRestoreResult
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
	require.Equal(t, sandboxID, result.SandboxID)
	require.Equal(t, templateID, result.TemplateID)
	require.Equal(t, older, result.BuildID)
	require.Equal(t, &current, result.PreviousBuildID)
	require.WithinDuration(t, time.Now(), result.AssignedAt, time.Minute)

	resumed, err := store.snapshotCache.Get(t.Context(), sandboxID)
	require.NoError(t, err)
	require.Equal(t, older, resumed.EnvBuild.ID)

	entries, total := adminTestBuildEntries(t, getAdminSandboxBuilds(t, store, sandboxID, api.GetUnstableAdminSandboxesSandboxIDBuildsParams{}))
	require.Equal(t, []adminTestBuildEntry{
		{buildID: older, isCurrent: true},
		{buildID: pausing},
		{buildID: current, restorable: true},
		{buildID: older, isCurrent: true},
	}, entries)
	require.Equal(t, int64(4), total)
}

func TestPostUnstableAdminSandboxesSandboxIDBuildsRestoreRefusesBuildsItCannotRestore(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	store := &APIStore{sqlcDB: testDB.SqlcClient}
	yesterday := new(time.Now().Add(-24 * time.Hour))

	tests := map[string]struct {
		seed        func(t *testing.T) (sandboxID, templateID string, buildID uuid.UUID)
		wantCode    int
		wantMessage string
	}{
		"a sandbox without a snapshot is not found": {
			seed: func(*testing.T) (string, string, uuid.UUID) {
				return "i" + id.Generate(), "", uuid.New()
			},
			wantCode:    http.StatusNotFound,
			wantMessage: "Sandbox has no snapshot",
		},
		"a sandbox whose snapshot template is deleted is not found": {
			seed: func(t *testing.T) (string, string, uuid.UUID) {
				t.Helper()
				sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
				buildID := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, yesterday)
				require.NoError(t, testDB.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, templateID))

				return sandboxID, templateID, buildID
			},
			wantCode:    http.StatusNotFound,
			wantMessage: "Sandbox has no snapshot",
		},
		"a build of another template is not found": {
			seed: func(t *testing.T) (string, string, uuid.UUID) {
				t.Helper()
				sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
				_, otherTemplateID := insertAdminTestSnapshot(t, testDB)

				return sandboxID, templateID, insertAdminTestBuild(t, testDB, otherTemplateID, "success", id.DefaultTag, yesterday)
			},
			wantCode:    http.StatusNotFound,
			wantMessage: "Build is not assigned to the sandbox's snapshot template",
		},
		"a build assigned only under another tag is not found": {
			seed: func(t *testing.T) (string, string, uuid.UUID) {
				t.Helper()
				sandboxID, templateID := insertAdminTestSnapshot(t, testDB)

				return sandboxID, templateID, insertAdminTestBuild(t, testDB, templateID, "success", "stable", yesterday)
			},
			wantCode:    http.StatusNotFound,
			wantMessage: "Build is not assigned to the sandbox's snapshot template",
		},
		"a build that is not ready conflicts": {
			seed: func(t *testing.T) (string, string, uuid.UUID) {
				t.Helper()
				sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
				buildID := insertAdminTestBuild(t, testDB, templateID, "failed", id.DefaultTag, yesterday)
				insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now()))

				return sandboxID, templateID, buildID
			},
			wantCode:    http.StatusConflict,
			wantMessage: "Only ready builds can be restored",
		},
		"the current build conflicts": {
			seed: func(t *testing.T) (string, string, uuid.UUID) {
				t.Helper()
				sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
				insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, yesterday)

				return sandboxID, templateID, insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now()))
			},
			wantCode:    http.StatusConflict,
			wantMessage: "Build is already the current build",
		},
		"the newest ready build conflicts while a newer build is still in progress": {
			seed: func(t *testing.T) (string, string, uuid.UUID) {
				t.Helper()
				sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
				buildID := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, yesterday)
				insertAdminTestBuild(t, testDB, templateID, "snapshotting", id.DefaultTag, new(time.Now()))

				return sandboxID, templateID, buildID
			},
			wantCode:    http.StatusConflict,
			wantMessage: "Build is already the current build",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sandboxID, templateID, buildID := tc.seed(t)
			before := testutils.GetBuildAssignments(t, t.Context(), testDB, templateID)

			recorder := postAdminSandboxBuildRestore(t, store, sandboxID, buildID)

			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
			require.JSONEq(t, fmt.Sprintf(`{"code":%d,"message":%q}`, tc.wantCode, tc.wantMessage), recorder.Body.String())
			require.Equal(t, before, testutils.GetBuildAssignments(t, t.Context(), testDB, templateID))
		})
	}
}

func adminTestLockWaiters(t *testing.T, db *testutils.Database) int {
	t.Helper()

	var waiters int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`,
		func(rows pgx.Rows) error {
			for rows.Next() {
				if err := rows.Scan(&waiters); err != nil {
					return err
				}
			}

			return rows.Err()
		},
	))

	return waiters
}

// A restore that starts while another restore of the same build is still
// open waits for it, then sees the build as current instead of writing it a
// second time.
func TestPostUnstableAdminSandboxesSandboxIDBuildsRestoreWaitsForAnOpenRestoreOfTheSameSandbox(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
	older := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-2*time.Hour)))
	insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-time.Hour)))

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		snapshotCache: snapshotcache.NewSnapshotCache(testDB.SqlcClient, redisClient),
	}

	openRestore, tx, err := testDB.SqlcClient.WithTx(t.Context())
	require.NoError(t, err)
	defer tx.Rollback(t.Context())
	_, err = openRestore.LockAdminSandboxSnapshotTemplate(t.Context(), sandboxID)
	require.NoError(t, err)
	_, err = openRestore.CreateAdminSandboxBuildAssignment(t.Context(), queries.CreateAdminSandboxBuildAssignmentParams{TemplateID: templateID, BuildID: older, Tag: id.DefaultTag})
	require.NoError(t, err)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- postAdminSandboxBuildRestore(t, store, sandboxID, older) }()

	require.Eventually(t, func() bool { return adminTestLockWaiters(t, testDB) > 0 }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, tx.Commit(t.Context()))

	recorder := <-done
	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	require.Len(t, testutils.GetBuildAssignments(t, t.Context(), testDB, templateID), 3)
}

func TestGetUnstableAdminSandboxesSandboxIDBuildsOffersNoRestoreForADeletedSnapshotTemplate(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
	older := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-2*time.Hour)))
	current := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-time.Hour)))
	require.NoError(t, testDB.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, templateID))

	entries, total := adminTestBuildEntries(t, getAdminSandboxBuilds(t, &APIStore{sqlcDB: testDB.SqlcClient}, sandboxID, api.GetUnstableAdminSandboxesSandboxIDBuildsParams{}))

	require.Equal(t, []adminTestBuildEntry{{buildID: current, isCurrent: true}, {buildID: older}}, entries)
	require.Equal(t, int64(2), total)
}

func setAdminTestBuildKind(t *testing.T, db *testutils.Database, buildID uuid.UUID, filesystemOnly bool) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.env_builds SET filesystem_only = $2 WHERE id = $1`, buildID, filesystemOnly))
}

func TestPostUnstableAdminSandboxesSandboxIDBuildsRestoreGivesTheSnapshotTheRestoredBuildsKind(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		snapshotFilesystemOnly bool
		buildFilesystemOnly    bool
	}{
		"a filesystem-only build makes a memory snapshot filesystem-only": {buildFilesystemOnly: true},
		"a memory build makes a filesystem-only snapshot a memory one":    {snapshotFilesystemOnly: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testDB := testutils.SetupDatabase(t)
			sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
			older := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-2*time.Hour)))
			current := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-time.Hour)))
			setAdminTestBuildKind(t, testDB, older, tc.buildFilesystemOnly)
			setAdminTestBuildKind(t, testDB, current, tc.snapshotFilesystemOnly)
			require.NoError(t, testDB.SqlcClient.UpdateSnapshotFilesystemOnly(t.Context(), queries.UpdateSnapshotFilesystemOnlyParams{SandboxID: sandboxID, FilesystemOnly: tc.snapshotFilesystemOnly}))

			store := &APIStore{
				sqlcDB:        testDB.SqlcClient,
				snapshotCache: snapshotcache.NewSnapshotCache(testDB.SqlcClient, redis_utils.SetupInstance(t)),
			}

			recorder := postAdminSandboxBuildRestore(t, store, sandboxID, older)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			resumed, err := store.snapshotCache.Get(t.Context(), sandboxID)
			require.NoError(t, err)
			require.Equal(t, older, resumed.EnvBuild.ID)
			require.NotNil(t, resumed.Snapshot.Config)
			require.Equal(t, tc.buildFilesystemOnly, resumed.Snapshot.Config.FilesystemOnly)
		})
	}
}

// An assignment committed while the restore waits for its lock can carry a
// later time than the restore's transaction start; the restored build must
// still end up newest.
func TestPostUnstableAdminSandboxesSandboxIDBuildsRestoreSortsAfterAnAssignmentCommittedWhileItWaited(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	sandboxID, templateID := insertAdminTestSnapshot(t, testDB)
	older := insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-2*time.Hour)))
	insertAdminTestBuild(t, testDB, templateID, "success", id.DefaultTag, new(time.Now().Add(-time.Hour)))
	other := testutils.CreateTestBuild(t, t.Context(), testDB, templateID, "success")

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		snapshotCache: snapshotcache.NewSnapshotCache(testDB.SqlcClient, redis_utils.SetupInstance(t)),
	}

	openRestore, tx, err := testDB.SqlcClient.WithTx(t.Context())
	require.NoError(t, err)
	defer tx.Rollback(t.Context())
	_, err = openRestore.LockAdminSandboxSnapshotTemplate(t.Context(), sandboxID)
	require.NoError(t, err)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- postAdminSandboxBuildRestore(t, store, sandboxID, older) }()

	require.Eventually(t, func() bool { return adminTestLockWaiters(t, testDB) > 0 }, 10*time.Second, 10*time.Millisecond)
	_, err = tx.Exec(t.Context(),
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag, created_at) VALUES ($1, $2, $3, clock_timestamp())`,
		templateID, other, id.DefaultTag,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(t.Context()))

	recorder := <-done
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	resumed, err := store.snapshotCache.Get(t.Context(), sandboxID)
	require.NoError(t, err)
	require.Equal(t, older, resumed.EnvBuild.ID)
}
