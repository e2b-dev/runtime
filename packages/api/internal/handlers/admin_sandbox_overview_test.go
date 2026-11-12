package handlers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

func TestAdminSandboxOverviewDescribesTheLastPauseAndTheNewestDatedDefaultBuild(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)
	sandboxID, templateID := insertAdminTestSnapshot(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.snapshots SET auto_pause = true, config = '{"version":"v1","filesystemOnly":true}' WHERE sandbox_id = $1`, sandboxID))
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(hours int) *time.Time { return new(start.Add(time.Duration(hours) * time.Hour)) }

	insertAdminTestBuild(t, db, templateID, "success", id.DefaultTag, at(1))
	newest := insertAdminTestBuild(t, db, templateID, "waiting", id.DefaultTag, at(2))
	insertAdminTestBuild(t, db, templateID, "success", "other", at(3))
	insertAdminTestBuild(t, db, templateID, "success", id.DefaultTag, nil)
	teamID := adminTestSnapshotTeamID(t, db, sandboxID)

	recorder := getAdminSandboxOverview(t, &APIStore{sqlcDB: db.SqlcClient}, sandboxID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, fmt.Sprintf(`{
		"sandboxId": %[1]q,
		"team": {"id": %[2]q, "name": "Test Team %[2]s", "email": "test-%[2]s@example.com"},
		"snapshotTemplateId": %[3]q,
		"lastPause": {"kind": "filesystem", "autoPause": true, "autoPauseKind": "memory"},
		"latestBuild": {
			"buildId": %[4]q,
			"templateId": %[3]q,
			"tag": "default",
			"status": "pending",
			"statusGroup": "pending",
			"clusterNodeId": "test-node",
			"assignedAt": "2026-01-01T02:00:00Z",
			"isCurrent": true,
			"restorable": false
		},
		"buildCount": 2
	}`, sandboxID, teamID, templateID, newest), recorder.Body.String())
}

func TestAdminSandboxOverviewOfASandboxThatWasNeverSnapshottedHasNoSnapshotFields(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	sandboxID := "i" + id.Generate()
	insertSandboxLog(t, db, teamID, sandboxID, "tmpl", 1, time.Now(), nil)

	recorder := getAdminSandboxOverview(t, &APIStore{sqlcDB: db.SqlcClient}, sandboxID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.JSONEq(t, fmt.Sprintf(`{
		"sandboxId": %[1]q,
		"team": {"id": %[2]q, "name": "Test Team %[2]s", "email": "test-%[2]s@example.com"},
		"snapshotTemplateId": null,
		"lastPause": null,
		"latestBuild": null,
		"buildCount": 0
	}`, sandboxID, teamID), recorder.Body.String())
}

func TestAdminSandboxOverviewFindsTheTeamThroughTheStoppedSandboxLogOrTheSnapshot(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)

	stopped := "i" + id.Generate()
	stoppedTeam := testutils.CreateTestTeam(t, db)
	insertSandboxLog(t, db, stoppedTeam, stopped, "tmpl", 1, time.Now(), new(time.Now()))

	snapshotted, _ := insertAdminTestSnapshot(t, db)
	snapshotTeam := adminTestSnapshotTeamID(t, db, snapshotted)

	// The snapshot moved to another team after the run the log recorded.
	both, _ := insertAdminTestSnapshot(t, db)
	logTeam := testutils.CreateTestTeam(t, db)
	insertSandboxLog(t, db, logTeam, both, "tmpl", 1, time.Now(), new(time.Now()))

	for name, tc := range map[string]struct {
		sandboxID string
		wantCode  int
		wantTeam  uuid.UUID
	}{
		"a stopped sandbox comes from the log":          {sandboxID: stopped, wantCode: http.StatusOK, wantTeam: stoppedTeam},
		"a sandbox snapshotted while running":           {sandboxID: snapshotted, wantCode: http.StatusOK, wantTeam: snapshotTeam},
		"the log's team wins over the snapshot's":       {sandboxID: both, wantCode: http.StatusOK, wantTeam: logTeam},
		"a sandbox ID with its client ID":               {sandboxID: stopped + "-clientid", wantCode: http.StatusOK, wantTeam: stoppedTeam},
		"a sandbox neither knows":                       {sandboxID: "i" + id.Generate(), wantCode: http.StatusNotFound},
		"an ID that is not a sandbox ID is not queried": {sandboxID: "sbx-client-id", wantCode: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := getAdminSandboxOverview(t, &APIStore{sqlcDB: db.SqlcClient}, tc.sandboxID)

			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
			if tc.wantCode == http.StatusOK {
				require.Contains(t, recorder.Body.String(), `"id":"`+tc.wantTeam.String()+`"`)
			}
		})
	}
}

func TestAdminSandboxOverviewFallsBackToTheSnapshotWithoutTheStoppedSandboxLog(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	snapshotted, _ := insertAdminTestSnapshot(t, db)
	snapshotTeam := adminTestSnapshotTeamID(t, db, snapshotted)

	for name, tc := range map[string]struct {
		sandboxID string
		wantCode  int
	}{
		"a snapshotted sandbox is still found": {sandboxID: snapshotted, wantCode: http.StatusOK},
		"a sandbox without a snapshot":         {sandboxID: "i" + id.Generate(), wantCode: http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := getAdminSandboxOverview(t, &APIStore{sqlcDB: db.SqlcClient}, tc.sandboxID)

			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
			if tc.wantCode == http.StatusOK {
				require.Contains(t, recorder.Body.String(), `"id":"`+snapshotTeam.String()+`"`)
			}
		})
	}
}

func adminTestSnapshotTeamID(t *testing.T, db *testutils.Database, sandboxID string) uuid.UUID {
	t.Helper()

	var teamID uuid.UUID
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT team_id FROM public.snapshots WHERE sandbox_id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&teamID)
		}, sandboxID))

	return teamID
}

func getAdminSandboxOverview(t *testing.T, store *APIStore, sandboxID string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/admin/sandboxes/"+sandboxID+"/overview", nil)

	store.GetUnstableAdminSandboxesSandboxIDOverview(ginCtx, sandboxID)

	return recorder
}
