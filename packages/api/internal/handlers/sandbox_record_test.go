package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

func TestSandboxRecordIsTheTeamsLatestRunWithItsSnapshotBaseTemplate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.clusters (id, name, endpoint, endpoint_tls, token, sandbox_proxy_domain)
		 VALUES ($1, 'record-cluster', 'api.example.test:5008', true, 'token', 'sandbox.example.test')`, clusterID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, teamID, clusterID))

	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAliasWithName(t, db, baseTemplateID, "base-alias", nil)
	snapshotTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID := "sbxrecord"
	testutils.CreateSnapshotRecord(t, t.Context(), db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)

	first := time.Date(2026, 3, 4, 5, 0, 0, 0, time.UTC)
	latest := first.Add(2 * time.Minute)
	stopped := latest.Add(time.Minute)
	// The latest run is recorded first, so only its start time picks it.
	insertSandboxLog(t, db, teamID, sandboxID, snapshotTemplateID, 2, latest, &stopped)
	insertSandboxLog(t, db, teamID, sandboxID, snapshotTemplateID, 1, first, new(first.Add(time.Minute)))

	recorder := getSandboxRecord(t, db, &types.Team{Team: &dbtypes.Team{ID: teamID}, Limits: &types.TeamLimits{EventsTTLDays: 7}}, sandboxID)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body api.SandboxRecord
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	body.StartedAt = body.StartedAt.UTC()
	require.NotNil(t, body.StoppedAt)
	body.StoppedAt = new(body.StoppedAt.UTC())
	require.Equal(t, api.SandboxRecord{
		TemplateID:             baseTemplateID,
		Alias:                  new("base-alias"),
		SandboxID:              sandboxID,
		StartedAt:              latest,
		StoppedAt:              &stopped,
		Domain:                 new("sandbox.example.test"),
		CpuCount:               2,
		MemoryMB:               512,
		DiskSizeMB:             1024,
		RetentionExpired:       true,
		EventsRetentionExpired: true,
	}, body)
}

func TestSandboxRecordIsNotFoundWithoutARunTheTeamCanSee(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	otherTeamID := testutils.CreateTestTeam(t, db)
	insertSandboxLog(t, db, otherTeamID, "sbxother", "tmpl", 1, time.Now(), nil)

	for name, sandboxID := range map[string]string{
		"another team's sandbox":   "sbxother",
		"a sandbox that never ran": "sbxmissing",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := getSandboxRecord(t, db, &types.Team{Team: &dbtypes.Team{ID: teamID}}, sandboxID)
			require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
		})
	}
}

func TestSandboxRecordAcceptsTheSandboxIDFormsOtherSandboxRoutesAccept(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	insertSandboxLog(t, db, teamID, "sbxrecord", "tmpl", 1, time.Now(), nil)

	for name, tc := range map[string]struct {
		pathID   string
		wantCode int
	}{
		"a bare sandbox ID":                {pathID: "sbxrecord", wantCode: http.StatusOK},
		"a sandbox ID with its client ID":  {pathID: "sbxrecord-clientid", wantCode: http.StatusOK},
		"an ID with more than one hyphen":  {pathID: "sbxrecord-client-id", wantCode: http.StatusBadRequest},
		"an ID with characters not in IDs": {pathID: "SBX_RECORD", wantCode: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := getSandboxRecord(t, db, &types.Team{Team: &dbtypes.Team{ID: teamID}, Limits: &types.TeamLimits{EventsTTLDays: 7}}, tc.pathID)
			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
		})
	}
}

func TestSandboxRecordIsNotFoundOnADatabaseWithoutTheStoppedSandboxLog(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)

	recorder := getSandboxRecord(t, db, &types.Team{Team: &dbtypes.Team{ID: teamID}}, "sbx1")
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
}

func TestSandboxRecordReportsWhichRetentionWindowsHavePassed(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	createSandboxLogTable(t, db)
	teamID := testutils.CreateTestTeam(t, db)

	for name, tc := range map[string]struct {
		stoppedAgo             *time.Duration
		eventsTTLDays          int64
		retentionExpired       bool
		eventsRetentionExpired bool
	}{
		"a running sandbox keeps everything":                                   {eventsTTLDays: 7},
		"a sandbox stopped a day ago keeps everything":                         {stoppedAgo: new(24 * time.Hour), eventsTTLDays: 7},
		"a sandbox stopped eight days ago keeps nothing under a week's events": {stoppedAgo: new(8 * 24 * time.Hour), eventsTTLDays: 7, retentionExpired: true, eventsRetentionExpired: true},
		"a thirty-day events retention keeps the events of that sandbox":       {stoppedAgo: new(8 * 24 * time.Hour), eventsTTLDays: 30, retentionExpired: true},
		"an unset events retention keeps events for the default week":          {stoppedAgo: new(24 * time.Hour), eventsTTLDays: 0},
		"the team events retention is capped at the maximum one":               {stoppedAgo: new(366 * 24 * time.Hour), eventsTTLDays: 1000, retentionExpired: true, eventsRetentionExpired: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sandboxID := "sbx" + strings.ReplaceAll(uuid.NewString(), "-", "")
			var stoppedAt *time.Time
			if tc.stoppedAgo != nil {
				stoppedAt = new(time.Now().Add(-*tc.stoppedAgo))
			}
			insertSandboxLog(t, db, teamID, sandboxID, "tmpl", 1, time.Now().Add(-400*24*time.Hour), stoppedAt)

			recorder := getSandboxRecord(t, db, &types.Team{Team: &dbtypes.Team{ID: teamID}, Limits: &types.TeamLimits{EventsTTLDays: tc.eventsTTLDays}}, sandboxID)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

			var body api.SandboxRecord
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			require.Equal(t, tc.retentionExpired, body.RetentionExpired)
			require.Equal(t, tc.eventsRetentionExpired, body.EventsRetentionExpired)
		})
	}
}

// createSandboxLogTable builds the stopped-sandbox log by hand: the
// migrations here do not create it.
func createSandboxLogTable(t *testing.T, db *testutils.Database) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `CREATE SCHEMA billing`))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `
		CREATE TABLE billing.sandbox_logs (
			sandbox_id TEXT NOT NULL,
			env_id TEXT NOT NULL,
			vcpu BIGINT NOT NULL,
			ram_mb BIGINT NOT NULL,
			total_disk_size_mb BIGINT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL,
			stopped_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			team_id UUID NOT NULL
		)`))
}

func insertSandboxLog(t *testing.T, db *testutils.Database, teamID uuid.UUID, sandboxID, templateID string, vcpu int64, startedAt time.Time, stoppedAt *time.Time) {
	t.Helper()

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `
		INSERT INTO billing.sandbox_logs (sandbox_id, env_id, vcpu, ram_mb, total_disk_size_mb, started_at, stopped_at, created_at, team_id)
		VALUES ($1, $2, $3, 512, 1024, $4, $5, now(), $6)`,
		sandboxID, templateID, vcpu, startedAt, stoppedAt, teamID))
}

func getSandboxRecord(t *testing.T, db *testutils.Database, team *types.Team, sandboxID string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/sandboxes/"+sandboxID+"/record", nil)
	auth.SetTeamInfoForTest(t, c, team)

	(&APIStore{sqlcDB: db.SqlcClient}).GetUnstableSandboxesSandboxIDRecord(c, sandboxID)

	return recorder
}
