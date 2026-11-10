package handlers

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestGetUnstableTeamsTeamIDLimitsReturnsTheTeamsPushedLimits(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.AuthDB.TestsRawSQL(t.Context(), `
INSERT INTO public.project_limits (
    team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
    max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
    api_team_rps_list, api_team_rps_delete, max_secrets
) VALUES ($1, 24, 500, 22, 16, 16384, 20480, 30, 10240, 51200, 40, 50, 10)
`, teamID))

	recorder := callTeamRead(t, &APIStore{authDB: db.AuthDB}, (*APIStore).GetUnstableTeamsTeamIDLimits, teamID.String(), &teamID)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body api.TeamLimitsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, api.TeamLimits{
		MaxLengthHours:           24,
		ConcurrentSandboxes:      500,
		ConcurrentTemplateBuilds: 22,
		MaxVcpu:                  16,
		MaxRamMb:                 16384,
		DiskMb:                   20480,
		EventsTtlDays:            30,
		DefaultFreeDiskSizeMb:    10240,
		MaxFreeDiskSizeMb:        51200,
		ApiTeamRpsList:           40,
		ApiTeamRpsDelete:         50,
		MaxSecrets:               10,
	}, body.Limits)
}
