package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const validLimitsBody = `{
	"revision": 2,
	"concurrent_sandboxes": 40,
	"max_sandbox_length_hours": 12,
	"max_vcpu": 16,
	"max_ram_mb": 32768,
	"disk_mb": 20480,
	"concurrent_template_builds": 30,
	"events_ttl_days": 14,
	"default_free_disk_size_mb": 10240,
	"api_team_rps_list": 60,
	"api_team_rps_delete": 100,
	"max_secrets": 25,
	"max_disk_size_mb": 51200
}`

// The push is the only way limits reach this side, and the team lookups are
// what gates sandbox creation. Asserting through a lookup rather than the
// table proves the two are actually wired together.
func TestUpsertProjectLimitsIsVisibleThroughTheTeamLookup(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"legacy ceiling name":    validLimitsBody,
		"canonical ceiling name": strings.Replace(validLimitsBody, `"max_disk_size_mb": 51200`, `"max_free_disk_size_mb": 51200`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			store, auth := newManagementStore(t, db)

			recorder := callUpsertProjectLimits(t, store, teamID, body)
			require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())

			limits := readTeamLimits(t, db, teamID)
			require.Equal(t, map[string]int64{
				"max_length_hours": 12, "concurrent_sandboxes": 40, "concurrent_template_builds": 30,
				"max_vcpu": 16, "max_ram_mb": 32768, "disk_mb": 20480, "events_ttl_days": 14,
				"default_free_disk_size_mb": 10240, "max_disk_size_mb": 51200,
				"max_free_disk_size_mb": 51200,
			}, limits)

			var legacyCeiling, canonicalCeiling int64
			require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `
				SELECT max_disk_size_mb, max_free_disk_size_mb
				FROM public.project_limits WHERE team_id = $1
			`, func(rows pgx.Rows) error {
				rows.Next()

				return rows.Scan(&legacyCeiling, &canonicalCeiling)
			}, teamID))
			require.EqualValues(t, 51200, legacyCeiling)
			require.Equal(t, legacyCeiling, canonicalCeiling,
				"the compatibility writer must store both ceiling names equally")

			// Limits are cached with the team, so a push that skips this is invisible
			// until the entry expires.
			require.Equal(t, []uuid.UUID{teamID}, *auth.invalidatedTeamIDs)
		})
	}
}

// The revision decides which delivery writes, so a body without one is a caller
// this side cannot fence and must refuse. The schema requires the field; this is
// the value the schema still admits.
func TestUpsertProjectLimitsRejectsARevisionThatIsNotPositive(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newManagementStore(t, db)

	body := strings.NewReplacer(`"revision": 2`, `"revision": 0`).Replace(validLimitsBody)

	recorder := callUpsertProjectLimits(t, store, teamID, body)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, *auth.invalidatedTeamIDs)
}

// The team is the row's only foreign key, so a violation means the project is
// unknown here — not that the payload was malformed.
func TestUpsertProjectLimitsRejectsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, auth := newManagementStore(t, db)

	recorder := callUpsertProjectLimits(t, store, uuid.New(), validLimitsBody)
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Empty(t, *auth.invalidatedTeamIDs, "nothing was written, so nothing should be invalidated")
}

// The free-disk-below-ceiling rule cannot be expressed in the request schema,
// so it is enforced by the column set and has to surface as the caller's error
// rather than ours.
func TestUpsertProjectLimitsRejectsFreeDiskAboveTheCeiling(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	body := strings.NewReplacer(
		`"default_free_disk_size_mb": 10240`, `"default_free_disk_size_mb": 99999`,
	).Replace(validLimitsBody)

	recorder := callUpsertProjectLimits(t, store, teamID, body)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestUpsertProjectLimitsCarriesTheListRateToTheTeamLookup(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	body := strings.Replace(validLimitsBody, `"api_team_rps_list": 60`, `"api_team_rps_list": 33`, 1)
	recorder := callUpsertProjectLimits(t, store, teamID, body)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())

	limits, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)
	require.EqualValues(t, 33, limits.ApiTeamRpsList)
}

func TestUpsertProjectLimitsRejectsANegativeListRate(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newManagementStore(t, db)

	body := strings.Replace(validLimitsBody, `"api_team_rps_list": 60`, `"api_team_rps_list": -1`, 1)
	recorder := callUpsertProjectLimits(t, store, teamID, body)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, *auth.invalidatedTeamIDs)
}

func TestUpsertProjectLimitsCarriesTheSecretLimitToTheTeamLookup(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	body := strings.Replace(validLimitsBody, `"max_secrets": 25`, `"max_secrets": 37`, 1)
	recorder := callUpsertProjectLimits(t, store, teamID, body)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())

	limits, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)
	require.EqualValues(t, 37, limits.MaxSecrets)
}

func TestUpsertProjectLimitsRejectsAZeroSecretLimit(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newManagementStore(t, db)

	body := strings.Replace(validLimitsBody, `"max_secrets": 25`, `"max_secrets": 0`, 1)
	recorder := callUpsertProjectLimits(t, store, teamID, body)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, *auth.invalidatedTeamIDs)
}

func TestUpsertProjectLimitsCarriesDeleteRatesToTheTeamLookup(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newManagementStore(t, db)

	const largeRate = int64(1<<32 + 7)
	for _, step := range []struct {
		revision int64
		rate     int64
		want     int64
		status   int
	}{
		{1, 33, 33, http.StatusNoContent},
		{2, largeRate, largeRate, http.StatusNoContent},
		{1, 3, largeRate, http.StatusNoContent},
		{3, -1, largeRate, http.StatusBadRequest},
		{3, 0, 0, http.StatusNoContent},
	} {
		var body map[string]any
		require.NoError(t, json.Unmarshal([]byte(validLimitsBody), &body))
		body["revision"] = step.revision
		body["api_team_rps_delete"] = step.rate
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		invalidations := len(*auth.invalidatedTeamIDs)
		recorder := callUpsertProjectLimits(t, store, teamID, string(encoded))
		require.Equal(t, step.status, recorder.Code, recorder.Body.String())
		if step.status == http.StatusBadRequest {
			assert.Len(t, *auth.invalidatedTeamIDs, invalidations)
		}
		limits, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
		require.NoError(t, err)
		assert.Equal(t, step.want, limits.ApiTeamRpsDelete, "revision %d", step.revision)
	}
}

func TestUpsertProjectLimitsRejectsAMalformedBody(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"malformed JSON":             `{"revision":`,
		"conflicting ceiling names":  strings.Replace(validLimitsBody, `"max_disk_size_mb": 51200`, `"max_disk_size_mb": 51200, "max_free_disk_size_mb": 25600`, 1),
		"missing both ceiling names": strings.Replace(validLimitsBody, `"max_disk_size_mb": 51200`, `"unrelated": 1`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			store, auth := newManagementStore(t, db)

			recorder := callUpsertProjectLimits(t, store, teamID, body)
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Empty(t, *auth.invalidatedTeamIDs)
		})
	}
}

func callUpsertProjectLimits(t *testing.T, store *APIStore, teamID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/projects/"+teamID.String()+"/limits", body)
}

func readTeamLimits(t *testing.T, db *testutils.Database, teamID uuid.UUID) map[string]int64 {
	t.Helper()

	limits, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)

	return map[string]int64{
		"max_length_hours":           limits.MaxLengthHours,
		"concurrent_sandboxes":       limits.ConcurrentSandboxes,
		"concurrent_template_builds": limits.ConcurrentTemplateBuilds,
		"max_vcpu":                   limits.MaxVcpu,
		"max_ram_mb":                 limits.MaxRamMb,
		"disk_mb":                    limits.DiskMb,
		"events_ttl_days":            limits.EventsTtlDays,
		"default_free_disk_size_mb":  limits.DefaultFreeDiskSizeMb,
		"max_disk_size_mb":           limits.MaxDiskSizeMb,
		"max_free_disk_size_mb":      limits.MaxFreeDiskSizeMb,
	}
}
