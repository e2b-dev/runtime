package handlers

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestALimitsDeliveryRecordsWhenItsRevisionWasDecided(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)
	body := strings.Replace(validLimitsBody, `"revision": 2,`, `"revision": 2, "decided_at": "2026-09-29T08:00:00.123456789Z",`, 1)

	require.Equal(t, http.StatusNoContent, callUpsertProjectLimits(t, store, teamID, body).Code)
	require.Equal(t, time.Date(2026, 9, 29, 8, 0, 0, 123456000, time.UTC), *readLimitsDecidedAt(t, db, teamID),
		"stored to the microsecond Postgres keeps")
}

func TestALimitsDeliveryWithoutDecidedAtStillApplies(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	require.Equal(t, http.StatusNoContent, callUpsertProjectLimits(t, store, teamID, validLimitsBody).Code)
	require.Nil(t, readLimitsDecidedAt(t, db, teamID))
}

func readLimitsDecidedAt(t *testing.T, db *testutils.Database, teamID uuid.UUID) *time.Time {
	t.Helper()

	var decidedAt *time.Time
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT decided_at FROM projection.project_limits WHERE project_id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next(), "no project_limits ledger row")

			return rows.Scan(&decidedAt)
		}, teamID))
	if decidedAt != nil {
		utc := decidedAt.UTC()
		decidedAt = &utc
	}

	return decidedAt
}
