package handlers

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const sentDecidedAtJSON = `"2026-09-29T08:00:00.123456789Z"`

var sentDecidedAt = time.Date(2026, 9, 29, 8, 0, 0, 123456000, time.UTC)

func TestManagementDeliveriesRecordWhenTheirRevisionWasDecided(t *testing.T) {
	t.Parallel()

	for name, deliver := range map[string]struct {
		ledger string
		call   func(t *testing.T, store *APIStore, teamID uuid.UUID, body string) int
		body   string
	}{
		"limits": {
			ledger: "project_limits",
			call: func(t *testing.T, store *APIStore, teamID uuid.UUID, body string) int {
				t.Helper()

				return callUpsertProjectLimits(t, store, teamID, body).Code
			},
			body: strings.Replace(validLimitsBody, `"revision": 2,`, `"revision": 2, "decided_at": `+sentDecidedAtJSON+`,`, 1),
		},
		"block": {
			ledger: "project_blocks",
			call: func(t *testing.T, store *APIStore, teamID uuid.UUID, body string) int {
				t.Helper()

				return callApplyProjectBlock(t, store, teamID, body).Code
			},
			body: `{"revision": 2, "blocked": true, "decided_at": ` + sentDecidedAtJSON + `}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			store, _ := newLimitsStore(db)

			require.Equal(t, http.StatusNoContent, deliver.call(t, store, teamID, deliver.body))
			require.Equal(t, sentDecidedAt, *readLedgerDecidedAt(t, db, deliver.ledger, teamID),
				"stored to the microsecond Postgres keeps")
		})
	}
}

func TestManagementDeliveriesWithoutDecidedAtStillApply(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newLimitsStore(db)

	require.Equal(t, http.StatusNoContent, callUpsertProjectLimits(t, store, teamID, validLimitsBody).Code)
	require.Equal(t, http.StatusNoContent, callApplyProjectBlock(t, store, teamID, `{"revision": 1, "blocked": true}`).Code)

	require.Nil(t, readLedgerDecidedAt(t, db, "project_limits", teamID))
	require.Nil(t, readLedgerDecidedAt(t, db, "project_blocks", teamID))
}

func TestManagementContractAdmitsDecidedAt(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	for _, schema := range []string{"ManagementProjectLimits", "ManagementProjectBlockRequest"} {
		property := swagger.Components.Schemas[schema].Value.Properties["decided_at"]
		require.NotNil(t, property, "%s has no decided_at", schema)
		require.Equal(t, "date-time", property.Value.Format, schema)
		require.NotContains(t, swagger.Components.Schemas[schema].Value.Required, "decided_at",
			"%s must keep admitting callers that do not send it", schema)
	}
}

func readLedgerDecidedAt(t *testing.T, db *testutils.Database, ledger string, teamID uuid.UUID) *time.Time {
	t.Helper()

	var decidedAt *time.Time
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT decided_at FROM projection."+ledger+" WHERE project_id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next(), "no %s ledger row", ledger)

			return rows.Scan(&decidedAt)
		}, teamID))
	if decidedAt != nil {
		utc := decidedAt.UTC()
		decidedAt = &utc
	}

	return decidedAt
}
