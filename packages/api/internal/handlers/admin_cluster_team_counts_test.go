package handlers

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

// The counts come from the teams alone, so they are served with no cluster
// service, and a deleted team or one on the local cluster counts for none.
func TestAdminListClusterTeamCountsCountsLiveTeamsWithoutAClusterService(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := &APIStore{sqlcDB: db.SqlcClient}
	busy, quiet, emptied := uuid.New(), uuid.New(), uuid.New()
	for range 2 {
		assignTeam(t, db, busy)
	}
	assignTeam(t, db, quiet)
	for _, clusterID := range []uuid.UUID{quiet, emptied} {
		deleted := assignTeam(t, db, clusterID)
		require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET deleted_at = now(), is_blocked = true WHERE id = $1`, deleted))
	}
	testutils.CreateTestTeam(t, db)

	response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/team-counts", "")

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var counts api.ClusterTeamCountList
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &counts))
	assert.ElementsMatch(t, []api.ClusterTeamCount{
		{ClusterId: busy, TeamCount: 2},
		{ClusterId: quiet, TeamCount: 1},
	}, counts.Items)
}
