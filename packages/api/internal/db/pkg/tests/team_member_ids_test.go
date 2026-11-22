package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

// GetTeamMemberIDs decides which cached auth entries a team-wide invalidation
// reaches. A member it misses keeps serving that team's old limits and blocked
// state until the entry expires, so the failure is silently stale auth rather
// than an error — worth pinning that it returns this team's members and only
// this team's.
func TestGetTeamMemberIDsReturnsExactlyTheTeamsMembers(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	otherTeamID := testutils.CreateTestTeam(t, db)

	members := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, userID := range members {
		require.NoError(t, db.AuthDB.UpsertPublicUser(ctx, userID))
		require.NoError(t, db.AuthDB.CreateTeamMembership(ctx, authqueries.CreateTeamMembershipParams{
			UserID: userID, TeamID: teamID, IsDefault: false,
		}))
	}

	stranger := uuid.New()
	require.NoError(t, db.AuthDB.UpsertPublicUser(ctx, stranger))
	require.NoError(t, db.AuthDB.CreateTeamMembership(ctx, authqueries.CreateTeamMembershipParams{
		UserID: stranger, TeamID: otherTeamID, IsDefault: false,
	}))

	got, err := db.AuthDB.GetTeamMemberIDs(ctx, teamID)
	require.NoError(t, err)
	require.ElementsMatch(t, members, got)

	empty, err := db.AuthDB.GetTeamMemberIDs(ctx, uuid.New())
	require.NoError(t, err)
	require.Empty(t, empty, "an unknown team has no members rather than an error")
}
