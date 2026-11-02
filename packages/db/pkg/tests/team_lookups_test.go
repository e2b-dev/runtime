package tests

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

// A later migration drops teams.tier while pods of this release still serve,
// so the team lookups must not select it.
func TestTeamLookupsDoNotReadTheTier(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	teamID := testutils.CreateTestTeam(t, db)
	userID := uuid.New()
	require.NoError(t, db.AuthDB.UpsertPublicUser(ctx, userID))
	require.NoError(t, db.AuthDB.CreateTeamMembership(ctx, authqueries.CreateTeamMembershipParams{
		UserID: userID, TeamID: teamID,
	}))
	const apiKeyHash = "hash-without-tier"
	_, err := db.AuthDB.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
		TeamID: teamID, ApiKeyHash: apiKeyHash, ApiKeyPrefix: "e2b_", ApiKeyLength: 40,
		ApiKeyMaskPrefix: "ab", ApiKeyMaskSuffix: "yz", Name: "without tier",
	})
	require.NoError(t, err)

	require.NoError(t, db.AuthDB.TestsRawSQL(ctx, `ALTER TABLE public.teams DROP COLUMN tier`))

	byKey, err := db.AuthDB.GetTeamByAPIKey(ctx, apiKeyHash)
	require.NoError(t, err)
	assert.Equal(t, teamID, byKey.ID)

	byMember, err := db.AuthDB.GetTeamByTeamAndUser(ctx, authqueries.GetTeamByTeamAndUserParams{UserID: userID, ID: teamID})
	require.NoError(t, err)
	assert.Equal(t, teamID, byMember.ID)

	byID, err := db.AuthDB.GetTeamByTeamID(ctx, teamID)
	require.NoError(t, err)
	assert.Equal(t, teamID, byID.ID)

	withLimits, err := db.AuthDB.GetUserTeamsWithLimits(ctx, userID)
	require.NoError(t, err)
	require.Len(t, withLimits, 1)
	assert.Equal(t, teamID, withLimits[0].ID)

	memberships, err := db.AuthDB.GetTeamsWithUsersTeams(ctx, userID)
	require.NoError(t, err)
	require.Len(t, memberships, 1)
	assert.Equal(t, teamID, memberships[0].ID)
}

// A column TeamFromRow leaves behind reaches every authenticated team as zero,
// and one copied from the wrong column carries another's value; setting one
// column per pass shows both.
func TestTeamFromRowCopiesEveryTeamColumn(t *testing.T) {
	t.Parallel()

	columns := reflect.TypeFor[authqueries.GetTeamByTeamIDRow]()
	for field := range reflect.TypeFor[dbtypes.Team]().Fields() {
		_, ok := columns.FieldByName(field.Name)
		assert.True(t, ok, "the lookups select no %s", field.Name)
	}

	for column := range columns.Fields() {
		var row authqueries.GetTeamByTeamIDRow
		testutils.SetNonZero(t, reflect.ValueOf(&row).Elem().FieldByIndex(column.Index))

		team := reflect.ValueOf(dbtypes.TeamFromRow(row))
		for field := range team.Type().Fields() {
			want := reflect.Zero(field.Type)
			if field.Name == column.Name {
				want = reflect.ValueOf(row).FieldByIndex(column.Index)
			}
			assert.Equal(t, want.Interface(), team.FieldByIndex(field.Index).Interface(), "%s after setting %s", field.Name, column.Name)
		}
	}
}
