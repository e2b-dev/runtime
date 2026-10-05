package handlers

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

func TestFindTeamAcceptsPublicProjectID(t *testing.T) {
	t.Parallel()

	defaultTeamID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	teamID := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	teams := []*types.TeamWithDefault{
		{Team: &types.Team{Team: &authqueries.Team{ID: defaultTeamID}}, IsDefault: true},
		{Team: &types.Team{Team: &authqueries.Team{ID: teamID}}},
	}

	for _, tc := range []struct {
		name    string
		teamID  *string
		want    uuid.UUID
		wantErr string
	}{
		{name: "default team", want: defaultTeamID},
		{name: "team UUID", teamID: new(teamID.String()), want: teamID},
		{name: "public project ID", teamID: new(id.ProjectID(teamID).String()), want: teamID},
		{name: "unknown public project ID", teamID: new(id.ProjectID(uuid.MustParse("019fa519-bf79-724d-8811-a2bfda9755fa")).String()), wantErr: "not found"},
		{name: "workspace ID", teamID: new(id.WorkspaceID(teamID).String()), wantErr: "invalid team ID"},
		{name: "malformed", teamID: new("team"), wantErr: "invalid team ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			team, err := findTeam(teams, tc.teamID)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, team.ID)
		})
	}
}

func TestTeamIDMatches(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")

	for _, tc := range []struct {
		name      string
		candidate string
		want      bool
	}{
		{name: "team UUID", candidate: teamID.String(), want: true},
		{name: "public project ID", candidate: id.ProjectID(teamID).String(), want: true},
		{name: "another project", candidate: id.ProjectID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")).String()},
		{name: "workspace ID", candidate: id.WorkspaceID(teamID).String()},
		{name: "empty", candidate: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, teamIDMatches(teamID, tc.candidate))
		})
	}
}
