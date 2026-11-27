package authservice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
)

func TestCheckTeamBanned(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		team    teamdata.Team
		wantErr bool
	}{
		{
			name:    "not banned",
			team:    teamdata.Team{IsBanned: false},
			wantErr: false,
		},
		{
			name:    "banned",
			team:    teamdata.Team{IsBanned: true},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := auth.CheckTeamBanned(tc.team)
			if !tc.wantErr {
				assert.NoError(t, err)

				return
			}

			var forbidden *auth.TeamForbiddenError
			assert.ErrorAs(t, err, &forbidden)
		})
	}
}

func TestCheckTeamDeletedRefusesOnlyADeletedTeam(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		team    teamdata.Team
		allowed bool
	}{
		"live team":    {team: teamdata.Team{}, allowed: true},
		"deleted team": {team: teamdata.Team{IsBlocked: true, DeletedAt: new(time.Now())}, allowed: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := checkTeamDeleted(tc.team)
			if tc.allowed {
				require.NoError(t, err)

				return
			}

			var forbidden *auth.TeamForbiddenError
			require.ErrorAs(t, err, &forbidden)
		})
	}
}
