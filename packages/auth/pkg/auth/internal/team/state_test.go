package team

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

func TestCheckTeamBanned(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		team    dbtypes.Team
		wantErr bool
	}{
		{
			name:    "not banned",
			team:    dbtypes.Team{IsBanned: false},
			wantErr: false,
		},
		{
			name:    "banned",
			team:    dbtypes.Team{IsBanned: true},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := CheckTeamBanned(tc.team)
			if !tc.wantErr {
				assert.NoError(t, err)

				return
			}

			var forbidden *ForbiddenError
			assert.ErrorAs(t, err, &forbidden)
		})
	}
}

func TestCheckTeamDeletedRefusesOnlyADeletedTeam(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		team    dbtypes.Team
		allowed bool
	}{
		"live team":    {team: dbtypes.Team{}, allowed: true},
		"deleted team": {team: dbtypes.Team{IsBlocked: true, DeletedAt: new(time.Now())}, allowed: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := CheckTeamDeleted(tc.team)
			if tc.allowed {
				require.NoError(t, err)

				return
			}

			var forbidden *ForbiddenError
			require.ErrorAs(t, err, &forbidden)
		})
	}
}

func TestCheckTeamBlocked(t *testing.T) {
	t.Parallel()

	reason := "payment failed"
	empty := ""

	cases := []struct {
		name       string
		team       *types.Team
		wantErr    bool
		wantMsgHas string
	}{
		{
			name:    "nil team pointer",
			team:    nil,
			wantErr: false,
		},
		{
			name:    "nil inner team row",
			team:    &types.Team{},
			wantErr: false,
		},
		{
			name:    "not blocked",
			team:    types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: false}),
			wantErr: false,
		},
		{
			name:       "blocked without reason",
			team:       types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true}),
			wantErr:    true,
			wantMsgHas: "team is blocked",
		},
		{
			name:       "blocked with reason",
			team:       types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true, BlockedReason: &reason}),
			wantErr:    true,
			wantMsgHas: reason,
		},
		{
			name:       "blocked with empty reason pointer",
			team:       types.NewTeam(authqueries.GetTeamByTeamIDRow{IsBlocked: true, BlockedReason: &empty}),
			wantErr:    true,
			wantMsgHas: "team is blocked",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := CheckTeamBlocked(tc.team)
			if !tc.wantErr {
				assert.NoError(t, err)

				return
			}

			var blocked *BlockedError
			require.ErrorAs(t, err, &blocked)
			assert.Contains(t, err.Error(), tc.wantMsgHas)
		})
	}
}
