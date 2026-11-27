package team

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

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
			team:    &types.Team{Team: &teamdata.Team{IsBlocked: false}},
			wantErr: false,
		},
		{
			name:       "blocked without reason",
			team:       &types.Team{Team: &teamdata.Team{IsBlocked: true}},
			wantErr:    true,
			wantMsgHas: "team is blocked",
		},
		{
			name:       "blocked with reason",
			team:       &types.Team{Team: &teamdata.Team{IsBlocked: true, BlockedReason: &reason}},
			wantErr:    true,
			wantMsgHas: reason,
		},
		{
			name:       "blocked with empty reason pointer",
			team:       &types.Team{Team: &teamdata.Team{IsBlocked: true, BlockedReason: &empty}},
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
