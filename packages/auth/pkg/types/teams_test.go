package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
)

func TestNewTeamPreservesAPIListRate(t *testing.T) {
	t.Parallel()

	team := NewTeam(&authqueries.Team{}, &authqueries.TeamLimit{ApiTeamRpsList: 1 << 32})
	assert.Equal(t, int64(1<<32), team.Limits.APITeamRPSList)
}
