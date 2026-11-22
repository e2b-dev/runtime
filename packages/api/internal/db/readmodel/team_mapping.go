package readmodel

import (
	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func newTeamLimits(
	row *authqueries.GetTeamByTeamIDRow,
) *types.TeamLimits {
	return &types.TeamLimits{
		SandboxConcurrency: row.ConcurrentSandboxes,
		BuildConcurrency:   row.ConcurrentTemplateBuilds,
		MaxLengthHours:     row.MaxLengthHours,
		MaxVcpu:            row.MaxVcpu,
		MaxRamMb:           row.MaxRamMb,
		DiskMb:             row.DiskMb,
		EventsTTLDays:      row.EventsTtlDays,
		APITeamRPSList:     row.ApiTeamRpsList,
		APITeamRPSDelete:   row.ApiTeamRpsDelete,

		DefaultFreeDiskSizeMb: row.DefaultFreeDiskSizeMb,
		MaxFreeDiskSizeMb:     row.MaxFreeDiskSizeMb,

		MaxSecrets: row.MaxSecrets,
	}
}

// NewTeam takes the row of a single-team lookup. The other lookups return the
// same columns, so their rows convert to this type; a lookup whose column list
// drifts stops compiling at the conversion.
func NewTeam(row authqueries.GetTeamByTeamIDRow) *types.Team {
	team := dbtypes.TeamFromRow(row)

	return &types.Team{
		Team:            &team,
		Limits:          newTeamLimits(&row),
		ProjectSettings: dbtypes.ProjectSettingsFromRow(row),
	}
}
