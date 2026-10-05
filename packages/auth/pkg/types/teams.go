package types

import (
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
)

type Team struct {
	*authqueries.Team

	Limits *TeamLimits
}

func (t *Team) TeamID() string {
	return t.Team.ID.String()
}

func newTeamLimits(
	teamLimits *authqueries.TeamLimit,
) *TeamLimits {
	return &TeamLimits{
		SandboxConcurrency: teamLimits.ConcurrentSandboxes,
		BuildConcurrency:   teamLimits.ConcurrentTemplateBuilds,
		MaxLengthHours:     teamLimits.MaxLengthHours,
		MaxVcpu:            teamLimits.MaxVcpu,
		MaxRamMb:           teamLimits.MaxRamMb,
		DiskMb:             teamLimits.DiskMb,
		EventsTTLDays:      teamLimits.EventsTtlDays,
		APITeamRPSList:     teamLimits.ApiTeamRpsList,

		DefaultFreeDiskSizeMb: teamLimits.DefaultFreeDiskSizeMb,
		MaxFreeDiskSizeMb:     teamLimits.MaxFreeDiskSizeMb,
	}
}

func NewTeam(
	team *authqueries.Team,
	teamLimits *authqueries.TeamLimit,
) *Team {
	return &Team{
		Team:   team,
		Limits: newTeamLimits(teamLimits),
	}
}

type TeamWithDefault struct {
	*Team

	IsDefault bool
}
