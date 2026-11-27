package types

import (
	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
)

const (
	TeamRoleOwner  = "owner"
	TeamRoleMember = "member"
	TeamRoleViewer = "viewer"
)

type Team struct {
	*teamdata.Team

	Limits          *TeamLimits
	ProjectSettings teamdata.ProjectSettings

	// Role is the authenticated user's role on the team, set by every lookup
	// made for a user. It is empty when the team was resolved without one, by
	// API key or by team ID.
	Role string
}

// ReadOnly reports whether the authenticated user may only read the team. A
// role this build does not know is read-only, so a role added later never
// grants more here than it was given. An empty role is not a user's.
func (t *Team) ReadOnly() bool {
	return t.Role != "" && RoleReadOnly(t.Role)
}

// RoleReadOnly reports whether a stored member role may only read.
func RoleReadOnly(role string) bool {
	return role != TeamRoleOwner && role != TeamRoleMember
}

func (t *Team) TeamID() string {
	return t.Team.ID.String()
}

type TeamWithDefault struct {
	*Team

	IsDefault bool
}
