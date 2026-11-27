package teamdata

import (
	"time"

	"github.com/google/uuid"
)

// Team holds the team columns the team lookups select. The auth cache stores
// it as JSON that pods of adjacent releases read, so renaming, retyping or
// tagging a field breaks the entries the other release wrote.
type Team struct {
	ID                      uuid.UUID
	CreatedAt               time.Time
	IsBlocked               bool
	Name                    string
	Email                   string
	IsBanned                bool
	BlockedReason           *string
	ClusterID               *uuid.UUID
	SandboxSchedulingLabels []string
	SsoOrganizationID       *uuid.UUID
	SsoAutoJoin             bool
	DeletedAt               *time.Time
	Slug                    string
}

type ProjectSettings struct {
	SandboxSettings SandboxSettings
}

type SandboxSettings struct {
	DefaultAllowPublicTraffic *bool
}
