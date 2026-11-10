// Typed object IDs are UUIDs rendered for public use with a resource prefix:
//
//	prj_7mfb0gpp6c8dsaasre0asc7n3s
//	wrk_2n1t201rmv87aae5j4csam8000
//	sec_01kyjhkfvse96rg4d2qzd9enft
//
// UUID generation and storage stay with each resource. This file only maps
// those UUIDs to and from their public TypeID representation.

package id

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/sumup/typeid"
)

const (
	projectPrefix   = "prj"
	workspacePrefix = "wrk"
	secretPrefix    = "sec"
	prefixSeparator = "_"

	projectIDPrefix   = projectPrefix + prefixSeparator
	workspaceIDPrefix = workspacePrefix + prefixSeparator

	// SecretIDPrefix is reserved for public secret identifiers.
	SecretIDPrefix = secretPrefix + prefixSeparator

	typeIDSuffixLen = 26

	// ObjectIDLen is the fixed width of every public object ID.
	ObjectIDLen = len(projectIDPrefix) + typeIDSuffixLen
)

type (
	projectTypeIDPrefix   struct{}
	workspaceTypeIDPrefix struct{}
	secretTypeIDPrefix    struct{}
)

func (projectTypeIDPrefix) Prefix() string   { return projectPrefix }
func (workspaceTypeIDPrefix) Prefix() string { return workspacePrefix }
func (secretTypeIDPrefix) Prefix() string    { return secretPrefix }

type (
	projectTypeID   = typeid.Random[projectTypeIDPrefix]   // uuidv4
	workspaceTypeID = typeid.Random[workspaceTypeIDPrefix] // uuidv4
	secretTypeID    = typeid.Sortable[secretTypeIDPrefix]  // uuidv7
)

// ProjectID is a project UUID with a public prj_ representation.
type ProjectID uuid.UUID

// WorkspaceID is a workspace UUID with a public wrk_ representation.
type WorkspaceID uuid.UUID

// SecretID is a secret UUID with a public sec_ representation.
type SecretID uuid.UUID

// String returns the lowercase public project ID.
func (id ProjectID) String() string {
	u := uuid.UUID(id)
	publicID := typeid.Must(typeid.FromUUIDBytes[projectTypeID](u[:]))

	// SumUp uses uppercase suffixes to distinguish UUIDv4 Random IDs. E2B's
	// public ID contract is lowercase for every resource.
	return strings.ToLower(publicID.String())
}

// String returns the lowercase public workspace ID.
func (id WorkspaceID) String() string {
	u := uuid.UUID(id)
	publicID := typeid.Must(typeid.FromUUIDBytes[workspaceTypeID](u[:]))

	return strings.ToLower(publicID.String())
}

// String returns the lowercase public secret ID.
func (id SecretID) String() string {
	u := uuid.UUID(id)
	publicID := typeid.Must(typeid.FromUUIDBytes[secretTypeID](u[:]))

	return publicID.String()
}

// ParseProjectID parses a lowercase public project ID.
func ParseProjectID(s string) (ProjectID, error) {
	internal, err := randomTypeIDInput(s, projectIDPrefix)
	if err != nil {
		return ProjectID{}, fmt.Errorf("parse project ID: %w", err)
	}

	publicID, err := typeid.FromString[projectTypeID](internal)
	if err != nil {
		return ProjectID{}, fmt.Errorf("parse project ID: %w", err)
	}

	return ProjectID(uuid.UUID(publicID.UUID())), nil
}

// ParseWorkspaceID parses a lowercase public workspace ID.
func ParseWorkspaceID(s string) (WorkspaceID, error) {
	internal, err := randomTypeIDInput(s, workspaceIDPrefix)
	if err != nil {
		return WorkspaceID{}, fmt.Errorf("parse workspace ID: %w", err)
	}

	publicID, err := typeid.FromString[workspaceTypeID](internal)
	if err != nil {
		return WorkspaceID{}, fmt.Errorf("parse workspace ID: %w", err)
	}

	return WorkspaceID(uuid.UUID(publicID.UUID())), nil
}

// ParseSecretID parses a lowercase public secret ID.
func ParseSecretID(s string) (SecretID, error) {
	publicID, err := typeid.FromString[secretTypeID](s)
	if err != nil {
		return SecretID{}, fmt.Errorf("parse secret ID: %w", err)
	}

	return SecretID(uuid.UUID(publicID.UUID())), nil
}

// ConvertTeamIDToProjectID gives a team UUID its public project identity.
func ConvertTeamIDToProjectID(teamID uuid.UUID) ProjectID {
	return ProjectID(teamID)
}

// ParseTeamID parses a team given either as its UUID or as its public project
// ID, so callers can accept both while teams are still addressed by UUID.
func ParseTeamID(s string) (uuid.UUID, error) {
	if strings.HasPrefix(s, projectIDPrefix) {
		projectID, err := ParseProjectID(s)
		if err != nil {
			return uuid.Nil, err
		}

		return uuid.UUID(projectID), nil
	}

	teamID, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse team ID: %w", err)
	}

	return teamID, nil
}

func randomTypeIDInput(s, prefix string) (string, error) {
	if len(s) != ObjectIDLen {
		return "", fmt.Errorf("%w: got %d characters, want %d", typeid.ErrParse, len(s), ObjectIDLen)
	}
	if !strings.HasPrefix(s, prefix) {
		return "", fmt.Errorf("%w: expected prefix %q", typeid.ErrParse, prefix)
	}
	if s != strings.ToLower(s) {
		return "", fmt.Errorf("%w: ID must be lowercase", typeid.ErrParse)
	}

	return prefix + strings.ToUpper(s[len(prefix):]), nil
}
