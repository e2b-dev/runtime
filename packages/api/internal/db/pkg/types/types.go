package types

import (
	"database/sql/driver"
	"encoding/json"

	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/auth/pkg/teamdata"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxconfig"
)

func jsonbValue(v any) (driver.Value, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return string(buf), nil
}

//nolint:recvcheck // JSONBStringMap needs pointer receiver for unmarshal allocation and value receivers for marshal/driver.Valuer.
type JSONBStringMap map[string]string

// MarshalJSON ensures a nil map serializes as "{}" instead of "null",
// preventing SQL NULL when pgx encodes the value for jsonb columns.
func (m JSONBStringMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}

	return json.Marshal(map[string]string(m))
}

func (m JSONBStringMap) Value() (driver.Value, error) {
	return jsonbValue(m)
}

// UnmarshalJSON ensures JSON null deserializes as an empty map instead of nil.
func (m *JSONBStringMap) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*m = JSONBStringMap{}

		return nil
	}

	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*m = JSONBStringMap(raw)

	return nil
}

type (
	Team            = teamdata.Team
	ProjectSettings = teamdata.ProjectSettings
	SandboxSettings = teamdata.SandboxSettings
)

func TeamFromRow(row authqueries.GetTeamByTeamIDRow) Team {
	return Team{
		ID:                      row.ID,
		CreatedAt:               row.CreatedAt,
		IsBlocked:               row.IsBlocked,
		Name:                    row.Name,
		Email:                   row.Email,
		IsBanned:                row.IsBanned,
		BlockedReason:           row.BlockedReason,
		ClusterID:               row.ClusterID,
		SandboxSchedulingLabels: row.SandboxSchedulingLabels,
		SsoOrganizationID:       row.SsoOrganizationID,
		SsoAutoJoin:             row.SsoAutoJoin,
		DeletedAt:               row.DeletedAt,
		Slug:                    row.Slug,
	}
}

func ProjectSettingsFromRow(row authqueries.GetTeamByTeamIDRow) ProjectSettings {
	return ProjectSettings{
		SandboxSettings: SandboxSettings{
			DefaultAllowPublicTraffic: row.SandboxDefaultAllowPublicTraffic,
		},
	}
}

type BuildReason struct {
	// Message with the status reason, currently reporting only for error status
	Message string `json:"message"`

	// Step that failed
	Step *string `json:"step,omitempty"`
}

func (r BuildReason) Value() (driver.Value, error) {
	return jsonbValue(r)
}

const PausedSandboxConfigVersion = "v1"

type (
	SandboxNetworkTransform     = sandboxconfig.SandboxNetworkTransform
	SandboxNetworkRule          = sandboxconfig.SandboxNetworkRule
	SandboxNetworkEgressConfig  = sandboxconfig.SandboxNetworkEgressConfig
	SandboxEgressProxyTLSConfig = sandboxconfig.SandboxEgressProxyTLSConfig
	SandboxNetworkIngressConfig = sandboxconfig.SandboxNetworkIngressConfig
	SandboxNetworkConfig        = sandboxconfig.SandboxNetworkConfig
	SandboxVolumeMountConfig    = sandboxconfig.SandboxVolumeMountConfig
	SandboxAutoResumePolicy     = sandboxconfig.SandboxAutoResumePolicy
	SandboxAutoResumeConfig     = sandboxconfig.SandboxAutoResumeConfig
	SandboxIam                  = sandboxconfig.SandboxIam
	SandboxIamToken             = sandboxconfig.SandboxIamToken
)

const (
	SandboxAutoResumeAny = sandboxconfig.SandboxAutoResumeAny
	SandboxAutoResumeOff = sandboxconfig.SandboxAutoResumeOff
)

const AllowPublicAccessDefault = true

type PausedSandboxConfig struct {
	Version      string                      `json:"version"`
	Network      *SandboxNetworkConfig       `json:"network,omitempty"`
	AutoResume   *SandboxAutoResumeConfig    `json:"autoResume,omitempty"`
	VolumeMounts []*SandboxVolumeMountConfig `json:"volumeMounts,omitempty"`

	// FilesystemOnly marks a snapshot that persists only the rootfs (no memory
	// snapshot); resuming it cold-boots from disk. The orchestrator decides
	// reboot-vs-resume from the snapshot's own metadata.json; this flag lets the
	// API gate implicit-resume paths (auto-resume, /connect) without a round-trip.
	// Pre-existing rows omit the key and decode to false (a memory snapshot).
	FilesystemOnly bool `json:"filesystemOnly,omitempty"`

	// AutoPauseFilesystemOnly is the sandbox's policy for the *next* auto-pause:
	// when true, an auto-pause on timeout takes a filesystem-only snapshot rather
	// than a full memory one. It is restored on resume so the policy survives a
	// pause/resume cycle. Distinct from FilesystemOnly, which records the kind of
	// *this* snapshot. Pre-existing rows omit the key and decode to false.
	AutoPauseFilesystemOnly bool `json:"autoPauseFilesystemOnly,omitempty"`

	// Iam preserves the sandbox workload identity configuration across
	// pause/resume. A resumed sandbox gets a freshly generated execution ID, so
	// any workload identity is rederived from the current execution rather than a
	// stored subject. Pre-existing rows omit the key and decode to nil.
	Iam *SandboxIam `json:"iam,omitempty"`

	// PausedTTL is how long, in seconds, the sandbox may stay paused after a
	// run ends before it expires. snapshots.expires_at holds the resulting
	// time. Nil means it never expires.
	PausedTTL *int64 `json:"pausedTtl,omitempty"`
}

func (c PausedSandboxConfig) Value() (driver.Value, error) {
	return jsonbValue(c)
}

// BuildStatus represents the raw status value written to the env_builds table.
// Use BuildStatusGroup for read-side comparisons.
type BuildStatus string

const (
	BuildStatusPending      BuildStatus = "pending"
	BuildStatusWaiting      BuildStatus = "waiting"
	BuildStatusBuilding     BuildStatus = "building"
	BuildStatusSnapshotting BuildStatus = "snapshotting"
	BuildStatusUploaded     BuildStatus = "uploaded"
	BuildStatusSuccess      BuildStatus = "success"
	BuildStatusFailed       BuildStatus = "failed"
)

// BuildStatusGroup represents the normalized status from the status_group
// computed column. Use this type for all read-side comparisons.
type BuildStatusGroup string

const (
	BuildStatusGroupPending    BuildStatusGroup = "pending"
	BuildStatusGroupInProgress BuildStatusGroup = "in_progress"
	BuildStatusGroupReady      BuildStatusGroup = "ready"
	BuildStatusGroupFailed     BuildStatusGroup = "failed"
)

func (g BuildStatusGroup) IsTerminal() bool {
	return g == BuildStatusGroupReady || g == BuildStatusGroupFailed
}
