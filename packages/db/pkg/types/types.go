package types

import (
	"database/sql/driver"
	"encoding/json"
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

type SandboxNetworkTransform struct {
	Headers map[string]string `json:"headers,omitempty"`
}

type SandboxNetworkRule struct {
	Transform *SandboxNetworkTransform `json:"transform,omitempty"`
}

type SandboxNetworkEgressConfig struct {
	AllowedAddresses []string                        `json:"allowedAddresses,omitempty"`
	DeniedAddresses  []string                        `json:"deniedAddresses,omitempty"`
	Rules            map[string][]SandboxNetworkRule `json:"rules,omitempty"`

	// SOCKS5 BYOP egress proxy configuration.
	EgressProxyAddress  string `json:"egressProxyAddress,omitempty"`
	EgressProxyUsername string `json:"egressProxyUsername,omitempty"`
	EgressProxyPassword string `json:"egressProxyPassword,omitempty"`
}

const AllowPublicAccessDefault = true

type SandboxNetworkIngressConfig struct {
	AllowPublicAccess *bool    `json:"allowPublicAccess,omitempty"`
	MaskRequestHost   *string  `json:"maskRequestHost,omitempty"`
	HTTPSPorts        []uint32 `json:"httpsPorts,omitempty"`
}

type SandboxNetworkConfig struct {
	Egress  *SandboxNetworkEgressConfig  `json:"egress,omitempty"`
	Ingress *SandboxNetworkIngressConfig `json:"ingress,omitempty"`
}

// HasEgressProxy reports whether a customer-supplied SOCKS5 egress proxy is
// configured. The address is the single source of truth: credentials are
// optional and are never set without it.
func (c *SandboxNetworkConfig) HasEgressProxy() bool {
	return c != nil && c.Egress != nil && c.Egress.EgressProxyAddress != ""
}

// HasEgressProxyAuth reports whether the egress proxy is configured with
// RFC 1929 credentials.
func (c *SandboxNetworkConfig) HasEgressProxyAuth() bool {
	return c.HasEgressProxy() && c.Egress.EgressProxyUsername != ""
}

type SandboxVolumeMountConfig struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type SandboxAutoResumePolicy string

const (
	SandboxAutoResumeAny SandboxAutoResumePolicy = "any"
	SandboxAutoResumeOff SandboxAutoResumePolicy = "off"
)

type SandboxAutoResumeConfig struct {
	Policy  SandboxAutoResumePolicy `json:"policy"`
	Timeout uint64                  `json:"timeout,omitempty"`
}

// SandboxIam is the sandbox workload identity configuration carried with the
// sandbox. A nil value means workload identity is disabled.
type SandboxIam struct {
	// Tokens holds the named workload-token definitions, keyed by token name.
	Tokens map[string]SandboxIamToken `json:"tokens,omitempty"`
}

// SandboxIamToken is a named workload-token definition. filePath is
// intentionally not stored: only absent/null is accepted at admission, so there
// is nothing to persist.
type SandboxIamToken struct {
	Audience  string `json:"audience"`
	TokenType string `json:"tokenType"`
}

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
	// pause/resume and fork. A resume gets a fresh execution ID, and a fork gets
	// fresh sandbox and execution IDs, so identity is rederived from the current
	// execution rather than a stored subject. Pre-existing rows omit the key and
	// decode to nil.
	Iam *SandboxIam `json:"iam,omitempty"`
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
