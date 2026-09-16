package sandboxtypes

import (
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

func NewSandbox(
	sandboxID string,
	templateID string,
	clientID string,
	alias *string,
	executionID string,
	teamID uuid.UUID,
	buildID uuid.UUID,
	metadata map[string]string,
	maxInstanceLength time.Duration,
	startTime time.Time,
	endTime time.Time,
	vcpu int64,
	totalDiskSizeMB int64,
	ramMB int64,
	kernelVersion string,
	firecrackerVersion string,
	envdVersion string,
	nodeID string,
	clusterID uuid.UUID,
	autoPause bool,
	autoPauseFilesystemOnly bool,
	autoResume *types.SandboxAutoResumeConfig,
	envdAccessToken *string,
	allowInternetAccess *bool,
	baseTemplateID string,
	domain *string,
	network *types.SandboxNetworkConfig,
	trafficAccessToken *string,
	mounts []*types.SandboxVolumeMountConfig,
	iam *types.SandboxIam,
) Sandbox {
	return Sandbox{
		SandboxID:  sandboxID,
		TemplateID: templateID,
		ClientID:   clientID,
		Alias:      alias,
		Domain:     domain,

		ExecutionID:             executionID,
		TeamID:                  teamID,
		BuildID:                 buildID,
		Metadata:                metadata,
		MaxInstanceLength:       maxInstanceLength,
		StartTime:               startTime,
		EndTime:                 endTime,
		VCpu:                    vcpu,
		TotalDiskSizeMB:         totalDiskSizeMB,
		RamMB:                   ramMB,
		KernelVersion:           kernelVersion,
		FirecrackerVersion:      firecrackerVersion,
		EnvdVersion:             envdVersion,
		EnvdAccessToken:         envdAccessToken,
		TrafficAccessToken:      trafficAccessToken,
		AllowInternetAccess:     allowInternetAccess,
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		AutoPause:               autoPause,
		AutoPauseFilesystemOnly: autoPauseFilesystemOnly,
		AutoResume:              autoResume,
		State:                   StateRunning,
		BaseTemplateID:          baseTemplateID,
		Network:                 network,
		VolumeMounts:            mounts,
		Iam:                     iam,
	}
}

type Sandbox struct {
	SandboxID  string  `json:"sandboxID"`
	TemplateID string  `json:"templateID"`
	ClientID   string  `json:"clientID"`
	Alias      *string `json:"alias,omitempty"`
	Domain     *string `json:"domain,omitempty"`

	ExecutionID        string            `json:"executionID"`
	TeamID             uuid.UUID         `json:"teamID"`
	BuildID            uuid.UUID         `json:"buildID"`
	BaseTemplateID     string            `json:"baseTemplateID"`
	Metadata           map[string]string `json:"metadata"`
	MaxInstanceLength  time.Duration     `json:"maxInstanceLength"`
	StartTime          time.Time         `json:"startTime"`
	EndTime            time.Time         `json:"endTime"`
	VCpu               int64             `json:"vCpu"`
	TotalDiskSizeMB    int64             `json:"totalDiskSizeMB"`
	RamMB              int64             `json:"ramMB"`
	KernelVersion      string            `json:"kernelVersion"`
	FirecrackerVersion string            `json:"firecrackerVersion"`
	// FirecrackerVersionResolved records whether FirecrackerVersion is the
	// orchestrator-RESOLVED version the sandbox actually runs (frozen at
	// start, echoed on the create response) rather than the build's declared
	// version. Version-gated features branch on it: a resolved version is
	// checked exactly, an unresolved one is only an approximation of the
	// running binary. False for records predating the echoed field and for
	// sandboxes started by orchestrators that predate it.
	FirecrackerVersionResolved bool      `json:"firecrackerVersionResolved,omitempty"`
	EnvdVersion                string    `json:"envdVersion"`
	EnvdAccessToken            *string   `json:"envdAccessToken,omitempty"`
	TrafficAccessToken         *string   `json:"trafficAccessToken"`
	AllowInternetAccess        *bool     `json:"allowInternetAccess,omitempty"`
	NodeID                     string    `json:"nodeID"`
	ClusterID                  uuid.UUID `json:"clusterID"`
	AutoPause                  bool      `json:"autoPause"`
	// RefusedUntil holds a node-refused, restored auto-pause out of every
	// replica's eviction sweep until it passes.
	RefusedUntil time.Time `json:"refusedUntil,omitzero"`
	// RefusedSince is when the node first refused this sandbox's pause in the
	// current refusal episode (see RefusalEpisodeStart); the evictor's retry
	// budget counts from here.
	RefusedSince time.Time `json:"refusedSince,omitzero"`
	// AutoPauseFilesystemOnly makes a timeout auto-pause take a filesystem-only
	// snapshot (no memory) instead of a full memory snapshot. Only consulted when
	// AutoPause is true; read by the evictor at pause time.
	AutoPauseFilesystemOnly bool                              `json:"autoPauseFilesystemOnly,omitempty"`
	AutoResume              *types.SandboxAutoResumeConfig    `json:"autoResume,omitempty"`
	Network                 *types.SandboxNetworkConfig       `json:"network"`
	VolumeMounts            []*types.SandboxVolumeMountConfig `json:"volumeMounts"`
	// Iam records the sandbox workload identity configuration. Persisted so it
	// survives re-sync and is carried into the paused snapshot used by resume or
	// fork.
	Iam *types.SandboxIam `json:"iam,omitempty"`

	State State `json:"state"`
}

func (s Sandbox) ToAPISandbox() *api.Sandbox {
	return &api.Sandbox{
		SandboxID:          s.SandboxID,
		TemplateID:         s.BaseTemplateID,
		ClientID:           s.ClientID,
		Alias:              s.Alias,
		EnvdVersion:        s.EnvdVersion,
		EnvdAccessToken:    s.EnvdAccessToken,
		TrafficAccessToken: s.TrafficAccessToken,
		Domain:             s.Domain,
	}
}

func (s Sandbox) LoggerMetadata() sbxlogger.SandboxMetadata {
	return sbxlogger.SandboxMetadata{
		SandboxID:  s.SandboxID,
		TemplateID: s.TemplateID,
		TeamID:     s.TeamID.String(),
	}
}

func (s Sandbox) IsExpired(now time.Time) bool {
	return now.After(s.EndTime)
}

// NodeSandbox is a sandbox as reported by an orchestrator node, reduced to the
// fields needed to reconcile the node against the store and to kill a sandbox
// the store does not know about. Redis is the source of truth for everything
// else, so a NodeSandbox is never written back to the store.
type NodeSandbox struct {
	SandboxID   string
	ExecutionID string
	TeamID      uuid.UUID
	NodeID      string
	ClusterID   uuid.UUID
	// StartTime is used to spare freshly started sandboxes from the orphan
	// check, since the store write may not have landed yet.
	StartTime time.Time
	// VCpu and RamMB correct the node's optimistic resource accounting after a
	// kill.
	VCpu  int64
	RamMB int64
}

// ToNodeSandbox reduces a stored sandbox to the node-reported view, so the kill
// path can take the same argument for orphans and for regular removals.
func (s Sandbox) ToNodeSandbox() NodeSandbox {
	return NodeSandbox{
		SandboxID:   s.SandboxID,
		ExecutionID: s.ExecutionID,
		TeamID:      s.TeamID,
		NodeID:      s.NodeID,
		ClusterID:   s.ClusterID,
		StartTime:   s.StartTime,
		VCpu:        s.VCpu,
		RamMB:       s.RamMB,
	}
}

// RefusalEpisodeGap is how long after a retry window ended a further refusal
// still belongs to the same episode. The evictor retries within seconds of the
// window, so a refusal any later than this is a new episode: a stamp left by
// a refused user pause hours ago must not count against a fresh one.
const RefusalEpisodeGap = 30 * time.Second

// RefusalEpisodeStart is when the current refusal episode began: the stamped
// first refusal while the episode is alive, otherwise now.
func (s Sandbox) RefusalEpisodeStart(now time.Time) time.Time {
	if s.RefusedSince.IsZero() || now.After(s.RefusedUntil.Add(RefusalEpisodeGap)) {
		return now
	}

	return s.RefusedSince
}
