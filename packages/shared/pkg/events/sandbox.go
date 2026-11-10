package events

import (
	"time"

	"github.com/google/uuid"
)

const (
	StructureVersionV1 = "v1"
	StructureVersionV2 = "v2"
)

// DefaultEventsTTLDays is the fallback event retention used when an event
// doesn't carry a team-specific TTL
const DefaultEventsTTLDays int64 = 7

// MaxEventsTTLDays caps the per-team event retention; the events writer
// clamps to this value, so readers must use the same bound.
const MaxEventsTTLDays int64 = 365

// RetentionDays is how many days the events writer keeps a team's events: a
// non-positive TTL falls back to the default and a larger one is capped.
// Readers judging whether events still exist must resolve it the same way.
func RetentionDays(teamTTLDays int64) int64 {
	if teamTTLDays <= 0 {
		return DefaultEventsTTLDays
	}

	return min(teamTTLDays, MaxEventsTTLDays)
}

const (
	SandboxCreatedEvent      = "sandbox.lifecycle.created"
	SandboxKilledEvent       = "sandbox.lifecycle.killed"
	SandboxPausedEvent       = "sandbox.lifecycle.paused"
	SandboxResumedEvent      = "sandbox.lifecycle.resumed"
	SandboxUpdatedEvent      = "sandbox.lifecycle.updated"
	SandboxCheckpointedEvent = "sandbox.lifecycle.checkpointed"
)

var ValidSandboxEventTypes = []string{
	SandboxCreatedEvent,
	SandboxKilledEvent,
	SandboxPausedEvent,
	SandboxResumedEvent,
	SandboxUpdatedEvent,
	SandboxCheckpointedEvent,
}

type SandboxEvent struct {
	ID        uuid.UUID `json:"id"`
	Version   string    `json:"version"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`

	// Deprecated: for new events use event field with dot syntax
	EventCategory string `json:"event_category"`
	// Deprecated: for new events use event field with dot syntax
	EventLabel string         `json:"event_label"`
	EventData  map[string]any `json:"event_data,omitempty"`

	SandboxID          string    `json:"sandbox_id"`
	SandboxExecutionID string    `json:"sandbox_execution_id"`
	SandboxTemplateID  string    `json:"sandbox_template_id"`
	SandboxBuildID     string    `json:"sandbox_build_id"`
	SandboxTeamID      uuid.UUID `json:"sandbox_team_id"`

	// Retention of the event in days
	EventsTTLDays int64 `json:"events_ttl_days,omitempty"`
}
