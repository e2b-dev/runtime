//go:build linux

package sandbox

import (
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// EndedLifetime is how long an ended sandbox stays listed. The node cannot
// tell which API replica listed it, or whether the response arrived, so it
// is listed on every sync in that time; the API's kill is idempotent.
const EndedLifetime = 2 * time.Minute

// EndedSandbox is what the node still reports about a sandbox that ended
// without being asked to, after its resources were released.
type EndedSandbox struct {
	SandboxID       string
	TeamID          string
	ExecutionID     string
	APIStoredConfig *orchestrator.SandboxConfig
	Vcpu            int64
	RamMB           int64
	StartedAt       time.Time
	EndAt           time.Time
	GuestEvent      fc.GuestEvent

	expiresAt time.Time
}

// AddEnded records that sbx ended without being asked to, so the API learns
// of it at its next sync. A later end of the same sandbox ID replaces it.
func (m *Map) AddEnded(sbx *Sandbox, event fc.GuestEvent, now time.Time) {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()

	m.ended[sbx.Runtime.SandboxID] = EndedSandbox{
		SandboxID:       sbx.Runtime.SandboxID,
		TeamID:          sbx.Runtime.TeamID,
		ExecutionID:     sbx.Runtime.ExecutionID,
		APIStoredConfig: sbx.APIStoredConfig,
		Vcpu:            sbx.Config.Vcpu,
		RamMB:           sbx.Config.RamMB,
		StartedAt:       sbx.GetStartedAt(),
		EndAt:           sbx.GetEndAt(),
		GuestEvent:      event,
		expiresAt:       now.Add(EndedLifetime),
	}
}

// EndedItems returns the ended sandboxes still within their lifetime and
// forgets the rest.
func (m *Map) EndedItems(now time.Time) []EndedSandbox {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()

	items := make([]EndedSandbox, 0, len(m.ended))
	for id, e := range m.ended {
		if !now.Before(e.expiresAt) {
			delete(m.ended, id)

			continue
		}
		items = append(items, e)
	}

	return items
}

// DropEnded forgets an ended sandbox, and reports whether there was one.
func (m *Map) DropEnded(sandboxID string) bool {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()

	_, ok := m.ended[sandboxID]
	delete(m.ended, sandboxID)

	return ok
}
