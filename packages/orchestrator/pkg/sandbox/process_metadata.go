//go:build linux

package sandbox

import (
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type ProcessMetadata struct {
	SandboxID      string
	LifecycleID    string
	StartedAt      time.Time
	MaxLengthHours int64
	Temporary      bool
	Build          bool
	Tracked        bool
}

// Network entries supply metadata for lost lifecycles, but do not prove lifecycle ownership.
func (m *Map) ProcessMetadata() map[string]ProcessMetadata {
	sandboxes := make(map[*Sandbox]bool)
	for _, sbx := range m.network.Items() {
		sandboxes[sbx] = false
	}
	for _, sbx := range m.lifecycles.Items() {
		sandboxes[sbx] = true
	}
	for _, sbx := range m.live.Items() {
		sandboxes[sbx] = true
	}

	result := make(map[string]ProcessMetadata, len(sandboxes))
	for sbx, tracked := range sandboxes {
		if sbx.files == nil {
			continue
		}
		result[sbx.files.SandboxFirecrackerSocketPath()] = ProcessMetadata{
			SandboxID:      sbx.Runtime.SandboxID,
			LifecycleID:    sbx.LifecycleID,
			StartedAt:      sbx.GetExecutionStartedAt(),
			MaxLengthHours: sbx.Config.MaxSandboxLengthHours,
			Temporary:      sbx.skipStartupMetrics,
			Build:          sbx.Runtime.SandboxType == sandboxtypes.SandboxTypeBuild,
			Tracked:        tracked,
		}
	}

	return result
}
