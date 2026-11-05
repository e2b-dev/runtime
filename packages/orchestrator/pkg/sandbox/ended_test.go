//go:build linux

package sandbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func endedTestSandbox(executionID string) *Sandbox {
	sbx := &Sandbox{
		Metadata: &Metadata{
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID:   "sbx-1",
				TeamID:      "team-id",
				ExecutionID: executionID,
			},
			Config: NewConfig(Config{Vcpu: 2, RamMB: 512}),
		},
	}
	sbx.SetStartedAt(time.Unix(1000, 0))
	sbx.SetEndAt(time.Unix(2000, 0))

	return sbx
}

func TestEndedSandboxes(t *testing.T) {
	t.Parallel()

	now := time.Unix(1500, 0)

	t.Run("listed with what the node knew, until its lifetime runs out", func(t *testing.T) {
		t.Parallel()

		m := NewSandboxesMap()
		m.AddEnded(endedTestSandbox("exec-1"), fc.GuestEventCrashNoRestart, now)

		items := m.EndedItems(now.Add(EndedLifetime - time.Nanosecond))
		require.Len(t, items, 1)
		got := items[0]
		assert.Equal(t, "sbx-1", got.SandboxID)
		assert.Equal(t, "team-id", got.TeamID)
		assert.Equal(t, "exec-1", got.ExecutionID)
		assert.Equal(t, int64(2), got.Vcpu)
		assert.Equal(t, int64(512), got.RamMB)
		assert.Equal(t, time.Unix(1000, 0), got.StartedAt)
		assert.Equal(t, time.Unix(2000, 0), got.EndAt)
		assert.Equal(t, fc.GuestEventCrashNoRestart, got.GuestEvent)

		// Listing is no acknowledgement: it stays for the next sync.
		assert.Len(t, m.EndedItems(now.Add(EndedLifetime-time.Nanosecond)), 1)

		assert.Empty(t, m.EndedItems(now.Add(EndedLifetime)))
		// Forgotten, not just hidden.
		assert.Empty(t, m.EndedItems(now))
	})

	t.Run("a delete drops it", func(t *testing.T) {
		t.Parallel()

		m := NewSandboxesMap()
		m.AddEnded(endedTestSandbox("exec-1"), fc.GuestEventFailure, now)

		assert.True(t, m.DropEnded("sbx-1"))
		assert.Empty(t, m.EndedItems(now))
		assert.False(t, m.DropEnded("sbx-1"))
	})

	t.Run("a later end of the same sandbox replaces it", func(t *testing.T) {
		t.Parallel()

		m := NewSandboxesMap()
		m.AddEnded(endedTestSandbox("exec-1"), fc.GuestEventFailure, now)
		m.AddEnded(endedTestSandbox("exec-2"), fc.GuestEventUnknown, now.Add(time.Minute))

		items := m.EndedItems(now.Add(EndedLifetime))
		require.Len(t, items, 1)
		assert.Equal(t, "exec-2", items[0].ExecutionID)
		assert.Equal(t, fc.GuestEventUnknown, items[0].GuestEvent)
	})

	t.Run("not live", func(t *testing.T) {
		t.Parallel()

		m := NewSandboxesMap()
		m.AddEnded(endedTestSandbox("exec-1"), fc.GuestEventFailure, now)

		_, ok := m.Get("sbx-1")
		assert.False(t, ok)
		assert.Zero(t, m.Count())
	})
}
