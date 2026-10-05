//go:build linux

package sandbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestProcessMetadataDoesNotTreatNetworkEntryAsTracking(t *testing.T) {
	t.Parallel()

	m := NewSandboxesMap()
	sbx := testMapSandbox(t, "lifecycle-1")
	sbx.files = (storage.CachePaths{}).NewSandboxFiles(sbx.Runtime.SandboxID)
	sbx.SetExecutionStartedAt(time.Now().Add(-2 * time.Hour))
	sbx.Config.MaxSandboxLengthHours = 1
	socket := sbx.files.SandboxFirecrackerSocketPath()
	m.AssignNetwork(t.Context(), sbx)
	meta := m.ProcessMetadata()[socket]
	require.False(t, meta.Tracked)
	require.Equal(t, sbx.GetExecutionStartedAt(), meta.StartedAt)
	require.EqualValues(t, 1, meta.MaxLengthHours)

	require.NoError(t, m.MarkRunning(t.Context(), sbx))
	require.True(t, m.ProcessMetadata()[socket].Tracked)
	require.True(t, m.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))
	require.True(t, m.ProcessMetadata()[socket].Tracked, "closing lifecycle remains owned")
	m.MarkStopped(t.Context(), sbx)
	require.False(t, m.ProcessMetadata()[socket].Tracked, "network-only orphan must be visible")
}

func TestProcessMetadataDistinguishesOverlappingLifecycles(t *testing.T) {
	t.Parallel()

	m := NewSandboxesMap()
	old, next := testMapSandbox(t, "old"), testMapSandbox(t, "next")
	for _, sbx := range []*Sandbox{old, next} {
		sbx.files = (storage.CachePaths{}).NewSandboxFiles(sbx.Runtime.SandboxID)
	}
	require.NoError(t, m.MarkRunning(t.Context(), old))
	require.True(t, m.MarkStopping(t.Context(), old.Runtime.SandboxID, old.LifecycleID))
	m.AssignNetwork(t.Context(), next)
	next.skipStartupMetrics = true
	meta := m.ProcessMetadata()
	require.Len(t, meta, 2)
	require.True(t, meta[old.files.SandboxFirecrackerSocketPath()].Tracked)
	current := meta[next.files.SandboxFirecrackerSocketPath()]
	require.False(t, current.Tracked)
	require.True(t, current.Temporary)
	require.Equal(t, next.LifecycleID, current.LifecycleID)
}

func TestProcessMetadataPreservesExecutionStartAfterReadinessReset(t *testing.T) {
	t.Parallel()

	now := time.Now()
	originalStart := now.Add(-65 * time.Minute)
	old := testMapSandbox(t, "original")
	old.SetExecutionStartedAt(originalStart)
	old.Config.MaxSandboxLengthHours = 1
	old.SetStartedAt(originalStart)

	replacement := testMapSandbox(t, "replacement")
	replacement.Runtime = old.Runtime
	var options resumeOptions
	WithExecutionStartedAt(old.GetExecutionStartedAt())(&options)
	replacement.executionStartedAt = options.executionStartedAt
	replacement.Config = old.Config
	replacement.files = (storage.CachePaths{}).NewSandboxFiles(replacement.Runtime.SandboxID)
	m := NewSandboxesMap()
	m.AssignNetwork(t.Context(), replacement)
	require.Equal(t, originalStart, m.ProcessMetadata()[replacement.files.SandboxFirecrackerSocketPath()].StartedAt, "inherited clock is available before readiness")
	replacement.SetStartedAt(now.Add(-10 * time.Minute))
	replacement.SetExecutionStartedAt(now.Add(-10 * time.Minute))
	meta := m.ProcessMetadata()[replacement.files.SandboxFirecrackerSocketPath()]

	require.Equal(t, originalStart, meta.StartedAt)
	require.True(t, now.After(meta.StartedAt.Add(time.Duration(meta.MaxLengthHours)*time.Hour)))
	require.Equal(t, now.Add(-10*time.Minute), replacement.GetStartedAt(), "readiness timing remains independent")
}

func TestProcessMetadataDoesNotGuessMissingExecutionStart(t *testing.T) {
	t.Parallel()

	sbx := testMapSandbox(t, "unknown-start")
	sbx.files = (storage.CachePaths{}).NewSandboxFiles(sbx.Runtime.SandboxID)
	sbx.SetStartedAt(time.Now())
	sbx.Config.MaxSandboxLengthHours = 1
	m := NewSandboxesMap()
	m.AssignNetwork(t.Context(), sbx)
	meta := m.ProcessMetadata()[sbx.files.SandboxFirecrackerSocketPath()]
	require.True(t, meta.StartedAt.IsZero())
}
