//go:build linux

package sandbox

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
)

func TestSetStopReasonFirstCallWins(t *testing.T) {
	t.Parallel()

	m := &Metadata{}
	m.SetStopReason(StopReasonPaused)
	m.SetStopReason(StopReasonKilled)

	// A Delete landing on an already-pausing sandbox must not relabel it.
	assert.Equal(t, StopReasonPaused, m.GetStopReason())
}

func TestGetStopReasonDefaultsToCrashed(t *testing.T) {
	t.Parallel()

	m := &Metadata{}

	assert.Equal(t, StopReasonCrashed, m.GetStopReason())
}

func TestSetStoppedAtFirstCallWins(t *testing.T) {
	t.Parallel()

	suspended := time.Now()

	m := &Metadata{}
	m.SetStartedAt(suspended.Add(-time.Hour))
	m.SetStoppedAt(suspended)
	// The teardown after a pause lands once the snapshot and upload are done.
	m.SetStoppedAt(suspended.Add(30 * time.Second))

	duration, ok := m.ExecutionDuration()
	require.True(t, ok)
	assert.Equal(t, time.Hour, duration)
}

func TestExecutionDuration(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name      string
		startedAt time.Time
		stoppedAt time.Time
		want      time.Duration
		wantOK    bool
	}{
		{
			name:      "started and stopped",
			startedAt: now.Add(-time.Minute),
			stoppedAt: now,
			want:      time.Minute,
			wantOK:    true,
		},
		{
			// A failed envd init records no start, and a zero time would put a
			// two-millennia outlier in the histogram.
			name:      "never started",
			stoppedAt: now,
		},
		{
			name:      "still running",
			startedAt: now.Add(-time.Minute),
		},
		{
			name:      "stopped before it started",
			startedAt: now,
			stoppedAt: now.Add(-time.Minute),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := &Metadata{}
			if !tt.startedAt.IsZero() {
				m.SetStartedAt(tt.startedAt)
			}
			if !tt.stoppedAt.IsZero() {
				m.SetStoppedAt(tt.stoppedAt)
			}

			duration, ok := m.ExecutionDuration()
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, duration)
		})
	}
}

// A memory handler that exits with an error is what crash reporting needs to
// see; one that has not exited, exited cleanly, or never existed reads nil.
func TestMemoryHandlerErr(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&Sandbox{}).MemoryHandlerErr(), "no resources")
	require.NoError(t, (&Sandbox{Resources: &Resources{}}).MemoryHandlerErr(), "no memory backend")

	running := uffd.NewNoopMemory(4096, 4096)
	require.NoError(t, (&Sandbox{Resources: &Resources{memory: running}}).MemoryHandlerErr(), "not exited")

	stopped := uffd.NewNoopMemory(4096, 4096)
	require.NoError(t, stopped.Stop())
	require.NoError(t, (&Sandbox{Resources: &Resources{memory: stopped}}).MemoryHandlerErr(), "exited cleanly")

	failed := uffd.NewNoopMemory(4096, 4096)
	handlerErr := errors.New("uffdio copy: cannot allocate memory")
	require.NoError(t, failed.Exit().SetError(handlerErr))
	assert.ErrorIs(t, (&Sandbox{Resources: &Resources{memory: failed}}).MemoryHandlerErr(), handlerErr)
}
