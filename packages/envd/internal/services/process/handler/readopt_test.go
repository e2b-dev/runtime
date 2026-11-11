//go:build unix

package handler

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReapByPidfd_EmitsEndWhenPidfdOpenFails guards against orphaning a
// re-adopted process: if pidfd_open fails, the reaper must still emit a terminal
// event (via the blocking-wait4 fallback) so the process is finalized rather
// than left in the live map with clients blocked on an EndEvent that never fires.
func TestReapByPidfd_EmitsEndWhenPidfdOpenFails(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	// A PID above pid_max can never exist, so pidfd_open fails deterministically.
	h := Readopt(ReadoptArgs{Pid: 0x7FFFFFFF}, &logger)

	endCh, cancel := h.EndEvent.Fork()
	defer cancel()

	h.BeginReaping()

	select {
	case ev, ok := <-endCh:
		require.True(t, ok, "EndEvent channel closed without a terminal event")
		assert.NotNil(t, ev.End, "reaper must emit a terminal event on pidfd_open failure")
	case <-time.After(5 * time.Second):
		require.Fail(t, "re-adopted process orphaned: no EndEvent after pidfd_open failure")
	}
}

// TestBeginReaping_ArmsDeadlineForChainedUpgrade guards the timeout carried
// across chained live-upgrades: a re-adopted handler with a remaining timeout
// must report it via Deadline(), because Upgrade re-carries the timeout forward
// exclusively through Deadline(). Without it, a timed process that survives a
// second handover would run unbounded.
func TestBeginReaping_ArmsDeadlineForChainedUpgrade(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	// A PID above pid_max can never exist, so the reaper's pidfd_open fails and
	// its wait4 fallback returns immediately — no real child needed.
	h := Readopt(ReadoptArgs{Pid: 0x7FFFFFFF, Timeout: time.Hour}, &logger)

	_, ok := h.Deadline()
	require.False(t, ok, "deadline should be armed by BeginReaping, not Readopt")

	h.BeginReaping()

	// The deadline is armed asynchronously by the reaper goroutine once the
	// workload thaws; with no Thawed channel it arms immediately, so wait for it.
	require.Eventually(t, func() bool {
		_, ok := h.Deadline()

		return ok
	}, time.Second, 5*time.Millisecond,
		"a re-adopted handler with a timeout must report a deadline once reaping begins")
	d, _ := h.Deadline()
	assert.WithinDuration(t, time.Now().Add(time.Hour), d, time.Minute)
}
