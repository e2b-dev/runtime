//go:build linux

package sandbox

import (
	"testing"
	"time"
)

// TestChecks_StopBeforeStart_DoesNotStartHealthLoop is a regression test for
// the Checks start/stop race.
//
// Checks.Start is launched in its own goroutine (go sbx.Checks().Start(execCtx)),
// so the sandbox teardown path can run Checks.Stop before that goroutine is
// scheduled. execCtx is derived via context.WithoutCancel and never
// auto-cancels, so the logHealth ticker loop is only ever ended by Stop
// cancelling the context Start installs. If Start enters logHealth after Stop
// already ran, the loop — and the per-tick health-check HTTP dials it spawns —
// leak for the process lifetime.
//
// Start must observe the prior Stop and return without entering logHealth.
func TestChecks_StopBeforeStart_DoesNotStartHealthLoop(t *testing.T) {
	t.Parallel()

	c := &Checks{}

	// Teardown wins the race: Stop runs before the Start goroutine.
	c.Stop()

	returned := make(chan struct{})
	go func() {
		c.Start(t.Context())
		close(returned)
	}()

	select {
	case <-returned:
		// Start observed the prior Stop and skipped logHealth.
	case <-time.After(2 * time.Second):
		t.Fatal("Checks.Start entered the health loop after a prior Stop — goroutine leak")
	}

	c.mu.Lock()
	cancelCtx := c.cancelCtx
	c.mu.Unlock()
	if cancelCtx != nil {
		t.Fatal("Checks.Start installed a health-loop context despite a prior Stop")
	}
}
