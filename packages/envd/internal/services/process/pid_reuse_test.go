//go:build unix

package process

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/process/handler"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
)

// TestConnect_LivePidNotServedStaleExit guards against PID reuse: a live process
// that happens to share a PID with a still-cached terminal event of an earlier
// process must have its OWN exit served, not the stale cached one.
func TestConnect_LivePidNotServedStaleExit(t *testing.T) {
	t.Parallel()

	client, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	const pid = uint32(6161)
	logger := zerolog.Nop()
	live := handler.Readopt(handler.ReadoptArgs{Pid: pid}, &logger)
	svc.processes.Store(pid, live)
	// A stale exit left by a PRIOR process that used this PID.
	svc.terminated.Store(pid, &retainedExit{
		pid: pid,
		end: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 99, Status: "stale"},
	})

	// Once Connect has subscribed to the live process, end it with code 7.
	go func() {
		for !live.EndEvent.HasSubscribers() {
			time.Sleep(5 * time.Millisecond)
		}
		live.EndEvent.Source <- rpc.ProcessEvent_End{
			End: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 7, Status: "exited"},
		}
		close(live.EndEvent.Source)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := client.Connect(ctx, connect.NewRequest(&rpc.ConnectRequest{
		Process: &rpc.ProcessSelector{Selector: &rpc.ProcessSelector_Pid{Pid: pid}},
	}))
	require.NoError(t, err)

	events := drainConnect(t, stream)
	require.NotEmpty(t, events)
	last := events[len(events)-1].GetEnd()
	require.NotNil(t, last, "last event should be End")
	assert.Equal(t, int32(7), last.GetExitCode(), "must serve the live process's exit, not the stale cached 99")
}

// TestTrackTermination_PidReuseKeepsSuccessor verifies the retention/eviction is
// identity-guarded: when a PID is reused before the previous process's exit is
// processed, the late exit must neither evict the successor from the live map nor
// cache a stale terminal event under the reused PID.
func TestTrackTermination_PidReuseKeepsSuccessor(t *testing.T) {
	t.Parallel()

	_, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	const pid = uint32(7272)
	logger := zerolog.Nop()
	a := handler.Readopt(handler.ReadoptArgs{Pid: pid}, &logger)
	b := handler.Readopt(handler.ReadoptArgs{Pid: pid}, &logger)

	svc.processes.Store(pid, a)
	svc.trackTermination(pid, a) // subscribes to A's EndEvent

	// The PID is reused by a new process before A's exit is processed.
	svc.processes.Store(pid, b)

	// A exits (late).
	for !a.EndEvent.HasSubscribers() {
		time.Sleep(5 * time.Millisecond)
	}
	a.EndEvent.Source <- rpc.ProcessEvent_End{
		End: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 1, Status: "exited"},
	}

	assert.Eventually(t, func() bool {
		cur, ok := svc.processes.Load(pid)
		_, retained := svc.terminated.Load(pid)

		return ok && cur == b && !retained
	}, 2*time.Second, 10*time.Millisecond, "reused PID must keep successor B and cache no stale exit")
}

// TestRestoreTerminated_SkipsLivePid verifies the handover retention restore
// does not cache a terminal event for a PID that was re-adopted as live: a PID
// carried in both the process table and the retention cache (the retain-before-
// delete race caught in the freeze window) must resolve to the live process, not
// a stale exit that Connect could later serve.
func TestRestoreTerminated_SkipsLivePid(t *testing.T) {
	t.Parallel()

	_, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	logger := zerolog.Nop()
	// pid 100 is re-adopted as live (Readopt does not start the reaper, so it
	// simply sits in the live map); pid 200 is genuinely terminated.
	svc.processes.Store(uint32(100), handler.Readopt(handler.ReadoptArgs{Pid: 100}, &logger))

	svc.restoreTerminated([]*upgrade.HandoverExit{
		{Pid: 100, RemainingMs: 10_000}, // live -> must be skipped
		{Pid: 200, RemainingMs: 10_000}, // gone -> restored
	})

	_, live := svc.terminated.Load(100)
	assert.False(t, live, "a live re-adopted PID must not get a stale retained exit")
	_, gone := svc.terminated.Load(200)
	assert.True(t, gone, "a genuinely terminated PID should be restored")
}

// TestReapByPidfd_RetainsExitBeforeClose guards the retain-before-close ordering
// on the re-adopt path: the reaper must retain the terminal event (via the
// Handler.OnExit hook) BEFORE it closes EndEvent, so a Connect that forks after
// the close always recovers the exit from the retention cache instead of racing
// an asynchronous retain and getting an error. It drives the real reaper with a
// controlled child, so moving the close ahead of the hook fails the test.
func TestReapByPidfd_RetainsExitBeforeClose(t *testing.T) {
	t.Parallel()

	_, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	// A real short-lived child so the reaper's wait4 harvests a genuine exit.
	cmd := exec.CommandContext(t.Context(), "sh", "-c", "exit 5")
	require.NoError(t, cmd.Start())
	pid := uint32(cmd.Process.Pid) //nolint:gosec // pid fits uint32
	// Deliberately not cmd.Wait(): the reaper's wait4 reaps the child.

	logger := zerolog.Nop()
	h := handler.Readopt(handler.ReadoptArgs{Pid: pid}, &logger)
	svc.processes.Store(pid, h)
	h.OnExit = func(end *rpc.ProcessEvent_EndEvent) {
		svc.finalizeTermination(pid, h, end)
	}

	h.BeginReaping()

	// Drain a subscription until EndEvent closes — the instant the close is
	// observable, the retain must already have happened.
	drained := make(chan struct{})
	go func() {
		ch, cancelFork := h.EndEvent.Fork()
		defer cancelFork()
		for {
			if _, open := <-ch; !open {
				break
			}
		}
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("reaper did not close EndEvent")
	}

	got, ok := svc.terminated.Load(pid)
	require.True(t, ok, "exit must be retained by the time the EndEvent close is observable")
	assert.Equal(t, int32(5), got.end.GetExitCode())
}

// TestRetain_StaleTimerDoesNotEvictNewer verifies the eviction timer is
// identity-guarded: replacing a retained exit for a PID with a newer one must
// not let the earlier entry's still-armed timer evict the newer entry early.
func TestRetain_StaleTimerDoesNotEvictNewer(t *testing.T) {
	t.Parallel()

	_, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	const pid = uint32(8383)
	old := &retainedExit{
		pid:    pid,
		end:    &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 1},
		expiry: time.Now().Add(50 * time.Millisecond),
	}
	svc.retain(pid, old)

	// Replace with a newer entry (long TTL) before the old timer fires.
	newer := &retainedExit{
		pid:    pid,
		end:    &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 2},
		expiry: time.Now().Add(10 * time.Second),
	}
	svc.retain(pid, newer)

	// Past when the OLD timer fires, the newer entry must still be present.
	time.Sleep(120 * time.Millisecond)

	got, ok := svc.terminated.Load(pid)
	require.True(t, ok, "newer retained entry must survive the stale timer")
	assert.Equal(t, int32(2), got.end.GetExitCode())
}

// TestClearTerminatedForTag_DropsPredecessorExit verifies a tagged restart drops
// the predecessor's retained exit under that tag (Start clears the reused pid;
// this covers the tag), while a different tag's entry is left alone.
func TestClearTerminatedForTag_DropsPredecessorExit(t *testing.T) {
	t.Parallel()

	_, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	worker, other := "worker", "other"
	future := time.Now().Add(time.Minute)
	svc.retain(100, &retainedExit{pid: 100, tag: &worker, end: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 1}, expiry: future})
	svc.retain(200, &retainedExit{pid: 200, tag: &other, end: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 9}, expiry: future})

	svc.clearTerminatedForTag(worker)

	_, ok := svc.terminated.Load(100)
	assert.False(t, ok, "predecessor exit under the reused tag must be cleared")
	_, ok = svc.terminated.Load(200)
	assert.True(t, ok, "a different tag's retained exit must be left alone")
}

// TestLookupByTag_AfterTaggedRestart_ServesSuccessorExit is the end-to-end guard
// for the finding: after a tagged process restarts within the retention TTL, a
// Connect-by-tag must be served the successor's exit code, never the
// predecessor's stale one (lookupTerminated returns the first tag match, so the
// predecessor entry has to be cleared when the tag is reused).
func TestLookupByTag_AfterTaggedRestart_ServesSuccessorExit(t *testing.T) {
	t.Parallel()

	_, svc, cleanup := newRetentionTestService(t)
	defer cleanup()

	tag := "worker"
	future := time.Now().Add(time.Minute)
	// Predecessor P1 (pid 100) exited, retained under tag with code 1.
	svc.retain(100, &retainedExit{pid: 100, tag: &tag, end: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 1}, expiry: future})
	// A successor claims the same tag (as handleStart would), then exits at pid
	// 200 with code 2.
	svc.clearTerminatedForTag(tag)
	svc.retain(200, &retainedExit{pid: 200, tag: &tag, end: &rpc.ProcessEvent_EndEvent{Exited: true, ExitCode: 2}, expiry: future})

	got, ok := svc.lookupTerminated(&rpc.ProcessSelector{Selector: &rpc.ProcessSelector_Tag{Tag: tag}})
	require.True(t, ok)
	assert.Equal(t, int32(2), got.end.GetExitCode(),
		"a tag-Connect after a restart must get the successor's exit, never the predecessor's stale one")
}

// trackTermination finalizes a handler's exit from a fork of its EndEvent, so a test can deliver the exit late.
func (s *Service) trackTermination(pid uint32, proc *handler.Handler) {
	endCh, cancel := proc.EndEvent.Fork()

	go func() {
		defer cancel()

		ev, ok := <-endCh
		var end *rpc.ProcessEvent_EndEvent
		if ok {
			end = ev.End
		}
		s.finalizeTermination(pid, proc, end)
	}()
}
