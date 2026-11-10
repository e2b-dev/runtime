//go:build linux

package server

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalevents "github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// countingKillCounter counts the samples the server adds to the kill counter.
type countingKillCounter struct {
	noop.Int64Counter

	samples atomic.Int64
}

func (c *countingKillCounter) Add(_ context.Context, incr int64, _ ...metric.AddOption) {
	c.samples.Add(incr)
}

// bootCancelFixture is a server with a sandbox held out of the live map by a
// reboot in place, as RebootInPlace holds it, and the kill surfaces the server
// publishes.
type bootCancelFixture struct {
	s       *Server
	sbx     *sandbox.Sandbox
	kills   *killRecorder
	counter *countingKillCounter
	// end finishes the boot with nothing live, as a cancelled boot's teardown
	// does, and returns the kill a Delete left it.
	end func() string
}

func newBootCancelFixture(t *testing.T) *bootCancelFixture {
	t.Helper()

	s := duplicateCreateTestServer()
	kills := &killRecorder{}
	s.sbxEventsService = internalevents.NewEventsService([]events.Delivery[events.SandboxEvent]{sandboxEventDeliveryFunc(kills.deliver)})
	counter := &countingKillCounter{}
	s.sandboxKilledCounter = counter

	sbx := drainTestSandbox(t, "rebooting")
	sbx.Runtime.TeamID = uuid.NewString()
	sbx.Runtime.ExecutionID = "execution-1"
	// The API's, so an unrequested end would be listed.
	sbx.APIStoredConfig = &orchestrator.SandboxConfig{}
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))
	end, err := s.sandboxFactory.Sandboxes.BeginBootForTest(context.WithoutCancel(t.Context()), sbx)
	require.NoError(t, err)

	return &bootCancelFixture{s: s, sbx: sbx, kills: kills, counter: counter, end: func() string {
		pendingKill, err := end(nil)
		require.NoError(t, err)

		return pendingKill
	}}
}

// deleteTimesOut runs a Delete with the reason "test-kill" against the held
// boot and asserts it answered its own deadline with nothing published.
func (f *bootCancelFixture) deleteTimesOut(t *testing.T) {
	t.Helper()

	start := time.Now()
	_, err := f.s.Delete(t.Context(), deleteRequest(f.sbx, "test-kill"))
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "the Delete answered its own deadline: %v", err)
	assert.Equal(t, requestTimeout, time.Since(start), "the Delete waited out its request budget")
	assert.True(t, f.s.sandboxFactory.Sandboxes.Rebooting(f.sbx.Runtime.SandboxID), "the boot still holds the sandbox")
	synctest.Wait()
	assert.Empty(t, f.kills.killed(), "nothing is published while the boot still runs")
}

// lifecycleEnds is what the lifecycle does once RebootInPlace returned with
// the boot cancelled and nothing live: the result carries the kill a Delete
// left, and rebootEnded publishes it.
func (f *bootCancelFixture) lifecycleEnds(t *testing.T, pendingKill string) {
	t.Helper()

	res := sandbox.GuestRebootResult{Outcome: sandbox.GuestRebootFailed, ConsecutiveResets: 1, Err: sandbox.ErrBootCancelled, PendingKill: pendingKill}
	next, line := f.s.rebootEnded(t.Context(), f.sbx, res, fc.GuestEventRebootOrCrash, 0, time.Now())
	assert.Nil(t, next)
	assert.Equal(t, "Sandbox stopped", line)
	synctest.Wait()
}

// A Delete that cancels a reboot in place whose teardown outlasts the request
// answers its deadline, and the boot publishes the kill as it ends: once,
// with that Delete's reason, and the lifecycle reads the kill, so nothing is
// listed as ended beside it. A retry after the boot ended gets NotFound, as
// the API expects for a sandbox already gone, and publishes nothing.
func TestDelete_BootOutlastingTheRequestPublishesTheKillOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newBootCancelFixture(t)
		f.deleteTimesOut(t)

		pendingKill := f.end()
		assert.Equal(t, "test-kill", pendingKill, "the boot ends with the Delete's kill")
		assert.Empty(t, f.kills.killed(), "the boot's end publishes nothing by itself")

		f.lifecycleEnds(t, pendingKill)
		assert.Equal(t, []string{"test-kill"}, f.kills.killed(), "published once, with the Delete's reason")
		assert.Equal(t, int64(1), f.counter.samples.Load(), "counted once")
		assert.Equal(t, sandbox.StopReasonKilled, f.sbx.GetStopReason())
		assert.False(t, endsUnrequested(f.sbx), "the lifecycle lists nothing as ended")

		_, lateErr := f.s.Delete(t.Context(), deleteRequest(f.sbx, "late-kill"))
		assert.Equal(t, codes.NotFound, status.Code(lateErr), "nothing is left once the boot ended: %v", lateErr)
		synctest.Wait()
		assert.Equal(t, []string{"test-kill"}, f.kills.killed(), "a late retry publishes nothing")
		assert.Equal(t, int64(1), f.counter.samples.Load())
		assert.Zero(t, f.s.info.OutstandingWork())
	})
}

// A retry that arrives while the cancelled boot's teardown still runs waits
// it out and is success, but publishes nothing: the first Delete left its
// kill to the boot, and the boot publishes it once.
func TestDelete_RetryDuringTheBootsTeardownPublishesNothing(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newBootCancelFixture(t)
		f.deleteTimesOut(t)

		retry := make(chan error, 1)
		go func() {
			_, err := f.s.Delete(t.Context(), deleteRequest(f.sbx, "retry-kill"))
			retry <- err
		}()
		synctest.Wait()
		select {
		case err := <-retry:
			t.Fatalf("the retry answered while the boot still held the sandbox: %v", err)
		default:
		}

		pendingKill := f.end()
		assert.Equal(t, "test-kill", pendingKill, "the first Delete's kill, not the retry's")
		select {
		case err := <-retry:
			require.NoError(t, err, "the retry that waited the boot out is success")
		case <-time.After(testWait):
			t.Fatal("the retry did not return once the boot ended")
		}
		synctest.Wait()
		assert.Empty(t, f.kills.killed(), "the retry publishes nothing: the boot does")

		f.lifecycleEnds(t, pendingKill)
		assert.Equal(t, []string{"test-kill"}, f.kills.killed(), "published once, with the first Delete's reason")
		assert.Equal(t, int64(1), f.counter.samples.Load(), "counted once")
		assert.Zero(t, f.s.info.OutstandingWork())
	})
}

// A boot that went live before the cancel landed, or ended for its own
// reason with no Delete waiting, publishes no kill.
func TestBootKillToPublish(t *testing.T) {
	t.Parallel()

	live := drainTestSandbox(t, "rebooted")
	for _, tc := range []struct {
		name string
		res  sandbox.GuestRebootResult
		want string
		ok   bool
	}{
		{"cancelled, delete stopped waiting", sandbox.GuestRebootResult{Err: sandbox.ErrBootCancelled, PendingKill: "test-kill"}, "test-kill", true},
		{"cancelled, delete waited", sandbox.GuestRebootResult{Err: sandbox.ErrBootCancelled}, "", false},
		{"failed on its own", sandbox.GuestRebootResult{Outcome: sandbox.GuestRebootFailed, Err: sandbox.ErrWaitForEnvdTimeout}, "", false},
		{"live before the cancel landed", sandbox.GuestRebootResult{Sandbox: live, PendingKill: "test-kill"}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := bootKillToPublish(tc.res)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
