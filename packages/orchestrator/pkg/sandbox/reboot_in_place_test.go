//go:build linux

//nolint:paralleltest // the reboot fixture moves the package-wide unstopped counter the reclaim tests read as a delta
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A handed-over resource is released once, by whichever chain holds it last.
func TestHandover(t *testing.T) {
	counting := func(n *atomic.Int32) func(context.Context) error {
		return func(context.Context) error {
			n.Add(1)

			return nil
		}
	}

	t.Run("released by its own chain", func(t *testing.T) {
		var n atomic.Int32
		h := newHandover(counting(&n))
		require.NoError(t, h.run(t.Context()))
		require.NoError(t, h.run(t.Context()))
		_, ok := h.take()
		assert.False(t, ok, "a released resource cannot be handed on")
		assert.Equal(t, int32(1), n.Load())
	})

	t.Run("taken, then released by the taker only", func(t *testing.T) {
		var n atomic.Int32
		h := newHandover(counting(&n))
		taken, ok := h.take()
		require.True(t, ok)
		require.NoError(t, h.run(t.Context()))
		assert.Equal(t, int32(0), n.Load(), "the old chain's entry does nothing once taken")
		_, ok = h.take()
		assert.False(t, ok, "taken once")
		require.NoError(t, taken(t.Context()))
		assert.Equal(t, int32(1), n.Load())
	})

	t.Run("never acquired", func(t *testing.T) {
		h := newHandover(nil)
		require.NoError(t, h.run(t.Context()))
		_, ok := h.take()
		assert.False(t, ok)
	})
}

func TestAdmitGuestReboot(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	boot := 10 * time.Second

	for _, tc := range []struct {
		name   string
		endAt  time.Time
		resets int
		want   GuestRebootOutcome
	}{
		{"first reset, time left", now.Add(time.Hour), 1, ""},
		{"last reset under the bound", now.Add(time.Hour), maxConsecutiveGuestResets, ""},
		{"one reset over the bound", now.Add(time.Hour), maxConsecutiveGuestResets + 1, GuestRebootRefusedLoop},
		{"end time within a typical boot", now.Add(boot), 1, GuestRebootRefusedTimeout},
		{"end time passed", now.Add(-time.Second), 1, GuestRebootRefusedTimeout},
		{"loop wins over timeout", now, maxConsecutiveGuestResets + 1, GuestRebootRefusedLoop},
	} {
		assert.Equal(t, tc.want, admitGuestReboot(now, tc.endAt, boot, tc.resets), tc.name)
	}
}

// rebootFixture is a live sandbox whose guest reset, on a real Map, with a
// boot step that hands its slot and overlay on through the real handovers and
// cleanup chains. Every release of the slot and the overlay is counted.
type rebootFixture struct {
	t         *testing.T
	sandboxes *Map
	old       *Sandbox
	factory   *Factory

	slotReleases   atomic.Int32
	rootfsReleases atomic.Int32
	boots          atomic.Int32
	lifecycles     atomic.Int32
}

// bootStep is one scripted boot: what the next lifecycle's guest does.
type bootStep func(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error)

// bootReady starts the next lifecycle and envd answers.
func bootReady(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
	return f.start(ctx, from, endAt), false, nil
}

// bootResets starts the next lifecycle and its guest resets before envd answers.
func bootResets(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
	return f.start(ctx, from, endAt), true, errors.New("firecracker exited")
}

// bootTimesOut starts the next lifecycle and envd never answers.
func bootTimesOut(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
	return f.start(ctx, from, endAt), false, ErrWaitForEnvdTimeout
}

// bootFailsToStart fails before anything is taken from the previous lifecycle.
func bootFailsToStart(context.Context, *rebootFixture, *Sandbox, time.Time) (*Sandbox, bool, error) {
	return nil, false, errors.New("create cgroup")
}

func newRebootFixture(t *testing.T, endAt time.Time, steps ...bootStep) *rebootFixture {
	t.Helper()

	f := &rebootFixture{t: t, sandboxes: NewSandboxesMap()}
	f.old = f.lifecycle(t.Context(), endAt)
	f.old.slotRelease = newHandover(f.countRelease(&f.slotReleases))
	f.old.cleanup.Add(t.Context(), f.old.slotRelease.run)
	f.old.rootfsRelease = newHandover(f.countRelease(&f.rootfsReleases))
	f.old.cleanup.Add(t.Context(), f.old.rootfsRelease.run)
	require.NoError(t, f.sandboxes.MarkRunning(t.Context(), f.old))

	f.factory = &Factory{
		Sandboxes: f.sandboxes,
		bootNext: func(ctx context.Context, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
			n := int(f.boots.Add(1))
			require.LessOrEqual(t, n, len(steps), "unscripted boot")

			return steps[n-1](ctx, f, from, endAt)
		},
	}

	return f
}

func (f *rebootFixture) countRelease(n *atomic.Int32) func(context.Context) error {
	return func(context.Context) error {
		n.Add(1)

		return nil
	}
}

// lifecycle is a sandbox whose process has already stopped.
func (f *rebootFixture) lifecycle(ctx context.Context, endAt time.Time) *Sandbox {
	sbx := testMapSandbox(f.t, fmt.Sprintf("lifecycle-%d", f.lifecycles.Add(1)))
	sbx.Runtime.ExecutionID = "execution-1"
	sbx.SetEndAt(endAt)
	sbx.cleanup = NewCleanup()
	sbx.sandboxes = f.sandboxes
	f.sandboxes.reclaimLiveEntryOnCleanup(ctx, sbx.cleanup, sbx.Runtime, sbx.LifecycleID)
	sbx.stop.GetOrInit(func() error { return nil })

	return sbx
}

// start takes from's slot and overlay into a new lifecycle, as CreateSandbox
// does with handed-over resources.
func (f *rebootFixture) start(ctx context.Context, from *Sandbox, endAt time.Time) *Sandbox {
	h, err := from.takeForReboot(ctx)
	require.NoError(f.t, err)

	next := f.lifecycle(ctx, endAt)
	next.Runtime = from.Runtime
	next.slotRelease = newHandover(h.releaseSlot)
	next.cleanup.Add(ctx, next.slotRelease.run)
	next.rootfsRelease = newHandover(h.releaseRootfs)
	next.cleanup.Add(ctx, next.rootfsRelease.run)

	return next
}

// releases is how many times the slot and the overlay were released.
func (f *rebootFixture) releases() [2]int32 {
	return [2]int32{f.slotReleases.Load(), f.rootfsReleases.Load()}
}

func TestRebootInPlace(t *testing.T) {
	inAnHour := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name  string
		endAt time.Time
		steps []bootStep
		// checkpointInFlight holds an in-place checkpoint at the reset.
		checkpointInFlight bool

		outcome GuestRebootOutcome
		resets  int
		boots   int32
		// live is whether the sandbox ID is live afterwards.
		live bool
		// releasedByReboot is how often the slot and the overlay were released
		// before anybody closed the old lifecycle or the rebooted one.
		releasedByReboot int32
		// releasedByOldClose is what closing the old lifecycle releases.
		releasedByOldClose int32
	}{
		{
			name: "boots again", endAt: inAnHour, steps: []bootStep{bootReady},
			outcome: GuestRebootRebooted, resets: 1, boots: 1, live: true,
		},
		{
			name: "resets during the boot, then boots", endAt: inAnHour, steps: []bootStep{bootResets, bootReady},
			outcome: GuestRebootRebooted, resets: 2, boots: 2, live: true,
		},
		{
			name: "keeps resetting", endAt: inAnHour, steps: []bootStep{bootResets, bootResets, bootResets},
			outcome: GuestRebootRefusedLoop, resets: maxConsecutiveGuestResets + 1, boots: maxConsecutiveGuestResets,
			releasedByReboot: 1,
		},
		{
			name: "too close to the end time", endAt: time.Now().Add(time.Second),
			outcome: GuestRebootRefusedTimeout, resets: 1, releasedByOldClose: 1,
		},
		{
			name: "boot fails before taking anything", endAt: inAnHour, steps: []bootStep{bootFailsToStart},
			outcome: GuestRebootFailed, resets: 1, boots: 1, releasedByOldClose: 1,
		},
		{
			name: "envd never answers", endAt: inAnHour, steps: []bootStep{bootTimesOut},
			outcome: GuestRebootFailed, resets: 1, boots: 1, releasedByReboot: 1,
		},
		{
			name: "in-place checkpoint in flight", endAt: inAnHour, checkpointInFlight: true,
			outcome: GuestRebootFailed, resets: 1, live: true, releasedByOldClose: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRebootFixture(t, tc.endAt, tc.steps...)
			if tc.checkpointInFlight {
				require.True(t, f.old.BeginCheckpoint())
			}

			res := f.factory.RebootInPlace(t.Context(), f.old)

			assert.Equal(t, tc.outcome, res.Outcome)
			assert.Equal(t, tc.resets, res.ConsecutiveResets)
			assert.Equal(t, tc.boots, f.boots.Load(), "boots started")
			assert.Equal(t, [2]int32{tc.releasedByReboot, tc.releasedByReboot}, f.releases(), "released during the reboot")

			live, ok := f.sandboxes.Get(f.old.Runtime.SandboxID)
			require.Equal(t, tc.live, ok, "live afterwards")

			if tc.outcome == GuestRebootRebooted {
				require.Same(t, res.Sandbox, live)
				assert.Equal(t, f.old.Runtime.SandboxID, live.Runtime.SandboxID)
				assert.Equal(t, f.old.Runtime.ExecutionID, live.Runtime.ExecutionID)
				assert.NotEqual(t, f.old.LifecycleID, live.LifecycleID)
				assert.Equal(t, f.old.GetEndAt(), live.GetEndAt())
				assert.Equal(t, StopReasonRebooted, f.old.GetStopReason())
			} else {
				assert.Nil(t, res.Sandbox)
				assert.Equal(t, StopReasonCrashed, f.old.GetStopReason(), "a sandbox not booted again ends as a crash")
			}

			require.NoError(t, f.old.Close(t.Context()))
			want := tc.releasedByReboot + tc.releasedByOldClose
			assert.Equal(t, [2]int32{want, want}, f.releases(), "released by closing the old lifecycle")

			if res.Sandbox != nil {
				require.NoError(t, res.Sandbox.Close(t.Context()))
				assert.Equal(t, [2]int32{1, 1}, f.releases(), "released once, by the rebooted lifecycle")
			}

			_, ok = f.sandboxes.Get(f.old.Runtime.SandboxID)
			assert.False(t, ok, "nothing live once every lifecycle closed")
		})
	}
}

// A pause, checkpoint or update arriving during the boot waits for it: it
// finds the rebooted sandbox, or nothing when the boot failed.
func TestRebootInPlaceLookupsWaitForTheBoot(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after bootStep
		found bool
	}{
		{"boot succeeds", bootReady, true},
		{"boot fails", bootTimesOut, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			booting := make(chan struct{})
			proceed := make(chan struct{})
			f := newRebootFixture(t, time.Now().Add(time.Hour),
				func(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
					close(booting)
					<-proceed

					return tc.after(ctx, f, from, endAt)
				})
			sandboxID := f.old.Runtime.SandboxID

			done := make(chan GuestRebootResult)
			go func() { done <- f.factory.RebootInPlace(t.Context(), f.old) }()
			<-booting

			_, ok := f.sandboxes.Get(sandboxID)
			require.False(t, ok, "not live during the boot")

			_, err := f.sandboxes.Reserve(sandboxID)
			require.ErrorIs(t, err, ErrSandboxAlreadyRunning, "a create cannot take the ID during the boot")

			type lookup struct {
				sbx *Sandbox
				ok  bool
				err error
			}
			looked := make(chan lookup)
			go func() {
				sbx, ok, err := f.sandboxes.AwaitBoot(t.Context(), sandboxID)
				looked <- lookup{sbx, ok, err}
			}()

			select {
			case <-looked:
				require.FailNow(t, "the lookup returned during the boot")
			case <-time.After(50 * time.Millisecond):
			}

			close(proceed)
			res := <-done
			got := <-looked

			require.NoError(t, got.err)
			require.Equal(t, tc.found, got.ok)
			if tc.found {
				assert.Same(t, res.Sandbox, got.sbx)
				require.NoError(t, res.Sandbox.Close(t.Context()))
			}
			require.NoError(t, f.old.Close(t.Context()))
		})
	}
}

// A kill during the boot cancels it and the sandbox does not come back.
func TestRebootInPlaceCancelBoot(t *testing.T) {
	booting := make(chan struct{})
	f := newRebootFixture(t, time.Now().Add(time.Hour),
		func(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
			next := f.start(ctx, from, endAt)
			close(booting)
			<-ctx.Done()

			return next, false, ctx.Err()
		})
	sandboxID := f.old.Runtime.SandboxID

	done := make(chan GuestRebootResult)
	go func() { done <- f.factory.RebootInPlace(t.Context(), f.old) }()
	<-booting

	from, publish, err := f.sandboxes.CancelBoot(t.Context(), sandboxID, "test-kill")
	require.NoError(t, err)
	assert.Same(t, f.old, from, "the kill is reported against the lifecycle that reset")
	assert.True(t, publish, "the cancel waited the boot out, so the caller publishes the kill")
	assert.Equal(t, StopReasonKilled, f.old.GetStopReason(), "recorded before the cancel returns, so the lifecycle never reads a crash")

	res := <-done
	assert.Equal(t, GuestRebootFailed, res.Outcome)
	require.ErrorIs(t, res.Err, ErrBootCancelled)
	assert.Equal(t, [2]int32{1, 1}, f.releases(), "the cancelled boot released what it took")

	_, ok := f.sandboxes.Get(sandboxID)
	assert.False(t, ok)

	from, publish, err = f.sandboxes.CancelBoot(t.Context(), sandboxID, "test-kill")
	require.NoError(t, err)
	assert.Nil(t, from, "no boot left to cancel")
	assert.False(t, publish)
	assert.Empty(t, res.PendingKill, "no delete left its kill to the boot")

	require.NoError(t, f.old.Close(t.Context()))
	assert.Equal(t, [2]int32{1, 1}, f.releases())
}

// A resume whose guest resets before envd answers is booted again when the
// caller asked for it. The rebooted lifecycle is not live: the create
// registers it under the reservation it already holds.
func TestRebootOnStart(t *testing.T) {
	inAnHour := time.Now().Add(time.Hour)

	for _, tc := range []struct {
		name  string
		endAt time.Time
		steps []bootStep
		// flagOff passes no report: the resume fails as it always has.
		flagOff bool

		outcome GuestRebootOutcome
		resets  int
		boots   int32
		// releasedByReboot is how often the slot and the overlay were released
		// before anybody closed the old lifecycle or the rebooted one.
		releasedByReboot int32
	}{
		{
			name: "boots again", endAt: inAnHour, steps: []bootStep{bootReady},
			outcome: GuestRebootRebooted, resets: 1, boots: 1,
		},
		{
			name: "resets during the boot, then boots", endAt: inAnHour, steps: []bootStep{bootResets, bootReady},
			outcome: GuestRebootRebooted, resets: 2, boots: 2,
		},
		{
			name: "envd never answers", endAt: inAnHour, steps: []bootStep{bootTimesOut},
			outcome: GuestRebootFailed, resets: 1, boots: 1, releasedByReboot: 1,
		},
		{
			name: "too close to the end time", endAt: time.Now().Add(time.Second),
			outcome: GuestRebootRefusedTimeout, resets: 1,
		},
		{name: "flag off", endAt: inAnHour, flagOff: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRebootFixture(t, tc.endAt, tc.steps...)
			sandboxID := f.old.Runtime.SandboxID
			// A resume's sandbox is not live yet, and its create holds the ID.
			require.True(t, f.sandboxes.MarkStopping(t.Context(), sandboxID, f.old.LifecycleID))
			reservation, err := f.sandboxes.Reserve(sandboxID)
			require.NoError(t, err)
			defer reservation.Release()

			var reports []GuestRebootResult
			var report StartRebootReport
			if !tc.flagOff {
				report = func(_ context.Context, res GuestRebootResult, _ time.Time) {
					reports = append(reports, res)
				}
			}

			next, err := f.factory.rebootOnStart(t.Context(), f.old, report)

			assert.Equal(t, tc.boots, f.boots.Load(), "boots started")
			assert.Equal(t, [2]int32{tc.releasedByReboot, tc.releasedByReboot}, f.releases(), "released during the reboot")
			if tc.flagOff {
				require.ErrorIs(t, err, ErrGuestResetOnStart)
				assert.Empty(t, reports)
			} else {
				require.Len(t, reports, 1, "reported once")
				assert.Equal(t, tc.outcome, reports[0].Outcome)
				assert.Equal(t, tc.resets, reports[0].ConsecutiveResets)
			}

			_, ok := f.sandboxes.Get(sandboxID)
			assert.False(t, ok, "nothing live before the create registers it")

			if tc.outcome == GuestRebootRebooted {
				require.NoError(t, err)
				assert.Same(t, reports[0].Sandbox, next)
				assert.Equal(t, f.old.Runtime.ExecutionID, next.Runtime.ExecutionID)
				assert.NotEqual(t, f.old.LifecycleID, next.LifecycleID)
				assert.Equal(t, f.old.GetEndAt(), next.GetEndAt())

				require.NoError(t, reservation.MarkRunning(t.Context(), next), "the create's reservation registers it")
				live, ok := f.sandboxes.Get(sandboxID)
				require.True(t, ok)
				assert.Same(t, next, live)
			} else {
				require.ErrorIs(t, err, ErrGuestResetOnStart)
				assert.Nil(t, next)
			}

			require.NoError(t, f.old.Close(t.Context()))
			if next == nil {
				assert.Equal(t, [2]int32{1, 1}, f.releases(), "released once, whichever lifecycle held them")

				return
			}

			assert.Equal(t, [2]int32{0, 0}, f.releases(), "the rebooted lifecycle holds the slot and the overlay")
			require.NoError(t, next.Close(t.Context()))
			assert.Equal(t, [2]int32{1, 1}, f.releases(), "released once, by the rebooted lifecycle")
		})
	}
}

// An envd wait that failed for any reason but the process exiting is not a
// reset: the resume fails without looking at the process.
func TestResetDuringBootNeedsTheProcessToExit(t *testing.T) {
	sbx := testMapSandbox(t, "lifecycle-1")
	for _, err := range []error{ErrWaitForEnvdTimeout, context.DeadlineExceeded, errors.New("init failed")} {
		assert.False(t, resetDuringBoot(t.Context(), sbx, fmt.Errorf("failed to wait for sandbox start: %w", err)), err.Error())
	}
}

// Every boot attempt carries a deadline, even under a context without one:
// the network-assignment hook refuses an unbounded start.
func TestRebootInPlaceBoundsEachBoot(t *testing.T) {
	var deadlines []time.Duration
	bounded := func(next bootStep) bootStep {
		return func(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
			assert.True(t, f.sandboxes.Rebooting(from.Runtime.SandboxID), "the boot holds the sandbox")
			d, ok := ctx.Deadline()
			require.True(t, ok, "boot attempt without a deadline")
			deadlines = append(deadlines, time.Until(d))

			return next(ctx, f, from, endAt)
		}
	}

	f := newRebootFixture(t, time.Now().Add(time.Hour), bounded(bootResets), bounded(bootReady))
	res := f.factory.RebootInPlace(context.WithoutCancel(t.Context()), f.old)
	require.NoError(t, res.Err)
	assert.Equal(t, GuestRebootRebooted, res.Outcome)
	assert.False(t, f.sandboxes.Rebooting(f.old.Runtime.SandboxID))
	require.Len(t, deadlines, 2)
	for _, d := range deadlines {
		assert.LessOrEqual(t, d, rebootAttemptTimeoutFor(f.old.Config))
	}
	require.NoError(t, res.Sandbox.Close(t.Context()))
	require.NoError(t, f.old.Close(t.Context()))
}

// A lifecycle whose network release fails keeps its address: the handover is
// refused, and the slot and the overlay go back to their owner, whose return
// retries the release.
func TestTakeForRebootKeepsAnAddressStillHeld(t *testing.T) {
	f := newRebootFixture(t, time.Now().Add(time.Hour))
	f.old.networkRelease = func(context.Context, string) error { return errors.New("nfs subscriber still holds the lifecycle") }

	h, err := f.old.takeForReboot(t.Context())
	require.ErrorContains(t, err, "nfs subscriber still holds the lifecycle")
	assert.Nil(t, h)
	assert.Equal(t, [2]int32{1, 1}, f.releases(), "both went back to their owner")

	require.NoError(t, f.old.Close(t.Context()))
	assert.Equal(t, [2]int32{1, 1}, f.releases(), "and only once")
}

// Giving the slot and the overlay back after a failed handover outlives the
// boot attempt: the pool's delayed return retries the release under it.
func TestTakeForRebootRollbackOutlivesTheAttempt(t *testing.T) {
	f := newRebootFixture(t, time.Now().Add(time.Hour))
	sbx := f.lifecycle(t.Context(), time.Now().Add(time.Hour))
	sbx.Slot = f.old.Slot
	var released []context.Context
	keep := func(ctx context.Context) error {
		released = append(released, ctx)

		return nil
	}
	sbx.slotRelease, sbx.rootfsRelease = newHandover(keep), newHandover(keep)
	sbx.networkRelease = func(context.Context, string) error { return errors.New("subscriber still holds the lifecycle") }

	attempt, cancel := context.WithCancel(t.Context())
	_, err := sbx.takeForReboot(attempt)
	require.Error(t, err)
	cancel()

	require.Len(t, released, 2)
	for _, ctx := range released {
		assert.NoError(t, ctx.Err(), "a cancelled attempt must not cancel the give-back")
	}
}

// A kill whose caller stops waiting before the cancelled boot's teardown
// finished leaves its kill to the boot: the boot ends with it in its result,
// and a second cancel that waits the boot out finds the kill taken, so the
// kill is reported once. Fake time keeps the retry's wait and the boot's end
// in a fixed order.
func TestRebootInPlaceCancelBootOutlastingTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		booting := make(chan struct{})
		proceed := make(chan struct{})
		f := newRebootFixture(t, time.Now().Add(time.Hour),
			func(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
				next := f.start(ctx, from, endAt)
				close(booting)
				<-ctx.Done()
				// The teardown that outlasts the caller: an overlay close, say.
				<-proceed

				return next, false, ctx.Err()
			})
		sandboxID := f.old.Runtime.SandboxID

		done := make(chan GuestRebootResult)
		go func() { done <- f.factory.RebootInPlace(t.Context(), f.old) }()
		<-booting

		callerCtx, giveUp := context.WithCancel(t.Context())
		giveUp()
		from, publish, err := f.sandboxes.CancelBoot(callerCtx, sandboxID, "test-kill")
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, from)
		assert.False(t, publish, "the caller answered first, so the boot publishes the kill")
		assert.True(t, f.sandboxes.Rebooting(sandboxID), "the boot still holds the sandbox")

		// A retry of the kill while the teardown still runs waits it out and
		// finds the kill taken by the first caller.
		type cancelled struct {
			from    *Sandbox
			publish bool
			err     error
		}
		retried := make(chan cancelled)
		go func() {
			from, publish, err := f.sandboxes.CancelBoot(t.Context(), sandboxID, "retry-kill")
			retried <- cancelled{from, publish, err}
		}()
		synctest.Wait()
		select {
		case <-retried:
			require.FailNow(t, "the retry returned while the boot still held the sandbox")
		default:
		}

		close(proceed)
		res := <-done
		retry := <-retried

		assert.Equal(t, GuestRebootFailed, res.Outcome)
		require.ErrorIs(t, res.Err, ErrBootCancelled)
		assert.Nil(t, res.Sandbox)
		assert.Equal(t, "test-kill", res.PendingKill, "the first caller's kill, not the retry's")
		assert.Equal(t, StopReasonKilled, f.old.GetStopReason(), "the lifecycle reads the kill, not a crash to list as ended")
		require.NoError(t, retry.err)
		assert.Same(t, f.old, retry.from)
		assert.False(t, retry.publish, "the retry publishes nothing: the boot does")
		assert.Equal(t, [2]int32{1, 1}, f.releases(), "the cancelled boot released what it took")

		_, ok := f.sandboxes.Get(sandboxID)
		assert.False(t, ok)
		from, publish, err = f.sandboxes.CancelBoot(t.Context(), sandboxID, "late-kill")
		require.NoError(t, err)
		assert.Nil(t, from, "no boot left to cancel")
		assert.False(t, publish)

		require.NoError(t, f.old.Close(t.Context()))
	})
}

// A kill that cancels the boot after envd answered, before the rebooted
// sandbox registered, is still a kill: the registration is refused, the
// rebooted lifecycle is torn down, and the old one reads the kill. Two kills
// waiting the boot out report it once between them. Fake time keeps both
// waits and the boot's end in a fixed order.
func TestRebootInPlaceCancelAfterEnvdAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		booting := make(chan struct{})
		proceed := make(chan struct{})
		f := newRebootFixture(t, time.Now().Add(time.Hour),
			func(ctx context.Context, f *rebootFixture, from *Sandbox, endAt time.Time) (*Sandbox, bool, error) {
				// The handover precedes envd's answer, so it precedes the cancel.
				next := f.start(ctx, from, endAt)
				close(booting)
				<-ctx.Done()
				<-proceed

				return next, false, nil
			})
		sandboxID := f.old.Runtime.SandboxID

		done := make(chan GuestRebootResult)
		go func() { done <- f.factory.RebootInPlace(t.Context(), f.old) }()
		<-booting

		type cancelled struct {
			from    *Sandbox
			publish bool
			err     error
		}
		waiters := make(chan cancelled, 2)
		for _, reason := range []string{"test-kill", "second-kill"} {
			go func() {
				from, publish, err := f.sandboxes.CancelBoot(t.Context(), sandboxID, reason)
				waiters <- cancelled{from, publish, err}
			}()
		}
		synctest.Wait()
		assert.Empty(t, waiters, "both kills wait for the boot to end")

		close(proceed)
		res := <-done
		first, second := <-waiters, <-waiters

		assert.Equal(t, GuestRebootFailed, res.Outcome)
		require.ErrorIs(t, res.Err, ErrBootCancelled)
		assert.Nil(t, res.Sandbox, "the rebooted sandbox did not register")
		assert.Empty(t, res.PendingKill, "no kill stopped waiting")
		assert.Equal(t, StopReasonKilled, f.old.GetStopReason())
		assert.Equal(t, int32(1), f.boots.Load())
		assert.Equal(t, [2]int32{1, 1}, f.releases(), "the rebooted lifecycle released what it took")
		for _, w := range []cancelled{first, second} {
			require.NoError(t, w.err)
			assert.Same(t, f.old, w.from, "the kill is reported against the lifecycle that reset")
		}
		assert.NotEqual(t, first.publish, second.publish, "exactly one of the two waiting kills publishes")

		_, ok := f.sandboxes.Get(sandboxID)
		assert.False(t, ok, "nothing is live after the kill")

		require.NoError(t, f.old.Close(t.Context()))
		assert.Equal(t, [2]int32{1, 1}, f.releases())
	})
}
