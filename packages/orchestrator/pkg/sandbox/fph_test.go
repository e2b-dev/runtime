//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func testStopCfg() featureflags.HintStopConfig {
	return featureflags.HintStopConfig{Timeout: 2 * time.Second, Grace: 100 * time.Millisecond}
}

func prePauseCfg(timeout time.Duration) featureflags.PrePauseHintConfig {
	return featureflags.PrePauseHintConfig{Timeout: timeout, Stop: testStopCfg()}
}

// inFlight is what DrainBalloon returns when the guest cycle may be running;
// guestSilent is the timeout of a guest that never echoed the command.
func inFlight(err error) error { return fmt.Errorf("%w: %w", fc.ErrHintingInFlight, err) }

func guestSilent() error {
	return inFlight(fmt.Errorf("%w: %w", fc.ErrHintingGuestSilent, context.DeadlineExceeded))
}

// fakeRunner records drain/stop calls and lets each test script the drain. Its
// stop checks the contract stopAbandonedHint owes it: an uncancelled context
// with a deadline no later than the configured stop timeout, and the
// configured grace.
type fakeRunner struct {
	drain     func(ctx context.Context) error
	drains    atomic.Int32
	stops     atomic.Int32
	badStopCx atomic.Int32
	grace     atomic.Int64
	stopErr   error
	stopHook  func()
	exited    atomic.Bool
	freed     atomic.Uint64
	// flushFreed, when set, is what a flush reveals; flushErr fails the flush.
	flushFreed func() uint64
	flushErr   error
	flushes    atomic.Int32
}

func (f *fakeRunner) DrainBalloon(ctx context.Context) error {
	f.drains.Add(1)
	if f.drain == nil {
		f.freed.Add(1 << 20)

		return nil
	}

	return f.drain(ctx)
}

func (f *fakeRunner) StopBalloonHinting(ctx context.Context, ackGrace time.Duration) error {
	f.stops.Add(1)
	f.grace.Store(int64(ackGrace))
	dl, ok := ctx.Deadline()
	if ctx.Err() != nil || !ok || time.Until(dl) > 2*time.Second {
		f.badStopCx.Add(1)
	}
	if f.stopHook != nil {
		f.stopHook()
	}

	return f.stopErr
}

func (f *fakeRunner) Exited() bool { return f.exited.Load() }

func (f *fakeRunner) HintFreedBytes() uint64 { return f.freed.Load() }

func (f *fakeRunner) FlushHintFreedBytes(ctx context.Context) (uint64, error) {
	f.flushes.Add(1)
	if ctx.Err() != nil || f.flushErr != nil {
		return f.freed.Load(), errors.Join(f.flushErr, ctx.Err())
	}
	if f.flushFreed != nil {
		return f.flushFreed(), nil
	}

	return f.freed.Load(), nil
}

// outcomes captures what the sandbox recorded, in order: drain outcomes as
// "phase:outcome" (periodic runs bare), stop outcomes in their own list, and
// freed-bytes deltas.
type outcomes struct {
	mu     sync.Mutex
	v      []string
	stop   []string
	freed  []uint64
	faults []faultRecord
}

type faultRecord struct {
	kind, window, outcome string
	n                     int64
}

func (o *outcomes) hookFaults(kind, window, outcome string, n int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.faults = append(o.faults, faultRecord{kind: kind, window: window, outcome: outcome, n: n})
}

// faultsFor returns the recorded deltas per kind for one window and outcome.
func (o *outcomes) faultsFor(window, outcome string) map[string][]int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string][]int64{}
	for _, f := range o.faults {
		if f.window == window && f.outcome == outcome {
			out[f.kind] = append(out[f.kind], f.n)
		}
	}

	return out
}

func (o *outcomes) hook(outcome string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if rest, ok := strings.CutPrefix(outcome, "stop:"); ok {
		o.stop = append(o.stop, rest)

		return
	}
	outcome = strings.TrimPrefix(outcome, "latch:")
	o.v = append(o.v, strings.TrimPrefix(outcome, "periodic:"))
}

func (o *outcomes) hookFreed(_ string, bytes uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.freed = append(o.freed, bytes)
}

func (o *outcomes) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.v...)
}

func (o *outcomes) stops() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.stop...)
}

func (o *outcomes) freedList() []uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]uint64(nil), o.freed...)
}

func (o *outcomes) count(want string) int {
	n := 0
	for _, v := range o.list() {
		if v == want {
			n++
		}
	}

	return n
}

func (o *outcomes) has(want string) bool { return o.count(want) > 0 }

func newFPHTestSandbox() (*Sandbox, *outcomes) {
	s := &Sandbox{Metadata: &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "fph-test"}}}
	o := &outcomes{}
	s.fphObserve = o.hook
	s.fphObserveFreed = o.hookFreed

	return s, o
}

// The pause drain never fails the pause, stops a cycle that outlived it, and
// records every outcome under its own phase.
func TestPrePauseHintDrain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		drain     func(context.Context) error
		wantStops int32
		want      string
	}{
		{"completed", nil, 0, "pre-pause:ok"},
		{"not configured", func(context.Context) error { return fc.ErrHintingNotConfigured }, 0, "pre-pause:not-configured"},
		{"outlived the timeout", func(ctx context.Context) error {
			<-ctx.Done()

			return inFlight(ctx.Err())
		}, 1, "pre-pause:timeout"},
		{"guest never engaged", func(context.Context) error { return guestSilent() }, 1, "pre-pause:timeout-guest-silent"},
		{"start hit the deadline", func(context.Context) error {
			return inFlight(fmt.Errorf("start balloon hinting: %w", context.DeadlineExceeded))
		}, 1, "pre-pause:timeout"},
		{"status read failed mid-cycle", func(context.Context) error { return inFlight(errors.New("balloon hinting status: EOF")) }, 1, "pre-pause:failed"},
		{"transient refusal", func(context.Context) error { return fmt.Errorf("%w: 400", fc.ErrHintingStartRefused) }, 0, "pre-pause:refused"},
		{"other failure", func(context.Context) error { return errors.New("start balloon hinting: EOF") }, 0, "pre-pause:failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, o := newFPHTestSandbox()
			r := &fakeRunner{drain: tc.drain}
			start := time.Now()
			s.prePauseHintDrain(t.Context(), prePauseCfg(30*time.Millisecond), r)
			assert.Less(t, time.Since(start), 500*time.Millisecond)
			assert.Equal(t, int32(1), r.drains.Load())
			assert.Equal(t, tc.wantStops, r.stops.Load())
			assert.Zero(t, r.badStopCx.Load(), "stop gets an uncancelled, bounded context")
			assert.Equal(t, []string{tc.want}, o.list())
			if tc.wantStops > 0 {
				assert.Equal(t, []string{"ok"}, o.stops(), "the abandoned cycle was stopped")
			} else {
				assert.Empty(t, o.stops())
			}
			if tc.want == "pre-pause:ok" {
				assert.Equal(t, []uint64{1 << 20}, o.freedList(), "a completed drain records what it freed")
			} else {
				assert.Empty(t, o.freedList())
			}
		})
	}
}

// What a drain freed is read after a metrics flush, so the drain's own
// discards are in it; a flush that fails falls back to the last observed value.
func TestPrePauseHintDrain_FreedBytesReadAfterFlush(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandbox()
	r := &fakeRunner{drain: func(context.Context) error { return nil }}
	r.freed.Store(10 << 20)
	r.flushFreed = func() uint64 { return 13 << 20 }
	s.prePauseHintDrain(t.Context(), prePauseCfg(time.Second), r)
	assert.Equal(t, int32(1), r.flushes.Load(), "a completed drain flushes once")
	assert.Equal(t, []uint64{3 << 20}, o.freedList(), "the delta is the flushed value minus the value before the drain")

	s2, o2 := newFPHTestSandbox()
	r2 := &fakeRunner{drain: func(context.Context) error { return nil }, flushErr: errors.New("fc gone")}
	r2.freed.Store(10 << 20)
	s2.prePauseHintDrain(t.Context(), prePauseCfg(time.Second), r2)
	assert.Empty(t, o2.freedList(), "no flush, no delta to attribute")
	assert.Equal(t, []string{"pre-pause:ok"}, o2.list(), "the drain itself is still a success")

	s3, o3 := newFPHTestSandbox()
	r3 := &fakeRunner{drain: func(context.Context) error { return guestSilent() }}
	r3.flushFreed = func() uint64 { return 99 << 20 }
	s3.prePauseHintDrain(t.Context(), prePauseCfg(20*time.Millisecond), r3)
	assert.Zero(t, r3.flushes.Load(), "only a completed drain is attributed")
	assert.Empty(t, o3.freedList())
}

// A request that is gone before the drain starts nothing; one that goes away
// mid-drain stops the cycle, records it as cancelled and does not alert.
func TestPrePauseHintDrain_CancelledRequest(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandbox()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := &fakeRunner{}
	s.prePauseHintDrain(ctx, prePauseCfg(time.Second), r)
	assert.Zero(t, r.drains.Load(), "nothing started on a dead request")
	assert.Empty(t, o.list())

	s2, o2 := newFPHTestSandbox()
	ctx2, cancel2 := context.WithCancel(t.Context())
	r2 := &fakeRunner{drain: func(ctx context.Context) error {
		cancel2()
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	s2.prePauseHintDrain(ctx2, prePauseCfg(time.Second), r2)
	assert.Equal(t, int32(1), r2.stops.Load(), "the cycle is still stopped")
	assert.Zero(t, r2.badStopCx.Load())
	assert.Equal(t, []string{"pre-pause:cancelled"}, o2.list())
	assert.Equal(t, []string{"ok"}, o2.stops())
}

// The flag's stop settings reach the stop: the grace is passed through, and a
// zero stop timeout leaves the cycle running as before the stop existed.
func TestPrePauseHintDrain_StopSettings(t *testing.T) {
	t.Parallel()
	s, o := newFPHTestSandbox()
	cfg := prePauseCfg(20 * time.Millisecond)
	cfg.Stop.Grace = 7 * time.Millisecond
	r := &fakeRunner{drain: func(ctx context.Context) error {
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	s.prePauseHintDrain(t.Context(), cfg, r)
	assert.Equal(t, int64(7*time.Millisecond), r.grace.Load())
	assert.Equal(t, []string{"pre-pause:timeout"}, o.list())
	assert.Equal(t, []string{"ok"}, o.stops())

	s2, o2 := newFPHTestSandbox()
	cfg.Stop.Timeout = 0
	r2 := &fakeRunner{drain: func(ctx context.Context) error {
		<-ctx.Done()

		return inFlight(ctx.Err())
	}}
	s2.prePauseHintDrain(t.Context(), cfg, r2)
	assert.Zero(t, r2.stops.Load(), "stop disabled by flag")
	assert.Equal(t, []string{"pre-pause:timeout"}, o2.list())
	assert.Equal(t, []string{"skipped-disabled"}, o2.stops())
}

// Stop outcomes: an unacknowledged stop landed and is a warning, a failed stop
// on a live process is an error, and a process that exits around the stop is
// neither.
func TestStopAbandonedHint(t *testing.T) {
	t.Parallel()
	cfg := testStopCfg()

	s, o := newFPHTestSandbox()
	r := &fakeRunner{stopErr: fmt.Errorf("%w: guest_cmd=7: %w", fc.ErrHintingStopUnacked, context.DeadlineExceeded)}
	s.stopAbandonedHint(t.Context(), cfg, r)
	assert.Equal(t, []string{"unacked"}, o.stops())

	s2, o2 := newFPHTestSandbox()
	r2 := &fakeRunner{stopErr: errors.New("fc api down")}
	s2.stopAbandonedHint(t.Context(), cfg, r2)
	assert.Equal(t, []string{"failed"}, o2.stops())

	s3, o3 := newFPHTestSandbox()
	r3 := &fakeRunner{stopErr: errors.New("connection refused")}
	r3.exited.Store(true)
	s3.stopAbandonedHint(t.Context(), cfg, r3)
	assert.Zero(t, r3.stops.Load(), "an exited process has no cycle to stop")
	assert.Equal(t, []string{"skipped-exited"}, o3.stops())

	// A crash mid-cycle kills the API before the exit is reaped.
	s4, o4 := newFPHTestSandbox()
	r4 := &fakeRunner{stopErr: errors.New("connection refused")}
	r4.stopHook = func() { time.AfterFunc(30*time.Millisecond, func() { r4.exited.Store(true) }) }
	s4.stopAbandonedHint(t.Context(), cfg, r4)
	assert.Equal(t, int32(1), r4.stops.Load())
	assert.Equal(t, []string{"skipped-exited"}, o4.stops())
	require.Zero(t, r4.badStopCx.Load())
	assert.Empty(t, o.list(), "stops are not drain outcomes")
}
