//go:build linux

package fc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A tick already pending when exit closes must not trigger a flush. Both
// channels are ready on every iteration, so the select must choose between
// them; repetition makes an unguarded loop fail despite the random pick.
func TestRunMetricsFlushLoop_NoFlushAfterExit(t *testing.T) {
	t.Parallel()

	const iterations = 1000

	exit := make(chan struct{})
	close(exit)

	ticks := make(chan time.Time, 1)
	flushes := 0

	for range iterations {
		// Re-arm the tick; the loop may or may not have consumed the last one.
		select {
		case <-ticks:
		default:
		}
		ticks <- time.Now()

		runMetricsFlushLoop(
			t.Context(),
			exit,
			ticks,
			func(context.Context) error {
				flushes++

				return nil
			},
			func(error) { t.Error("unexpected flush error") },
		)
	}

	assert.Zero(t, flushes, "flushed Firecracker metrics after the VM had exited")
}

func TestRunMetricsFlushLoop_FlushesWhileRunning(t *testing.T) {
	t.Parallel()

	exit := make(chan struct{})
	ticks := make(chan time.Time)
	flushed := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runMetricsFlushLoop(
			t.Context(),
			exit,
			ticks,
			func(context.Context) error {
				flushed <- struct{}{}

				return nil
			},
			func(error) { t.Error("unexpected flush error") },
		)
	}()

	ticks <- time.Now()
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("tick did not trigger a flush")
	}

	close(exit)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop after exit")
	}
}

func TestRunMetricsFlushLoop_ReportsFlushError(t *testing.T) {
	t.Parallel()

	exit := make(chan struct{})
	ticks := make(chan time.Time)
	errs := make(chan error, 1)

	go runMetricsFlushLoop(
		t.Context(),
		exit,
		ticks,
		func(context.Context) error { return assert.AnError },
		func(err error) { errs <- err },
	)

	ticks <- time.Now()
	select {
	case err := <-errs:
		require.ErrorIs(t, err, assert.AnError)
	case <-time.After(5 * time.Second):
		t.Fatal("flush error was not reported")
	}

	close(exit)
}

// Firecracker can exit while a flush request is in flight, after the pre-flush
// re-check: the flush then fails with a shutdown reset that must be swallowed,
// not reported. The flush closes exit (as the cmd.Wait goroutine would) and
// returns an error; the loop must return without calling onFlushErr.
func TestRunMetricsFlushLoop_SuppressesErrorWhenExitRacesFlush(t *testing.T) {
	t.Parallel()

	exit := make(chan struct{})
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()

	done := make(chan struct{})
	go func() {
		defer close(done)
		runMetricsFlushLoop(
			t.Context(),
			exit,
			ticks,
			func(context.Context) error {
				close(exit)

				return assert.AnError
			},
			func(error) { t.Error("reported a flush error that was really VM shutdown") },
		)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop after exit raced the flush")
	}
}

// The flush-and-read wait keys on the line's own timestamp, not on "the
// accumulator moved": a line from the periodic flusher that predates the
// request does not satisfy it, one stamped at or after the request does.
func TestWaitMetricsLineSince(t *testing.T) {
	t.Parallel()
	p := &Process{}
	sinceMs := time.Now().UnixMilli()

	p.metricsLineMs.Store(sinceMs - 1)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()
	require.Error(t, p.waitMetricsLineSince(ctx, sinceMs), "an older line is not this flush")

	p.metricsLineMs.Store(sinceMs)
	require.NoError(t, p.waitMetricsLineSince(t.Context(), sinceMs), "a line stamped at the request satisfies it")

	p2 := &Process{}
	time.AfterFunc(20*time.Millisecond, func() { p2.metricsLineMs.Store(sinceMs + 5) })
	require.NoError(t, p2.waitMetricsLineSince(t.Context(), sinceMs))
}

// A metrics line's timestamp decodes alongside the balloon counters.
func TestFirecrackerMetricsDecodeTimestamp(t *testing.T) {
	t.Parallel()
	var m firecrackerMetrics
	require.NoError(t, json.Unmarshal([]byte(`{"utc_timestamp_ms": 1739000000000, "balloon": {"free_page_hint_freed": 4096}}`), &m))
	assert.Equal(t, int64(1739000000000), m.UtcTimestampMs)
	assert.Equal(t, uint64(4096), m.Balloon.FreePageHintFreed)
}
