//go:build linux

package nbd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// blockingProv holds every backend call until release is closed.
type blockingProv struct {
	entered chan struct{}
	release chan struct{}
}

func (p blockingProv) wait() {
	p.entered <- struct{}{}
	<-p.release
}

func (p blockingProv) ReadAt(_ context.Context, b []byte, _ int64) (int, error) {
	p.wait()

	return len(b), nil
}

func (p blockingProv) Size(context.Context) (int64, error) { return 1 << 30, nil }

func (p blockingProv) WriteAt(b []byte, _ int64) (int, error) {
	p.wait()

	return len(b), nil
}

func (p blockingProv) WriteZeroesAt(_, length int64) (int, error) {
	p.wait()

	return int(length), nil
}

// A backend call has to stay visible for as long as it waits, including after
// the dispatcher gave up on it, since a call stuck in the kernel is what the
// oldest-in-flight gauge is for.
func TestDispatchTracksBackendCallsUntilTheyReturn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		op   string
		send func(context.Context, *Dispatch) error
	}{
		{opRead, func(ctx context.Context, d *Dispatch) error {
			return d.cmdRead(ctx, command{op: opRead, handle: 1, length: 4096})
		}},
		{opWrite, func(ctx context.Context, d *Dispatch) error {
			return d.cmdWrite(ctx, command{op: opWrite, handle: 1, length: 4096}, make([]byte, 4096))
		}},
		{opWriteZeroes, func(ctx context.Context, d *Dispatch) error {
			return d.cmdWriteZeroes(ctx, command{op: opWriteZeroes, handle: 1, length: 4096})
		}},
		{opTrim, func(ctx context.Context, d *Dispatch) error {
			return d.cmdWriteZeroes(ctx, command{op: opTrim, handle: 1, length: 4096})
		}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			prov := blockingProv{entered: make(chan struct{}), release: make(chan struct{})}
			d := NewDispatch(&countingConn{}, prov, true, logger.L())
			d.inflight = newInflightTracker()

			ctx, cancel := context.WithCancel(t.Context())
			require.NoError(t, tc.send(ctx, d))
			<-prov.entered

			ages := d.inflight.oldest(time.Now())
			assert.Contains(t, ages, tc.op, "a waiting call is in flight")
			assert.Len(t, ages, 1)

			// The dispatcher answers and moves on, but the backend call has not returned.
			cancel()
			d.Drain()
			assert.Contains(t, d.inflight.oldest(time.Now()), tc.op, "an abandoned call still waiting stays in flight")

			close(prov.release)
			assert.Eventually(t, func() bool { return len(d.inflight.oldest(time.Now())) == 0 },
				time.Second, time.Millisecond, "a returned call leaves the in-flight set")
		})
	}
}

func TestInflightTrackerReportsOldestPerOp(t *testing.T) {
	t.Parallel()

	tracker := newInflightTracker()
	oldID := tracker.begin(opWrite)
	time.Sleep(5 * time.Millisecond)
	newID := tracker.begin(opWrite)
	readID := tracker.begin(opRead)

	now := time.Now()
	ages := tracker.oldest(now)
	require.Len(t, ages, 2)
	assert.GreaterOrEqual(t, ages[opWrite], 5*time.Millisecond, "the older write sets the age")

	tracker.end(oldID)
	assert.Less(t, tracker.oldest(now)[opWrite], ages[opWrite], "ending the oldest moves the age to the next")

	tracker.end(newID)
	tracker.end(readID)
	assert.Empty(t, tracker.oldest(now))
}

func (t *inflightTracker) oldest(now time.Time) map[string]time.Duration {
	ages := make(map[string]time.Duration)
	for op, stat := range t.stats(now) {
		ages[op] = stat.oldest
	}

	return ages
}
