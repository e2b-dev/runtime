//go:build linux

package userfaultfd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func newTestCounterReporter(t *testing.T) (*counterReporter, *observer.ObservedLogs, *time.Time) {
	t.Helper()

	core, logs := observer.New(zapcore.DebugLevel)
	c := newCounterReporter(logger.NewTracedLoggerFromCore(core), "test counter")
	now := time.Unix(1_000, 0)
	c.now = func() time.Time { return now }

	return c, logs, &now
}

// The serve loop calls Log on every pass that handles faults. A busy sandbox
// must still get one line per window, carrying everything since the last one.
func TestCounterReporterLogWaitsForInterval(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	c, logs, now := newTestCounterReporter(t)

	for range 1_000 {
		c.Increase("EMPTY_DRAIN")
		c.Log(ctx)
		*now = now.Add(time.Millisecond)
	}
	assert.Equal(t, 0, logs.Len(), "1 s of events is inside the window")

	*now = now.Add(counterReportInterval)
	c.Increase("EMPTY_DRAIN")
	c.Log(ctx)

	entries := logs.TakeAll()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	assert.Equal(t, uint64(1_001), fields["count"])
	assert.Equal(t, uint64(1_001), fields["EMPTY_DRAIN"])
	assert.Equal(t, false, fields["closing"])

	// The window restarts at the next event, not at the flush.
	*now = now.Add(time.Hour)
	c.Log(ctx)
	assert.Equal(t, 0, logs.Len(), "nothing accumulated, nothing to write")

	c.Increase("EMPTY_DRAIN")
	*now = now.Add(counterReportInterval - time.Nanosecond)
	c.Log(ctx)
	assert.Equal(t, 0, logs.Len(), "window measured from the first event after the flush")

	*now = now.Add(time.Nanosecond)
	c.Log(ctx)
	assert.Equal(t, 1, logs.Len())
}

// Close writes what the window has not flushed yet, so short-lived sandboxes
// still report their counts.
func TestCounterReporterCloseFlushesInsideWindow(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	c, logs, _ := newTestCounterReporter(t)

	c.Increase("POLLIN")
	c.Increase("POLLIN")
	c.Log(ctx)
	require.Equal(t, 0, logs.Len())

	c.Close(ctx)

	entries := logs.TakeAll()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	assert.Equal(t, uint64(2), fields["count"])
	assert.Equal(t, true, fields["closing"])

	c.Close(ctx)
	assert.Equal(t, 0, logs.Len(), "a second Close has nothing left")
}
