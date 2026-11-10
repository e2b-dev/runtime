package sandbox

import (
	"errors"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAttemptPhaseRecordsFailedDial(t *testing.T) {
	t.Parallel()

	var phase attemptPhase
	trace := httptrace.ContextClientTrace(phase.trace(t.Context()))
	trace.ConnectStart("tcp", "127.0.0.1:1")
	start := time.Now().Add(-envdInitSlowDial - time.Millisecond)
	phase.dialStart.Store(&start)
	trace.ConnectDone("tcp", "127.0.0.1:1", errors.New("connection refused"))
	var attempts envdInitAttempts
	attempts.dialed(phase.dial())
	assert.Equal(t, int64(1), attempts.slowDials)
	assert.Greater(t, attempts.maxDial, envdInitSlowDial)
}

// A start's attempts land on its span: the count, the failures by phase, the
// slow connects, and the served attempt's connect time.
func TestEnvdInitAttemptsSpanAttrs(t *testing.T) {
	t.Parallel()

	var a envdInitAttempts
	missing := attrMap(a.spanAttrs())
	assert.NotContains(t, missing, "envd.init.max_dial_ms")
	assert.NotContains(t, missing, "envd.init.dial_ms")
	assert.NotContains(t, missing, "envd.init.served_age_ms")

	a.count = 3
	a.fail(envdInitPhaseConnect)
	a.fail(envdInitPhaseConnect)
	a.dialed(envdInitSlowDial + time.Millisecond)
	a.dialed(10 * time.Millisecond)
	a.dialed(2 * envdInitSlowDial)
	a.servedDial = 2 * envdInitSlowDial
	a.servedAge = 70 * time.Millisecond

	got := attrMap(a.spanAttrs())
	assert.Equal(t, int64(3), got["envd.init.attempts"])
	assert.Equal(t, int64(2), got["envd.init.failed"])
	assert.Equal(t, int64(2), got["envd.init.failed.connect"])
	assert.NotContains(t, got, "envd.init.failed.write")
	assert.Equal(t, int64(2), got["envd.init.slow_dials"])
	assert.InDelta(t, float64(2*envdInitSlowDial)/float64(time.Millisecond), got["envd.init.max_dial_ms"], 1e-9)
	assert.InDelta(t, 1000, got["envd.init.dial_ms"], 1e-9)
	assert.InDelta(t, 70, got["envd.init.served_age_ms"], 1e-9)
}

func TestEnvdInitReportSpanAttrs(t *testing.T) {
	t.Parallel()

	assert.Empty(t, envdInitReport{}.spanAttrs(), "a response that reported nothing adds nothing")

	correction := 12 * time.Millisecond
	unobserved := int64(1)
	got := attrMap(envdInitReport{clockCorrection: &correction, unobserved: &unobserved}.spanAttrs())
	assert.InDelta(t, 12, got["envd.init.clock_correction_ms"], 1e-9)
	assert.Equal(t, int64(1), got["envd.init.unobserved_handlers"])
}
