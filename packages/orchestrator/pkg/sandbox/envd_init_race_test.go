//go:build linux

package sandbox

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// thawingEnvd stands in for envd's /init handler, whose completion thaws the workload. Each
// handler reads the body, works for work or until its client has gone, whichever comes
// first (the first slowFor handlers only, then none), and
// on completion counts a thaw, observed when its client is still there and unobserved when
// it is gone. It answers with the unobserved count since the last observed handler, as
// envd's X-Envd-Init-Unobserved does, and dropFirst makes it close the first handler's
// connection instead of answering.
type thawingEnvd struct {
	srv        *httptest.Server
	work       time.Duration
	slowFor    int64
	dropFirst  bool
	n          atomic.Int64
	handlers   sync.WaitGroup
	mu         sync.Mutex
	observed   int64
	unobserved int64
	pending    int64
}

func newThawingEnvd(t *testing.T, work time.Duration, slowFor int64, dropFirst bool) *thawingEnvd {
	t.Helper()

	e := &thawingEnvd{work: work, slowFor: slowFor, dropFirst: dropFirst}
	e.srv = httptest.NewServer(http.HandlerFunc(e.serve))
	t.Cleanup(e.srv.Close)

	return e
}

func (e *thawingEnvd) serve(w http.ResponseWriter, r *http.Request) {
	e.handlers.Add(1)
	defer e.handlers.Done()

	_, _ = io.Copy(io.Discard, r.Body)
	n := e.n.Add(1)
	if n == 1 && e.dropFirst {
		// Counted before the connection closes: the close releases the client's re-send,
		// whose response must carry this count.
		e.thaw(false)
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			conn.Close()
		}

		return
	}
	if n <= e.slowFor {
		select {
		case <-time.After(e.work):
		case <-r.Context().Done():
		}
	}

	if r.Context().Err() != nil {
		e.thaw(false)

		return
	}
	pending := e.thaw(true)
	w.Header().Set("X-Envd-Version", "0.10.0")
	w.Header().Set(envdUnobservedHeader, strconv.FormatInt(pending, 10))
	w.WriteHeader(http.StatusNoContent)
}

// thaw counts one completed handler and, for an observed one, returns and resets the
// unobserved count it carries.
func (e *thawingEnvd) thaw(observed bool) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !observed {
		e.unobserved++
		e.pending++

		return 0
	}
	e.observed++
	p := e.pending
	e.pending = 0

	return p
}

// settled waits for every handler to complete and returns the observed and unobserved
// thaws.
func (e *thawingEnvd) settled() (observed, unobserved int64) {
	e.handlers.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	return e.observed, e.unobserved
}

// TestHeldInitLeavesNoUnobservedThaw is the early-thaw race as a test: every handler's
// completion thaws, and the first three are slow. The legacy loop, abandoning each attempt at
// 50 ms, leaves handlers that thaw after their client has gone: each works until its client
// leaves (bounded at 10 s), so the abandonment itself, not a timer, decides that they thaw
// unobserved. The held request is the one handler, slow for 200 ms, and it is observed.
func TestHeldInitLeavesNoUnobservedThaw(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		held           bool
		work           time.Duration
		wantUnobserved bool
	}{
		{name: "legacy loop at 50 ms", held: false, work: 10 * time.Second, wantUnobserved: true},
		{name: "held request", held: true, work: 200 * time.Millisecond, wantUnobserved: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newThawingEnvd(t, tc.work, 3, false)
			s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", tc.held)
			s.internalConfig.EnvdInitRequestTimeout = 50 * time.Millisecond
			require.NoError(t, initHeld(t, s))

			observed, unobserved := e.settled()
			if tc.wantUnobserved {
				// The double counts a handler observed before its response is written, and the
				// legacy loop can abandon even that one at 50 ms and retry: at least one.
				assert.GreaterOrEqual(t, observed, int64(1), "a handler thawed with its client there")
				assert.Positive(t, unobserved, "abandoned handlers thawed with no client")
			} else {
				assert.Equal(t, int64(1), observed, "one handler thawed with its client there: the served one")
				assert.Zero(t, unobserved, "the held request leaves no handler behind")
			}
		})
	}
}

// TestHeldInitCountsTheThawOfADroppedConnection: the one unobserved thaw the held path keeps
// is a delivered request whose connection died. envd counts it, the re-sent /init carries
// the count, and the start ends on that /init's observed thaw.
func TestHeldInitCountsTheThawOfADroppedConnection(t *testing.T) {
	t.Parallel()

	e := newThawingEnvd(t, 0, 0, true)
	s, version := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	require.NoError(t, initHeld(t, s))

	observed, unobserved := e.settled()
	assert.Equal(t, int64(1), observed, "the re-sent /init thawed with its client there")
	assert.Equal(t, int64(1), unobserved, "the dropped one thawed with no client")

	points := counterPointsFor(t, telemetry.EnvdInitUnobservedHandlers, version)
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value, "the re-sent /init's count reaches the counter")
}

// TestHeldInitAgainstAnEnvdThatPredatesIt: a held start on an envd with none of the release's
// rules -- a plain server that thaws inline and sends no clock correction -- still succeeds,
// since the 100 Continue is net/http's, and records no clock correction: a held start with no
// correction sample is a version the gate admitted that lacks it.
func TestHeldInitAgainstAnEnvdThatPredatesIt(t *testing.T) {
	t.Parallel()

	var expect atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expect.Store(r.Header.Get("Expect"))
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s, version := newHeldSandbox(t, srv.URL, "0.10.0", true)
	require.NoError(t, initHeld(t, s))

	assert.Equal(t, "100-continue", expect.Load())
	assert.Equal(t, envdInitLoopHeld, s.envdInitLoop())
	assert.Len(t, timestampAgePoints(t, version), 1)
	assert.Empty(t, floatHistogramPointsFor(t, telemetry.EnvdInitClockCorrectionHistogramName, version))
	assert.Empty(t, counterPointsFor(t, telemetry.EnvdInitUnobservedHandlers, version))
}

// TestNoCustomerInitSendsAThawField: whichever loop and caller, a customer /init leaves the
// thaw to envd's handler.
func TestNoCustomerInitSendsAThawField(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		held         bool
		recordStart  bool
		reinitHandle bool
	}{
		{name: "legacy first start", held: false, recordStart: true},
		{name: "held first start", held: true, recordStart: true},
		{name: "held later /init (the re-checks)", held: true, recordStart: false},
		{name: "held re-init", held: true, reinitHandle: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", tc.held)
			opts := envdInitOptions{budget: 5 * time.Second}
			if tc.reinitHandle {
				opts.reinit = newEnvdReinit(func(error) {})
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, s.initEnvd(ctx, StartTypeResume, tc.recordStart, opts))

			reqs := e.requests()
			require.Len(t, reqs, 1)
			assert.Empty(t, reqs[0].body.Thaw)
			assert.NotContains(t, string(reqs[0].rawBody), `"thaw"`, "the field is absent, not empty")
			assert.Contains(t, string(reqs[0].rawBody), `"timestamp"`, "the assertion reads the body envd got")
		})
	}
}
