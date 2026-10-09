package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is the reading the clock correction is measured with; tests advance it at the
// points of the handler they want the correction to span.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

// recordClockSteps sends the API's clock steps to a recorder in place of the system clock, so
// a test asserts which targets were stepped to without ever moving the host's clock.
func recordClockSteps(api *API) func() []time.Time {
	var mu sync.Mutex
	var steps []time.Time
	api.setSystemTime = func(t time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		steps = append(steps, t)

		return nil
	}

	return func() []time.Time {
		mu.Lock()
		defer mu.Unlock()

		return append([]time.Time(nil), steps...)
	}
}

// clockFixture is an API whose clock correction spans exactly handlerDelay: the clock moves
// by that much inside the MMDS token check, between the body read and the clock gates.
func clockFixture(t *testing.T, handlerDelay time.Duration) (*API, func() []time.Time, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer
	api := newAPIWithCgroupManagerLogging(&fakeCgroupManager{}, &logs)
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	api.now = clk.now
	api.mmdsClient = &mockMMDSClient{onGet: func() { clk.advance(handlerDelay) }}

	return api, recordClockSteps(api), &logs
}

func initAt(t *testing.T, api *API, ts time.Time) *httptest.ResponseRecorder {
	t.Helper()

	rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{Timestamp: &ts})
	require.Equal(t, http.StatusNoContent, rec.Code)

	return rec
}

// The clock is stepped to the timestamp plus the handler's own time from the body read to
// the gates, and that term is answered in whole milliseconds, truncated, and logged exactly.
func TestPostInit_ClockCorrectionAddsTheHandlerDelay(t *testing.T) {
	t.Parallel()

	const delay = 300*time.Millisecond + 900*time.Microsecond
	api, steps, logs := clockFixture(t, delay)
	ts := time.Now().Add(time.Hour)

	rec := initAt(t, api, ts)

	require.Len(t, steps(), 1)
	assert.Equal(t, ts.Add(delay).UnixNano(), steps()[0].UnixNano())
	assert.Equal(t, "300", rec.Header().Get(clockCorrectionHeader), "whole milliseconds, truncated")

	var logged float64
	for line := range strings.SplitSeq(logs.String(), "\n") {
		var fields map[string]any
		if json.Unmarshal([]byte(line), &fields) == nil && fields["clock_correction"] != nil {
			logged = fields["clock_correction"].(float64)
		}
	}
	assert.InDelta(t, 300.9, logged, 1e-9, "the log carries the exact term, in milliseconds")
}

// bodyThatTakes advances the clock while the handler reads it: the correction must not
// include the body's own transit, which happens before the reading it is measured from.
type bodyThatTakes struct {
	r   io.Reader
	clk *fakeClock
	d   time.Duration
}

func (b *bodyThatTakes) Read(p []byte) (int, error) {
	b.clk.advance(b.d)

	return b.r.Read(p)
}

// The correction is measured from the end of the body read, not from the handler's entry.
func TestPostInit_ClockCorrectionStartsAtTheBodyRead(t *testing.T) {
	t.Parallel()

	api := newAPIWithCgroupManager(&fakeCgroupManager{})
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	api.now = clk.now
	api.mmdsClient = &mockMMDSClient{onGet: func() { clk.advance(300 * time.Millisecond) }}
	steps := recordClockSteps(api)

	ts := time.Now().Add(time.Hour)
	raw, err := json.Marshal(PostInitJSONBody{Timestamp: &ts})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", &bodyThatTakes{r: bytes.NewReader(raw), clk: clk, d: time.Hour})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	api.PostInit(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "300", rec.Header().Get(clockCorrectionHeader))
	require.Len(t, steps(), 1)
	assert.Equal(t, ts.Add(300*time.Millisecond).UnixNano(), steps()[0].UnixNano())
}

// The step's bands apply to the corrected target, with a 1 s handler delay. A timestamp 40 ms
// ahead of the guest is inside the 50 ms band on its own, but 1.04 s ahead once corrected, so
// it steps; one 5.5 s behind would step the clock back on its own, but is 4.5 s behind once
// corrected, inside the 5 s band, so it is ignored. The bands read the real guest clock, so
// the 0.5 s left to the band's edge is the test's slack for its own running time. The header is
// answered either way.
func TestPostInit_ClockCorrectionMeetsTheBands(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		offset time.Duration
		steps  bool
	}{
		{name: "40 ms ahead steps", offset: 40 * time.Millisecond, steps: true},
		{name: "5.5 s behind is ignored", offset: -5500 * time.Millisecond, steps: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api, steps, _ := clockFixture(t, time.Second)
			ts := time.Now().Add(tt.offset)

			rec := initAt(t, api, ts)

			assert.Equal(t, "1000", rec.Header().Get(clockCorrectionHeader))
			if tt.steps {
				require.Len(t, steps(), 1)
				assert.Equal(t, ts.Add(time.Second).UnixNano(), steps()[0].UnixNano())
			} else {
				assert.Empty(t, steps())
			}
		})
	}
}

// The newest-wins rule compares corrected values: one older than the last accepted is refused
// and steps nothing, one equal to it is accepted, and both still answer the header.
func TestPostInit_ClockCorrectionMeetsNewestWins(t *testing.T) {
	t.Parallel()

	api, steps, _ := clockFixture(t, 300*time.Millisecond)
	ts := time.Now().Add(time.Hour)
	initAt(t, api, ts)
	require.Len(t, steps(), 1)

	rec := initAt(t, api, ts.Add(-time.Second))
	assert.Equal(t, "300", rec.Header().Get(clockCorrectionHeader))
	assert.Len(t, steps(), 1, "an older corrected timestamp is refused")

	rec = initAt(t, api, ts)
	assert.Equal(t, "300", rec.Header().Get(clockCorrectionHeader))
	assert.Len(t, steps(), 2, "an equal corrected timestamp is accepted")
}

// A request with no timestamp has nothing to correct: no header, no step.
func TestPostInit_ClockCorrectionNeedsATimestamp(t *testing.T) {
	t.Parallel()

	api, steps, _ := clockFixture(t, 300*time.Millisecond)

	rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{})

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.NotContains(t, rec.Header(), clockCorrectionHeader)
	assert.Empty(t, steps())
}
