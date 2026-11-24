package analyticscollector

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/posthog/posthog-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keysCounted returns the number of distinct counts in the current window.
// The goroutine that sends replaces the window, so it is read under the lock.
func (r *eventRollups) keysCounted() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.window.keys.Load()
}

// rolledUp returns the recorded captures. Call it after Close: the rollup
// goroutine is the one enqueueing them.
func rolledUp(t *testing.T, recorder *captureRecorder) []posthog.Capture {
	t.Helper()

	captures := make([]posthog.Capture, 0, len(recorder.messages))
	for _, msg := range recorder.messages {
		capture, ok := msg.(posthog.Capture)
		require.True(t, ok)
		captures = append(captures, capture)
	}

	return captures
}

func TestCountedTeamEventIsSentOncePerInterval(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		recorder := &captureRecorder{}
		p := newPosthogClient(recorder, teamIdentifyTTL)
		p.startRollups(t.Context(), rollupInterval)

		count := func() {
			p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("lang", "python"))
		}

		count()
		time.Sleep(rollupInterval - time.Second)
		synctest.Wait()
		count()
		time.Sleep(time.Second)
		synctest.Wait()
		count()
		require.NoError(t, p.Close())

		captures := rolledUp(t, recorder)
		require.Len(t, captures, 2, "one event for the interval and one for the rest at Close")

		first := captures[0]
		assert.Equal(t, "listed sandboxes", first.Event)
		assert.Equal(t, "team_team-uuid", first.DistinctId)
		assert.Equal(t, "team-uuid", first.Properties[teamIDKey])
		assert.Equal(t, "team-uuid", first.Groups[teamGroup])
		assert.Equal(t, "python", first.Properties["lang"])
		assert.Equal(t, false, first.Properties[processPersonProfileKey])
		assert.EqualValues(t, 2, first.Properties[rollupCountKey])

		assert.EqualValues(t, 1, captures[1].Properties[rollupCountKey])
	})
}

func TestCountedTeamEventsSplitByTeamEventAndProperties(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newPosthogClient(recorder, teamIdentifyTTL)
	p.startRollups(t.Context(), rollupInterval)

	p.CountAnalyticsTeamEvent(t.Context(), "team-a", "listed sandboxes", posthog.NewProperties().Set("lang", "python"))
	p.CountAnalyticsTeamEvent(t.Context(), "team-a", "listed sandboxes", posthog.NewProperties().Set("lang", "python"))
	p.CountAnalyticsTeamEvent(t.Context(), "team-a", "listed sandboxes", posthog.NewProperties().Set("lang", "js"))
	p.CountAnalyticsTeamEvent(t.Context(), "team-a", "listed environments", posthog.NewProperties().Set("lang", "python"))
	p.CountAnalyticsTeamEvent(t.Context(), "team-b", "listed sandboxes", posthog.NewProperties().Set("lang", "python"))
	require.NoError(t, p.Close())

	counts := map[[3]any]any{}
	for _, capture := range rolledUp(t, recorder) {
		counts[[3]any{capture.Properties[teamIDKey], capture.Event, capture.Properties["lang"]}] = capture.Properties[rollupCountKey]
	}

	assert.Equal(t, map[[3]any]any{
		{"team-a", "listed sandboxes", "python"}:    int64(2),
		{"team-a", "listed sandboxes", "js"}:        int64(1),
		{"team-a", "listed environments", "python"}: int64(1),
		{"team-b", "listed sandboxes", "python"}:    int64(1),
	}, counts)
}

// A caller controls its request headers, so the number of distinct property
// sets in one interval has no natural bound.
func TestCountedTeamEventsAreSentEarlyAtKeyLimit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		recorder := &captureRecorder{}
		p := newPosthogClient(recorder, teamIdentifyTTL)
		p.startRollups(t.Context(), rollupInterval)

		count := func(userAgent int) {
			p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("user_agent", strconv.Itoa(userAgent)))
		}

		for i := range rollupMaxKeys {
			count(i)
		}
		synctest.Wait()

		// Already sent, so this starts a new count instead of adding to the first.
		count(0)
		require.NoError(t, p.Close())

		assert.Len(t, rolledUp(t, recorder), rollupMaxKeys+1)
	})
}

// Header values can be long as well as many.
func TestCountedTeamEventsAreSentEarlyAtByteLimit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		recorder := &captureRecorder{}
		p := newPosthogClient(recorder, teamIdentifyTTL)
		p.startRollups(t.Context(), rollupInterval)

		// Four of these reach the limit and three do not.
		const userAgents = 4

		padding := strings.Repeat("a", rollupMaxBytes/userAgents)
		count := func(userAgent int) {
			p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("user_agent", padding+strconv.Itoa(userAgent)))
		}

		for i := range userAgents {
			count(i)
		}
		synctest.Wait()

		// Already sent, so this starts a new count instead of adding to the first.
		count(0)
		require.NoError(t, p.Close())

		// Not assert.Len: on failure it prints every capture, megabytes here.
		sent := len(rolledUp(t, recorder))
		assert.Equal(t, userAgents+1, sent)
	})
}

// discardClient takes events from many goroutines at once and keeps none.
type discardClient struct {
	posthog.Client
}

func (discardClient) Enqueue(posthog.Message) error { return nil }

func (discardClient) Close() error { return nil }

// Callers that check for room at the same moment must not all get it.
func TestWindowLimitHoldsForConcurrentNewKeys(t *testing.T) {
	t.Parallel()

	const (
		// Eight of these reach the byte limit and seven do not.
		toFill  = 8
		callers = 32
		// The callers have to overlap for the limit to be at risk, which one
		// round does not guarantee.
		rounds = 25
	)

	padding := strings.Repeat("a", rollupMaxBytes/toFill)

	for range rounds {
		p := newPosthogClient(discardClient{}, teamIdentifyTTL)
		// Counting without the goroutine that sends: the window is never taken.
		p.rollups.started = true

		count := func(userAgent int) {
			p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("user_agent", padding+strconv.Itoa(userAgent)))
		}

		for i := range toFill - 1 {
			count(i)
		}

		start := make(chan struct{})

		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				<-start
				count(toFill + i)
			})
		}

		close(start)
		wg.Wait()

		require.EqualValues(t, toFill, p.rollups.keysCounted(), "one caller's key fills the window and the rest are sent by themselves")
	}
}

// posthog-go drops a message when its queue is full. A counted event stands
// for every call in the interval, so it must not be lost with the message.
func TestCountRejectedByFullQueueIsSentWithTheNext(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		recorder := &captureRecorder{enqueueErr: posthog.ErrQueueFull}
		p := newPosthogClient(recorder, teamIdentifyTTL)
		p.startRollups(t.Context(), rollupInterval)

		count := func() {
			p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("lang", "python"))
		}

		for range 3 {
			count()
		}
		time.Sleep(rollupInterval)
		synctest.Wait()

		for range 2 {
			count()
		}
		require.NoError(t, p.Close())

		captures := rolledUp(t, recorder)
		require.Len(t, captures, 1, "the rejected send recorded nothing")
		assert.EqualValues(t, 5, captures[0].Properties[rollupCountKey])
		assert.Equal(t, "python", captures[0].Properties["lang"])
	})
}

// A window full of counts that were put back asks for a send on every
// request, and the queue that rejected them may still be full.
func TestSendAfterFullQueueWaitsForTheTimer(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		recorder := &captureRecorder{enqueueErr: posthog.ErrQueueFull}
		p := newPosthogClient(recorder, teamIdentifyTTL)
		p.startRollups(t.Context(), rollupInterval)

		const userAgents = 4

		padding := strings.Repeat("a", rollupMaxBytes/userAgents)
		count := func(userAgent int) {
			p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("user_agent", padding+strconv.Itoa(userAgent)))
		}

		// The window fills and is sent early; the queue rejects one count,
		// which is put back.
		for i := range userAgents {
			count(i)
		}
		synctest.Wait()
		require.EqualValues(t, 1, p.rollups.keysCounted())

		// It fills again and asks for another early send.
		for i := range userAgents - 1 {
			count(userAgents + i)
		}
		synctest.Wait()
		assert.EqualValues(t, userAgents, p.rollups.keysCounted(), "not sent before the timer")

		time.Sleep(rollupInterval)
		synctest.Wait()
		assert.Zero(t, p.rollups.keysCounted(), "sent on the timer")

		require.NoError(t, p.Close())
	})
}

// The early send runs on another goroutine, so a full window must not grow
// while it waits for that send.
func TestFullWindowTakesNoNewKey(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newPosthogClient(recorder, teamIdentifyTTL)
	// Counting without the goroutine that sends: the window stays full.
	p.rollups.started = true

	const userAgents = 4

	padding := strings.Repeat("a", rollupMaxBytes/userAgents)
	count := func(userAgent int) {
		p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("user_agent", padding+strconv.Itoa(userAgent)))
	}

	for i := range userAgents {
		count(i)
	}
	require.Empty(t, recorder.messages)

	// A key the window already has is still counted there.
	count(0)
	require.Empty(t, recorder.messages)

	// A new one is sent by itself and not kept.
	count(userAgents)

	capture := recorder.capture(t)
	assert.Equal(t, "listed sandboxes", capture.Event)
	assert.Equal(t, "team-uuid", capture.Properties[teamIDKey])
	assert.EqualValues(t, 1, capture.Properties[rollupCountKey])
	assert.EqualValues(t, userAgents, p.rollups.keysCounted())
}

// The count is added to the properties when they are sent, outside any
// request.
func TestCountedTeamEventWithoutProperties(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newPosthogClient(recorder, teamIdentifyTTL)
	p.startRollups(t.Context(), rollupInterval)

	p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", nil)
	require.NoError(t, p.Close())

	captures := rolledUp(t, recorder)
	require.Len(t, captures, 1)
	assert.EqualValues(t, 1, captures[0].Properties[rollupCountKey])
}

// Run with -race: counts land while the interval's counts are being taken.
func TestCountedTeamEventsKeepEveryCountAcrossSends(t *testing.T) {
	t.Parallel()

	const (
		callers = 16
		calls   = 2000
	)

	recorder := &captureRecorder{}
	p := newPosthogClient(recorder, teamIdentifyTTL)
	p.startRollups(t.Context(), time.Millisecond)

	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			for range calls {
				p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties().Set("lang", "python"))
			}
		})
	}
	wg.Wait()
	require.NoError(t, p.Close())

	var total int64
	for _, capture := range rolledUp(t, recorder) {
		n, ok := capture.Properties[rollupCountKey].(int64)
		require.True(t, ok)
		total += n
	}

	assert.EqualValues(t, callers*calls, total)
}

func TestCountAfterCloseIsDropped(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newPosthogClient(recorder, teamIdentifyTTL)
	p.startRollups(t.Context(), rollupInterval)
	require.NoError(t, p.Close())

	p.CountAnalyticsTeamEvent(t.Context(), "team-uuid", "listed sandboxes", posthog.NewProperties())

	assert.Zero(t, p.rollups.keysCounted(), "nothing would send the count")
	assert.Empty(t, recorder.messages)
	require.NoError(t, p.Close(), "a second Close is a no-op")
}
