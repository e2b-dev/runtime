package analyticscollector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/posthog/posthog-go"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	// rollupInterval is how often counted events are sent.
	rollupInterval = 5 * time.Minute

	// rollupMaxKeys and rollupMaxBytes are the number of distinct counts, and
	// the bytes of their keys, that one interval holds. Request headers are
	// part of what is counted and a caller controls both how many different
	// ones it sends and how long they are, so the interval alone does not
	// bound memory. A window that reaches either limit is sent early, and
	// until it is taken it accepts no new key: a call that would add one is
	// sent as its own event. Such a caller costs one event per request, as it
	// did before counting. The key that reaches the byte limit is kept, so a
	// window holds at most rollupMaxBytes and one key more.
	//
	// posthog-go rejects a message when its queue is full. rollupMaxKeys is
	// half of that queue's default size, which leaves room for the other
	// events but does not rule out a full queue; sendRollups keeps the counts
	// that did not fit.
	rollupMaxKeys  = 5_000
	rollupMaxBytes = 4 << 20

	// rollupCountKey holds the number of occurrences a counted event stands
	// for.
	rollupCountKey = "count"
)

// rollupWindow holds what was counted since the last send.
type rollupWindow struct {
	// counts maps a rollup key to its *rollupCount.
	counts sync.Map
	keys   atomic.Int64
	// bytes is the total length of the keys. A count holds its property
	// values a second time, so the window holds about twice this.
	bytes atomic.Int64
	// admitting is held to add a key, so that the two totals above are
	// checked and raised in one step.
	admitting sync.Mutex
}

// rollupCount is the number of occurrences of one event for one team with one
// set of properties.
type rollupCount struct {
	key        string
	teamID     string
	event      string
	properties posthog.Properties
	n          atomic.Int64
}

// eventRollups counts events between sends.
type eventRollups struct {
	// mu guards window and stopped. Counting holds it shared and taking the
	// window holds it exclusively, so no count lands in a window that was
	// already taken.
	mu     sync.RWMutex
	window *rollupWindow
	// stopped is set when the last window is taken. Nothing sends what is
	// counted after that, so it is dropped.
	stopped bool

	// sendNow asks for a send before the interval ends.
	sendNow chan struct{}

	started  bool
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

func newEventRollups() *eventRollups {
	return &eventRollups{
		window:  &rollupWindow{},
		sendNow: make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// add counts one occurrence. It reports false when the window is full and
// has no count for this key yet, in which case nothing was counted.
func (r *eventRollups) add(teamID, event string, properties posthog.Properties) bool {
	// Nothing sends the counts of a client that was never started.
	if !r.started {
		return true
	}

	key := rollupKey(teamID, event, properties)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.stopped {
		return true
	}

	count, created := r.window.count(key, teamID, event, properties)
	if count == nil {
		r.requestSend()

		return false
	}

	count.n.Add(1)

	if created && r.window.full() {
		r.requestSend()
	}

	return true
}

// putBack adds a count that could not be sent to the current window, so the
// next send carries it. It reports false when the window cannot take it: the
// window is full and has no count for this key, or counting has stopped.
func (r *eventRollups) putBack(unsent *rollupCount, n int64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.stopped {
		return false
	}

	count, _ := r.window.count(unsent.key, unsent.teamID, unsent.event, unsent.properties)
	if count == nil {
		return false
	}

	count.n.Add(n)

	return true
}

// count returns the window's count for a key and whether this call created
// it. It returns nil when the window is full and has no count for the key.
func (w *rollupWindow) count(key, teamID, event string, properties posthog.Properties) (*rollupCount, bool) {
	if count := w.load(key); count != nil {
		return count, false
	}

	// Callers that all found room before any of them took it would together
	// go past the limits, so new keys are let in one at a time. A key the
	// window already has does not come here.
	w.admitting.Lock()
	defer w.admitting.Unlock()

	if count := w.load(key); count != nil {
		return count, false
	}

	if w.full() {
		return nil, false
	}

	count := &rollupCount{
		key:        key,
		teamID:     teamID,
		event:      event,
		properties: copyProperties(properties),
	}

	w.counts.Store(key, count)
	w.keys.Add(1)
	w.bytes.Add(int64(len(key)))

	return count, true
}

func (w *rollupWindow) load(key string) *rollupCount {
	counted, ok := w.counts.Load(key)
	if !ok {
		return nil
	}

	count, _ := counted.(*rollupCount)

	return count
}

// full reports whether the window has reached a limit.
func (w *rollupWindow) full() bool {
	return w.keys.Load() >= rollupMaxKeys || w.bytes.Load() >= rollupMaxBytes
}

// requestSend asks for a send before the interval ends.
func (r *eventRollups) requestSend() {
	select {
	case r.sendNow <- struct{}{}:
	default:
	}
}

// take returns what was counted so far and starts a new window.
func (r *eventRollups) take() *rollupWindow {
	r.mu.Lock()
	defer r.mu.Unlock()

	window := r.window
	r.window = &rollupWindow{}

	return window
}

// takeLast returns what was counted so far and stops the counting.
func (r *eventRollups) takeLast() *rollupWindow {
	r.mu.Lock()
	defer r.mu.Unlock()

	window := r.window
	r.window = &rollupWindow{}
	r.stopped = true

	return window
}

// copyProperties returns a copy the count can be added to, which a nil map
// is not.
func copyProperties(properties posthog.Properties) posthog.Properties {
	if properties == nil {
		return posthog.NewProperties()
	}

	return maps.Clone(properties)
}

// rollupKey identifies a team, an event and a set of properties.
func rollupKey(teamID, event string, properties posthog.Properties) string {
	var key strings.Builder

	key.WriteString(teamID)
	key.WriteByte(0)
	key.WriteString(event)

	for _, name := range slices.Sorted(maps.Keys(properties)) {
		key.WriteByte(0)
		key.WriteString(name)
		key.WriteByte(0)
		fmt.Fprint(&key, properties[name])
	}

	return key.String()
}

// CountAnalyticsTeamEvent counts one occurrence of an event about a team
// instead of sending it. Every rollupInterval, and once more on Close, the
// client sends one event for each team, event and set of properties counted,
// with the number of occurrences in its count property. It is for events
// that happen too often to send one by one, where the count is what is read.
// What is counted after Close is dropped.
func (p *PosthogClient) CountAnalyticsTeamEvent(ctx context.Context, teamID, event string, properties posthog.Properties) {
	if p.rollups.add(teamID, event, properties) {
		return
	}

	// The window is full, so this occurrence goes out by itself and nothing
	// of it is kept.
	p.CreateAnalyticsTeamEvent(ctx, teamID, event, copyProperties(properties).Set(rollupCountKey, int64(1)))
}

// startRollups sends the counted events every interval until Close. Call it
// once, before the client is shared.
func (p *PosthogClient) startRollups(ctx context.Context, interval time.Duration) {
	p.rollups.started = true

	go p.runRollups(context.WithoutCancel(ctx), interval)
}

func (p *PosthogClient) runRollups(ctx context.Context, interval time.Duration) {
	defer close(p.rollups.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// held is set when the last send put counts back because the posthog-go
	// queue was full. The next send then waits for the timer: a window that
	// is full of counts put back would otherwise ask for a send on every
	// request while the queue stays full.
	held := false

	for {
		select {
		case <-ticker.C:
		case <-p.rollups.sendNow:
			if held {
				continue
			}
		case <-p.rollups.stop:
			p.sendRollups(ctx, p.rollups.takeLast())

			return
		}

		held = p.sendRollups(ctx, p.rollups.take())
	}
}

// stopRollups sends what is still counted and ends the sends.
func (p *PosthogClient) stopRollups() {
	p.rollups.stopOnce.Do(func() { close(p.rollups.stop) })

	if p.rollups.started {
		<-p.rollups.done
	}
}

// sendRollups sends the counts of a window that was taken. posthog-go drops
// a message when its queue is full, and here one message stands for every
// occurrence counted, so a count the queue rejected is put back into the
// current window for the next send. It reports whether any was put back.
// What cannot be put back, and what fails for another reason, is lost and
// logged once for the window.
func (p *PosthogClient) sendRollups(ctx context.Context, window *rollupWindow) bool {
	var (
		putBack     bool
		lostEvents  int64
		lostCounted int64
		lostErr     error
	)

	window.counts.Range(func(_, counted any) bool {
		count, ok := counted.(*rollupCount)
		if !ok {
			return true
		}

		n := count.n.Load()

		// The count goes on a copy, so a count put back keeps the properties
		// it was counted with.
		err := p.capture(teamDistinctIDPrefix+count.teamID, count.teamID, count.event, maps.Clone(count.properties).Set(rollupCountKey, n))

		switch {
		case err == nil:
		case errors.Is(err, posthog.ErrQueueFull) && p.rollups.putBack(count, n):
			putBack = true
		default:
			lostEvents++
			lostCounted += n
			lostErr = err
		}

		return true
	})

	if lostEvents > 0 {
		logger.L().Error(ctx, "counted events were not sent to Posthog",
			zap.Int64("events", lostEvents),
			zap.Int64("occurrences", lostCounted),
			zap.Error(lostErr),
		)
	}

	return putBack
}
