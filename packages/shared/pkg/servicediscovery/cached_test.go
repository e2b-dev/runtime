package servicediscovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// refreshInterval outlasts every test here: none waits for a second read.
const refreshInterval = 10 * time.Second

// scriptedLister returns each canned result in turn, repeating the last.
type scriptedLister struct {
	mu      sync.Mutex
	results []listResult
	calls   int
}

type listResult struct {
	instances []Instance
	err       error
}

func (l *scriptedLister) Start(context.Context) {}
func (l *scriptedLister) Stop(context.Context)  {}

func (l *scriptedLister) ListInstances(context.Context) ([]Instance, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r := l.results[min(l.calls, len(l.results)-1)]
	l.calls++

	return r.instances, r.err
}

func (l *scriptedLister) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.calls
}

func startCached(t *testing.T, lister Discoverer) Discoverer {
	t.Helper()

	d := Cached(lister, logger.NewNopLogger(), refreshInterval)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	d.Start(ctx)
	t.Cleanup(func() { d.Stop(ctx) })

	return d
}

// Before the first refresh lands there is no set. Reporting an empty one with
// no error is what let a source that had never been reached read as a source
// with nothing on it.
func TestCached_ReportsNotYetSyncedBeforeTheFirstRefresh(t *testing.T) {
	t.Parallel()

	d := Cached(&scriptedLister{results: []listResult{{instances: []Instance{{WorkloadID: "a"}}}}}, logger.NewNopLogger(), refreshInterval)

	instances, err := d.ListInstances(t.Context())
	require.ErrorIs(t, err, ErrNotYetSynced)
	assert.Empty(t, instances)
}

func TestCached_ServesTheRefreshedSet(t *testing.T) {
	t.Parallel()

	d := startCached(t, &scriptedLister{results: []listResult{{instances: []Instance{{WorkloadID: "a", IPAddress: "10.0.0.1"}}}}})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		instances, err := d.ListInstances(t.Context())
		assert.NoError(c, err)
		assert.Len(c, instances, 1)
	}, 5*time.Second, 10*time.Millisecond)
}

// The set is kept — a source that blinks must not empty the fleet — but the
// failure is reported, so a caller skips the cycle instead of reconciling
// against a set it cannot trust. The two adapter families disagreeing about
// this is what made a dead source read as an indefinitely stale one.
func TestCached_KeepsTheSetButReportsAFailedRefresh(t *testing.T) {
	t.Parallel()

	refreshFailed := errors.New("source unreachable")
	lister := &scriptedLister{results: []listResult{
		{instances: []Instance{{WorkloadID: "a", IPAddress: "10.0.0.1"}}},
		{err: refreshFailed},
	}}
	d := startCached(t, lister)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, lister.callCount(), 1)
		_, err := d.ListInstances(t.Context())
		assert.NoError(c, err)
	}, 5*time.Second, 10*time.Millisecond, "the first refresh must land before the failing one")

	// Drive the failing refresh directly rather than waiting out the interval.
	d.(*cachedDiscovery).refresh(t.Context())

	instances, err := d.ListInstances(t.Context())
	require.ErrorIs(t, err, refreshFailed)
	assert.Equal(t, []Instance{{WorkloadID: "a", IPAddress: "10.0.0.1"}}, instances,
		"the last known set is kept so a blip does not empty the fleet")
}

func TestCached_StopEndsTheRefreshLoop(t *testing.T) {
	t.Parallel()

	lister := &scriptedLister{results: []listResult{{instances: []Instance{{WorkloadID: "a"}}}}}
	d := Cached(lister, logger.NewNopLogger(), refreshInterval)

	ctx := t.Context()
	d.Start(ctx)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, lister.callCount(), 1)
	}, 5*time.Second, 10*time.Millisecond)

	d.Stop(ctx)
	settled := lister.callCount()
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, settled, lister.callCount(), "no refresh may run after Stop")
}
