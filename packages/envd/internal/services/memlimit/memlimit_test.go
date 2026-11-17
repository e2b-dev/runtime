package memlimit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/host"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

const (
	mib = 1024 * 1024
	gib = 1024 * mib
)

type fakeLimiter struct {
	mu     sync.Mutex
	writes map[cgroups.ProcessType][]uint64
	fail   error
}

func (f *fakeLimiter) SetMemoryLimit(procType cgroups.ProcessType, limit uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	if f.writes == nil {
		f.writes = map[cgroups.ProcessType][]uint64{}
	}
	f.writes[procType] = append(f.writes[procType], limit)

	return nil
}

func (f *fakeLimiter) setFail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = err
}

func (f *fakeLimiter) written(procType cgroups.ProcessType) []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]uint64(nil), f.writes[procType]...)
}

type harness struct {
	limiter  *fakeLimiter
	memTotal atomic.Uint64
	readErr  atomic.Pointer[error]
	// added overrides the fake /proc/iomem; by default it reports everything above 1 GiB
	// as added by virtio-mem, as the kernel's record would.
	added  atomic.Pointer[func() (uint64, error)]
	notify chan func()
	logs   *syncBuffer
	mgr    Manager

	// syncs holds the reasons each completed recompute covered, in order.
	syncsMu sync.Mutex
	syncs   [][]string
	seq     atomic.Int64
}

// withSynced reports every completed recompute's reasons; tests wait on it instead of
// any production state.
func withSynced(f func(reasons []string)) Option {
	return func(m *manager) { m.synced = f }
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

// newHarness starts a manager over a fake MemTotal of 1 GiB and limiter. The periodic
// check is an hour unless a test overrides it, so only explicit triggers run.
func newHarness(t *testing.T, opts ...Option) *harness {
	t.Helper()

	h := &harness{limiter: &fakeLimiter{}, notify: make(chan func(), 1), logs: &syncBuffer{}}
	h.memTotal.Store(1 * gib)
	base := []Option{
		WithMemTotalReader(func() (uint64, error) {
			if err := h.readErr.Load(); err != nil {
				return 0, *err
			}

			return h.memTotal.Load(), nil
		}),
		WithEventWatcher(func(ctx context.Context, notify func()) error {
			h.notify <- notify
			<-ctx.Done()

			return ctx.Err()
		}),
		WithLayoutReader(func() host.MemoryLayout { return host.MemoryLayout{OnlinePolicy: "auto-movable"} }),
		WithHotplugAddedReader(func() (uint64, error) {
			if f := h.added.Load(); f != nil {
				return (*f)()
			}

			return h.memTotal.Load() - min(h.memTotal.Load(), 1*gib), nil
		}),
		WithPeriod(time.Hour),
		WithEventWindow(5 * time.Millisecond),
		withSynced(func(reasons []string) {
			h.syncsMu.Lock()
			h.syncs = append(h.syncs, reasons)
			h.syncsMu.Unlock()
		}),
	}
	h.mgr = New(zerolog.New(h.logs), h.limiter, append(base, opts...)...)
	h.mgr.Start(t.Context())

	return h
}

// waitSynced waits for a completed recompute that covered every one of reasons. A
// recompute takes the reasons recorded before it starts and reads MemTotal after, so the
// one that covers a reason saw every change made before that reason was recorded.
func (h *harness) waitSynced(t *testing.T, reasons ...string) {
	t.Helper()

	require.Eventually(t, func() bool {
		h.syncsMu.Lock()
		defer h.syncsMu.Unlock()
		for _, covered := range h.syncs {
			if !slices.ContainsFunc(reasons, func(r string) bool { return !slices.Contains(covered, r) }) {
				return true
			}
		}

		return false
	}, 5*time.Second, time.Millisecond)
}

// settle waits for the recompute Start requested.
func (h *harness) settle(t *testing.T) {
	t.Helper()

	h.waitSynced(t, "start")
}

// request asks for a recompute under a reason no other call uses and waits for it.
func (h *harness) request(t *testing.T) {
	t.Helper()

	reason := fmt.Sprintf("test-%d", h.seq.Add(1))
	h.mgr.Request(reason)
	h.waitSynced(t, reason)
}

func TestLimit(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		memTotal, added, want uint64
	}{
		"nothing online":               {0, 0, 0},
		"nothing plugged, small guest": {512 * mib, 0, 448 * mib},
		"nothing plugged, 1 GiB":       {1 * gib, 0, 896 * mib},
		"nothing plugged, 16 GiB":      {16 * gib, 0, 16*gib - 128*mib},
		"2 GiB added on 1 GiB":         {3 * gib, 2 * gib, 3*gib - 128*mib - 32*mib},
		"8 GiB added on 1 GiB":         {9 * gib, 8 * gib, 9*gib - 128*mib - 128*mib},
		"8 GiB added on 3 GiB":         {11 * gib, 8 * gib, 11*gib - 128*mib - 128*mib},
		// 2114 MiB plugged onlines 17 whole blocks: the map is for 2176 MiB.
		"a partial last block counts whole": {1*gib + 2114*mib, 2176 * mib, 1*gib + 2114*mib - 128*mib - 34*mib},
		"a partial page rounds up":          {1*gib + 64, 64, 1*gib + 64 - 128*mib - 1},
		"added above MemTotal is capped":    {1 * gib, 64 * gib, 1*gib - 128*mib - 16*mib},
	} {
		assert.Equal(t, tc.want, Limit(tc.memTotal, tc.added), name)
	}
}

func TestHotplugAdded_Fallback(t *testing.T) {
	t.Parallel()
	unreadable := func() (uint64, error) { return 0, errors.New("permission denied") }
	read := func() (uint64, error) { return 2176 * mib, nil }

	added, err := hotplugAdded(read, 3*gib, 3*gib, host.MemoryLayout{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2176*mib), added, "/proc/iomem wins whatever the baseline")

	added, err = hotplugAdded(unreadable, 3*gib, 1*gib, host.MemoryLayout{Movable: 1536 * mib})
	require.Error(t, err)
	assert.Equal(t, uint64(2*gib), added, "growth since envd started")

	added, err = hotplugAdded(unreadable, 3*gib, 3*gib, host.MemoryLayout{Movable: 2 * gib})
	require.Error(t, err)
	assert.Equal(t, uint64(2*gib), added, "an envd restarted in a grown guest falls back to the movable zone")
}

func TestRecompute_RestartedInAGrownGuestHoldsBackTheAddedMap(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithBaseline(9*gib))
	h.memTotal.Store(9 * gib)
	h.request(t)

	assert.Equal(t, uint64(9*gib-256*mib), h.mgr.Status().Limit, "/proc/iomem still shows the 8 GiB added before envd started")
}

func TestRecompute_UnreadableIomemFallsBackAndLogsOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithBaseline(1*gib), WithLayoutReader(func() host.MemoryLayout { return host.MemoryLayout{Movable: 2 * gib} }))
	h.settle(t)
	unreadable := func() (uint64, error) { return 0, errors.New("permission denied") }
	h.added.Store(&unreadable)

	h.memTotal.Store(3 * gib)
	h.request(t)
	h.request(t)

	st := h.mgr.Status()
	assert.Equal(t, Limit(3*gib, 2*gib), st.Limit)
	assert.False(t, st.Pending, "the fallback is not an error of the limit")
	assert.Equal(t, 1, strings.Count(h.logs.String(), "could not read the hotplugged memory"))
}

func TestRecompute_HoldsBackBookkeepingForPluggedMemory(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithBaseline(1*gib))
	h.settle(t)

	h.memTotal.Store(9 * gib)
	h.request(t)

	assert.Equal(t, uint64(9*gib-256*mib), h.mgr.Status().Limit, "an 8 GiB plug holds back 128 MiB of page bookkeeping on top")
}

func TestStart_AppliesToUserAndPTY(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	assert.Equal(t, State{MemTotal: 1 * gib, Observed: 1 * gib, Limit: 896 * mib, Layout: host.MemoryLayout{OnlinePolicy: "auto-movable"}}, h.mgr.Status())
	assert.Equal(t, []uint64{896 * mib}, h.limiter.written(cgroups.ProcessTypeUser))
	assert.Equal(t, []uint64{896 * mib}, h.limiter.written(cgroups.ProcessTypePTY))
}

func TestWithApplied_SeedsStateWithoutRewriting(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithApplied(1*gib, 896*mib))
	h.settle(t)

	assert.Equal(t, uint64(896*mib), h.mgr.Status().Limit)
	assert.Empty(t, h.limiter.written(cgroups.ProcessTypeUser), "the limit written at creation is not written again")
}

func TestWithApplied_FailedFirstRecomputeStillReportsTheAppliedLimit(t *testing.T) {
	t.Parallel()
	readErr := errors.New("meminfo unreadable")
	h := newHarness(t, WithApplied(1*gib, 896*mib), WithMemTotalReader(func() (uint64, error) { return 0, readErr }))
	h.settle(t)

	st := h.mgr.Status()
	assert.Equal(t, uint64(896*mib), st.Limit)
	assert.Contains(t, st.Err, "meminfo unreadable")
}

func TestRequest_ReturnsAtOnce(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	h := newHarness(t, WithMemTotalReader(func() (uint64, error) {
		<-block

		return 1 * gib, nil
	}))

	start := time.Now()
	h.mgr.Request("init")
	assert.Less(t, time.Since(start), 100*time.Millisecond)

	close(block)
	h.waitSynced(t, "init")
	assert.Equal(t, uint64(896*mib), h.mgr.Status().Limit)
}

func TestRecompute_SkipsUnchangedLimit(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	h.request(t)
	h.request(t)

	assert.Len(t, h.limiter.written(cgroups.ProcessTypeUser), 1)
}

func TestRecompute_FollowsGrowthAndShrink(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	h.memTotal.Store(3 * gib)
	h.request(t)
	assert.Equal(t, Limit(3*gib, 2*gib), h.mgr.Status().Limit)

	h.memTotal.Store(2 * gib)
	h.request(t)
	assert.Equal(t, Limit(2*gib, 1*gib), h.mgr.Status().Limit)

	assert.Equal(t, []uint64{896 * mib, Limit(3*gib, 2*gib), Limit(2*gib, 1*gib)}, h.limiter.written(cgroups.ProcessTypeUser))
}

func TestRecompute_FailedWriteIsRetried(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	h.limiter.setFail(errors.New("EBUSY"))
	h.memTotal.Store(3 * gib)
	h.request(t)

	st := h.mgr.Status()
	assert.Equal(t, uint64(896*mib), st.Limit, "a failed write leaves the last applied limit")
	assert.Contains(t, st.Err, "EBUSY")

	h.limiter.setFail(nil)
	h.request(t)

	st = h.mgr.Status()
	assert.Equal(t, Limit(3*gib, 2*gib), st.Limit)
	assert.Empty(t, st.Err)
}

func TestRecompute_ZeroMemTotalWritesNothing(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)
	before := len(h.limiter.written(cgroups.ProcessTypeUser))

	h.memTotal.Store(0)
	h.request(t)

	st := h.mgr.Status()
	assert.Len(t, h.limiter.written(cgroups.ProcessTypeUser), before, "a limit of 0 would OOM-kill the cgroup")
	assert.Equal(t, uint64(896*mib), st.Limit)
	assert.Contains(t, st.Err, "MemTotal read as 0")
	assert.True(t, st.Pending)
}

func TestRecompute_RepeatedFailureIsLoggedOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	err := errors.New("meminfo unreadable")
	h.readErr.Store(&err)
	h.request(t)
	h.request(t)
	h.request(t)

	assert.Equal(t, 1, strings.Count(h.logs.String(), "could not read MemTotal"))
}

func TestMemoryEvents_BurstIsCoalesced(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithEventWindow(50*time.Millisecond))
	h.settle(t)
	notify := <-h.notify

	// A 2 GiB plug onlines sixteen 128 MiB blocks, one event each.
	for i := 1; i <= 16; i++ {
		h.memTotal.Store(1*gib + uint64(i)*128*mib)
		notify()
	}
	h.waitSynced(t, "memory-event")
	assert.False(t, h.mgr.Status().Pending)

	assert.Equal(t, Limit(3*gib, 2*gib), h.mgr.Status().Limit)
	assert.Equal(t, []uint64{896 * mib, Limit(3*gib, 2*gib)}, h.limiter.written(cgroups.ProcessTypeUser), "one write for the whole burst")
}

func TestPeriodicCheck_CatchesChangesWithoutEvents(t *testing.T) {
	t.Parallel()
	h := newHarness(t, WithPeriod(5*time.Millisecond))
	h.settle(t)

	// A sub-block plug inside an online memory block changes MemTotal with no event.
	h.memTotal.Store(1*gib + 64*mib)

	assert.Eventually(t, func() bool { return h.mgr.Status().Limit == Limit(1*gib+64*mib, 64*mib) }, 5*time.Second, time.Millisecond)
}

func TestRecompute_LogsEveryMergedReason(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	block := make(chan struct{})
	var reads atomic.Int32
	h := newHarness(t, WithMemTotalReader(func() (uint64, error) {
		if reads.Add(1) == 1 {
			close(entered)
			<-block

			return 1 * gib, nil
		}

		return 3 * gib, nil
	}))

	// Both arrive while the start recompute is inside its MemTotal read, so the next
	// recompute answers both.
	<-entered
	h.mgr.Request("init")
	h.mgr.Request("memory-event")
	close(block)
	h.waitSynced(t, "init", "memory-event")

	assert.Contains(t, h.logs.String(), `"reasons":["init","memory-event"]`)
}

func TestPending_SteadyStateIsNotPendingAfterARequest(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	// /init requests a recompute on every call; with nothing changed the applied limit
	// already matches, whether or not that recompute has run yet.
	h.mgr.Request("init")
	assert.False(t, h.mgr.Status().Pending)
	h.waitSynced(t, "init")
	st := h.mgr.Status()
	assert.False(t, st.Pending)
	assert.Equal(t, uint64(1*gib), st.MemTotal)
	assert.Equal(t, uint64(896*mib), st.Limit)
}

func TestPending_ObservedChangeNotAppliedIsPending(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	h.limiter.setFail(errors.New("EBUSY"))
	h.memTotal.Store(3 * gib)
	h.request(t)

	st := h.mgr.Status()
	assert.True(t, st.Pending)
	assert.Equal(t, uint64(3*gib), st.Observed)
	assert.Equal(t, uint64(1*gib), st.MemTotal, "mem_total is what the applied limit was computed from")

	h.limiter.setFail(nil)
	h.request(t)
	st = h.mgr.Status()
	assert.False(t, st.Pending)
	assert.Equal(t, uint64(3*gib), st.MemTotal)
	assert.Equal(t, Limit(3*gib, 2*gib), st.Limit)
}

// Status reads nothing, so a change no event or request reported is not pending until a
// recompute observes it; the host's check against what it plugged covers the gap.
func TestPending_LagsAChangeUntilARecompute(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	h.memTotal.Store(3 * gib)
	st := h.mgr.Status()
	assert.False(t, st.Pending)
	assert.Equal(t, uint64(1*gib), st.Observed)

	h.request(t)
	st = h.mgr.Status()
	assert.False(t, st.Pending)
	assert.Equal(t, State{MemTotal: 3 * gib, Observed: 3 * gib, Limit: Limit(3*gib, 2*gib), Layout: host.MemoryLayout{OnlinePolicy: "auto-movable"}}, st)
}

func TestPending_FailedWriteIsPendingUntilASuccess(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	h.limiter.setFail(errors.New("EBUSY"))
	h.memTotal.Store(3 * gib)
	h.request(t)
	assert.True(t, h.mgr.Status().Pending)

	// Back to the size the applied limit was computed from: the write is retried and
	// fails again, so the last recompute failed and none has succeeded since.
	h.memTotal.Store(1 * gib)
	h.request(t)
	assert.True(t, h.mgr.Status().Pending, "a failure stays pending until a recompute succeeds")

	h.limiter.setFail(nil)
	h.request(t)
	assert.False(t, h.mgr.Status().Pending)
}

func TestPending_UnreadableMemTotalIsPending(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.settle(t)

	err := errors.New("meminfo unreadable")
	h.readErr.Store(&err)
	h.request(t)
	assert.True(t, h.mgr.Status().Pending)
}
