//go:build linux

package block

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func fakeMemoryTracker(pageBytes uint64) (*Tracker, *time.Duration) {
	var now time.Duration
	t := NewMemoryTracker(pageBytes)
	t.memory.now = func() time.Duration { return now }

	return t, &now
}

func TestMemoryUsageSpikeAndRemainders(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(4096)
	require.Zero(t, tr.MemoryUsage())
	tr.MarkInstalled(0, 262144, Clean)
	*now += 125 * time.Millisecond
	tr.SetRange(0, 262144, Removed)
	*now += time.Second
	require.Equal(t, MemoryUsage{PageStateBytes: [NumStates]int64{Removed: 1 << 30}, HeldMiBMillis: 128000}, tr.MemoryUsage())
	for range 256 {
		tr.MarkInstalled(0, 1, Zero)
		*now += time.Millisecond
		tr.SetRange(0, 1, Removed)
		tr.MemoryUsage()
	}
	require.Equal(t, MemoryUsage{PageStateBytes: [NumStates]int64{Removed: 1 << 30}, HeldMiBMillis: 128001}, tr.MemoryUsage())
}

func TestMemoryUsageYearAndStop(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(4096)
	tr.SetRange(0, 1<<28, Dirty)
	year := 365 * 24 * time.Hour
	*now += year
	want := MemoryUsage{PageStateBytes: [NumStates]int64{Dirty: 1 << 40}, HeldMiBMillis: (1 << 20) * uint64(year/time.Millisecond)}
	require.Equal(t, want, tr.MemoryUsage())
	*now += time.Second
	want.HeldMiBMillis += (1 << 20) * 1000
	tr.StopMemoryUsage()
	*now += year
	tr.StopMemoryUsage()
	require.Equal(t, want, tr.MemoryUsage())
}

func TestMemoryUsageTransitions(t *testing.T) {
	t.Parallel()
	operations := map[string]func(*Tracker){
		"claim":         func(tr *Tracker) { tr.BeginInstall(0) },
		"abort":         func(tr *Tracker) { tr.AbortInstall(0) },
		"clean":         func(tr *Tracker) { tr.MarkInstalled(0, 1, Clean) },
		"zero":          func(tr *Tracker) { tr.MarkInstalled(0, 1, Zero) },
		"range-clean":   func(tr *Tracker) { tr.MarkInstalled(0, 8, Clean) },
		"range-zero":    func(tr *Tracker) { tr.MarkInstalled(0, 8, Zero) },
		"write":         func(tr *Tracker) { tr.MarkWritten(0) },
		"write-present": func(tr *Tracker) { tr.MarkWrittenPresent(0) },
		"export":        func(tr *Tracker) { tr.MarkExported(0) },
	}
	for state := range numStates {
		operations["set-"+state.String()] = func(tr *Tracker) { tr.SetRange(0, 8, state) }
		operations["set-single-"+state.String()] = func(tr *Tracker) { tr.SetRange(0, 1, state) }
	}
	for initial := range numStates {
		for name, op := range operations {
			t.Run(initial.String()+"/"+name, func(t *testing.T) {
				t.Parallel()
				tr, now := fakeMemoryTracker(1 << 20)
				tr.SetRange(0, 2, initial)
				tr.SetRange(2, 4, Dirty)
				tr.SetRange(4, 6, Installing)
				tr.SetRange(6, 8, Removed)
				counts := tr.Counts()
				before := counts[Dirty] + counts[Clean] + counts[Zero]
				*now += 7 * time.Millisecond
				op(tr)
				counts = tr.Counts()
				after := counts[Dirty] + counts[Clean] + counts[Zero]
				*now += 11 * time.Millisecond
				usage := tr.MemoryUsage()
				require.Equal(t, after<<20, usage.HeldBytes())
				require.Equal(t, before*7+after*11, usage.HeldMiBMillis)
				for state, pages := range counts {
					require.Equal(t, int64(pages<<20), usage.PageStateBytes[state])
				}
			})
		}
	}
}

func TestMemoryUsageInstallRaces(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(1 << 20)
	tr.BeginInstall(0)
	*now += time.Second
	tr.AbortInstall(0)
	require.Zero(t, tr.MemoryUsage())
	tr.BeginInstall(0)
	tr.MarkWritten(0)
	*now += time.Second
	tr.MarkInstalled(0, 1, Clean)
	tr.MarkWritten(0)
	tr.MarkExported(0)
	*now += time.Second
	tr.SetRange(0, 1, Removed)
	tr.MarkWritten(0)
	*now += time.Second
	require.Equal(t, MemoryUsage{PageStateBytes: [NumStates]int64{Removed: 1 << 20}, HeldMiBMillis: 2000}, tr.MemoryUsage())
	tr.MarkInstalled(0, 1, Zero)
	*now += time.Second
	require.Equal(t, MemoryUsage{PageStateBytes: [NumStates]int64{Zero: 1 << 20}, HeldMiBMillis: 3000}, tr.MemoryUsage())
}

func memoryAnomalyReader(t *testing.T, tr *Tracker) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	counter, err := provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block").Int64Counter("anomalies")
	require.NoError(t, err)
	tr.memory.anomalies = counter

	return reader
}

func memoryAnomalyCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	counts := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				reason, _ := point.Attributes.Value(attribute.Key("reason"))
				counts[reason.AsString()] += point.Value
			}
		}
	}

	return counts
}

func TestMemoryUsageWithoutAccounting(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	tr.SetRange(0, 1, Dirty)
	require.Zero(t, tr.MemoryUsage())
	tr.StopMemoryUsage()
	require.Zero(t, tr.MemoryUsage())
	require.Equal(t, Dirty, tr.Get(0))
}

func TestMemoryUsageUnderflowClampsAndCounts(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(1 << 20)
	reader := memoryAnomalyReader(t, tr)
	tr.MarkInstalled(0, 1, Clean)
	*now += time.Second
	tr.mu.Lock()
	anomalies := tr.changeHeld(-2)
	tr.mu.Unlock()
	anomalies.report(tr.memory.anomalies)
	require.Zero(t, tr.memory.heldBytes)
	require.Equal(t, uint64(1), tr.memory.anomalyCount)
	require.Equal(t, uint64(1000), tr.memory.totalMiBMillis())
	*now += 365 * 24 * time.Hour
	require.Zero(t, tr.memory.accrue(*now))
	require.Equal(t, uint64(1000), tr.memory.totalMiBMillis())
	require.Equal(t, map[string]int64{"underflow": 1}, memoryAnomalyCounts(t, reader))
}

func TestMemoryUsageReconcilesDrift(t *testing.T) {
	t.Parallel()
	for _, stop := range []bool{false, true} {
		for _, incorrect := range []uint64{0, math.MaxUint64} {
			t.Run(fmt.Sprintf("stop=%t/held=%d", stop, incorrect), func(t *testing.T) {
				t.Parallel()
				tr, now := fakeMemoryTracker(1 << 20)
				reader := memoryAnomalyReader(t, tr)
				tr.MarkInstalled(0, 3, Clean)
				*now += 7 * time.Millisecond
				require.Equal(t, uint64(21), tr.MemoryUsage().HeldMiBMillis)
				tr.memory.heldBytes = incorrect
				*now += 365 * 24 * time.Hour
				if stop {
					tr.StopMemoryUsage()
				}
				usage := tr.MemoryUsage()
				require.Equal(t, uint64(3<<20), usage.HeldBytes())
				require.Equal(t, uint64(3<<20), tr.memory.heldBytes)
				require.Equal(t, uint64(21), usage.HeldMiBMillis)
				require.Equal(t, uint64(1), usage.Anomalies)
				*now += 11 * time.Millisecond
				want := uint64(54)
				if stop {
					want = 21
				}
				require.Equal(t, want, tr.MemoryUsage().HeldMiBMillis)
				require.Equal(t, map[string]int64{"drift": 1}, memoryAnomalyCounts(t, reader))
			})
		}
	}
}

func TestMemoryUsageOverflowDoesNotWrapOrPanic(t *testing.T) {
	t.Parallel()
	for _, product := range []bool{false, true} {
		t.Run(fmt.Sprint(product), func(t *testing.T) {
			t.Parallel()
			tr, now := fakeMemoryTracker(1 << 20)
			reader := memoryAnomalyReader(t, tr)
			tr.memory.heldBytes = 1 << 20
			tr.memory.byteNanosHi, tr.memory.byteNanosLo = bits.Mul64(math.MaxUint64-99, heldMiBMillisUnit)
			if product {
				tr.memory.heldBytes = math.MaxUint64
			}
			*now += time.Hour
			anomalies := tr.memory.accrue(*now)
			anomalies.report(tr.memory.anomalies)
			require.Equal(t, memoryOverflow, anomalies)
			require.Equal(t, uint64(math.MaxUint64-99), tr.memory.totalMiBMillis())
			require.Equal(t, map[string]int64{"overflow": 1}, memoryAnomalyCounts(t, reader))
		})
	}
}

func TestMemoryUsageUnchangedReadsNoClock(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(4096)
	reads := 0
	tr.memory.now = func() time.Duration {
		reads++

		return *now
	}
	tr.SetRange(0, 8, Removed)
	tr.SetRange(0, 8, Removed)
	tr.MarkInstalled(0, 1, Clean)
	before := reads
	tr.MarkWritten(0)
	tr.SetRange(4, 8, Removed)
	require.Equal(t, before, reads)
	require.Equal(t, 1, before)
}

func TestMemoryUsageClockRunsUnderLock(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(4096)
	tr.memory.now = func() time.Duration {
		if tr.mu.TryLock() {
			tr.mu.Unlock()
			assert.Fail(t, "clock sampled outside the tracker lock")
		}

		return *now
	}
	tr.MarkInstalled(0, 1, Clean)
	*now += time.Second
	tr.MemoryUsage()
	tr.SetRange(0, 1, Removed)
	tr.StopMemoryUsage()
}

func TestMemoryUsageCarriesWideAccumulator(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(1 << 20)
	tr.SetRange(0, 1<<20, Clean)
	*now += (1<<24 - 1) * time.Nanosecond
	tr.SetRange(0, 1<<20, Removed)
	tr.SetRange(0, 1<<20, Clean)
	*now += 2 * time.Nanosecond
	tr.SetRange(0, 1<<20, Removed)
	want := uint64(1<<20) * uint64(1<<24+1) / uint64(time.Millisecond)
	require.Equal(t, want, tr.MemoryUsage().HeldMiBMillis)
}

type unlockedMemoryCounter struct {
	metric.Int64Counter

	tracker *Tracker
	calls   atomic.Int64
	locked  atomic.Bool
}

func (c *unlockedMemoryCounter) Add(context.Context, int64, ...metric.AddOption) {
	c.calls.Add(1)
	if c.tracker.mu.TryLock() {
		c.tracker.mu.Unlock()
	} else {
		c.locked.Store(true)
	}
}

func TestMemoryUsageReportsAnomaliesAfterUnlock(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*Tracker){
		"range":         func(tr *Tracker) { tr.SetRange(0, 1, Removed) },
		"install":       func(tr *Tracker) { tr.MarkInstalled(1, 2, Clean) },
		"write":         func(tr *Tracker) { tr.MarkWritten(1) },
		"write-present": func(tr *Tracker) { tr.MarkWrittenPresent(1) },
		"snapshot":      func(tr *Tracker) { tr.MemoryUsage() },
		"repair":        func(tr *Tracker) { tr.memory.heldBytes = 0; tr.MemoryUsage() },
		"stop":          func(tr *Tracker) { tr.StopMemoryUsage() },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr, now := fakeMemoryTracker(1 << 20)
			tr.MarkInstalled(0, 1, Clean)
			counter := &unlockedMemoryCounter{Int64Counter: noop.Int64Counter{}, tracker: tr}
			tr.memory.anomalies = counter
			tr.memory.byteNanosHi, tr.memory.byteNanosLo = bits.Mul64(math.MaxUint64-99, heldMiBMillisUnit)
			*now += time.Second
			mutate(tr)
			require.Positive(t, counter.calls.Load())
			require.False(t, counter.locked.Load(), "metric emission held a tracker lock")
		})
	}
}

func TestMemoryUsageOverflowKeepsReturnedTotal(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"read", "change", "stop"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			tr, now := fakeMemoryTracker(1 << 20)
			tr.MarkInstalled(0, 1, Clean)
			tr.memory.byteNanosHi, tr.memory.byteNanosLo = bits.Mul64(math.MaxUint64-100, heldMiBMillisUnit)
			*now += 10 * time.Millisecond
			returned := tr.MemoryUsage().HeldMiBMillis
			require.Equal(t, uint64(math.MaxUint64-90), returned)
			*now += time.Hour
			switch action {
			case "change":
				tr.SetRange(0, 1, Removed)
			case "stop":
				tr.StopMemoryUsage()
			}
			require.Equal(t, returned, tr.MemoryUsage().HeldMiBMillis)
		})
	}
}

func TestMemoryUsageDriftKeepsAnEarlierRead(t *testing.T) {
	t.Parallel()
	tr, _ := fakeMemoryTracker(1 << 20)
	tr.MarkInstalled(0, 3, Clean)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	tr.memory.now = func() time.Duration {
		if calls.Add(1) == 1 {
			close(entered)
			<-release

			return 7 * time.Millisecond
		}

		return time.Second
	}
	read := make(chan MemoryUsage, 1)
	repaired := make(chan MemoryUsage, 1)
	go func() { read <- tr.MemoryUsage() }()
	<-entered
	go func() {
		tr.mu.Lock()
		tr.memory.heldBytes = 0
		tr.mu.Unlock()
		repaired <- tr.MemoryUsage()
	}()
	close(release)
	require.Equal(t, uint64(21), (<-read).HeldMiBMillis)
	require.Equal(t, uint64(21), (<-repaired).HeldMiBMillis)
}

func TestMemoryUsageConcurrentSnapshotsAndTransitions(t *testing.T) {
	t.Parallel()
	tr := NewMemoryTracker(4096)
	var ticks atomic.Int64
	tr.memory.now = func() time.Duration { return time.Duration(ticks.Add(1_000_000)) }
	var wg sync.WaitGroup
	for worker := range uint32(8) {
		wg.Go(func() {
			for range 1000 {
				tr.MarkInstalled(worker, worker+1, Clean)
				tr.MarkWritten(worker)
				tr.SetRange(worker, worker+1, Removed)
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			var previous uint64
			for range 1000 {
				usage := tr.MemoryUsage()
				if !assert.GreaterOrEqual(t, usage.HeldMiBMillis, previous, "total went backwards") {
					return
				}
				previous = usage.HeldMiBMillis
			}
		})
	}
	wg.Wait()
	tr.StopMemoryUsage()
	final := tr.MemoryUsage()
	require.Zero(t, final.HeldBytes())
	require.Positive(t, final.HeldMiBMillis)
}

func TestMemoryUsageRecoveryPreservesStoredFraction(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(4096)
	tr.MarkInstalled(0, 1, Clean)
	*now += 384 * time.Millisecond
	tr.SetRange(0, 1, Removed)
	require.Equal(t, uint64(1), tr.MemoryUsage().HeldMiBMillis)
	tr.memory.heldBytes = 4096
	require.Equal(t, uint64(1), tr.MemoryUsage().HeldMiBMillis)
	tr.MarkInstalled(0, 1, Clean)
	*now += 128 * time.Millisecond
	tr.SetRange(0, 1, Removed)
	require.Equal(t, uint64(2), tr.MemoryUsage().HeldMiBMillis)
}

func TestMemoryUsageDriftDropsOnlyTheOpenInterval(t *testing.T) {
	t.Parallel()
	tr, now := fakeMemoryTracker(1 << 20)
	tr.MarkInstalled(0, 1, Clean)
	*now += 24 * time.Hour
	returned := tr.MemoryUsage().HeldMiBMillis
	require.Equal(t, uint64(24*time.Hour/time.Millisecond), returned)
	tr.memory.heldBytes = 0
	*now += time.Hour
	require.Equal(t, returned, tr.MemoryUsage().HeldMiBMillis)
	*now += time.Millisecond
	require.Equal(t, returned+1, tr.MemoryUsage().HeldMiBMillis)
}

func TestMemoryUsageHighWordCarryOut(t *testing.T) {
	t.Parallel()
	tr, _ := fakeMemoryTracker(4096)
	reader := memoryAnomalyReader(t, tr)
	// A corrupt count and backwards timestamp can overflow the high word to zero.
	tr.memory.heldBytes = math.MaxUint64
	tr.memory.last = time.Nanosecond
	tr.memory.byteNanosHi, tr.memory.byteNanosLo = 1, math.MaxUint64
	before := tr.memory.totalMiBMillis()
	anomalies := tr.memory.accrue(0)
	anomalies.report(tr.memory.anomalies)
	require.Equal(t, memoryOverflow, anomalies)
	require.Equal(t, before, tr.memory.totalMiBMillis())
	require.Equal(t, map[string]int64{"overflow": 1}, memoryAnomalyCounts(t, reader))
}
