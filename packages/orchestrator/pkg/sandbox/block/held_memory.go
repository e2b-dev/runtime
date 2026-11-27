//go:build linux

package block

import (
	"context"
	"math/bits"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var heldMemoryAnomalies = utils.Must(meter.Int64Counter(
	"orchestrator.sandbox.memory.accounting.anomalies",
	metric.WithDescription("Held-memory accounting anomalies, by reason"),
	metric.WithUnit("{anomaly}"),
))

// MemoryUsage is a consistent snapshot of page states and accumulated held memory.
type MemoryUsage struct {
	PageStateBytes [NumStates]int64
	// HeldMiBMillis is cumulative MiB × milliseconds; 1 TiB for a year fits in uint64.
	HeldMiBMillis uint64
	// Anomalies counts the operations that hit an accounting anomaly, for a reader that can name the sandbox.
	Anomalies uint64
}

// HeldBytes returns installed memory that has not been handed back.
func (m MemoryUsage) HeldBytes() uint64 {
	return uint64(m.PageStateBytes[Clean] + m.PageStateBytes[Dirty] + m.PageStateBytes[Zero])
}

const heldMiBMillisUnit = uint64(1<<20) * uint64(time.Millisecond)

type memoryAnomalies uint8

const (
	memoryUnderflow memoryAnomalies = 1 << iota
	memoryOverflow
	memoryDrift
)

// report runs after the tracker lock is released; page-state transitions carry no context.
//
//nolint:contextcheck // the tracker's callers hold no context for a metric
func (a memoryAnomalies) report(counter metric.Int64Counter) {
	for i, reason := range [...]string{"underflow", "overflow", "drift"} {
		if a&(1<<i) != 0 {
			counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
		}
	}
}

type heldMemory struct {
	pageBytes uint64
	now       func() time.Duration
	last      time.Duration
	heldBytes uint64
	// The two words retain exact byte-nanoseconds; conversion happens on a read.
	byteNanosHi, byteNanosLo uint64
	stopped                  bool
	anomalies                metric.Int64Counter
	anomalyCount             uint64
}

// countAnomalies keeps the tracker's own total beside the process-wide counter.
func (m *heldMemory) countAnomalies(anomalies memoryAnomalies) memoryAnomalies {
	if anomalies != 0 {
		m.anomalyCount++
	}

	return anomalies
}

func (m *heldMemory) accrue(now time.Duration) memoryAnomalies {
	if m.stopped {
		return 0
	}
	elapsed := uint64(now - m.last)
	m.last = now
	hi, lo := bits.Mul64(m.heldBytes, elapsed)
	lo, carry := bits.Add64(lo, m.byteNanosLo, 0)
	hi, overflow := bits.Add64(hi, m.byteNanosHi, carry)
	if overflow != 0 || hi >= heldMiBMillisUnit {
		return memoryOverflow
	}
	m.byteNanosHi, m.byteNanosLo = hi, lo

	return 0
}

func (m *heldMemory) totalMiBMillis() uint64 {
	whole, _ := bits.Div64(m.byteNanosHi, m.byteNanosLo, heldMiBMillisUnit)

	return whole
}

// changeHeld runs under the same lock as the bitmap transition.
func (t *Tracker) changeHeld(pages int64) memoryAnomalies {
	if t.memory == nil {
		return 0
	}

	return t.memory.countAnomalies(t.memory.change(pages))
}

func (t *Tracker) changeHeldRange(start, end uint64, state State) memoryAnomalies {
	if t.memory == nil {
		return 0
	}
	var before uint64
	if end-start == 1 {
		switch t.stateOf(uint32(start)) {
		case Dirty, Zero, Clean:
			before = 1
		}
	} else {
		before = t.dirty.CardinalityInRange(start, end) + t.zero.CardinalityInRange(start, end) + t.clean.CardinalityInRange(start, end)
	}
	var after uint64
	if state == Dirty || state == Zero || state == Clean {
		after = end - start
	}

	return t.changeHeld(int64(after) - int64(before))
}

func (m *heldMemory) change(pages int64) memoryAnomalies {
	if pages == 0 {
		return 0
	}
	anomalies := m.accrue(m.now())
	heldBytes := int64(m.heldBytes) + pages*int64(m.pageBytes)
	if heldBytes < 0 {
		anomalies |= memoryUnderflow
		heldBytes = 0
	}
	m.heldBytes = uint64(heldBytes)

	return anomalies
}

// MemoryUsage reads a consistent snapshot, or zero for a tracker without memory accounting.
func (t *Tracker) MemoryUsage() MemoryUsage {
	t.mu.Lock()
	if t.memory == nil {
		t.mu.Unlock()

		return MemoryUsage{}
	}
	usage, anomalies := t.memoryUsageLocked()
	t.mu.Unlock()
	anomalies.report(t.memory.anomalies)

	return usage
}

// StopMemoryUsage freezes the total while leaving the final snapshot readable.
func (t *Tracker) StopMemoryUsage() {
	t.mu.Lock()
	if t.memory == nil {
		t.mu.Unlock()

		return
	}
	_, anomalies := t.memoryUsageLocked()
	t.memory.stopped = true
	t.mu.Unlock()
	anomalies.report(t.memory.anomalies)
}

func (t *Tracker) memoryUsageLocked() (MemoryUsage, memoryAnomalies) {
	var usage MemoryUsage
	for state, pages := range t.countsLocked() {
		usage.PageStateBytes[state] = int64(pages * t.memory.pageBytes)
	}
	now := t.memory.now()
	var anomalies memoryAnomalies
	if held := usage.HeldBytes(); held != t.memory.heldBytes {
		// A mismatched count has no trustworthy transition time; restart the interval.
		anomalies |= memoryDrift
		t.memory.last = now
		t.memory.heldBytes = held
	}
	anomalies |= t.memory.accrue(now)
	usage.HeldMiBMillis = t.memory.totalMiBMillis()
	t.memory.countAnomalies(anomalies)
	usage.Anomalies = t.memory.anomalyCount

	return usage, anomalies
}
