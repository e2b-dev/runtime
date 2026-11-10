package telemetry

import (
	"context"
	rtmetrics "runtime/metrics"
	"sync"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.43.0/goconv"
)

const liveHeapSample = "/gc/heap/live:bytes"

const gcCyclesSample = "/gc/cycles/total:gc-cycles"

const mutexWaitSample = "/sync/mutex/wait/total:seconds"

// mutexWaitMetric retains the observed high-water for one registration. Native
// totals can temporarily decrease while thread-owned counts are transferred.
type mutexWaitMetric struct {
	instrument metric.Float64ObservableCounter
	mu         sync.Mutex
	total      float64
}

func newMutexWaitMetric(meter metric.Meter) (*mutexWaitMetric, error) {
	instrument, err := meter.Float64ObservableCounter("go.sync.mutex.wait.time",
		metric.WithUnit("s"),
		metric.WithDescription("Observed high-water of approximate cumulative time blocked on mutexes, including runtime-internal locks."),
	)
	if err != nil {
		return nil, err
	}

	return &mutexWaitMetric{instrument: instrument}, nil
}

func (m *mutexWaitMetric) observe(o metric.Observer, native float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if native > m.total {
		m.total = native
	}
	// Serialize publication too, so a delayed callback cannot publish an older
	// peak after another callback has already published a newer one.
	o.ObserveFloat64(m.instrument, m.total)
}

// cpuStateSamples maps each go.cpu.state value to the runtime/metrics class
// it is computed from.
var cpuStateSamples = []struct {
	state  goconv.CPUStateAttr
	sample string
}{
	{goconv.CPUStateUser, "/cpu/classes/user:cpu-seconds"},
	{goconv.CPUStateGC, "/cpu/classes/gc/total:cpu-seconds"},
	{goconv.CPUStateScavenge, "/cpu/classes/scavenge/total:cpu-seconds"},
	{goconv.CPUStateIdle, "/cpu/classes/idle:cpu-seconds"},
}

// StartRuntimeInstrumentation registers OTEL Go runtime metric callbacks.
//
// Collected metrics (semantic-convention names, except go.memory.live and go.sync.mutex.wait.time):
//   - go.memory.used
//   - go.memory.limit
//   - go.memory.allocated
//   - go.memory.allocations
//   - go.memory.gc.goal
//   - go.goroutine.count
//   - go.processor.limit
//   - go.config.gogc
//   - go.memory.live
//   - go.memory.gc.cycles
//   - go.cpu.time
//   - go.sync.mutex.wait.time
//
// The callbacks are invoked by the MeterProvider and stop automatically
// when it shuts down — no separate goroutine is spawned.
func (t *Client) StartRuntimeInstrumentation() error {
	err := runtime.Start(
		runtime.WithMeterProvider(t.MeterProvider),
		runtime.WithMinimumReadMemStatsInterval(metricExportPeriod),
	)
	if err != nil {
		return err
	}

	return startGCMetrics(t.MeterProvider)
}

// startGCMetrics exports the runtime/metrics series the contrib
// instrumentation leaves out: the live heap the GC goal is derived from, the
// number of completed cycles, the runtime's CPU time by state, and cumulative
// mutex wait time (including runtime-internal locks). Delete these here once
// the contrib instrumentation emits them, or the two meters report the same
// series. Mutex wait time indicates contention, not which lock caused it.
func startGCMetrics(provider metric.MeterProvider) error {
	meter := provider.Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")

	// No semantic convention covers the live heap.
	live, err := meter.Int64ObservableGauge("go.memory.live",
		metric.WithUnit("By"),
		metric.WithDescription("Heap memory marked live by the last completed GC cycle."),
	)
	if err != nil {
		return err
	}

	cycles, err := goconv.NewMemoryGCCyclesObservable(meter)
	if err != nil {
		return err
	}

	cpuTime, err := goconv.NewCPUTimeObservable(meter)
	if err != nil {
		return err
	}

	mutexWait, err := newMutexWaitMetric(meter)
	if err != nil {
		return err
	}

	cpuStates := make([]metric.ObserveOption, len(cpuStateSamples))
	for i, s := range cpuStateSamples {
		cpuStates[i] = metric.WithAttributeSet(attribute.NewSet(cpuTime.AttrCPUState(s.state)))
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		samples := make([]rtmetrics.Sample, 0, 3+len(cpuStateSamples))
		samples = append(samples, rtmetrics.Sample{Name: liveHeapSample}, rtmetrics.Sample{Name: gcCyclesSample})
		for _, s := range cpuStateSamples {
			samples = append(samples, rtmetrics.Sample{Name: s.sample})
		}
		samples = append(samples, rtmetrics.Sample{Name: mutexWaitSample})
		rtmetrics.Read(samples)

		if v := samples[0].Value; v.Kind() == rtmetrics.KindUint64 {
			o.ObserveInt64(live, int64(v.Uint64()))
		}
		if v := samples[1].Value; v.Kind() == rtmetrics.KindUint64 {
			o.ObserveInt64(cycles.Inst(), int64(v.Uint64()))
		}
		for i := range cpuStateSamples {
			if v := samples[2+i].Value; v.Kind() == rtmetrics.KindFloat64 {
				o.ObserveFloat64(cpuTime.Inst(), v.Float64(), cpuStates[i])
			}
		}

		if v := samples[2+len(cpuStateSamples)].Value; v.Kind() == rtmetrics.KindFloat64 {
			mutexWait.observe(o, v.Float64())
		}

		return nil
	}, live, cycles.Inst(), cpuTime.Inst(), mutexWait.instrument)

	return err
}
