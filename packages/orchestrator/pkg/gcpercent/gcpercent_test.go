package gcpercent

import (
	"context"
	"runtime/debug"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type fakeFlag struct {
	mu     sync.Mutex
	value  int
	served bool
}

func (f *fakeFlag) source(context.Context) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.value, f.served
}

func (f *fakeFlag) serve(value int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.value, f.served = value, true
}

// fail models a failed evaluation, which hands back the flag's fallback.
func (f *fakeFlag) fail() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.value, f.served = Disabled, false
}

type fakeRuntime struct {
	mu      sync.Mutex
	current int
	calls   []int
}

func (r *fakeRuntime) set(percent int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.current
	r.current = percent
	r.calls = append(r.calls, percent)

	return previous
}

func (r *fakeRuntime) writes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]int(nil), r.calls...)
}

type harness struct {
	c       *Controller
	flag    *fakeFlag
	runtime *fakeRuntime
	reader  *sdkmetric.ManualReader
	logs    *observer.ObservedLogs
}

func newHarness(t *testing.T, original int) *harness {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.WithoutCancel(t.Context())) })

	core, logs := observer.New(zap.InfoLevel)
	flag := &fakeFlag{value: Disabled, served: true}
	rt := &fakeRuntime{current: original}

	c, err := newController(provider, flag.source, rt.set, original, logger.NewTracedLoggerFromCore(core))
	require.NoError(t, err)

	return &harness{c: c, flag: flag, runtime: rt, reader: reader, logs: logs}
}

// gauge collects the outcome gauge as outcome -> value.
func (h *harness) gauge(t *testing.T) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, h.reader.Collect(t.Context(), &rm))

	points := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorGOGCOutcomeGaugeName) {
				continue
			}
			data, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "gauge is %T", m.Data)
			for _, dp := range data.DataPoints {
				require.Equal(t, 1, dp.Attributes.Len(), "attribute set must be outcome alone")
				outcome, ok := dp.Attributes.Value("outcome")
				require.True(t, ok)
				points[outcome.AsString()] = dp.Value
			}
		}
	}

	return points
}

// set returns the one outcome the gauge sets to 1, after checking that all
// four outcomes are reported and the other three are 0.
func (h *harness) set(t *testing.T) string {
	t.Helper()

	points := h.gauge(t)
	require.Len(t, points, 4, "every outcome is reported: %v", points)

	var current []string
	for outcome, v := range points {
		require.Contains(t, []int64{0, 1}, v, "outcome %s", outcome)
		if v == 1 {
			current = append(current, outcome)
		}
	}
	require.Len(t, current, 1, "exactly one outcome is set: %v", points)

	return current[0]
}

func TestResolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  int
		served bool
		want   Outcome
	}{
		{"sentinel", -1, true, OutcomeDisabled},
		{"floor", 10, true, OutcomeApplied},
		{"below floor", 9, true, OutcomeRefused},
		{"ceiling", 100, true, OutcomeApplied},
		{"above ceiling", 101, true, OutcomeRefused},
		{"inside band", 25, true, OutcomeApplied},
		{"zero", 0, true, OutcomeRefused},
		{"other negative", -2, true, OutcomeRefused},
		{"large negative", -100, true, OutcomeRefused},
		{"error over disabled", -1, false, OutcomeError},
		{"error over refused", 0, false, OutcomeError},
		{"error over applied", 25, false, OutcomeError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, resolve(tt.value, tt.served))
		})
	}
}

// Walks every value from -300 to 300 forwards and back, so each value is
// reached from its neighbours, applied ones included, and then again with a
// failed evaluation before each one. An original outside the band shows the
// restore is not clamped.
func TestOnlyAcceptedValuesOrTheOriginalReachTheRuntime(t *testing.T) {
	t.Parallel()

	for _, original := range []int{100, 200} {
		h := newHarness(t, original)

		var values []int
		for v := -300; v <= 300; v++ {
			values = append(values, v)
		}
		for v := 300; v >= -300; v-- {
			values = append(values, v)
		}

		for _, v := range values {
			h.flag.serve(v)
			h.c.tick(t.Context())
		}
		for _, v := range values {
			h.flag.fail()
			h.c.tick(t.Context())
			h.flag.serve(v)
			h.c.tick(t.Context())
		}

		writes := h.runtime.writes()
		require.NotEmpty(t, writes)
		for _, w := range writes {
			if w != original {
				assert.GreaterOrEqual(t, w, MinPercent, "original %d", original)
				assert.LessOrEqual(t, w, MaxPercent, "original %d", original)
			}
		}
		assert.Contains(t, writes, original, "restores must write the original")
	}
}

func TestDisabledFromStartWritesNothing(t *testing.T) {
	t.Parallel()

	h := newHarness(t, 100)
	for range 5 {
		h.c.tick(t.Context())
	}

	assert.Empty(t, h.runtime.writes())
}

func TestAppliedPercentIsWrittenOnlyWhenItChanges(t *testing.T) {
	t.Parallel()

	h := newHarness(t, 100)

	h.flag.serve(25)
	for range 3 {
		h.c.tick(t.Context())
	}
	assert.Equal(t, []int{25}, h.runtime.writes())

	h.flag.serve(30)
	h.c.tick(t.Context())
	h.c.tick(t.Context())
	assert.Equal(t, []int{25, 30}, h.runtime.writes())
}

func TestApplyingTheOriginalValueWritesNothing(t *testing.T) {
	t.Parallel()

	h := newHarness(t, 100)
	h.flag.serve(100)
	h.c.tick(t.Context())

	assert.Empty(t, h.runtime.writes())
	assert.Equal(t, "applied", h.set(t))
}

func TestNonAppliedOutcomesRestoreTheOriginal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		apply   func(*fakeFlag)
		outcome string
	}{
		{"disabled", func(f *fakeFlag) { f.serve(Disabled) }, "disabled"},
		{"refused zero", func(f *fakeFlag) { f.serve(0) }, "refused"},
		{"refused above ceiling", func(f *fakeFlag) { f.serve(101) }, "refused"},
		{"error", (*fakeFlag).fail, "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t, 100)

			h.flag.serve(25)
			h.c.tick(t.Context())
			tt.apply(h.flag)
			h.c.tick(t.Context())
			assert.Equal(t, []int{25, 100}, h.runtime.writes())
			assert.Equal(t, tt.outcome, h.set(t))

			// Re-applying the same value after a restore writes it again.
			h.flag.serve(25)
			h.c.tick(t.Context())
			assert.Equal(t, []int{25, 100, 25}, h.runtime.writes())
		})
	}
}

func TestGaugeSetsExactlyOneOutcome(t *testing.T) {
	t.Parallel()

	h := newHarness(t, 100)
	assert.Empty(t, h.gauge(t), "nothing is observed before the first evaluation")

	steps := []struct {
		name  string
		apply func(*fakeFlag)
		want  string
	}{
		{"disabled", func(f *fakeFlag) { f.serve(Disabled) }, "disabled"},
		{"applied", func(f *fakeFlag) { f.serve(25) }, "applied"},
		{"refused", func(f *fakeFlag) { f.serve(5) }, "refused"},
		{"applied again", func(f *fakeFlag) { f.serve(40) }, "applied"},
		// The value handed back is -1, which must not read as disabled.
		{"error", (*fakeFlag).fail, "error"},
		{"disabled again", func(f *fakeFlag) { f.serve(Disabled) }, "disabled"},
	}

	for _, step := range steps {
		step.apply(h.flag)
		h.c.tick(t.Context())
		assert.Equal(t, step.want, h.set(t), step.name)
	}
}

// A percent some other code wrote is reported when the controller next
// writes over it, and a percent only the controller wrote is not.
func TestAWriteOutsideTheControllerIsWarnedWhenOverwritten(t *testing.T) {
	t.Parallel()

	h := newHarness(t, 100)
	foreign := func() []observer.LoggedEntry {
		return h.logs.FilterMessage("GC percent was changed outside the controller; overwriting it").All()
	}

	h.flag.serve(25)
	h.c.tick(t.Context())
	h.flag.serve(Disabled)
	h.c.tick(t.Context())
	assert.Empty(t, foreign(), "the controller's own writes are not foreign")

	h.runtime.mu.Lock()
	h.runtime.current = 60
	h.runtime.mu.Unlock()

	h.flag.serve(30)
	h.c.tick(t.Context())
	assert.Equal(t, []int{25, 100, 30}, h.runtime.writes())
	entries := foreign()
	require.Len(t, entries, 1)
	assert.Equal(t, zap.WarnLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	assert.EqualValues(t, 60, fields["found"])
	assert.EqualValues(t, 100, fields["last_written"])
	assert.EqualValues(t, 30, fields["gc_percent"])
}

func TestWarningsRelogOnTheIntervalWhileTheyHold(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, 100)
		warnings := func() int { return h.logs.FilterLevelExact(zap.WarnLevel).Len() }

		// 25 minutes of refused ticks: logged at 0, 10 and 20 minutes.
		h.flag.serve(0)
		for range 51 {
			h.c.tick(t.Context())
			time.Sleep(Interval)
		}
		assert.Equal(t, 3, warnings())

		// Leaving the condition and re-entering it logs at once, inside the
		// interval of the last warning.
		h.flag.serve(25)
		h.c.tick(t.Context())
		h.flag.serve(0)
		h.c.tick(t.Context())
		assert.Equal(t, 4, warnings())

		// A different refused value logs at once, so each one is on record.
		h.flag.serve(200)
		h.c.tick(t.Context())
		assert.Equal(t, 5, warnings())

		// Switching between the two warned outcomes logs at once too.
		h.flag.fail()
		h.c.tick(t.Context())
		assert.Equal(t, 6, warnings())

		// Disabled and applied never warn.
		before := warnings()
		for _, v := range []int{Disabled, 25, Disabled} {
			h.flag.serve(v)
			h.c.tick(t.Context())
		}
		assert.Equal(t, before, warnings())
	})
}

func TestRunEvaluatesAtOnceAndOnTheIntervalUntilItsContextEnds(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, 100)
		h.flag.serve(25)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			h.c.Run(ctx)
			close(done)
		}()

		synctest.Wait()
		assert.Equal(t, []int{25}, h.runtime.writes(), "the flag is evaluated when Run starts")
		assert.Equal(t, "applied", h.set(t))

		h.flag.serve(Disabled)
		time.Sleep(Interval - time.Second)
		synctest.Wait()
		assert.Equal(t, []int{25}, h.runtime.writes(), "nothing is evaluated between ticks")

		time.Sleep(time.Second)
		synctest.Wait()
		assert.Equal(t, []int{25, 100}, h.runtime.writes())

		// Hours of ticks and a failing flag do not end the loop.
		h.flag.fail()
		time.Sleep(4 * time.Hour)
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("Run returned while its context was live")
		default:
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run did not return after its context ended")
		}
	})
}

// Drives the real constructor, flag client and runtime: -1, refused values
// and a wrong-type value never change the GC percent in force, and a refused
// value after an applied one restores it.
//
//nolint:paralleltest // Writes the process-wide GC percent.
func TestUnappliedValuesNeverReachTheRuntime(t *testing.T) {
	original, err := readGCPercent()
	require.NoError(t, err)
	t.Cleanup(func() { debug.SetGCPercent(original) })

	source := ldtestdata.DataSource()
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	t.Cleanup(func() { _ = provider.Shutdown(context.WithoutCancel(t.Context())) })

	c, err := New(provider, flags)
	require.NoError(t, err)

	key := featureflags.OrchestratorGOGCPercentFlag.Key()
	serve := func(v ldvalue.Value) {
		source.Update(source.Flag(key).ValueForAll(v))
		c.tick(t.Context())
	}
	inForce := func() int {
		v, err := readGCPercent()
		require.NoError(t, err)

		return v
	}

	for _, v := range []ldvalue.Value{
		ldvalue.Int(Disabled), ldvalue.Int(0), ldvalue.Int(5), ldvalue.Int(101), ldvalue.String("25"),
	} {
		serve(v)
		assert.Equal(t, original, inForce(), "flag value %s", v.JSONString())
	}

	serve(ldvalue.Int(25))
	assert.Equal(t, 25, inForce())

	serve(ldvalue.Int(0))
	assert.Equal(t, original, inForce(), "a refused value restores the original")
}

// An environment that never defined the flag reads as disabled through the
// real flag client, not as a failed evaluation.
//
//nolint:paralleltest // Reads the process-wide GC percent through New.
func TestAnUndefinedFlagReadsAsDisabled(t *testing.T) {
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.WithoutCancel(t.Context())) })

	c, err := New(provider, flags)
	require.NoError(t, err)
	c.tick(t.Context())

	h := &harness{c: c, reader: reader}
	assert.Equal(t, "disabled", h.set(t))
}
