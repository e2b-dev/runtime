package telemetry

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type mutexWaitInputKey struct{}

// The production registration/observation helper is used with real SDK
// callbacks. Inputs are supplied floats, not a native thread-retirement probe.
func mutexWaitReaders(t *testing.T, count int) []*sdkmetric.ManualReader {
	t.Helper()

	readers := make([]*sdkmetric.ManualReader, count)
	options := make([]sdkmetric.Option, count)
	for i := range readers {
		readers[i] = sdkmetric.NewManualReader()
		options[i] = sdkmetric.WithReader(readers[i])
	}
	provider := sdkmetric.NewMeterProvider(options...)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })
	meter := provider.Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	wait, err := newMutexWaitMetric(meter)
	require.NoError(t, err)
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		wait.observe(observer, ctx.Value(mutexWaitInputKey{}).(float64))

		return nil
	}, wait.instrument)
	require.NoError(t, err)

	return readers
}

func collectMutexWait(reader *sdkmetric.ManualReader, input float64) (metricdata.Metrics, error) {
	var result metricdata.ResourceMetrics
	ctx := context.WithValue(context.Background(), mutexWaitInputKey{}, input)
	if err := reader.Collect(ctx, &result); err != nil {
		return metricdata.Metrics{}, err
	}

	return result.ScopeMetrics[0].Metrics[0], nil
}

func TestMutexWaitObservationPreservesCounterContract(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		input  []float64
		output []float64
	}{
		{name: "zero", input: []float64{0, 0}, output: []float64{0, 0}},
		{name: "nondecreasing", input: []float64{0, 1, 5, 10}, output: []float64{0, 1, 5, 10}},
		{name: "temporary_drop", input: []float64{10, 5, 10, 12}, output: []float64{10, 10, 10, 12}},
		{name: "same_registration_zero", input: []float64{10, 0, 1, 12}, output: []float64{10, 10, 10, 12}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader := mutexWaitReaders(t, 1)[0]
			var first metricdata.DataPoint[float64]
			for i, input := range tc.input {
				result, err := collectMutexWait(reader, input)
				require.NoError(t, err)
				assert.Equal(t, "go.sync.mutex.wait.time", result.Name)
				assert.Equal(t, "s", result.Unit)
				assert.Contains(t, result.Description, "Observed high-water")
				sum, ok := result.Data.(metricdata.Sum[float64])
				require.True(t, ok)
				assert.True(t, sum.IsMonotonic)
				assert.Equal(t, metricdata.CumulativeTemporality, sum.Temporality)
				require.Len(t, sum.DataPoints, 1)
				point := sum.DataPoints[0]
				assert.Zero(t, point.Attributes.Len())
				assert.InDelta(t, tc.output[i], point.Value, 0, "counter observation for input %v", input)
				if i == 0 {
					first = point
				}
				assert.Equal(t, first.StartTime, point.StartTime)
			}
		})
	}
}

func TestMutexWaitObservationRegistrationOwnership(t *testing.T) {
	t.Parallel()

	oldReader := mutexWaitReaders(t, 1)[0]
	newReader := mutexWaitReaders(t, 1)[0]
	for _, tc := range []struct {
		reader *sdkmetric.ManualReader
		input  float64
		want   float64
	}{
		{oldReader, 10, 10},
		{oldReader, 0, 10},
		{newReader, 0, 0},
		{newReader, 1, 1},
		{oldReader, 2, 10},
	} {
		result, err := collectMutexWait(tc.reader, tc.input)
		require.NoError(t, err)
		assert.InDelta(t, tc.want, result.Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0)
	}
}

func TestMutexWaitObservationConcurrentReaders(t *testing.T) {
	t.Parallel()

	readers := mutexWaitReaders(t, 4)
	var joined sync.WaitGroup
	for readerIndex, reader := range readers {
		joined.Go(func() {
			previous := 0.0
			for i := range 40 {
				input := float64((i*7 + readerIndex*3) % 23)
				result, err := collectMutexWait(reader, input)
				if err != nil {
					t.Errorf("collect mutex wait: %v", err)

					return
				}
				sum := result.Data.(metricdata.Sum[float64])
				point := sum.DataPoints[0]
				assert.GreaterOrEqual(t, point.Value, previous, "concurrent reader decreased")
				assert.GreaterOrEqual(t, point.Value, input)
				previous = point.Value
			}
		})
	}
	joined.Wait()
	for _, reader := range readers {
		result, err := collectMutexWait(reader, 0)
		require.NoError(t, err)
		assert.InDelta(t, float64(22), result.Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0)
	}
}

type gatedMutexWaitObserver struct {
	metric.Observer

	before func()
}

func (o gatedMutexWaitObserver) ObserveFloat64(instrument metric.Float64Observable, value float64, options ...metric.ObserveOption) {
	o.before()
	o.Observer.ObserveFloat64(instrument, value, options...)
}

func TestMutexWaitObservationSerializesPublication(t *testing.T) {
	t.Parallel()

	firstReader := sdkmetric.NewManualReader()
	secondReader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(firstReader), sdkmetric.WithReader(secondReader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(t.Context())) })
	meter := provider.Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	wait, err := newMutexWaitMetric(meter)
	require.NoError(t, err)
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var secondPublication atomic.Bool
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		input := ctx.Value(mutexWaitInputKey{}).(float64)
		if input == 12 {
			close(secondEntered)
		}
		wait.observe(gatedMutexWaitObserver{Observer: observer, before: func() {
			if input == 10 {
				close(firstEntered)
				<-releaseFirst
			} else {
				secondPublication.Store(true)
			}
		}}, input)

		return nil
	}, wait.instrument)
	require.NoError(t, err)
	var first, second metricdata.Metrics
	var firstErr, secondErr error
	var joined sync.WaitGroup
	var release sync.Once
	defer func() {
		release.Do(func() { close(releaseFirst) })
		joined.Wait()
	}()
	joined.Go(func() {
		first, firstErr = collectMutexWait(firstReader, 10)
	})
	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first SDK callback never reached publication")
	}
	// This is the actual publication barrier, not a guessed elapsed-time or
	// post-callback marker. The guard must still be held inside ObserveFloat64.
	if wait.mu.TryLock() {
		wait.mu.Unlock()
		t.Error("guard released before SDK publication")
	}
	joined.Go(func() {
		second, secondErr = collectMutexWait(secondReader, 12)
	})
	select {
	case <-secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second SDK callback never reached the guard")
	}
	assert.False(t, secondPublication.Load(), "newer callback overtook blocked publication")
	release.Do(func() { close(releaseFirst) })
	joined.Wait()
	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	assert.True(t, secondPublication.Load())
	assert.InDelta(t, float64(10), first.Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0)
	assert.InDelta(t, float64(12), second.Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0)
}
