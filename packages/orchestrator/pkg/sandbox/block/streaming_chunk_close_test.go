//go:build linux

package block

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Closing from the Slice metric deterministically schedules eviction after the
// cached range was found but before ReadAt copies it. No timing or sleeps needed.
func TestChunker_ReadAtClosedAfterReadiness(t *testing.T) {
	t.Parallel()

	chunker := newTestChunker(t, testBlockSize)
	payload := makeTestData(int(testBlockSize))
	_, err := chunker.cache.WriteAt(payload, 0)
	require.NoError(t, err)

	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, chunker.Close())
		}
	})
	meter := closeOnRecordMeter{
		Meter: noop.NewMeterProvider().Meter("test"),
		onRecord: func() {
			require.False(t, closed)
			require.NoError(t, chunker.Close())
			closed = true
		},
	}
	chunker.metrics.ChunkSliceTimerFactory, err = telemetry.NewFloatTimerFactory(meter, "test.slice", "duration", "bytes")
	require.NoError(t, err)

	dst := bytes.Repeat([]byte{0xff}, int(testBlockSize))
	var n int
	err = RunFaultSafe(t.Context(), func() error {
		var readErr error
		n, readErr = chunker.ReadAt(t.Context(), dst, 0, nil, nil)

		return readErr
	})
	require.True(t, closed)
	var closedErr *CacheClosedError
	require.ErrorAs(t, err, &closedErr)
	require.Zero(t, n)
	require.Equal(t, bytes.Repeat([]byte{0xff}, int(testBlockSize)), dst)
}

// ReadAt must still return a streamed prefix while the rest of the chunk is
// blocked upstream: the full-chunk cache bitmap is not a readiness requirement.
func TestChunker_ReadAtPartialChunk(t *testing.T) {
	t.Parallel()

	payload := makeTestData(testFileSize)
	chunker := newControlledChunker(t, payload)
	t.Cleanup(func() {
		close(chunker.advance)
		<-chunker.closed
		require.NoError(t, chunker.Close())
	})

	dst := make([]byte, testBlockSize)
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := chunker.ReadAt(t.Context(), dst, 0, chunker.file, nil)
		done <- result{n: n, err: err}
	}()
	chunker.advance <- struct{}{}
	<-chunker.consumed
	r := <-done
	require.NoError(t, r.err)
	require.Equal(t, len(dst), r.n)
	require.Equal(t, payload[:testBlockSize], dst)
	require.False(t, chunker.cache.isCached(0, int64(len(payload))))
}

func TestChunker_EmptyReadAtEnd(t *testing.T) {
	t.Parallel()

	chunker := newTestChunker(t, testBlockSize)
	t.Cleanup(func() { require.NoError(t, chunker.Close()) })
	n, err := chunker.ReadAt(t.Context(), nil, testBlockSize, nil, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	slice, err := chunker.Slice(t.Context(), testBlockSize, 0, nil, nil)
	require.NoError(t, err)
	require.Empty(t, slice)
}

type closeOnRecordMeter struct {
	metric.Meter

	onRecord func()
}

func (m closeOnRecordMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	h, err := m.Meter.Float64Histogram(name, options...)
	if err != nil {
		return nil, err
	}

	return closeOnRecordHistogram{Float64Histogram: h, onRecord: m.onRecord}, nil
}

type closeOnRecordHistogram struct {
	metric.Float64Histogram

	onRecord func()
}

func (h closeOnRecordHistogram) Record(context.Context, float64, ...metric.RecordOption) {
	h.onRecord()
}
