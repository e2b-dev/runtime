package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadSourceIntoKeepsHighestRanked(t *testing.T) {
	t.Parallel()

	var sink SourceSink
	ctx := ReadSourceInto(t.Context(), &sink)

	ReportSource(ctx, SourceMmap)
	require.Equal(t, SourceMmap, sink.Source())

	ReportSource(ctx, SourceGCS)
	ReportSource(ctx, SourceMmap)
	require.Equal(t, SourceGCS, sink.Source(), "a lower-ranked later read does not replace the source")

	ReportSource(ctx, SourceNFS)
	require.Equal(t, SourceGCS, sink.Source())
	ReportSource(ctx, NumSources)
	require.Equal(t, SourceGCS, sink.Source(), "an invalid source cannot replace a valid one")
}

func TestReadSourceIntoParallelSegments(t *testing.T) {
	t.Parallel()

	var sink SourceSink
	ctx := ReadSourceInto(t.Context(), &sink)
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			ReportSource(ctx, SourceMmap)
			ReportSource(ctx, SourceGCS)
		})
	}
	wg.Wait()
	require.Equal(t, SourceGCS, sink.Source())
}

// A zero-filled range ranks below every real backend, so a read that also
// touched one reports that backend.
func TestReportSourceZeroRanksLowest(t *testing.T) {
	t.Parallel()

	var zeroOnly SourceSink
	ReportSource(ReadSourceInto(t.Context(), &zeroOnly), SourceZero)
	require.Equal(t, SourceZero, zeroOnly.Source())

	var mixed SourceSink
	ctx := ReadSourceInto(t.Context(), &mixed)
	ReportSource(ctx, SourceMmap)
	ReportSource(ctx, SourceZero)
	require.Equal(t, SourceMmap, mixed.Source())
}

func TestReportSourceWithoutDestinationIsNoop(t *testing.T) {
	t.Parallel()

	ReportSource(t.Context(), SourceGCS)
}

func BenchmarkReadSourceInto(b *testing.B) {
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		var sink SourceSink
		ReportSource(ReadSourceInto(ctx, &sink), SourceMmap)
		if sink.Source() != SourceMmap {
			b.Fatal("missing source")
		}
	}
}
