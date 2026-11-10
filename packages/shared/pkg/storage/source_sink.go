package storage

import (
	"context"
	"sync/atomic"
)

type readSourceKey struct{}

// SourceSink collects the highest-ranked backend touched by one read. The
// atomic handles reads whose segments run in parallel. Rank is a fixed source
// precedence, not a measurement of which segment took longest.
type SourceSink struct{ source atomic.Int32 }

func (s *SourceSink) Source() Source { return Source(s.source.Load()) }

// ReadSourceInto lets storage report a read's backend to its caller without
// changing every ReadAt signature between the caller and the chunker.
func ReadSourceInto(ctx context.Context, sink *SourceSink) context.Context {
	return context.WithValue(ctx, readSourceKey{}, sink)
}

// ReportSource records src on the context's destination, keeping the
// highest-ranked source. A context without one ignores it.
func ReportSource(ctx context.Context, src Source) {
	sink, _ := ctx.Value(readSourceKey{}).(*SourceSink)
	if sink == nil || src <= UnknownSource || src >= NumSources {
		return
	}
	for {
		current := sink.source.Load()
		if int32(src) <= current || sink.source.CompareAndSwap(current, int32(src)) {
			return
		}
	}
}
