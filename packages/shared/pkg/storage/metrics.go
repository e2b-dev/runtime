package storage

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/lock"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Attribute vocabulary for the orchestrator.read.* / orchestrator.chunk.* /
// orchestrator.writeback metrics.
const (
	AttrSource   = "source"
	AttrCodec    = "codec"
	AttrOutcome  = "outcome"
	AttrFileType = "file_type"
	AttrTrigger  = "trigger"
)

// Writeback triggers: a read-miss cache fill vs a build/store write-through.
const (
	TriggerRead  = "read"
	TriggerWrite = "write"
)

const (
	OutcomeOK           = "ok"
	OutcomeNotFound     = "not_found"
	OutcomeErrCanceled  = "err_canceled"
	OutcomeErrIO        = "err_io"
	OutcomeErrTimeout   = "err_timeout"
	OutcomeTransitioned = "transitioned"
	// OutcomeArchived: the object exists but is in an archived storage class
	// that does not allow direct reads.
	OutcomeArchived = "archived"
	// OutcomeContended is a writeback skipped because another goroutine held the
	// NFS chunk lock — normal cache dedup, nothing written.
	OutcomeContended = "contended"
)

// Source identifies the backend that served a read.
type Source int8

const (
	// Order is latency-ascending and load-bearing: Slice records the slowest
	// source it touched via max() over per-fetch sources.
	UnknownSource Source = iota
	SourceMmap
	SourceFS
	SourcePeer
	SourceNFS
	SourceGCS
	SourceAWS
	SourceAzure
	numSources
)

func (s Source) String() string { return sourceStrings[s] }

var sourceStrings = [numSources]string{
	UnknownSource: "unknown",
	SourceMmap:    "mmap",
	SourceFS:      "fs",
	SourcePeer:    "peer",
	SourceNFS:     "nfs",
	SourceGCS:     "gcs",
	SourceAWS:     "aws",
	SourceAzure:   "azure",
}

// Outcome maps a read-path error to the outcome enum. PeerTransitionedError is a
// routing signal, not a failure, so it gets its own bucket (not err_io).
func Outcome(err error) string {
	var transErr *PeerTransitionedError
	switch {
	case err == nil:
		return OutcomeOK
	case errors.Is(err, ErrObjectNotExist), errors.Is(err, fs.ErrNotExist):
		return OutcomeNotFound
	case errors.Is(err, ErrObjectArchived):
		return OutcomeArchived
	case errors.Is(err, context.Canceled):
		return OutcomeErrCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return OutcomeErrTimeout
	case errors.As(err, &transErr):
		return OutcomeTransitioned
	case errors.Is(err, lock.ErrLockAlreadyHeld):
		return OutcomeContended
	default:
		return OutcomeErrIO
	}
}

// numCodecs tracks the CompressionType enum so a new codec grows the table
// instead of being clamped to none in safeAttrIdx.
const numCodecs = int(numCompressionTypes)

// tableOK precomputes the OK-outcome attribute set for the hot path; error and
// blob paths build attrs inline.
var tableOK [numSeekableObjectTypes][numSources][numCodecs]metric.MeasurementOption

func init() {
	set := func(kvs ...attribute.KeyValue) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(kvs...))
	}

	for ot := range numSeekableObjectTypes {
		ftAttr := attribute.String(AttrFileType, ot.String())
		for s := range numSources {
			srcAttr := attribute.String(AttrSource, sourceStrings[s])
			for ct := range CompressionType(numCodecs) {
				tableOK[ot][s][ct] = set(ftAttr, srcAttr,
					attribute.String(AttrCodec, ct.String()),
					attribute.String(AttrOutcome, OutcomeOK))
			}
		}
	}
}

func safeAttrIdx(o SeekableObjectType, s Source, c CompressionType) (SeekableObjectType, Source, CompressionType) {
	if uint(o) >= uint(numSeekableObjectTypes) {
		o = UnknownSeekableObjectType
	}
	if uint(s) >= uint(numSources) {
		s = UnknownSource
	}
	if uint(c) >= uint(numCodecs) {
		c = CompressionNone
	}

	return o, s, c
}

func OKAttrs(o SeekableObjectType, s Source, c CompressionType) metric.MeasurementOption {
	o, s, c = safeAttrIdx(o, s, c)

	return tableOK[o][s][c]
}

func ErrAttrs(o SeekableObjectType, s Source, c CompressionType, err error) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String(AttrFileType, o.String()),
		attribute.String(AttrSource, s.String()),
		attribute.String(AttrCodec, c.String()),
		attribute.String(AttrOutcome, Outcome(err)),
	)
}

func mustFloatHist(name, desc, unit string) metric.Float64Histogram {
	return utils.Must(meter.Float64Histogram(name,
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	))
}

var (
	readOpen = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.read.open",
		"OpenRangeReader (open / TTFB) wall",
		"Bytes (always 0 — open transfers no payload)",
	))
	readRead = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.read.read",
		"Raw source-read wall (decompression excluded)",
		"Compressed/stored bytes read from the source",
	))
	readDecompress = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.read.decompress",
		"Decompression CPU wall (decoder read time minus source transfer)",
		"Uncompressed bytes produced",
	))
	readFetch = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.read.fetch",
		"Total fetch wall — should ≈ open + read + decompress; any excess is overhead (see read.pipeline.efficiency)",
		"Bytes delivered to the app",
	))
	readPipelineEfficiency = mustFloatHist(
		"orchestrator.read.pipeline.efficiency",
		"fetch / (open + read + decompress) — 1.0 = fetch wall fully explained by work, >1 = overhead", "1",
	)

	// writeback covers every NFS cache write: a read-miss fill (trigger=read) or
	// a build/store write-through (trigger=write). A dedup skip is outcome=contended.
	writeback = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.writeback",
		"NFS cache write wall",
		"Bytes written to NFS",
	))

	// Size() transfers nothing, so duration + count only (no bytes counter).
	readSize = utils.Must(meter.Float64Histogram(
		"orchestrator.read.size",
		metric.WithDescription("Size() metadata-lookup wall"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(telemetry.SubMillisecondMsBuckets...),
	))
	readSizeCount = utils.Must(meter.Int64Counter(
		"orchestrator.read.size",
		metric.WithDescription("Total orchestrator.read.size events recorded"),
	))

	// read.blob / read.blob.decompress: the blob-path analog of read.read /
	// read.decompress. read.blob carries source — the transfer cascades
	// peer->NFS->leaf and is recorded per layer; read.blob.decompress is CPU on
	// resolved bytes, so it has none.
	readBlob = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.read.blob",
		"Whole-object blob transfer (WriteTo) wall",
		"Bytes transferred",
	))
	readBlobDecompress = utils.Must(telemetry.NewFloatTimerFactory(meter,
		"orchestrator.read.blob.decompress",
		"Blob deserialize/decompress wall (e.g. header LZ4 decode + parse)",
		"Uncompressed bytes produced",
	))
)

// RecordReadOpen records one layer's own open attempt (not the delegated inner
// call), so a slow peer/NFS open isn't misattributed to the resolved source.
func RecordReadOpen(ctx context.Context, dur time.Duration, ot SeekableObjectType, src Source, ct CompressionType, err error) {
	attrs := OKAttrs(ot, src, ct)
	if err != nil {
		attrs = ErrAttrs(ot, src, ct, err)
	}
	readOpen.Record(ctx, dur, 0, attrs)
}

func RecordReadRead(ctx context.Context, dur time.Duration, bytes int64, attrs metric.MeasurementOption) {
	readRead.Record(ctx, dur, bytes, attrs)
}

func recordDecompressStep(ctx context.Context, r *decompressReader, stats *ReadStats, readErr error) {
	attrs := OKAttrs(r.objType, r.source, r.ct)
	if readErr != nil {
		attrs = ErrAttrs(r.objType, r.source, r.ct, readErr)
	}
	readDecompress.Record(ctx, stats.Decompress, stats.DeliveredBytes, attrs)
}

func RecordReadFetch(ctx context.Context, dur time.Duration, bytes int64, attrs metric.MeasurementOption) {
	readFetch.Record(ctx, dur, bytes, attrs)
}

func RecordPipelineEfficiency(ctx context.Context, ratio float64, attrs metric.MeasurementOption) {
	readPipelineEfficiency.Record(ctx, ratio, attrs)
}

// recordWriteback emits orchestrator.writeback. src is the byte origin (the
// read's fetch source, or fs for a build write-through); the write itself always
// targets NFS. trigger is TriggerRead/TriggerWrite. A contended skip wrote
// nothing, so its byte count is zeroed.
func recordWriteback(ctx context.Context, dur time.Duration, bytes int64, ot SeekableObjectType, src Source, ct CompressionType, trigger string, err error) {
	outcome := Outcome(err)
	if outcome == OutcomeContended {
		bytes = 0
	}
	writeback.Record(ctx, dur, bytes, metric.WithAttributes(
		attribute.String(AttrFileType, ot.String()),
		attribute.String(AttrSource, src.String()),
		attribute.String(AttrCodec, ct.String()),
		attribute.String(AttrOutcome, outcome),
		attribute.String(AttrTrigger, trigger),
	))
}

func RecordReadSize(ctx context.Context, dur time.Duration, ot SeekableObjectType, src Source, err error) {
	attrs := OKAttrs(ot, src, CompressionNone)
	if err != nil {
		attrs = ErrAttrs(ot, src, CompressionNone, err)
	}
	readSize.Record(ctx, float64(dur)/float64(time.Millisecond), attrs)
	readSizeCount.Add(ctx, 1, attrs)
}

// blob file_type is not a SeekableObjectType, so read.blob* build attrs inline
// rather than via the precomputed OKAttrs table.
func RecordReadBlob(ctx context.Context, dur time.Duration, bytes int64, path string, src Source, err error) {
	readBlob.Record(ctx, dur, bytes, metric.WithAttributes(
		attribute.String(AttrFileType, blobType(path)),
		attribute.String(AttrSource, src.String()),
		attribute.String(AttrCodec, CompressionNone.String()),
		attribute.String(AttrOutcome, Outcome(err)),
	))
}

func RecordReadBlobDecompress(ctx context.Context, dur time.Duration, bytes int64, path string, ct CompressionType, err error) {
	readBlobDecompress.Record(ctx, dur, bytes, metric.WithAttributes(
		attribute.String(AttrFileType, blobType(path)),
		attribute.String(AttrCodec, ct.String()),
		attribute.String(AttrOutcome, Outcome(err)),
	))
}

var meter = otel.Meter("github.com/e2b-dev/infra/packages/shared/pkg/storage")

// uploadFramesHeld counts the memory compressed frames occupy, by capacity,
// from compression until their part is uploaded. See heldFrames.
var uploadFramesHeld = utils.Must(meter.Int64UpDownCounter(
	"orchestrator.storage.upload.frames.held",
	metric.WithDescription("Memory held by compressed frames of in-progress uploads, by allocated capacity, until their part is uploaded"),
	metric.WithUnit("By"),
))

var googleWriteTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
	"orchestrator.storage.gcs.write",
	"Duration of GCS writes",
	"Total bytes written to GCS",
	"Total writes to GCS",
))
