//go:build linux

package sandbox

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	snapshotDiffBytes  = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotDiffBytes))
	snapshotDiffRatio  = utils.Must(telemetry.GetFloatHistogram(meter, telemetry.SnapshotDiffRatio))
	snapshotTotalBytes = utils.Must(telemetry.GetHistogram(meter, telemetry.SnapshotTotalBytes))
)

type SnapshotUseCase string

const (
	SnapshotUseCasePause SnapshotUseCase = "pause"
	SnapshotUseCaseBuild SnapshotUseCase = "build"
)

func recordSnapshotDiff(
	ctx context.Context,
	teamID string,
	fileType string,
	dm *header.DiffMetadata,
	original *header.Header,
) {
	if dm == nil || original == nil || original.Metadata == nil {
		return
	}
	bs := int64(original.Metadata.BlockSize)
	total := int64(original.Metadata.Size)

	ft := attribute.String("file_type", fileType)
	team := telemetry.WithTeamID(teamID)

	snapshotTotalBytes.Record(ctx, total, metric.WithAttributes(ft, team))

	var dirtyBytes, emptyBytes int64
	if dm.Dirty != nil {
		dirtyBytes = int64(dm.Dirty.GetCardinality()) * bs
	}
	if dm.Empty != nil {
		emptyBytes = int64(dm.Empty.GetCardinality()) * bs
	}
	for kind, b := range map[string]int64{
		"dirty": dirtyBytes,
		"empty": emptyBytes,
	} {
		attrs := metric.WithAttributes(ft, attribute.String("kind", kind), team)
		snapshotDiffBytes.Record(ctx, b, attrs)
		snapshotDiffRatio.Record(ctx, ratioFraction(b, total), attrs)
	}
}

// recordSnapshotDedup records bytes saved by dedup on the snapshot.diff.*
// histograms with kind ∈ {none, dedup, best_effort_dedup}; the modes are
// mutually exclusive per pause so dashboards split cleanly by kind.
func recordSnapshotDedup(
	ctx context.Context,
	teamID string,
	fileType string,
	pre, post *header.DiffMetadata,
	bestEffort bool,
) {
	var kind string
	switch {
	case post == nil:
		kind = "none"
	case bestEffort:
		kind = "best_effort_dedup"
	default:
		kind = "dedup"
	}

	var preBytes, savings int64
	if pre != nil && pre.Dirty != nil {
		preBytes = int64(pre.Dirty.GetCardinality()) * pre.BlockSize
	}
	if post != nil && post.Dirty != nil {
		savings = max(preBytes-int64(post.Dirty.GetCardinality())*post.BlockSize, 0)
	}

	attrs := metric.WithAttributes(
		attribute.String("file_type", fileType),
		attribute.String("kind", kind),
		telemetry.WithTeamID(teamID),
	)
	snapshotDiffBytes.Record(ctx, savings, attrs)
	snapshotDiffRatio.Record(ctx, ratioFraction(savings, preBytes), attrs)
}

// ratioFraction returns num/denom as a fraction in [0,1] (1.0 = 100%). Emitted
// with unit {1} so Grafana percent formatting renders it directly.
func ratioFraction(num, denom int64) float64 {
	if denom <= 0 {
		return 0
	}
	r := float64(num) / float64(denom)
	if r < 0 {
		return 0
	}
	if r > 1 {
		return 1
	}

	return r
}
