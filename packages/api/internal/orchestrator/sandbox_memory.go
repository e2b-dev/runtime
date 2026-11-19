package orchestrator

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/db/queries"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	errCodeSandboxMemoryOutOfRange        = "sandbox_memory_out_of_range"
	errCodeSandboxMemoryShrinkUnsupported = "sandbox_memory_shrink_unsupported"
	// errCodeSandboxMemoryRecordMismatch is a node refusing a size the API
	// sent from its own record, not from the request: the record disagrees
	// with the node's snapshot.
	errCodeSandboxMemoryRecordMismatch = proxygrpc.SandboxMemoryRecordMismatch
)

var memoryMeter = otel.Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")

// memorySizeUnknownCounter counts starts and pauses of a sandbox whose build
// has a memory maximum but whose size the API does not know, by path. It
// stays at zero unless a writer without the size field (an older API or
// scheduling-api) touched the sandbox's record.
var memorySizeUnknownCounter, _ = memoryMeter.Int64Counter("api.sandbox.memory_size_unknown",
	metric.WithDescription("Number of starts and pauses of a sandbox whose memory size the API does not know, by path"))

// BuildStartMemoryMB is the memory a start from a build runs with when it names
// no size, and whether the API knows it. A build without a memory maximum runs
// at boot RAM. One with a maximum records the size in start_memory_mb: boot RAM
// for a template build, the size the sandbox ran with for a snapshot build. A
// build that does not record it was made without the size; the orchestrator
// then keeps the size its own snapshot recorded, which the API does not know.
func BuildStartMemoryMB(ramMB int64, maxRAMMB, startMemoryMB *int64) (int64, bool) {
	if maxRAMMB == nil {
		return ramMB, true
	}
	if startMemoryMB == nil {
		return ramMB, false
	}

	return max(ramMB, *startMemoryMB), true
}

// placementMemoryMiB is the memory placement accounts a start at. A build
// with a memory maximum whose start size the API does not know is accounted
// at the maximum: the orchestrator keeps its snapshot's size, which is at
// most that, and over-counting a node only delays a placement while
// under-counting it lets the node take more than its huge pages hold.
func placementMemoryMiB(build queries.EnvBuild, memoryMB int64, known bool) int64 {
	if build.MaxRamMb != nil && !known {
		return *build.MaxRamMb
	}

	return memoryMB
}

// reportMemorySizeUnknown logs and counts a start or pause of a sandbox
// whose build has a memory maximum but whose size the API does not know.
func reportMemorySizeUnknown(ctx context.Context, path string, bootMB, maxMB int64, fields ...zap.Field) {
	logger.L().Warn(ctx, "sandbox memory size unknown for a build with a memory maximum; an older API or scheduling-api may have dropped it",
		append(fields, zap.String("path", path), zap.Int64("boot_memory_mb", bootMB), zap.Int64("max_memory_mb", maxMB))...)
	telemetry.ReportEvent(ctx, "sandbox memory size unknown",
		attribute.String("path", path), attribute.Int64("boot_memory_mb", bootMB), attribute.Int64("max_memory_mb", maxMB))
	if memorySizeUnknownCounter != nil {
		memorySizeUnknownCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("path", path)))
	}
}

// reportMemoryRecordMismatch logs, and records on the start's span, a node refusing
// the memory size the API sent from its own record, with every size involved.
func reportMemoryRecordMismatch(ctx context.Context, err error, sandboxID string, sbxData SandboxMetadata, refusal string, sentMB int64) {
	attrs := []attribute.KeyValue{
		telemetry.WithSandboxID(sandboxID),
		telemetry.WithTemplateID(sbxData.TemplateID),
		telemetry.WithBuildID(sbxData.Build.ID.String()),
		attribute.String("refusal", refusal),
		attribute.Int64("sent_memory_mb", sentMB),
		attribute.Int64("boot_memory_mb", sbxData.Build.RamMb),
	}
	if sbxData.Build.StartMemoryMb != nil {
		attrs = append(attrs, attribute.Int64("start_memory_mb", *sbxData.Build.StartMemoryMb))
	}
	if sbxData.Build.MaxRamMb != nil {
		attrs = append(attrs, attribute.Int64("max_memory_mb", *sbxData.Build.MaxRamMb))
	}
	telemetry.ReportCriticalError(ctx, "Node refused the memory size recorded for the sandbox", err, attrs...)
}
