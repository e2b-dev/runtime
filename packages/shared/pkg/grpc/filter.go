package grpc

import (
	"context"

	"google.golang.org/grpc/stats"
)

// IsResumeMetadataKey is the gRPC metadata key used to pass the is_resume/snapshot
// value from the API client to the orchestrator server. This allows the server-side
// otelgrpc stats handler to include sandbox.resume in metric attributes during TagRPC,
// before the request payload is available.
const IsResumeMetadataKey = "x-sandbox-resume"

// TeamIDMetadataKey carries the sandbox's team ID for the same reason
// IsResumeMetadataKey does: so the server's otelgrpc metrics can be split by
// team before the request payload is decoded.
const TeamIDMetadataKey = "x-team-id"

type noTraceKey struct{}

var noTrace = struct{}{}

// statsWrapper wraps grpc stats.Handler and removes healthchecks from tracing.
type statsWrapper struct {
	statsHandler stats.Handler
}

// NewStatsWrapper wraps grpc stats.Handler and removes healthchecks from tracing.
func NewStatsWrapper(statsHandler stats.Handler) stats.Handler {
	return &statsWrapper{statsHandler: statsHandler}
}

// HandleConn exists to satisfy gRPC stats.Handler.
func (s *statsWrapper) HandleConn(context.Context, stats.ConnStats) {
	// no-op
}

// TagConn exists to satisfy gRPC stats.Handler.
func (s *statsWrapper) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	// no-op
	return ctx
}

// HandleRPC implements per-RPC tracing and stats instrumentation.
func (s *statsWrapper) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	// Check if the context contains noTraceKey, and trace only when its
	// not present.
	_, skip := ctx.Value(noTraceKey{}).(struct{})
	if skip {
		return
	}

	s.statsHandler.HandleRPC(ctx, rs)
}

// TagRPC implements per-RPC context management.
func (s *statsWrapper) TagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	if rti.FullMethodName == "/grpc.health.v1.Health/Check" {
		// Add to context we don't want to trace this.
		return context.WithValue(ctx, noTraceKey{}, noTrace)
	}

	return s.statsHandler.TagRPC(ctx, rti)
}
