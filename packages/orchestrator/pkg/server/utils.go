//go:build linux

package server

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// The start-gate histogram's labels: what the slot is for, how the attempt ended.
const (
	startGateCreate     = "create"
	startGateResume     = "resume"
	startGateCheckpoint = "checkpoint"
	startGateHarvest    = "harvest"

	startGateAcquired  = "acquired"
	startGateCancelled = "cancelled"
	startGateRefused   = "refused"
)

// waitForAcquire takes a start slot for kind, waiting up to acquireTimeout for one to free.
func (s *Server) waitForAcquire(ctx context.Context, kind string) error {
	ctx, cancel := context.WithTimeout(ctx, acquireTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "wait-for-acquire")
	defer span.End()

	started := time.Now()
	err := s.startingSandboxes.Acquire(ctx, 1)
	waited := time.Since(started)
	if err != nil {
		s.recordStartGateWait(ctx, kind, startGateCancelled, waited)
		telemetry.ReportEvent(ctx, "too many resuming sandboxes on node")

		return status.Errorf(codes.ResourceExhausted, "too many sandboxes resuming on this node: current=%d, max=%d, please retry", s.startingSandboxes.Current(), s.startingSandboxes.Limit())
	}
	s.recordStartGateWait(ctx, kind, startGateAcquired, waited)

	return nil
}

// tryAcquireStart takes a start slot for kind without waiting; a refusal is the caller's to report.
func (s *Server) tryAcquireStart(ctx context.Context, kind string) bool {
	acquired := s.startingSandboxes.TryAcquire(1)
	outcome := startGateRefused
	if acquired {
		outcome = startGateAcquired
	}
	s.recordStartGateWait(ctx, kind, outcome, 0)

	return acquired
}

func (s *Server) recordStartGateWait(ctx context.Context, kind, outcome string, waited time.Duration) {
	s.startGateWaitDuration.Record(ctx, waited.Milliseconds(), metric.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("outcome", outcome),
	))
}
