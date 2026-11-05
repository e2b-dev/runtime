package servicestatus

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// HandleOverride serves ServiceStatusOverride through apply, a Machine's
// Override or a wrapper of it. A refusal is counted by reason on refused, when
// there is one, and returned as its gRPC status.
func HandleOverride(ctx context.Context, apply func(context.Context, Request) error, req *orchestratorinfo.ServiceStatusChangeRequest, refused metric.Int64Counter) (*emptypb.Empty, error) {
	err := apply(ctx, Request{Status: req.GetServiceStatus(), Owner: req.GetOwner(), Force: req.GetForce()})
	if err == nil {
		return &emptypb.Empty{}, nil
	}

	if refusal, ok := errors.AsType[*RefusedError](err); ok {
		if refused != nil {
			refused.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", string(refusal.Reason))))
		}

		return nil, refusal.GRPCStatus().Err()
	}

	return nil, err
}

// Fill writes the snapshot and the capabilities into a ServiceInfo response.
func Fill(resp *orchestratorinfo.ServiceInfoResponse, snap Snapshot) {
	resp.ServiceStatus = snap.Status
	resp.ServiceStatusChangedAt = timestamppb.New(snap.ChangedAt)
	resp.StatusOwner = snap.Owner
	resp.Capabilities = Capabilities
}
