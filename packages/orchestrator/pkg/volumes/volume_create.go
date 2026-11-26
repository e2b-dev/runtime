//go:build linux

package volumes

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func (s *Service) CreateVolume(
	ctx context.Context,
	request *orchestrator.CreateVolumeRequest,
) (r *orchestrator.CreateVolumeResponse, err error) {
	_, span := tracer.Start(ctx, "create volume")
	defer func() {
		setSpanStatus(span, err)
		span.End()
	}()

	fullPath, err := s.getVolumeRootPath(ctx, request.GetVolume())
	if err != nil {
		return nil, fmt.Errorf("failed to build volume path: %w", err)
	}

	span.AddEvent("creating volume", trace.WithAttributes(
		attribute.String("path", fullPath),
	))

	// Fail fast if a volume filesystem type is configured but the path is not on
	// such a mount - otherwise MkdirAll would silently create the volume on the
	// local root disk when the distributed FS is not mounted. No-op by default.
	if err := verifyVolumeMount(fullPath, s.config.VolumeExpectedFSType); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(fullPath, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create volume: %w", err)
	}

	response := &orchestrator.CreateVolumeResponse{}

	if vt := request.GetVolume().GetVolumeType(); vt != "" {
		response.VolumeType = &vt
	}

	return response, nil
}
