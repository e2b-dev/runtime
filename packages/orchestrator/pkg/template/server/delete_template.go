//go:build linux

package server

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/builderrors"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/template"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/server")

func (s *ServerStore) TemplateBuildDelete(ctx context.Context, in *templatemanager.TemplateBuildDeleteRequest) (*emptypb.Empty, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	done := s.info.TrackWork()
	defer done()

	ctx, childSpan := tracer.Start(ctx, "template-delete-request", trace.WithAttributes(
		telemetry.WithTemplateID(in.GetTemplateID()),
		telemetry.WithBuildID(in.GetBuildID()),
	))
	defer childSpan.End()

	if in.GetTemplateID() == "" || in.GetBuildID() == "" {
		return nil, errors.New("template id and build id are required fields")
	}

	buildInfo, err := s.buildCache.Get(in.GetBuildID())
	if err == nil && buildInfo.IsRunning() {
		// Cancel the build if it is running
		logger.L().Info(ctx, "Canceling running template build", logger.WithTemplateID(in.GetTemplateID()), logger.WithBuildID(in.GetBuildID()))
		telemetry.ReportEvent(ctx, "cancel in progress template build")
		buildInfo.SetFail(&templatemanager.TemplateBuildStatusReason{
			Message: builderrors.ErrCanceled.Error(),
		})
	}

	err = template.Delete(ctx, s.artifactsregistry, s.templateStorage, in.GetTemplateID(), in.GetBuildID())
	if err != nil {
		return nil, err
	}

	return nil, nil
}
