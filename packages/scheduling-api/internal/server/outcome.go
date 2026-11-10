package server

import (
	"context"
	"path"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/scheduling-api/internal/server")

	outcomes = utils.Must(meter.Int64Counter("api.scheduling.rpc.outcomes",
		metric.WithDescription("Successful calls whose response carries an outcome, by method and outcome."), metric.WithUnit("{call}")))
)

// recordOutcomes counts the successful responses that carry an outcome enum.
func recordOutcomes(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}
	msg, ok := resp.(proto.Message)
	if !ok {
		return resp, nil
	}
	reflected := msg.ProtoReflect()
	field := reflected.Descriptor().Fields().ByName("outcome")
	if field == nil || field.Kind() != protoreflect.EnumKind {
		return resp, nil
	}
	value := field.Enum().Values().ByNumber(reflected.Get(field).Enum())
	if value == nil {
		return resp, nil
	}
	outcomes.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", path.Base(info.FullMethod)),
		attribute.String("outcome", string(value.Name())),
	))

	return resp, nil
}
