package grpc

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/metadata"
)

// WithSandboxClientMetricAttributes labels a client's otelgrpc metrics with the
// sandbox.resume and team.id values the caller put on the outgoing metadata,
// the same values the orchestrator's server metrics carry. The caller's side
// of every sandbox RPC then splits by team on its own deploy, and it also
// counts the calls that never reached a server.
func WithSandboxClientMetricAttributes() otelgrpc.Option {
	return otelgrpc.WithMetricAttributesFn(func(ctx context.Context) []attribute.KeyValue {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			return nil
		}

		return sandboxMetricAttrs(md)
	})
}
