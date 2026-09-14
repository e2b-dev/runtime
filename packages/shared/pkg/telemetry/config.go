package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

var otelCollectorGRPCEndpoint = os.Getenv("OTEL_COLLECTOR_GRPC_ENDPOINT")

func OTELCollectorGRPCEndpoint() string {
	return otelCollectorGRPCEndpoint
}

// ServiceCommitKey carries the source commit beside service.version. A
// published version names one commit already; a from-source build reports
// the tree's SemVer default, and this is what still tells its builds apart.
const ServiceCommitKey = attribute.Key("service.commit")

func GetResource(ctx context.Context, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID string, additional ...attribute.KeyValue) (*resource.Resource, error) {
	attributes := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(serviceVersion),
		semconv.ServiceInstanceID(serviceInstanceID),
		semconv.TelemetrySDKName("otel"),
		semconv.HostID(nodeID),
		semconv.TelemetrySDKLanguageGo,
	}
	if serviceCommit != "" {
		attributes = append(attributes, ServiceCommitKey.String(serviceCommit))
	}

	attributes = append(attributes, additional...)
	hostname, err := os.Hostname()
	if err == nil {
		attributes = append(attributes, semconv.HostName(hostname))
	}

	res, err := resource.New(
		ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(attributes...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	return res, nil
}
