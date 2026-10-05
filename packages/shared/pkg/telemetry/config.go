package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

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

const HostKernelVersionKey = attribute.Key("host.kernel.version")

const kernelOSReleasePath = "/proc/sys/kernel/osrelease"

// HostKernelVersion returns the host kernel release, or "" where it cannot be
// read (non-Linux hosts, or /proc not mounted).
func HostKernelVersion() string {
	return hostKernelVersion(kernelOSReleasePath)
}

func hostKernelVersion(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(data))
}

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
	if kernel := HostKernelVersion(); kernel != "" {
		attributes = append(attributes, HostKernelVersionKey.String(kernel))
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
