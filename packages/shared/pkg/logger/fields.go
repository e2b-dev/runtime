package logger

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func WithSandboxID(sandboxID string) zap.Field {
	return zap.String("sandbox.id", sandboxID)
}

func WithLifecycleID(lifecycleID string) zap.Field {
	return zap.String("sandbox.lifecycle.id", lifecycleID)
}

func WithTemplateID(templateID string) zap.Field {
	return zap.String("template.id", templateID)
}

func WithBuildID(buildID string) zap.Field {
	return zap.String("build.id", buildID)
}

// WithError is the internal field helper for attaching an error to a log line,
// so callers use the logger package's API instead of importing zap directly.
func WithError(err error) zap.Field {
	return zap.Error(err)
}

func WithExecutionID(executionID string) zap.Field {
	return zap.String("execution.id", executionID)
}

func WithUserID(userID string) zap.Field {
	return zap.String("user.id", userID)
}

func WithTeamID(teamID string) zap.Field {
	return zap.String("team.id", teamID)
}

func WithNodeID(nodeID string) zap.Field {
	return zap.String("node.id", nodeID)
}

func WithClusterID(clusterID uuid.UUID) zap.Field {
	return zap.String("cluster.id", clusterID.String())
}

func WithEdgeTraceID(traceID string) zap.Field {
	return zap.String("edge.trace.id", traceID)
}

func WithServiceInstanceID(instanceID string) zap.Field {
	return zap.String("service.instance.id", instanceID)
}

func WithSandboxIP(sandboxIP string) zap.Field {
	return zap.String("sandbox.ip", sandboxIP)
}

func WithEnvdVersion(envdVersion string) zap.Field {
	return zap.String("envd.version", envdVersion)
}

func WithKernelVersion(kernelVersion string) zap.Field {
	return zap.String("sandbox.kernel.version", kernelVersion)
}

func WithFirecrackerVersion(firecrackerVersion string) zap.Field {
	return zap.String("sandbox.firecracker.version", firecrackerVersion)
}

func WithClientIP(clientIP string) zap.Field {
	return zap.String("http.client_ip", clientIP)
}

func WithMaskedAPIKey(maskedAPIKey string) zap.Field {
	return zap.String("auth.api_key", maskedAPIKey)
}

// timeFields emits both a human-readable RFC3339Nano string and a unix timestamp.
type timeFields struct {
	key string
	val time.Time
}

func (t timeFields) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString(t.key, t.val.Format(time.RFC3339Nano))
	enc.AddInt64(t.key+"_unix", t.val.UnixNano())

	return nil
}

// Time returns a zap.Field that logs the given time as both an RFC3339Nano
// string (key) and a unix epoch in nanoseconds (key_unix).
func Time(key string, val time.Time) zap.Field {
	return zap.Inline(timeFields{key: key, val: val})
}

// ProxyRequestFields returns the common logger fields for a proxied HTTP request.
func ProxyRequestFields(r *http.Request, sandboxID string, sandboxPort uint64) []zap.Field {
	return []zap.Field{
		zap.String("origin_host", r.Host),
		WithSandboxID(sandboxID),
		zap.Uint64("sandbox_req_port", sandboxPort),
		zap.String("sandbox_req_path", r.URL.Path),
		zap.String("sandbox_req_method", r.Method),
		zap.String("sandbox_req_user_agent", r.UserAgent()),
		zap.String("remote_addr", r.RemoteAddr),
		WithClientIP(clientIP(r)),
		zap.Int64("content_length", r.ContentLength),
	}
}

// clientIP extracts the real client IP from the request.
// It reads the first entry from X-Forwarded-For, falling back to RemoteAddr with the port stripped.
//
// This assumes a trusted upstream proxy overwrites the
// X-Forwarded-For header with the real client IP. The header value is NOT
// client-controllable in this setup because the LB always replaces it.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip := strings.TrimSpace(strings.SplitN(xff, ",", 2)[0]); ip != "" {
			return ip
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}
