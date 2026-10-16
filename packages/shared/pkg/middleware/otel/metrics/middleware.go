package metrics

import (
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
)

const MetricPrefix = "metric."

// processingStartTimeKey is the gin context key for storing when request processing
// (after body parsing) began. This allows metrics to exclude body upload/parsing time.
const processingStartTimeKey = "metrics.processingStartTime"

const HTTPStatusCodeGranularKey = attribute.Key("http.status_code_granular")

// SetProcessingStartTime stores the current time as the processing start time in the gin context.
func SetProcessingStartTime(c *gin.Context) {
	c.Set(processingStartTimeKey, time.Now())
}

// getProcessingStartTime retrieves the processing start time from the gin context.
// Returns the time and true if set, or zero time and false if not set.
func getProcessingStartTime(c *gin.Context) (time.Time, bool) {
	if val, exists := c.Get(processingStartTimeKey); exists {
		if t, ok := val.(time.Time); ok {
			return t, true
		}
	}

	return time.Time{}, false
}

// Middleware returns middleware that will trace incoming requests.
// The service parameter should describe the name of the (virtual)
// server handling the request.
func Middleware(meterProvider metric.MeterProvider, service string, options ...Option) gin.HandlerFunc {
	cfg := defaultConfig()
	for _, option := range options {
		option.apply(cfg)
	}

	recorder := cfg.recorder
	if recorder == nil {
		recorder = GetRecorder(meterProvider, service)
	}

	return func(ginCtx *gin.Context) {
		ctx := ginCtx.Request.Context()

		// Install the request-scoped joined holder so descendant code paths
		// (orchestrator, storage layer, etc.) can call joined.Mark via
		// context.Context. Idempotent: if the tracing middleware already
		// installed the holder, this reuses it.
		ctx = joined.WithHolder(ctx)
		ginCtx.Request = ginCtx.Request.WithContext(ctx)

		route := ginCtx.FullPath()
		if len(route) == 0 {
			route = "nonconfigured"
		}

		if !cfg.shouldRecord(service, route, ginCtx.Request) {
			ginCtx.Next()

			return
		}

		start := time.Now()
		reqAttributes := cfg.attributes(service, route, ginCtx.Request)

		defer func() {
			resAttributes := append(
				reqAttributes[0:0],
				reqAttributes...,
			)

			code := ginCtx.Writer.Status()
			if sharedmiddleware.IsClientDisconnect(sharedmiddleware.CancelCause(ginCtx)) {
				// 499 is the nginx convention for "client closed request before server responded".
				// A request cut by server teardown keeps its real status rather than being
				// counted as a client disconnect.
				code = sharedmiddleware.StatusClientClosedRequest
			}

			groupedCode := code / 100 * 100
			resAttributes = append(resAttributes, semconv.HTTPStatusCodeKey.Int(groupedCode))
			resAttributes = append(resAttributes, HTTPStatusCodeGranularKey.Int(code))

			// Append attributes from ginCtx
			resAttributes = append(resAttributes, attributesFromGinContext(ginCtx, MetricPrefix)...)

			// Distinguish between regular and joined requests
			resAttributes = append(resAttributes, joined.Attribute(ctx))

			// Use processing start time if set, otherwise fall back to the middleware start time.
			effectiveStart := start
			if processingStart, ok := getProcessingStartTime(ginCtx); ok {
				effectiveStart = processingStart
			}

			duration := time.Since(effectiveStart)
			recorder.ObserveHTTPRequestDuration(ctx, duration, resAttributes)
		}()

		ginCtx.Next()
	}
}

func attributesFromGinContext(ginCtx *gin.Context, filterPrefix string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(ginCtx.Keys))
	for keyRaw, v := range ginCtx.Keys {
		key, ok := keyRaw.(string)
		if !ok || !strings.HasPrefix(key, filterPrefix) {
			continue
		}
		k := strings.TrimPrefix(key, filterPrefix)
		switch val := v.(type) {
		case string:
			attrs = append(attrs, attribute.String(k, val))
		case int:
			attrs = append(attrs, attribute.Int(k, val))
		case bool:
			attrs = append(attrs, attribute.Bool(k, val))
		default:
			attrs = append(attrs, attribute.String(k, fmt.Sprintf("%v", val)))
		}
	}

	return attrs
}
