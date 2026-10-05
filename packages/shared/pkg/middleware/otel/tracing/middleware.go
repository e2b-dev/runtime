// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Based on https://github.com/DataDog/dd-trace-go/blob/8fb554ff7cf694267f9077ae35e27ce4689ed8b6/contrib/gin-gonic/gin/gintrace.go

package tracing

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	tracerKey  = "otel-go-contrib-tracer"
	tracerName = "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

type requestStartTimeKey struct{}

// WithRequestStartTime stores the request start time in the context.
func WithRequestStartTime(ctx context.Context, startTime time.Time) context.Context {
	return context.WithValue(ctx, requestStartTimeKey{}, startTime)
}

// GetRequestStartTime retrieves the request start time from the context.
// Returns the start time and true if found, or zero time and false if not found.
func GetRequestStartTime(ctx context.Context) (time.Time, bool) {
	startTime, ok := ctx.Value(requestStartTimeKey{}).(time.Time)

	return startTime, ok
}

type config struct {
	TracerProvider oteltrace.TracerProvider
}

// Middleware returns middleware that will trace incoming requests.
// The service parameter should describe the name of the (virtual)
// server handling the request.
func Middleware(tracerProvider oteltrace.TracerProvider, service string) gin.HandlerFunc {
	cfg := config{}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = tracerProvider
	}
	tracer := cfg.TracerProvider.Tracer(
		tracerName,
		oteltrace.WithInstrumentationVersion(otelgin.Version),
	)

	return func(c *gin.Context) {
		c.Set(tracerKey, tracer)
		ctx := c.Request.Context()

		// Store the server receive time as the request start time
		// This allows us to calculate the whole request duration from server receive to completion
		ctx = WithRequestStartTime(ctx, time.Now())

		defer func() {
			c.Request = c.Request.WithContext(ctx)
		}()

		// Remove traceparent (it's coming from our users and it can cause multiple calls share the same trace ID)
		if c.Request.Header.Get("traceparent") != "" {
			c.Request.Header.Del("traceparent")
		}
		opts := []oteltrace.SpanStartOption{
			oteltrace.WithAttributes(semconv.NetAttributesFromHTTPRequest("tcp", c.Request)...),
			oteltrace.WithAttributes(semconv.EndUserAttributesFromHTTPRequest(c.Request)...),
			oteltrace.WithAttributes(semconv.HTTPServerAttributesFromHTTPRequest(service, c.FullPath(), c.Request)...),
			oteltrace.WithSpanKind(oteltrace.SpanKindServer),
		}

		if edgeTraceID, ok := telemetry.ParseEdgeTraceID(
			c.Request.Header.Get(telemetry.GCPTraceContextHeader),
			c.Request.Header.Get(telemetry.AWSTraceContextHeader),
		); ok {
			ctx = logger.ContextWithEdgeTraceID(ctx, edgeTraceID)
			opts = append(opts, oteltrace.WithAttributes(telemetry.WithEdgeTraceID(edgeTraceID)))
		}

		spanName := c.FullPath()
		if spanName == "" {
			spanName = fmt.Sprintf("HTTP %s route not found", c.Request.Method)
		}

		ctx, span := tracer.Start(ctx, spanName, opts...)
		defer span.End()

		// Install the request-scoped joined holder
		ctx = joined.WithHolder(ctx)

		// pass the span through the request context
		c.Request = c.Request.WithContext(ctx)

		// serve the request to the next middleware
		c.Next()

		status := c.Writer.Status()
		cause := sharedmiddleware.CancelCause(c)
		if errors.Is(cause, sharedmiddleware.ErrRequestTimeout) {
			span.SetAttributes(attribute.Bool("request.timeout", true))
		} else if errors.Is(cause, context.Canceled) {
			status = sharedmiddleware.StatusClientClosedRequest
			span.SetAttributes(attribute.Bool("client.canceled", true))
		}

		attrs := semconv.HTTPAttributesFromHTTPStatusCode(status)
		span.SetAttributes(attrs...)
		spanStatus, spanMessage := semconv.SpanStatusFromHTTPStatusCode(status)
		span.SetStatus(spanStatus, spanMessage)

		// Marks the joined requests for telemetry purposes.
		span.SetAttributes(joined.Attribute(ctx))

		if len(c.Errors) > 0 {
			span.SetAttributes(attribute.String("gin.errors", strings.TrimSpace(c.Errors.String())))
		}
	}
}
