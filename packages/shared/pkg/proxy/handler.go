package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/cors"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// resumeRetryAfter is sent with every answer a resumed sandbox can turn into a
// success on retry, so SDKs and browsers wait before asking again.
const resumeRetryAfter = "2"

// Allow a terminal stream error to reach a reading client, but do not let a
// stalled frontend hold sandbox network release indefinitely.
const retiredWriteTimeout = time.Second

// setRetryAfter sends the retry delay and lists it for cross-origin
// callers: a browser lets a page read only the headers the response
// exposes, so without that the delay would reach a script as nothing.
func setRetryAfter(w http.ResponseWriter) {
	w.Header().Set("Retry-After", resumeRetryAfter)
	w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
}

func handler(p *pool.ProxyPool, getDestination func(r *http.Request) (*pool.Destination, error), connLimitConfig *ConnectionLimitConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		l := logger.L()

		edgeTraceID, hasEdgeTraceID := telemetry.ParseEdgeTraceID(
			r.Header.Get(telemetry.GCPTraceContextHeader),
			r.Header.Get(telemetry.AWSTraceContextHeader),
		)
		if hasEdgeTraceID {
			l = l.With(zap.String("edge_trace_id", edgeTraceID))
		}

		d, err := getDestination(r)

		// The request ended before it was routed. A client that only
		// half-closed its connection is still reading, and an unwritten
		// answer would reach it as an empty 200, so the status says the
		// request ended; a client that is gone reads nothing either way.
		if ctxErr := ctx.Err(); err != nil && ctxErr != nil && errors.Is(err, ctxErr) {
			l.Debug(ctx, "request ended before it was routed", zap.String("host", r.Host))
			p.RecordOutcome(ctx, pool.OutcomeClientClosed, "")
			cors.Error(w, "Request ended before it was routed", http.StatusRequestTimeout)

			return
		}

		// With no destination there is nobody upstream to answer the preflight,
		// and a browser drops the real request unless the preflight gets a 2xx.
		if err != nil && cors.HandlePreflight(w, r) {
			return
		}

		if mhe, ok := errors.AsType[MissingHeaderError](err); ok {
			l.Warn(ctx, "missing header", zap.Error(mhe))
			cors.Error(w, "missing header", http.StatusBadRequest)

			return
		}

		if errors.Is(err, ErrInvalidHost) {
			l.Warn(ctx, "invalid host", zap.String("host", r.Host))
			cors.Error(w, "Invalid host", http.StatusBadRequest)

			return
		}

		if errors.Is(err, ErrInvalidSandboxID) {
			l.Warn(ctx, "invalid sandbox ID", zap.String("host", r.Host))
			cors.Error(w, "Invalid sandbox ID", http.StatusBadRequest)

			return
		}

		if invalidPortErr, ok := errors.AsType[InvalidSandboxPortError](err); ok {
			l.Warn(ctx, "invalid sandbox port", zap.String("host", r.Host), zap.String("port", invalidPortErr.Port))
			cors.Error(w, "Invalid sandbox port", http.StatusBadRequest)

			return
		}

		if notFoundErr, ok := errors.AsType[*SandboxNotFoundError](err); ok {
			l.Warn(ctx, "sandbox not found",
				zap.String("host", r.Host),
				logger.WithSandboxID(notFoundErr.SandboxId))

			p.RecordOutcome(ctx, pool.OutcomeSandboxNotFound, "")

			err := template.
				NewSandboxNotFoundError(notFoundErr.SandboxId, r.Host).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle sandbox not found error", zap.Error(err), logger.WithSandboxID(notFoundErr.SandboxId))
				cors.Error(w, "Failed to handle sandbox not found error", http.StatusInternalServerError)

				return
			}

			return
		}

		if resumeDeniedErr, ok := errors.AsType[*SandboxResumePermissionDeniedError](err); ok {
			l.Warn(ctx, "sandbox resume permission denied",
				zap.String("host", r.Host),
				logger.WithSandboxID(resumeDeniedErr.SandboxId))

			err := template.
				NewSandboxResumePermissionDeniedError(resumeDeniedErr.SandboxId, r.Host).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle sandbox resume permission denied error", zap.Error(err), logger.WithSandboxID(resumeDeniedErr.SandboxId))
				cors.Error(w, "Failed to handle sandbox resume permission denied error", http.StatusInternalServerError)

				return
			}

			return
		}

		if stillTransitioningErr, ok := errors.AsType[*SandboxStillTransitioningError](err); ok {
			l.Warn(ctx, "sandbox still transitioning",
				zap.String("host", r.Host),
				logger.WithSandboxID(stillTransitioningErr.SandboxId))

			setRetryAfter(w)
			page := template.NewSandboxStillTransitioningError(stillTransitioningErr.SandboxId, r.Host)
			if stillTransitioningErr.Unavailable {
				page = template.NewSandboxStillTransitioningUnavailableError(stillTransitioningErr.SandboxId, r.Host)
			}
			err := page.HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle sandbox still transitioning error", zap.Error(err), logger.WithSandboxID(stillTransitioningErr.SandboxId))
				cors.Error(w, "Failed to handle sandbox still transitioning error", http.StatusInternalServerError)

				return
			}

			return
		}

		if capacityErr, ok := errors.AsType[*SandboxCapacityError](err); ok {
			// The proxy that produced it logged it with the request's
			// fields; this line only ties the page to the answer.
			l.Debug(ctx, capacityErr.Error(),
				zap.String("host", r.Host),
				zap.String("reason", capacityErr.Message),
				zap.Bool("permanent", capacityErr.Permanent),
				logger.WithSandboxID(capacityErr.SandboxId))

			page := template.NewSandboxCapacityError(capacityErr.SandboxId, r.Host)
			switch {
			case capacityErr.Permanent:
				// No retry helps, so no retry hint.
				page = template.NewSandboxPlacementRefusedError(capacityErr.SandboxId, r.Host, capacityErr.ErrorCode, capacityErr.StatusCode)
			case capacityErr.Shed:
				page = template.NewSandboxShedError(capacityErr.SandboxId, r.Host)
			default:
				setRetryAfter(w)
			}
			err := page.HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle sandbox capacity error", zap.Error(err), logger.WithSandboxID(capacityErr.SandboxId))
				cors.Error(w, "Failed to handle sandbox capacity error", http.StatusInternalServerError)

				return
			}

			return
		}

		if internalRouteErr, ok := errors.AsType[*InternalRouteError](err); ok {
			l.Warn(ctx, "internal route requested through the proxy",
				zap.String("host", r.Host),
				zap.String("path", internalRouteErr.Path),
				logger.WithSandboxID(internalRouteErr.SandboxId))

			err := template.
				NewInternalRouteError(r.Host, internalRouteErr.Path).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle internal route error", zap.Error(err), logger.WithSandboxID(internalRouteErr.SandboxId))
				cors.Error(w, "Failed to handle internal route error", http.StatusInternalServerError)

				return
			}

			return
		}

		if resourceExhaustedErr, ok := errors.AsType[*SandboxResourceExhaustedError](err); ok {
			l.Warn(ctx, "team sandbox limit reached",
				zap.String("host", r.Host),
				logger.WithSandboxID(resourceExhaustedErr.SandboxId))

			err := template.
				NewTeamSandboxLimitError(resourceExhaustedErr.SandboxId, r.Host, resourceExhaustedErr.Message).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle team sandbox limit error", zap.Error(err), logger.WithSandboxID(resourceExhaustedErr.SandboxId))
				cors.Error(w, "Failed to handle team sandbox limit error", http.StatusInternalServerError)

				return
			}

			return
		}

		if trafficMissingTokenErr, ok := errors.AsType[*MissingTrafficAccessTokenError](err); ok {
			l.Warn(ctx, "traffic access token is missing", zap.String("host", r.Host))

			err = template.
				NewTrafficAccessTokenMissingHeader(trafficMissingTokenErr.SandboxId, r.Host, trafficMissingTokenErr.Header).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle traffic missing traffic access token header error", zap.Error(err), logger.WithSandboxID(trafficMissingTokenErr.SandboxId))
				cors.Error(w, "Failed to handle invalid missing access token header error", http.StatusInternalServerError)

				return
			}

			return
		}

		if trafficInvalidTokenErr, ok := errors.AsType[*InvalidTrafficAccessTokenError](err); ok {
			l.Warn(ctx, "traffic access token is invalid", zap.String("host", r.Host))

			err = template.
				NewTrafficAccessTokenInvalidHeader(trafficInvalidTokenErr.SandboxId, r.Host, trafficInvalidTokenErr.Header).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle traffic invalid traffic access token header error", zap.Error(err), logger.WithSandboxID(trafficInvalidTokenErr.SandboxId))
				cors.Error(w, "Failed to handle invalid traffic access token header error", http.StatusInternalServerError)

				return
			}

			return
		}

		if err != nil {
			l.Error(ctx, "failed to route request", zap.Error(err), zap.String("host", r.Host))
			p.RecordOutcome(ctx, pool.OutcomeRoutingFailed, "")
			cors.Error(w, fmt.Sprintf("Unexpected error when routing request: %s", err), http.StatusInternalServerError)

			return
		}

		if hasEdgeTraceID {
			// Do not mutate destinations that a resolver reuses across requests.
			destination := *d
			destination.RequestLogger = d.RequestLogger.With(zap.String("edge_trace_id", edgeTraceID))
			d = &destination
		}

		// An HTTP/1 request with a body is drained by the server before a
		// refusal is written, and its transport read is what retirement must
		// interrupt. The read deadline that does so reaches the server's
		// background read of a body it has already taken, which cancels the
		// connection's context, inherited by every later request on that
		// frontend; so a frontend whose read was cut is closed after the
		// response, never kept.
		uploading := r.ProtoMajor == 1 && r.ContentLength != 0
		proxy := p.Get(ctx, d)
		ctx, release, err := proxy.Acquire(ctx)
		if err != nil {
			if cors.HandlePreflight(w, r) {
				return
			}
			if uploading {
				closeUnreadBody(w)
			}
			p.RecordOutcome(ctx, pool.OutcomeUpstreamUnreachable, d.Node)
			cors.Error(w, "Failed to route request to sandbox", http.StatusBadGateway)

			return
		}
		defer release()
		// A canceled backend alone cannot unblock a slow frontend write.
		// Join this callback before releasing ownership or reusing the frontend.
		interrupted := make(chan struct{})
		//nolint:contextcheck // Retirement is independent of frontend request cancellation.
		stop := context.AfterFunc(proxy.Context(), func() {
			controller := http.NewResponseController(w)
			if uploading {
				_ = controller.SetReadDeadline(time.Now())
			}
			_ = controller.SetWriteDeadline(time.Now().Add(retiredWriteTimeout))
			close(interrupted)
		})
		defer func() {
			if !stop() {
				<-interrupted
				if uploading {
					// Before the headers went out the server announces the
					// close itself; after, HTTP/1 has no way to, and a client
					// that reuses the frontend meanwhile sees EOF and retries.
					closeUnreadBody(w)
				}
			}
		}()

		// Connection limiting
		if connLimitConfig != nil {
			maxLimit := connLimitConfig.GetMaxLimit(ctx)
			count, acquired := connLimitConfig.Limiter.TryAcquire(d.ConnectionKey, maxLimit)
			if !acquired {
				l.Warn(ctx, "sandbox too many incoming connections",
					zap.String("host", r.Host),
					logger.WithSandboxID(d.SandboxId),
					zap.Int("connection_limit", maxLimit))

				if connLimitConfig.OnConnectionBlocked != nil {
					connLimitConfig.OnConnectionBlocked(ctx)
				}

				p.RecordOutcome(ctx, pool.OutcomeTooManyConnections, d.Node)

				// The sandbox is reachable, but the proxy is refusing to dial it,
				// so the proxy is the only party that can answer this preflight.
				if cors.HandlePreflight(w, r) {
					return
				}

				err := template.
					NewSandboxTooManyConnectionsError(d.SandboxId, r.Host, maxLimit).
					HandleError(w, r)
				if err != nil {
					l.Error(ctx, "failed to handle too many connections error", zap.Error(err), logger.WithSandboxID(d.SandboxId))
					cors.Error(w, "Failed to handle too many connections error", http.StatusInternalServerError)

					return
				}

				return
			}

			start := time.Now()
			defer func() {
				connLimitConfig.Limiter.Release(d.ConnectionKey)
				if connLimitConfig.OnConnectionReleased != nil {
					connLimitConfig.OnConnectionReleased(ctx, time.Since(start).Milliseconds())
				}
			}()

			if connLimitConfig.OnConnectionAcquired != nil {
				connLimitConfig.OnConnectionAcquired(ctx, count)
			}
		}

		d.RequestLogger.Debug(ctx, "proxying request")

		ctx = pool.WithDestination(ctx, d)
		r = r.WithContext(ctx)

		// Keep the request body readable while the response streams; serveStream settles what is left of it.
		if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
			l.Warn(ctx, "failed to enable full duplex", zap.Error(err), logger.WithSandboxID(d.SandboxId))
		}

		serveStream(w, r, proxy, fmt.Sprintf("the connection to sandbox %s ended before the stream completed", d.SandboxId))
	}
}
