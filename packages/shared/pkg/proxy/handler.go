package proxy

import (
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

		if invalidPortErr, ok := errors.AsType[*InvalidSandboxPortError](err); ok {
			l.Warn(ctx, "invalid sandbox port", zap.String("host", r.Host), zap.String("port", invalidPortErr.Port))
			cors.Error(w, "Invalid sandbox port", http.StatusBadRequest)

			return
		}

		if notFoundErr, ok := errors.AsType[*SandboxNotFoundError](err); ok {
			l.Warn(ctx, "sandbox not found",
				zap.String("host", r.Host),
				logger.WithSandboxID(notFoundErr.SandboxId))

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

			err := template.
				NewSandboxStillTransitioningError(stillTransitioningErr.SandboxId, r.Host).
				HandleError(w, r)
			if err != nil {
				l.Error(ctx, "failed to handle sandbox still transitioning error", zap.Error(err), logger.WithSandboxID(stillTransitioningErr.SandboxId))
				cors.Error(w, "Failed to handle sandbox still transitioning error", http.StatusInternalServerError)

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
			cors.Error(w, fmt.Sprintf("Unexpected error when routing request: %s", err), http.StatusInternalServerError)

			return
		}

		if hasEdgeTraceID {
			// Do not mutate destinations that a resolver reuses across requests.
			destination := *d
			destination.RequestLogger = d.RequestLogger.With(zap.String("edge_trace_id", edgeTraceID))
			d = &destination
		}

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

		proxy := p.Get(ctx, d)
		serveStream(w, r, proxy, fmt.Sprintf("the connection to sandbox %s ended before the stream completed", d.SandboxId))
	}
}
