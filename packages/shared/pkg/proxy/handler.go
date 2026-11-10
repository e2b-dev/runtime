package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
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

func handler(p *pool.ProxyPool, getDestination func(r *http.Request) (*pool.Destination, error), connLimitConfig *ConnectionLimitConfig, o options) http.HandlerFunc {
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
			cors.Error(w, "Request ended before it was routed", http.StatusRequestTimeout)

			return
		}

		// A probe for a routed sandbox is answered here, before any connection
		// to it; any other outcome gets its usual page, now carrying the answer.
		if o.probe && isProbeRequest(r) {
			switch routePresence(err) {
			case PresenceHeld:
				w.Header().Set(HeaderProbe, "true")
			case PresenceAbsent:
				w.Header().Set(HeaderProbe, "false")
			case PresenceUnknown:
				// No header: the asker cannot tell from this proxy.
			}

			if err == nil {
				w.WriteHeader(http.StatusNoContent)

				return
			}
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

		if sandboxErr, ok := errors.AsType[SandboxError](err); ok {
			fields := []zap.Field{zap.Error(err), zap.String("host", r.Host), logger.WithSandboxID(sandboxErr.SandboxID())}
			switch sandboxErr.(type) {
			case *SandboxCapacityError, *SandboxPlacementRefusedError:
				// The proxy that produced it logged it with the request's
				// fields; this line only ties the page to the answer.
				l.Debug(ctx, sandboxErr.Error(), fields...)
			default:
				l.Warn(ctx, sandboxErr.Error(), fields...)
			}

			if err := writeSandboxError(w, r, sandboxErr); err != nil {
				l.Error(ctx, "failed to write the sandbox error page", zap.Error(err), logger.WithSandboxID(sandboxErr.SandboxID()))
				cors.Error(w, "Failed to write the sandbox error page", http.StatusInternalServerError)
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

		// An HTTP/1 request with a body is drained by the server before a
		// refusal is written, and its transport read is what retirement must
		// interrupt. The read deadline that does so reaches the server's
		// background read of a body it has already taken, which cancels the
		// connection's context, inherited by every later request on that
		// frontend; so a frontend whose read was cut is closed after the
		// response, never kept. A body already taken in full needs no cut, and
		// its frontend stays reusable: closing it once the response is out
		// cannot be announced, and the client may already be writing its next
		// request into it.
		uploading := r.ProtoMajor == 1 && r.ContentLength != 0
		var body *requestBody
		if uploading {
			// On a copy: the server reads the original request's body type to
			// decide how to answer before it arrives (Expect: 100-continue, or
			// too much left to drain), as for a refusal below.
			r = r.WithContext(ctx)
			body = trackBody(r)
		}
		proxy := p.Get(ctx, d)
		ctx, release, err := proxy.Acquire(ctx)
		if err != nil {
			if cors.HandlePreflight(w, r) {
				return
			}
			if uploading {
				closeUnreadBody(w)
			}
			cors.Error(w, "Failed to route request to sandbox", http.StatusBadGateway)

			return
		}
		defer release()
		// A canceled backend alone cannot unblock a slow frontend write.
		// Join this callback before releasing ownership or reusing the frontend.
		interrupted := make(chan struct{})
		var cut atomic.Bool
		//nolint:contextcheck // Retirement is independent of frontend request cancellation.
		stop := context.AfterFunc(proxy.Context(), func() {
			controller := http.NewResponseController(w)
			if uploading && !body.finished() {
				cut.Store(true)
				_ = controller.SetReadDeadline(time.Now())
			}
			_ = controller.SetWriteDeadline(time.Now().Add(retiredWriteTimeout))
			close(interrupted)
		})
		defer func() {
			if !stop() {
				<-interrupted
				if cut.Load() {
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

// writeSandboxError writes the page sent in place of the sandbox's response.
// The switch has no default: gochecksumtype fails the build when a
// SandboxError is missing from it.
func writeSandboxError(w http.ResponseWriter, r *http.Request, err SandboxError) error {
	id, host := err.SandboxID(), r.Host

	switch e := err.(type) {
	case *SandboxNotFoundError:
		return template.NewSandboxNotFoundError(id, host).HandleError(w, r)
	case *SandboxResumePermissionDeniedError:
		return template.NewSandboxResumePermissionDeniedError(id, host).HandleError(w, r)
	case *SandboxStillTransitioningError:
		setRetryAfter(w)
		if e.Unavailable {
			return template.NewSandboxStillTransitioningUnavailableError(id, host).HandleError(w, r)
		}

		return template.NewSandboxStillTransitioningError(id, host).HandleError(w, r)
	case *TrafficAccessTokenError:
		if e.Missing {
			return template.NewTrafficAccessTokenMissingHeader(id, host, e.Header).HandleError(w, r)
		}

		return template.NewTrafficAccessTokenInvalidHeader(id, host, e.Header).HandleError(w, r)
	case *InternalRouteError:
		return template.NewInternalRouteError(host, e.Path).HandleError(w, r)
	case *SandboxCapacityError:
		if e.Shed {
			// No Retry-After: a fixed delay brings shed clients back in one wave.
			return template.NewSandboxShedError(id, host).HandleError(w, r)
		}
		setRetryAfter(w)

		return template.NewSandboxCapacityError(id, host).HandleError(w, r)
	case *SandboxPlacementRefusedError:
		// No retry helps, so no retry hint.
		return template.NewSandboxPlacementRefusedError(id, host, e.ErrorCode, e.StatusCode).HandleError(w, r)
	case *SandboxResourceExhaustedError:
		return template.NewTeamSandboxLimitError(id, host, e.Message).HandleError(w, r)
	}

	return fmt.Errorf("no page for %T", err)
}
