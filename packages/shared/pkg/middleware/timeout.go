package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
)

// ErrRequestTimeout is the cancel cause set when the per-request timeout fires.
// Callers can distinguish this from a client disconnection by checking:
//
//	errors.Is(CancelCause(c), middleware.ErrRequestTimeout)
var ErrRequestTimeout = errors.New("request timeout exceeded")

// ErrServerShuttingDown is the cancel cause a server sets on the parent of
// every request context when it tears down, so an in-flight request cut by
// the shutdown is not mistaken for a client disconnect. Wire it as the cause
// of the http.Server BaseContext:
//
//	ctx, cancel := context.WithCancelCause(context.Background())
//	defer cancel(middleware.ErrServerShuttingDown)
//	// server.BaseContext returns ctx
//
// The cause propagates through net/http into r.Context() and through the
// per-request timeout wrapper below, so IsServerShutdown(CancelCause(c))
// reports it after the handler returns.
var ErrServerShuttingDown = errors.New("server shutting down")

// IsClientDisconnect reports whether cause is a client that closed the
// connection before the server responded — a bare context.Canceled that is
// neither the per-request timeout nor a server teardown. The two server-side
// causes are checked first because they also satisfy errors.Is(_, Canceled)
// once they propagate as a context cancellation.
func IsClientDisconnect(cause error) bool {
	if cause == nil {
		return false
	}
	if errors.Is(cause, ErrRequestTimeout) || errors.Is(cause, ErrServerShuttingDown) {
		return false
	}

	return errors.Is(cause, context.Canceled)
}

// IsServerShutdown reports whether cause is a request cut by server teardown.
func IsServerShutdown(cause error) bool {
	return errors.Is(cause, ErrServerShuttingDown)
}

// cancelCauseKey is the gin context key where RequestTimeout snapshots the
// cancel cause before defer-cancel runs.
const cancelCauseKey = "middleware.cancelCause"

// CancelCause returns the cancel cause captured by the timeout middleware.
// It returns nil for normal (non-canceled/non-timed-out) requests.
func CancelCause(c *gin.Context) error {
	if val, exists := c.Get(cancelCauseKey); exists {
		if err, ok := val.(error); ok {
			return err
		}
	}

	return nil
}

// StatusClientClosedRequest is the de-facto status code (used by nginx) for a
// client that closed the connection before the server could send a response.
// It is not an official IANA code but is widely recognised in logs and metrics.
const StatusClientClosedRequest = 499

// RequestTimeout returns a Gin middleware that sets a context deadline on each
// request. This is needed because http.Server.WriteTimeout does NOT cancel
// r.Context() (see https://github.com/golang/go/issues/59602), so without an
// explicit deadline, blocking calls like pgxpool.Acquire will wait indefinitely
// when the connection pool is saturated.
//
// After the handler returns, the middleware checks context.Cause to distinguish
// two cancellation scenarios and sets an appropriate status if nothing was
// written yet:
//   - server-side timeout (cause is ErrRequestTimeout)  → 408 Request Timeout
//   - client disconnect (ctx.Err() == context.Canceled) → 499 Client Closed Request
//
// Routes matching any of the excludedRoutes patterns are skipped (useful for
// health checks and long-polling endpoints).
func RequestTimeout(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeoutCause(c.Request.Context(), timeout, ErrRequestTimeout)
		defer cancel()

		c.Request = c.Request.WithContext(ctx)
		c.Next()

		// Snapshot the cause *before* defer-cancel fires so outer
		// middlewares can distinguish timeout vs client-disconnect
		// via CancelCause(c) without racing with the deferred cancel.
		if err := context.Cause(ctx); err != nil {
			c.Set(cancelCauseKey, err)
		}
	}
}
