package idempotency

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	completionTimeout         = 5 * time.Second
	reservationCleanupTimeout = 5 * time.Second
)

type executor struct {
	store            store
	key              string
	retentionSeconds int
}

// Execute runs operation only after admission; requestContent must describe every input that affects it.
// The operation writes success responses and returns errors. CanRetry permits a fresh execution
// with the same key, so it must only describe a failure with no possible committed side effects.
func Execute(c *gin.Context, requestContent any, operation func() *apierrors.APIError) {
	if c.IsAborted() {
		return
	}
	if value, ok := c.Get(executorKey{}); ok {
		value.(*executor).run(c, requestContent, operation)

		return
	}
	sendOperationError(c, c.Writer.Written(), operation())
}

// sendOperationError sends the error an operation returned and reports whether it was sent.
// Operations must return errors rather than write them; an error after a written response
// breaks that contract, so it is logged and never sent or allowed to permit a fresh execution.
func sendOperationError(c *gin.Context, written bool, apiErr *apierrors.APIError) bool {
	if apiErr == nil {
		return false
	}
	if written {
		logger.L().Error(c.Request.Context(), "Idempotent operation returned an error after writing a response; the error was not sent",
			zap.Int("status", apiErr.Code),
			zap.String("error_code", apiErr.ErrorCode),
			zap.String("client_message", apiErr.ClientMsg),
			zap.Error(apiErr.Err),
		)

		return false
	}
	apierrors.SendAPIError(c, apiErr)

	return true
}

func (e *executor) run(c *gin.Context, requestContent any, operation func() *apierrors.APIError) {
	ctx := c.Request.Context()
	deadline, ok := ctx.Deadline()
	if !ok {
		unavailable(c, errors.New("idempotency requires a request deadline"), apierrors.NoRetry)

		return
	}
	digest, err := fingerprint(c.Request, requestContent)
	if err != nil {
		reject(c, http.StatusBadRequest, "Invalid request for idempotency", apierrors.NoRetry)

		return
	}
	owner := uuid.NewString()
	result, err := e.store.reserve(ctx, e.key, digest, owner, e.retentionSeconds, deadline.Add(completionTimeout))
	if err != nil {
		unavailable(c, err, apierrors.CanRetry)

		return
	}
	if result.state == stateOwner {
		if err := ctx.Err(); err != nil {
			releaseUnusedReservation(ctx, e.store, e.key, owner)
			unavailable(c, err, apierrors.CanRetry)

			return
		}
		execute(c, e.store, e.key, owner, operation)

		return
	}
	switch result.state {
	case stateComplete:
		replay(c, result.response)
	case stateMismatch:
		c.Abort()
		apierrors.SendAPIError(c, &apierrors.APIError{
			Code:      http.StatusConflict,
			ClientMsg: "Idempotency-Key was already used with a different request",
			ErrorCode: "idempotency_request_mismatch",
			Retryable: apierrors.NoRetry,
		})
	case statePending:
		c.Abort()
		apierrors.SendAPIError(c, &apierrors.APIError{
			Code:      http.StatusConflict,
			ClientMsg: "The original request has not recorded a response yet. Retry with the same Idempotency-Key.",
			ErrorCode: "idempotency_in_progress",
			Retryable: apierrors.CanRetry,
		})
	case stateOutcomeUnknown:
		c.Abort()
		apierrors.SendAPIError(c, &apierrors.APIError{
			Code:      http.StatusUnprocessableEntity,
			ClientMsg: "The original request has no recorded response and its outcome is unknown. This request was not executed again.",
			ErrorCode: "idempotency_outcome_unknown",
			Retryable: apierrors.NoRetry,
		})
	default:
		unavailable(c, errors.New("idempotency result is unavailable"), apierrors.NoRetry)
	}
}

func releaseUnusedReservation(ctx context.Context, store store, key, owner string) {
	// Keep request tracing values while giving cleanup its own cancellation budget.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reservationCleanupTimeout)
	defer cancel()
	if err := store.release(ctx, key, owner); err != nil {
		logger.L().Warn(ctx, "Failed to release unused idempotency reservation", zap.Error(err))
	}
}

func execute(c *gin.Context, store store, key, owner string, operation func() *apierrors.APIError) {
	original := c.Writer
	buffer := newResponseCapture(original)
	defer buffer.release()
	c.Writer = buffer
	completed := false
	defer func() {
		c.Writer = original
		// The buffer shares the live header map; drop the hint of the response a panic discarded.
		if !completed {
			c.Writer.Header().Del(apierrors.ShouldRetryHeader)
		}
	}()
	// A panic can follow side effects even if nothing was written; keep the reservation.
	apiErr := operation()
	retryable := sendOperationError(c, buffer.Written(), apiErr) && apiErr.Retryable == apierrors.CanRetry
	c.Writer = original

	// Final persistence survives caller cancellation but stays inside HTTP shutdown's grace.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), completionTimeout)
	defer cancel()
	result := buffer.response()
	if err := store.complete(ctx, key, owner, result, retryable); err != nil {
		logger.L().Warn(c.Request.Context(), "Failed to persist idempotency response; returning original response", zap.Error(err))
	}
	writeResponse(c, result)
	completed = true
}

func replay(c *gin.Context, result cachedResponse) {
	maps.Copy(c.Writer.Header(), result.Headers)
	writeResponse(c, result)
}

func writeResponse(c *gin.Context, result cachedResponse) {
	c.Abort()
	c.Status(result.Status)
	if _, err := c.Writer.Write(result.Body); err != nil {
		_ = c.Error(err)
	}
}

func reject(c *gin.Context, status int, message string, retryable apierrors.Retryability) {
	c.Abort()
	apierrors.SendAPIError(c, &apierrors.APIError{Code: status, ClientMsg: message, Retryable: retryable})
}

func unavailable(c *gin.Context, err error, retryable apierrors.Retryability) {
	logger.L().Warn(c.Request.Context(), "Idempotency unavailable", zap.Error(err))
	reject(c, http.StatusServiceUnavailable, "Request result is unavailable; retry with the same Idempotency-Key", retryable)
}
