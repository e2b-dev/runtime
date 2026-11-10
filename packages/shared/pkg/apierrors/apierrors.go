// Package apierrors sends API errors from gin handlers. The error type and
// its HTTP encoding live in httperror, which has no gin dependency.
package apierrors

import (
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors/httperror"
)

// APIError represents a structured error with an HTTP status code and client-facing message.
type APIError = httperror.APIError

type Retryability = httperror.Retryability

const ShouldRetryHeader = httperror.ShouldRetryHeader

const (
	UndefinedRetry = httperror.UndefinedRetry
	CanRetry       = httperror.CanRetry
	NoRetry        = httperror.NoRetry
)

// SendAPIStoreError sends a JSON error response and records the error on the gin context.
func SendAPIStoreError(c *gin.Context, code int, message string) {
	SendAPIError(c, &APIError{Code: code, ClientMsg: message})
}

// SendAPIError sends a JSON error response like SendAPIStoreError, adding
// error_code to the body when the error carries a semantic code, and setting
// Should-Retry when Retryable is explicit, otherwise leaving the header unchanged.
func SendAPIError(c *gin.Context, apiErr *APIError) {
	c.Error(errors.New(apiErr.ClientMsg))

	// Like gin's own rendering: on failure the status is sent, the error
	// recorded and the request aborted.
	if err := httperror.Write(c.Writer, apiErr); err != nil {
		_ = c.Error(err)
		c.Abort()
	}
}
