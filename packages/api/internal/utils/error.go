package utils

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// ErrorHandler answers a request the validator refused. It returns and
// records the validator's message, never the request body.
func ErrorHandler(c *gin.Context, message string, statusCode int) {
	ctx := c.Request.Context()

	errMsg := fmt.Errorf("OpenAPI validation error: %s", message)

	telemetry.ReportError(ctx, message, errMsg, attribute.Int("http.status_code", statusCode))

	c.Error(errMsg)

	// Forbidden and blocked teams authenticated; they get a 403, not a 401.
	for _, prefix := range []string{sharedauth.ForbiddenErrPrefix, sharedauth.BlockedErrPrefix} {
		if after, ok := strings.CutPrefix(message, prefix); ok {
			c.AbortWithStatusJSON(
				http.StatusForbidden,
				gin.H{
					"code":    http.StatusForbidden,
					"message": after,
				},
			)

			return
		}
	}

	// Handle security requirements errors from the openapi3filter
	if after, ok := strings.CutPrefix(message, sharedauth.SecurityErrPrefix); ok {
		// Keep the original status code as it can be also timeout (read body timeout) error code.
		// The SecurityErrPrefix is added for all errors going through ProcessSecurityErrors.
		c.AbortWithStatusJSON(
			statusCode,
			gin.H{
				"code":    statusCode,
				"message": after,
			},
		)

		return
	}

	c.AbortWithStatusJSON(statusCode, gin.H{"code": statusCode, "message": fmt.Errorf("validation error: %s", message).Error()})
}

// MultiErrorHandler handles wrapped SecurityRequirementsError, so there are no multiple errors returned to the user.
func MultiErrorHandler(me openapi3.MultiError) error {
	if len(me) == 0 {
		return nil
	}
	err := me[0]

	// Recreate logic from oapi-codegen/gin-middleware to handle the error
	// Source: https://github.com/oapi-codegen/gin-middleware/blob/main/oapi_validate.go
	switch e := err.(type) { //nolint:errorlint  // we copied this and don't want it to change
	case *openapi3filter.RequestError:
		// We've got a bad request
		// Split up the verbose error by lines and return the first one
		// openapi errors seem to be multi-line with a decent message on the first
		errorLines := strings.Split(e.Error(), "\n")

		return fmt.Errorf("error in openapi3filter.RequestError: %s", errorLines[0])
	case *openapi3filter.SecurityRequirementsError:
		return sharedauth.ProcessSecurityErrors(e)
	default:
		// This should never happen today, but if our upstream code changes,
		// we don't want to crash the server, so handle the unexpected error.
		return fmt.Errorf("error validating request: %w", err)
	}
}
