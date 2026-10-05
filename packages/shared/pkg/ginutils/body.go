package ginutils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func ParseBody[B any](ctx context.Context, c *gin.Context) (body B, err error) {
	err = c.Bind(&body)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return body, fmt.Errorf("error when parsing request: %w", err)
	}

	return body, nil
}

// ParseOptionalBody parses an optional JSON request body. Unlike ParseBody it
// tolerates an absent or empty body, returning the zero value and no error — and
// it does so without consulting Content-Length, which is -1 under chunked
// transfer encoding even when a body is present. A present-but-malformed body
// still returns an error.
func ParseOptionalBody[B any](ctx context.Context, c *gin.Context) (body B, err error) {
	if c.Request == nil || c.Request.Body == nil {
		return body, nil
	}

	if decErr := json.NewDecoder(c.Request.Body).Decode(&body); decErr != nil {
		if errors.Is(decErr, io.EOF) {
			// Empty body: the caller defaults from the zero value.
			return body, nil
		}

		telemetry.ReportCriticalError(ctx, "error when parsing request", decErr)

		return body, fmt.Errorf("error when parsing request: %w", decErr)
	}

	return body, nil
}
