package middleware

import (
	"context"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
)

const APIGroupExtension = "x-api-group"

type apiGroupContextKey struct{}

func APIGroupFromContext(c *gin.Context) string {
	return c.GetString(apiGroupContextKey{})
}

// WithAPIGroup records the validator's matched API group in Gin after authentication succeeds.
func WithAPIGroup(authenticate openapi3filter.AuthenticationFunc) openapi3filter.AuthenticationFunc {
	return func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
		if err := authenticate(ctx, input); err != nil {
			return err
		}

		group, _ := input.RequestValidationInput.Route.Operation.Extensions[APIGroupExtension].(string)
		ginmiddleware.GetGinContext(ctx).Set(apiGroupContextKey{}, group)

		return nil
	}
}
