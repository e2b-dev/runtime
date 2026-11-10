package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/cors"

	"github.com/e2b-dev/infra/packages/shared/pkg/apicors"
)

// CORS returns the gin CORS middleware for the api service. A preflight is
// answered and the chain stopped, since no route serves OPTIONS.
func CORS() gin.HandlerFunc {
	c := cors.New(apicors.Options())

	return func(ctx *gin.Context) {
		c.HandlerFunc(ctx.Writer, ctx.Request)
		if ctx.Request.Method == http.MethodOptions && ctx.GetHeader("Access-Control-Request-Method") != "" {
			ctx.AbortWithStatus(http.StatusNoContent)
		}
	}
}
