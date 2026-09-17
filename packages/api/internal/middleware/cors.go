package middleware

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

// allowedRequestHeaders enumerates the request headers a browser may send
// cross-origin.
var allowedRequestHeaders = []string{
	// Default headers
	"Origin",
	"Content-Length",
	"Content-Type",
	"User-Agent",
	// API Key header
	"Authorization",
	"X-API-Key",
	"Idempotency-Key",
	auth.HeaderTeamID,
	// Custom headers sent from SDK
	"browser",
	"lang",
	"lang_version",
	"machine",
	"os",
	"package_version",
	"processor",
	"publisher",
	"release",
	"sdk_runtime",
	"system",
}

// exposedResponseHeaders enumerates the response headers a browser hands to JS
// cross-origin. Only the CORS-safelisted headers are readable by default, so a
// custom response header the API sets is invisible to browser clients until it
// is listed here.
var exposedResponseHeaders = []string{
	// Pagination cursor of the list endpoints
	"X-Next-Token",
	// Running sandbox total, set by GET /v2/sandboxes
	"X-Total-Running",
	// Durable Cathedral sandbox create acknowledgement
	"X-E2B-Idempotency-Key",
	// Rate limiting
	"RateLimit-Limit",
	"RateLimit-Remaining",
	"RateLimit-Reset",
	"Retry-After",
}

// CORS returns the gin CORS middleware for the api service.
func CORS() gin.HandlerFunc {
	config := cors.DefaultConfig()
	// Allow all origins
	config.AllowAllOrigins = true
	config.AllowHeaders = allowedRequestHeaders
	config.ExposeHeaders = exposedResponseHeaders

	return cors.New(config)
}
