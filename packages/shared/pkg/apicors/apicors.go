// Package apicors is the API's CORS policy. It is shared so a proxy in front
// of the API answers browsers exactly as the API does.
package apicors

import (
	"net/http"
	"slices"
	"time"

	"github.com/rs/cors"

	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors/httperror"
)

// allowedRequestHeaders enumerates the request headers a browser may send
// cross-origin.
var allowedRequestHeaders = []string{
	// Default headers
	"Origin",
	"Content-Length",
	"Content-Type",
	"Idempotency-Key",
	"User-Agent",
	// API Key header
	"Authorization",
	"X-API-Key",
	"X-Team-ID",
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
	// Rate limiting
	"RateLimit-Limit",
	"RateLimit-Remaining",
	"RateLimit-Reset",
	"Retry-After",
	httperror.ShouldRetryHeader,
}

// maxAge is how long a browser may cache a preflight answer.
const maxAge = 12 * time.Hour

// Options returns the API's rs/cors options. Each call returns new slices, so
// a caller may extend them.
func Options() cors.Options {
	return cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
			http.MethodHead,
			http.MethodOptions,
		},
		AllowedHeaders: slices.Clone(allowedRequestHeaders),
		ExposedHeaders: slices.Clone(exposedResponseHeaders),
		MaxAge:         int(maxAge.Seconds()),
	}
}
