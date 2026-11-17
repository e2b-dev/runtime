package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

// readOnlyDeniedReads enumerates the GET routes a read-only member may not
// reach on the api service.
var readOnlyDeniedReads = auth.ReadOnlyDeniedReads{
	http.MethodGet: {
		// Returns a link that uploads build files.
		"/templates/:templateID/files/:hash": {},
		// Returns a token that writes the volume's content.
		"/volumes/:volumeID": {},
	},
}

// EnforceReadOnlyMember returns the gin middleware for the api service,
// configured with the api-specific denied reads.
func EnforceReadOnlyMember() gin.HandlerFunc {
	return auth.EnforceReadOnlyMember(readOnlyDeniedReads)
}
