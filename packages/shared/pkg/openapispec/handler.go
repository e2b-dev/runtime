// Package openapispec serves the OpenAPI document a service validates requests
// against, so a client can learn which operations the deployment supports.
package openapispec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Path is where the API serves its document. A document is not an operation
// in its own spec, so its route must be registered before the request
// validator, which rejects paths the spec does not declare.
const Path = "/openapi.json"

// Handler serves doc as JSON. The document is rendered once. Its ETag is a
// hash of the rendered bytes, so a client that sends it back in
// If-None-Match gets a 304 until the document changes.
func Handler(doc json.Marshaler) (gin.HandlerFunc, error) {
	body, err := doc.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("render the OpenAPI document: %w", err)
	}

	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`

	return func(c *gin.Context) {
		c.Header("Content-Type", "application/json")
		c.Header("ETag", etag)
		c.Header("Cache-Control", "no-cache")
		// ServeContent answers HEAD, ranges and a matching If-None-Match.
		http.ServeContent(c.Writer, c.Request, "openapi.json", time.Time{}, bytes.NewReader(body))
	}, nil
}
