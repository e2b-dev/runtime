package middleware

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

// ReadRequestBody reads the whole request body, up to limit bytes, before the
// request validator runs, and answers a body it cannot read itself: 413 when
// it is over the limit, else 400. The validator reads a routed request's body
// before authenticating anyway, so reading it here first adds only a copy in
// memory. A request no route matches gets 404, and nothing reads its body.
func ReadRequestBody(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.FullPath() == "" || c.Request.Body == nil || c.Request.Body == http.NoBody {
			c.Next()

			return
		}

		data, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, limit))
		if err != nil {
			// The read error is neither returned nor recorded, only a fixed
			// message: its text can quote the request, e.g. the line of a
			// malformed chunked body, and a body may carry a secret.
			if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
				apierrors.SendAPIStoreError(c, http.StatusRequestEntityTooLarge, "request body too large")
			} else {
				apierrors.SendAPIStoreError(c, http.StatusBadRequest, "request body could not be read")
			}
			c.Abort()

			return
		}

		c.Request.Body = io.NopCloser(bytes.NewReader(data))
		c.Request.ContentLength = int64(len(data))
		c.Request.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		}

		c.Next()
	}
}
