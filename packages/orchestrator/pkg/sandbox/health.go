//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

func (c *Checks) getHealth(ctx context.Context, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	address := c.sandbox.envdServerURL() + "/health"

	request, err := http.NewRequestWithContext(WithEnvdSandbox(ctx, c.sandbox), http.MethodGet, address, nil)
	if err != nil {
		return false, err
	}

	response, err := sandboxHttpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, errors.New("health check timed out")
		}

		return false, err
	}
	defer func() {
		// Drain the response body to reuse the connection
		// From response.Body docstring:
		//  // The default HTTP client's Transport may not reuse HTTP/1.x "keep-alive" TCP connections
		//  if the Body is not read to completion and closed.
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}()

	if response.StatusCode != http.StatusNoContent {
		return false, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	return true, nil
}
