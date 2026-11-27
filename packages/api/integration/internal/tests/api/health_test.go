package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
)

func TestHealth(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	resp, err := c.GetHealthWithResponse(ctx)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode())
	assert.Equal(t, "Health check successful", string(resp.Body))
}
