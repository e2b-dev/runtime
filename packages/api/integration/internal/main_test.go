package internal

import (
	"context"
	"log"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestMain(m *testing.M) {
	log.Println("Setting up test environment")
	m.Run()
	log.Println("Environment set up")
}

// TestCacheTemplate starts a sandbox before all tests to cache the necessary files for the base template.
func TestCacheTemplate(t *testing.T) {
	t.Parallel()

	utils.AcquireSandboxSlot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbxTimeout := int32(60)
	sbx, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, sbx.StatusCode())

	t.Cleanup(func() {
		switch {
		case sbx == nil:
			t.Logf("Error: %v", err)
		case sbx.JSON201 == nil:
			t.Logf("Response error: %d %v", sbx.StatusCode(), string(sbx.Body))
		default:
			utils.TeardownSandbox(t, c, sbx.JSON201.SandboxID)
		}
	})
}
