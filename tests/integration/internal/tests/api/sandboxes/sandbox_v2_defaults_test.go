package sandboxes

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

const sandboxV2DefaultTimeout = 300 * time.Second

func TestCreateSandboxV2DefaultTimeout(t *testing.T) {
	t.Parallel()

	utils.AcquireSandboxSlot(t)

	c := setup.GetAPIClient()

	resp, err := c.PostV2SandboxesWithResponse(t.Context(), api.NewSandboxV2{
		TemplateID: setup.SandboxTemplateID,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(resp.Body))
		}

		if resp.JSON201 != nil {
			utils.TeardownSandbox(t, c, resp.JSON201.SandboxID)
		}
	})

	require.Equal(t, http.StatusCreated, resp.StatusCode())
	require.NotNil(t, resp.JSON201)

	detail := getSandboxDetail(t, c, resp.JSON201.SandboxID)
	assert.WithinDuration(t, detail.StartedAt.Add(sandboxV2DefaultTimeout), detail.EndAt, 5*time.Second)
}

func getSandboxDetail(t *testing.T, c *api.ClientWithResponses, sandboxID string) *api.SandboxDetail {
	t.Helper()

	res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
	require.NotNil(t, res.JSON200)

	return res.JSON200
}

func TestSandboxConnectV2(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("omitted timeout defaults to 300s on resume", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))
		pauseSandbox(t, c, sbx.SandboxID)

		sbxConnect, err := c.PostV2SandboxesSandboxIDConnectWithResponse(t.Context(), sbx.SandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, sbxConnect.StatusCode(), string(sbxConnect.Body))
		require.NotNil(t, sbxConnect.JSON201)
		assert.Equal(t, sbx.SandboxID, sbxConnect.JSON201.SandboxID)

		detail := getSandboxDetail(t, c, sbx.SandboxID)
		assert.Equal(t, api.Running, detail.State)
		assert.WithinDuration(t, time.Now().Add(sandboxV2DefaultTimeout), detail.EndAt, 30*time.Second)
	})

	t.Run("no request body", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(60))

		sbxConnect, err := c.PostV2SandboxesSandboxIDConnectWithBodyWithResponse(t.Context(), sbx.SandboxID, "application/json", http.NoBody, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, sbxConnect.StatusCode(), string(sbxConnect.Body))
		require.NotNil(t, sbxConnect.JSON200)
		assert.Equal(t, sbx.SandboxID, sbxConnect.JSON200.SandboxID)

		detail := getSandboxDetail(t, c, sbx.SandboxID)
		assert.WithinDuration(t, time.Now().Add(sandboxV2DefaultTimeout), detail.EndAt, 30*time.Second, "TTL should be extended to the default timeout")
	})

	t.Run("explicit timeout is respected", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(60))

		timeout := int32(600)
		sbxConnect, err := c.PostV2SandboxesSandboxIDConnectWithResponse(t.Context(), sbx.SandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{
			Timeout: &timeout,
		}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, sbxConnect.StatusCode(), string(sbxConnect.Body))
		require.NotNil(t, sbxConnect.JSON200)

		detail := getSandboxDetail(t, c, sbx.SandboxID)
		assert.WithinDuration(t, time.Now().Add(time.Duration(timeout)*time.Second), detail.EndAt, 30*time.Second)
	})

	t.Run("zero timeout is rejected", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(60))

		sbxConnect, err := c.PostV2SandboxesSandboxIDConnectWithBodyWithResponse(t.Context(), sbx.SandboxID, "application/json", strings.NewReader(`{"timeout":0}`), setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, sbxConnect.StatusCode(), string(sbxConnect.Body))
	})
}
