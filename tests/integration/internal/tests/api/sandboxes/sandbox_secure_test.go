package sandboxes

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestCreateSandboxWithSecuredEnvd(t *testing.T) {
	t.Parallel()

	utils.AcquireSandboxSlot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	sbxTimeout := int32(60)
	sbxSecure := true

	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &sbxTimeout,
		Secure:     &sbxSecure,
	}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

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
	assert.NotNil(t, resp.JSON201.EnvdAccessToken)

	getResp, getErr := c.GetSandboxesSandboxIDWithResponse(ctx, resp.JSON201.SandboxID, setup.WithAPIKey())
	require.NoError(t, getErr, "Failed to get sandbox after creation")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)

	assert.Equal(t, *resp.JSON201.EnvdAccessToken, *getResp.JSON200.EnvdAccessToken)
}

func TestCreateSandboxV2IsSecured(t *testing.T) {
	t.Parallel()

	utils.AcquireSandboxSlot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	sbxTimeout := int32(60)

	resp, err := c.PostV2SandboxesWithResponse(ctx, api.NewSandboxV2{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

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
	require.NotNil(t, resp.JSON201.EnvdAccessToken)

	getResp, getErr := c.GetSandboxesSandboxIDWithResponse(ctx, resp.JSON201.SandboxID, setup.WithAPIKey())
	require.NoError(t, getErr, "Failed to get sandbox after creation")
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)
	require.NotNil(t, getResp.JSON200.EnvdAccessToken)

	assert.Equal(t, *resp.JSON201.EnvdAccessToken, *getResp.JSON200.EnvdAccessToken)
}

func TestCreateSandboxV2WithDisabledPublicTraffic(t *testing.T) {
	t.Parallel()

	utils.AcquireSandboxSlot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	sbxTimeout := int32(60)
	sbxAllowPublicTraffic := false

	resp, err := c.PostV2SandboxesWithResponse(ctx, api.NewSandboxV2{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &sbxTimeout,
		Network: &api.SandboxNetworkConfig{
			AllowPublicTraffic: &sbxAllowPublicTraffic,
		},
	}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

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
	assert.NotNil(t, resp.JSON201.EnvdAccessToken)
}

func TestCreateSandboxWithDisabledPublicTrafficAndDisabledEnvdSecure(t *testing.T) {
	t.Parallel()

	utils.AcquireSandboxSlot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	sbxTimeout := int32(60)
	sbxSecure := false
	sbxAllowPublicTraffic := false

	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &sbxTimeout,
		Secure:     &sbxSecure,
		Network: &api.SandboxNetworkConfig{
			AllowPublicTraffic: &sbxAllowPublicTraffic,
		},
	}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(resp.Body))
		}

		if resp.JSON201 != nil {
			utils.TeardownSandbox(t, c, resp.JSON201.SandboxID)
		}
	})

	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.Equal(t, "You cannot create a sandbox without public access unless you enable secure envd access via 'secure' flag.", resp.JSON400.Message)
}
