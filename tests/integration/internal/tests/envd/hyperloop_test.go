package envd

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/envd"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestAccessingHyperloopServerViaIP(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, client, utils.WithTimeout(120))

	envdClient := setup.GetEnvdClient(t, ctx)

	err := utils.ExecCommand(t, ctx, sbx, envdClient, "/bin/bash", "-c", "curl --connect-timeout 3 --max-time 10 -o output.txt http://192.0.2.1/me")
	require.NoError(t, err, "Should be able to contact hyperloop server")

	readPath := "output.txt"
	readRes, readErr := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &readPath, Username: new("user")},
		setup.WithSandbox(t, sbx),
	)

	require.NoError(t, readErr)
	assert.Equal(t, http.StatusOK, readRes.StatusCode())
	assert.JSONEq(t, fmt.Sprintf("{\"sandboxID\": \"%s\"}", sbx.SandboxID), string(readRes.Body))
}

func TestAccessingHyperloopServerViaDomain(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, client, utils.WithTimeout(120))

	envdClient := setup.GetEnvdClient(t, ctx)

	err := utils.ExecCommand(t, ctx, sbx, envdClient, "/bin/bash", "-c", "curl --connect-timeout 3 --max-time 10 -o output.txt http://events.e2b.local/me")
	require.NoError(t, err, "Should be able to contact hyperloop server")

	readPath := "output.txt"
	readRes, readErr := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &readPath, Username: new("user")},
		setup.WithSandbox(t, sbx),
	)

	require.NoError(t, readErr)
	assert.Equal(t, http.StatusOK, readRes.StatusCode())
	assert.JSONEq(t, fmt.Sprintf("{\"sandboxID\": \"%s\"}", sbx.SandboxID), string(readRes.Body))
}

func TestAccessingHyperloopServerViaIPWithBlockedInternet(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, client, utils.WithTimeout(120), utils.WithAllowInternetAccess(false))

	envdClient := setup.GetEnvdClient(t, ctx)

	err := utils.ExecCommand(t, ctx, sbx, envdClient, "/bin/bash", "-c", "curl --connect-timeout 3 --max-time 10 -o output.txt http://192.0.2.1/me")
	require.NoError(t, err, "Should be able to contact hyperloop server")

	readPath := "output.txt"
	readRes, readErr := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &readPath, Username: new("user")},
		setup.WithSandbox(t, sbx),
	)

	require.NoError(t, readErr)
	assert.Equal(t, http.StatusOK, readRes.StatusCode())
	assert.JSONEq(t, fmt.Sprintf("{\"sandboxID\": \"%s\"}", sbx.SandboxID), string(readRes.Body))
}
