package api

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestMaskRequestHostAPIParameter(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	ctx := t.Context()

	hostname := "localhost"
	maskedURL := fmt.Sprintf("%s:%s", hostname, pool.MaskRequestHostPortPlaceholder)
	sbxNet := &api.SandboxNetworkConfig{
		MaskRequestHost: &maskedURL,
	}

	// Create sandbox with maskRequestHost set
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithNetwork(sbxNet), utils.WithTimeout(120))

	envdClient := setup.GetEnvdClient(t, ctx)

	port := 8080
	// Single-shot listener that answers with the raw request it received. Uses the
	// preinstalled python instead of installing netcat via apt-get, which is
	// page-fault-heavy enough to wedge the sandbox under parallel test load.
	listener := `import socket, sys
s = socket.socket()
s.bind(("0.0.0.0", int(sys.argv[1])))
s.listen(1)
c, _ = s.accept()
req = b""
while b"\r\n\r\n" not in req:
    chunk = c.recv(65536)
    if not chunk:
        break
    req += chunk
c.sendall(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: " + str(len(req)).encode() + b"\r\n\r\n" + req)
c.close()
`
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- utils.ExecCommandAsRoot(t, ctx, sbx, envdClient, "python3", "-c", listener, strconv.Itoa(port))
	}()

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	// The proxy answers 502 until python has bound the port, which takes seconds under parallel load.
	resp := utils.WaitForStatus(t, &http.Client{Timeout: 10 * time.Second}, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, resp, "listener never answered through the proxy")
	data, err := io.ReadAll(resp.Body)
	t.Logf("Data: %s", data)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, <-listenerDone)

	// Verify the Host header the listener received is the one set via maskRequestHost
	assert.Contains(t, string(data), fmt.Sprintf("Host: %s:%d", hostname, port))
}

func TestMaskRequestHostIncorrectUrl(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	ctx := t.Context()

	// Create sandbox without maskRequestHost
	utils.AcquireSandboxSlot(t)
	sbxNet := &api.SandboxNetworkConfig{
		MaskRequestHost: new("-https://abcd"),
	}
	createSandboxResponse, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		Network:    sbxNet,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	require.Equal(t, http.StatusBadRequest, createSandboxResponse.StatusCode())
	require.NotNil(t, createSandboxResponse.JSON400)
}
