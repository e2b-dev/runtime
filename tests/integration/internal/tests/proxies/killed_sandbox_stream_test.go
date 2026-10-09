package api

import (
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

// Killing a sandbox resets the proxy's connection to envd mid-response. Without
// a terminal frame the client's body simply stops, and a Connect client reports
// "incomplete envelope: unexpected EOF" under CodeInvalidArgument, which names
// neither the sandbox nor the fact that the peer went away.
func TestExecStreamEndsWithStatusWhenSandboxIsKilled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	client := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, client, utils.WithTimeout(60))
	envdClient := setup.GetEnvdClient(t, ctx)

	stdin := false
	req := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{Cmd: "sleep", Args: []string{"60"}},
		Stdin:   &stdin,
	})

	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")

	// A frame can only be appended between frames. sleep writes nothing, and
	// this pushes envd's keepalive well past the test, so the stream is idle on
	// a frame boundary when the kill lands. A chatty command could be cut
	// mid-frame, which the proxy is right to leave as an abort.
	req.Header().Set("Keepalive-Ping-Interval", "3600")

	stream, err := envdClient.ProcessClient.Start(ctx, req)
	require.NoError(t, err)

	defer stream.Close()

	require.True(t, stream.Receive(), "the process should start: %v", stream.Err())

	killResp, err := client.DeleteSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, killResp.StatusCode())

	for stream.Receive() {
		// Drain anything that races the kill.
	}

	streamErr := stream.Err()
	require.Error(t, streamErr, "a killed sandbox must not end the stream cleanly")
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(streamErr))
	assert.Contains(t, streamErr.Error(), sbx.SandboxID)
	assert.NotContains(t, streamErr.Error(), "incomplete envelope")
}
