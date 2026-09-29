package envd

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestCommandKeepaliveInterval(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		interval string
	}{
		{name: "valid", interval: "50"},
		{name: "zero", interval: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Each case owns its sandbox so an envd crash cannot affect other tests.
			sbx := utils.SetupSandboxWithCleanup(t, setup.GetAPIClient(), utils.WithTimeout(60))
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()

			client := setup.GetEnvdClient(t, ctx)
			backgroundReq := connect.NewRequest(&process.StartRequest{
				Process: &process.ProcessConfig{
					Cmd:  "/bin/sleep",
					Args: []string{"60"},
				},
			})
			setup.SetSandboxHeader(t, backgroundReq.Header(), sbx.SandboxID)
			setup.SetUserHeader(t, backgroundReq.Header(), "user")
			background, err := client.ProcessClient.Start(ctx, backgroundReq)
			require.NoError(t, err)
			defer background.Close()

			require.True(t, background.Receive(), "background command must start: %v", background.Err())
			start := background.Msg().GetEvent().GetStart()
			require.NotNil(t, start)
			pid := start.GetPid()

			req := connect.NewRequest(&process.StartRequest{
				Process: &process.ProcessConfig{
					Cmd:  "/bin/echo",
					Args: []string{"ok"},
				},
			})
			setup.SetSandboxHeader(t, req.Header(), sbx.SandboxID)
			setup.SetUserHeader(t, req.Header(), "user")
			// A zero interval is invalid input. Like a non-numeric interval,
			// it should fall back to the default without interrupting the command.
			req.Header().Set("Keepalive-Ping-Interval", tc.interval)

			stream, err := client.ProcessClient.Start(ctx, req)
			require.NoError(t, err)
			defer stream.Close()

			var stdout strings.Builder
			var end *process.ProcessEvent_EndEvent
			for stream.Receive() {
				event := stream.Msg().GetEvent()
				stdout.Write(event.GetData().GetStdout())
				if event.GetEnd() != nil {
					end = event.GetEnd()
				}
			}

			require.NoError(t, stream.Err(), "command stream must finish normally")
			require.NotNil(t, end, "command must report its exit status")
			assert.EqualValues(t, 0, end.GetExitCode())
			assert.Equal(t, "ok\n", stdout.String())

			listReq := connect.NewRequest(&process.ListRequest{})
			setup.SetSandboxHeader(t, listReq.Header(), sbx.SandboxID)
			setup.SetUserHeader(t, listReq.Header(), "user")
			listed, err := client.ProcessClient.List(ctx, listReq)
			require.NoError(t, err)
			pids := make([]uint32, 0, len(listed.Msg.GetProcesses()))
			for _, proc := range listed.Msg.GetProcesses() {
				pids = append(pids, proc.GetPid())
			}
			require.Contains(t, pids, pid, "envd must retain the existing command")

			selector := &process.ProcessSelector{Selector: &process.ProcessSelector_Pid{Pid: pid}}
			connectReq := connect.NewRequest(&process.ConnectRequest{Process: selector})
			setup.SetSandboxHeader(t, connectReq.Header(), sbx.SandboxID)
			setup.SetUserHeader(t, connectReq.Header(), "user")
			connected, err := client.ProcessClient.Connect(ctx, connectReq)
			require.NoError(t, err)
			defer connected.Close()
			require.True(t, connected.Receive(), "existing command must remain connectable: %v", connected.Err())
			require.Equal(t, pid, connected.Msg().GetEvent().GetStart().GetPid())

			killReq := connect.NewRequest(&process.SendSignalRequest{
				Process: selector,
				Signal:  process.Signal_SIGNAL_SIGTERM,
			})
			setup.SetSandboxHeader(t, killReq.Header(), sbx.SandboxID)
			setup.SetUserHeader(t, killReq.Header(), "user")
			_, err = client.ProcessClient.SendSignal(ctx, killReq)
			require.NoError(t, err)

			var backgroundEnd *process.ProcessEvent_EndEvent
			for background.Receive() {
				if event := background.Msg().GetEvent().GetEnd(); event != nil {
					backgroundEnd = event
				}
			}
			require.NoError(t, background.Err(), "the original stream must survive the invalid interval")
			require.NotNil(t, backgroundEnd, "the original stream must report termination")

			var connectedEnd *process.ProcessEvent_EndEvent
			for connected.Receive() {
				if event := connected.Msg().GetEvent().GetEnd(); event != nil {
					connectedEnd = event
				}
			}
			require.NoError(t, connected.Err())
			require.NotNil(t, connectedEnd, "the reconnected stream must report termination")
		})
	}
}
