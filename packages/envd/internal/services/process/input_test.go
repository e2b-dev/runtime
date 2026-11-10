//go:build unix

package process

import (
	"os"
	"testing"

	"connectrpc.com/connect"
	"github.com/creack/pty"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/process/handler"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

func pidSelector(pid uint32) *rpc.ProcessSelector {
	return &rpc.ProcessSelector{Selector: &rpc.ProcessSelector_Pid{Pid: pid}}
}

func stdinInput() *rpc.ProcessInput {
	return &rpc.ProcessInput{Input: &rpc.ProcessInput_Stdin{Stdin: []byte("x")}}
}

func ptyInput() *rpc.ProcessInput {
	return &rpc.ProcessInput{Input: &rpc.ProcessInput_Pty{Pty: []byte("x")}}
}

// Input to a process that cannot take it reaches the client as
// failed_precondition through every input RPC, not as internal.
func TestInputRPCs_ProcessStateErrorsAreFailedPrecondition(t *testing.T) {
	t.Parallel()

	client, svc, cleanup := newRetentionTestService(t)
	t.Cleanup(cleanup)

	logger := zerolog.Nop()

	const noStdinPid, ptyPid, exitedPid = 101, 102, 103

	svc.processes.Store(noStdinPid, handler.Readopt(handler.ReadoptArgs{Pid: noStdinPid}, &logger))

	ptmx, tty, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tty.Close() })
	svc.processes.Store(ptyPid, handler.Readopt(handler.ReadoptArgs{Pid: ptyPid, Tty: ptmx}, &logger))

	r, w, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, r.Close())
	svc.processes.Store(exitedPid, handler.Readopt(handler.ReadoptArgs{Pid: exitedPid, Stdin: w}, &logger))

	sendInput := func(pid uint32, in *rpc.ProcessInput) error {
		_, err := client.SendInput(t.Context(), connect.NewRequest(&rpc.SendInputRequest{
			Process: pidSelector(pid),
			Input:   in,
		}))

		return err
	}

	streamInput := func(pid uint32, in *rpc.ProcessInput) error {
		stream := client.StreamInput(t.Context())
		start := &rpc.StreamInputRequest{Event: &rpc.StreamInputRequest_Start{
			Start: &rpc.StreamInputRequest_StartEvent{Process: pidSelector(pid)},
		}}
		if err := stream.Send(start); err != nil {
			return err
		}
		data := &rpc.StreamInputRequest{Event: &rpc.StreamInputRequest_Data{
			Data: &rpc.StreamInputRequest_DataEvent{Input: in},
		}}
		if err := stream.Send(data); err != nil {
			return err
		}
		_, err := stream.CloseAndReceive()

		return err
	}

	closeStdin := func(pid uint32) error {
		_, err := client.CloseStdin(t.Context(), connect.NewRequest(&rpc.CloseStdinRequest{
			Process: pidSelector(pid),
		}))

		return err
	}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"SendInput stdin with stdin disabled", func() error { return sendInput(noStdinPid, stdinInput()) }},
		{"SendInput pty to a non-pty process", func() error { return sendInput(noStdinPid, ptyInput()) }},
		{"StreamInput stdin with stdin disabled", func() error { return streamInput(noStdinPid, stdinInput()) }},
		{"SendInput stdin to a pty process", func() error { return sendInput(ptyPid, stdinInput()) }},
		{"CloseStdin on a pty process", func() error { return closeStdin(ptyPid) }},
		{"SendInput stdin after the reader is gone", func() error { return sendInput(exitedPid, stdinInput()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call()
			require.Error(t, err)
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "error: %v", err)
		})
	}
}
