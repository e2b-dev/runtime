package process

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/user"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	spec "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process/processconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func newTestService(t *testing.T, middleware ...func(http.Handler) http.Handler) (spec.ProcessClient, func()) {
	t.Helper()

	// handler.New sets SysProcAttr.Credential to switch uid/gid,
	// which requires root.
	if os.Geteuid() != 0 {
		t.Skip("skipping: requires root (handler.New sets SysProcAttr.Credential)")
	}

	u, err := user.Current()
	require.NoError(t, err)

	cwd := t.TempDir()
	logger := zerolog.Nop()

	svc := newService(&logger, &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    u.Username,
		Workdir: &cwd,
	}, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()))

	mux := http.NewServeMux()
	path, handler := spec.NewProcessHandler(svc)
	mux.Handle(path, handler)

	var h http.Handler = mux
	for _, mw := range middleware {
		h = mw(h)
	}

	srv := httptest.NewServer(h)
	client := spec.NewProcessClient(srv.Client(), srv.URL)

	return client, srv.Close
}

// TestStart_ShortCommand verifies that a short-lived command streams
// start, data, and end events, then the handler returns cleanly.
func TestStart_ShortCommand(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestService(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{
			Cmd:  "echo",
			Args: []string{"hello"},
		},
	}))
	require.NoError(t, err)

	var events []*rpc.ProcessEvent
	for stream.Receive() {
		events = append(events, stream.Msg().GetEvent())
	}
	require.NoError(t, stream.Err())
	require.NoError(t, stream.Close())

	// Expect at least a start event and an end event.
	require.GreaterOrEqual(t, len(events), 2, "expected at least start + end events")
	assert.NotNil(t, events[0].GetStart(), "first event should be Start")
	assert.NotNil(t, events[len(events)-1].GetEnd(), "last event should be End")
}

// TestStart_CancelBeforeStartEvent verifies that the handler returns instead
// of blocking forever when the request context is cancelled before the
// bootstrap start event reaches the sender goroutine.
//
// The middleware cancels the request context at handler entry, exercising the
// ordering where the sender goroutine exits before handleStart emits the start
// event. The process itself still starts because it runs on an independent
// context. A stuck handler keeps its connection open and makes the server
// close at the end of the test hang.
func TestStart_CancelBeforeStartEvent(t *testing.T) {
	t.Parallel()

	cancelAtEntry := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithCancel(r.Context())
			cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	client, cleanup := newTestService(t, cancelAtEntry)

	// Bound the drain below in case the handler never responds.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{
			Cmd:  "echo",
			Args: []string{"hello"},
		},
	}))
	if err == nil {
		for stream.Receive() {
		}
		_ = stream.Close()
	}

	// Server close waits for outstanding handlers, so a wedged
	// handler makes it hang.
	closed := make(chan struct{})
	go func() {
		defer close(closed)

		cleanup()
	}()

	select {
	case <-closed:
	case <-time.After(20 * time.Second):
		t.Fatal("server close timed out: handler goroutine leaked on cancelled start")
	}
}

// TestStart_ClientDisconnectMidStream verifies that when a client
// cancels mid-stream, the handler returns without racing.  This is
// the scenario that caused the nil-pointer panic in production:
// the outer handler must wait for the inner sender goroutine to
// finish before returning.
//
// Run with -race to verify no data race.
func TestStart_ClientDisconnectMidStream(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestService(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Use a long-running command so there's data flowing when we cancel.
	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{
			Cmd: "yes",
		},
	}))
	require.NoError(t, err)

	// Receive a few events to ensure the stream is established and
	// the inner sender goroutine is actively calling stream.Send.
	for range 3 {
		if !stream.Receive() {
			break
		}
	}

	// Cancel the client context, simulating an abrupt disconnect.
	// Before the fix, this would cause the outer handler to return
	// while the inner goroutine was still mid-Send, leading to a
	// nil-pointer panic in bufio.(*Writer).Flush.
	cancel()

	// Drain remaining events — the stream should close cleanly.
	for stream.Receive() {
	}
	_ = stream.Close()
}

// TestStart_StdinRoundTrip covers the stdin pipe end to end: input written through the RPC
// reaches the process, closing stdin delivers EOF, and the process then exits. The handler
// owns its stdio pipes rather than taking them from exec.Cmd, so it -- not exec.Cmd.Wait --
// is what closes them, and a pipe it failed to hand over or close would keep the stream
// open until the test's 10 s deadline fails it.
func TestStart_StdinRoundTrip(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestService(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{Cmd: "cat"},
	}))
	require.NoError(t, err)
	defer stream.Close()

	require.True(t, stream.Receive(), "expected a start event: %v", stream.Err())
	pid := stream.Msg().GetEvent().GetStart().GetPid()
	require.NotZero(t, pid)

	selector := &rpc.ProcessSelector{Selector: &rpc.ProcessSelector_Pid{Pid: pid}}
	_, err = client.SendInput(ctx, connect.NewRequest(&rpc.SendInputRequest{
		Process: selector,
		Input:   &rpc.ProcessInput{Input: &rpc.ProcessInput_Stdin{Stdin: []byte("hello\n")}},
	}))
	require.NoError(t, err)
	_, err = client.CloseStdin(ctx, connect.NewRequest(&rpc.CloseStdinRequest{Process: selector}))
	require.NoError(t, err)

	var stdout []byte
	var end *rpc.ProcessEvent_EndEvent
	for stream.Receive() {
		ev := stream.Msg().GetEvent()
		stdout = append(stdout, ev.GetData().GetStdout()...)
		if e := ev.GetEnd(); e != nil {
			end = e
		}
	}
	require.NoError(t, stream.Err())

	assert.Equal(t, "hello\n", string(stdout))
	require.NotNil(t, end, "cat must exit once stdin is closed")
	assert.Zero(t, end.GetExitCode())
}

// TestStart_KilledProcessEndsItsStreamWhileAGrandchildHoldsStdout pins that the Start stream
// ends once the process is killed, even though a grandchild it left behind still holds the
// stdout and stderr write ends. The readers never see EOF in that state, so the stream ends
// only because Wait closes the read ends after reaping the process, as exec.Cmd does for the
// pipes it creates itself.
func TestStart_KilledProcessEndsItsStreamWhileAGrandchildHoldsStdout(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestService(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	// The grandchild outlives the test's own deadline, so only the read ends being closed
	// can end the stream in time.
	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{
			Cmd:  "/bin/sh",
			Args: []string{"-c", "sleep 60 & echo started; wait"},
		},
	}))
	require.NoError(t, err)

	var pid uint32
	for pid == 0 && stream.Receive() {
		pid = stream.Msg().GetEvent().GetStart().GetPid()
	}
	require.NotZero(t, pid, "no start event")

	var sawOutput bool
	for !sawOutput && stream.Receive() {
		sawOutput = len(stream.Msg().GetEvent().GetData().GetStdout()) > 0
	}
	require.True(t, sawOutput, "the shell never printed, so the grandchild may not be running yet")

	_, err = client.SendSignal(ctx, connect.NewRequest(&rpc.SendSignalRequest{
		Process: &rpc.ProcessSelector{Selector: &rpc.ProcessSelector_Pid{Pid: pid}},
		Signal:  rpc.Signal_SIGNAL_SIGKILL,
	}))
	require.NoError(t, err)

	var ended bool
	for stream.Receive() {
		if stream.Msg().GetEvent().GetEnd() != nil {
			ended = true
		}
	}
	require.NoError(t, ctx.Err(), "the stream stayed open until the deadline: the grandchild held it")
	assert.True(t, ended, "the stream closed without an end event")
}
