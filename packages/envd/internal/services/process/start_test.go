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

// TestStart_WithWriteDeadlineMiddleware verifies that the streamDeadlineMiddleware
// (mounted in production via Handle) does not break normal streaming: a short
// command must still produce start, data, and end events end-to-end.
//
// This test exercises the set/clear write deadline path that wraps every
// stream.Send in connect.go and start.go.
func TestStart_WithWriteDeadlineMiddleware(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestService(t, streamDeadlineMiddleware)
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

	require.GreaterOrEqual(t, len(events), 2, "expected at least start + end events")
	assert.NotNil(t, events[0].GetStart(), "first event should be Start")
	assert.NotNil(t, events[len(events)-1].GetEnd(), "last event should be End")

	// Verify there is at least one data event containing the echo output.
	hasData := false
	for _, e := range events {
		if d := e.GetData(); d != nil {
			hasData = true
			break
		}
	}
	assert.True(t, hasData, "stream should carry at least one Data event with stdout")
}

// TestStart_WriteDeadlineDoesNotKillSlowButLiveClient verifies that a client
// reading more slowly than process emission rate is NOT disconnected. The write
// deadline should only fire for dead peers (where TCP blocks indefinitely), not
// for clients that are merely slower than the producer.
//
// We start a process that emits a few lines quickly, then read them with a
// small artificial delay between reads. The handler must wait for the client
// and deliver all events.
func TestStart_WriteDeadlineDoesNotKillSlowButLiveClient(t *testing.T) {
	t.Parallel()

	client, cleanup := newTestService(t, streamDeadlineMiddleware)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// printf emits three lines of stdout quickly.
	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{
			Cmd:  "printf",
			Args: []string{"line1\nline2\nline3\n"},
		},
	}))
	require.NoError(t, err)

	var events []*rpc.ProcessEvent
	for stream.Receive() {
		events = append(events, stream.Msg().GetEvent())
		// Simulate a client that is a bit slow to process each event.
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, stream.Err(), "slow client should not be disconnected by the write deadline")
	require.NoError(t, stream.Close())

	assert.NotNil(t, events[0].GetStart(), "first event should be Start")
	assert.NotNil(t, events[len(events)-1].GetEnd(), "last event should be End")
}
