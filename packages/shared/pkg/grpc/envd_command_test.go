package grpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process/processconnect"
)

// failingProcessHandler streams msgCount messages from Start and then fails
// the stream with streamErr. If msgCount is negative it streams until the
// request context is cancelled.
type failingProcessHandler struct {
	processconnect.UnimplementedProcessHandler

	msgCount  int
	streamErr *connect.Error
}

func (h *failingProcessHandler) Start(ctx context.Context, _ *connect.Request[process.StartRequest], stream *connect.ServerStream[process.StartResponse]) error {
	for i := 0; h.msgCount < 0 || i < h.msgCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := stream.Send(&process.StartResponse{}); err != nil {
			return err
		}
	}

	return h.streamErr
}

func newProcessTestServer(t *testing.T, handler *failingProcessHandler) processconnect.ProcessClient {
	t.Helper()

	path, httpHandler := processconnect.NewProcessHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, httpHandler)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return processconnect.NewProcessClient(server.Client(), server.URL)
}

// TestStreamToChannel_StreamErrorAvailableOnClose pins the terminal status
// contract: the stream error is in the error channel before the message
// channel closes, so a consumer that drains the error channel after
// observing the close never reports a failed stream as a clean end.
func TestStreamToChannel_StreamErrorAvailableOnClose(t *testing.T) {
	t.Parallel()

	client := newProcessTestServer(t, &failingProcessHandler{
		msgCount:  3,
		streamErr: connect.NewError(connect.CodeInternal, errors.New("command failed on purpose")),
	})

	stream, err := client.Start(t.Context(), connect.NewRequest(&process.StartRequest{}))
	require.NoError(t, err)
	defer stream.Close()

	msgCh, errCh := StreamToChannel(t.Context(), stream)

	received := 0
	for range msgCh {
		received++
	}
	require.Equal(t, 3, received)

	select {
	case err := <-errCh:
		require.ErrorContains(t, err, "command failed on purpose")
	default:
		require.Fail(t, "terminal status not available after the message channel closed")
	}
}

// TestStreamToChannel_CancellationIsTerminalStatus verifies that a context
// cancellation while the producer is forwarding messages is reported through
// the error channel instead of being indistinguishable from a clean end of
// stream.
func TestStreamToChannel_CancellationIsTerminalStatus(t *testing.T) {
	t.Parallel()

	client := newProcessTestServer(t, &failingProcessHandler{
		// Stream until the request context is cancelled.
		msgCount: -1,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stream, err := client.Start(ctx, connect.NewRequest(&process.StartRequest{}))
	require.NoError(t, err)
	defer stream.Close()

	msgCh, errCh := StreamToChannel(ctx, stream)

	// Receive one message to make sure the stream is live, then cancel.
	select {
	case _, ok := <-msgCh:
		require.True(t, ok, "stream ended before cancellation")
	case <-time.After(5 * time.Second):
		require.Fail(t, "timed out waiting for the first message")
	}

	cancel()

	// Drain the message channel until the producer exits.
	for range msgCh { //nolint:revive // draining until closed
	}

	select {
	case err := <-errCh:
		require.Error(t, err)
	default:
		require.Fail(t, "cancellation was indistinguishable from a clean end of stream")
	}
}
