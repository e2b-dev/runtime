package grpc

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// streamingChunkService answers a chunk read with a fixed number of messages.
type streamingChunkService struct {
	orchestrator.UnimplementedChunkServiceServer

	messages int
}

func (s streamingChunkService) ReadAtBuildSeekable(_ *orchestrator.ReadAtBuildSeekableRequest, stream grpc.ServerStreamingServer[orchestrator.ReadAtBuildSeekableResponse]) error {
	for range s.messages {
		if err := stream.Send(&orchestrator.ReadAtBuildSeekableResponse{Data: []byte{1}}); err != nil {
			return err
		}
	}

	return nil
}

// A stream is one call, however many messages it carries. Payload logging
// writes a line per message, so a chunk read of thousands of messages used to
// write thousands of info lines for one call, enough to flood the node's log
// pipeline; the stream's start and finish are the only lines it may write.
//
//nolint:paralleltest // the middleware logs through the global logger, which this test replaces
func TestNewGRPCServerStreamsLogTheCallNotEveryMessage(t *testing.T) {
	logs := captureLogs(t)
	server := NewGRPCServer(noopTelemetry())
	orchestrator.RegisterChunkServiceServer(server, streamingChunkService{messages: 50})
	client := orchestrator.NewChunkServiceClient(serveOn(t, server))

	stream, err := client.ReadAtBuildSeekable(t.Context(), &orchestrator.ReadAtBuildSeekableRequest{BuildId: "build"})
	require.NoError(t, err)
	received := 0
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		received++
	}
	require.Equal(t, 50, received)

	var perMessage, perCall int
	for _, entry := range logs.All() {
		switch {
		case strings.Contains(entry.Message, "response sent"), strings.Contains(entry.Message, "request received"):
			perMessage++
		case strings.Contains(entry.Message, "ReadAtBuildSeekable"):
			perCall++
		}
	}
	require.Zero(t, perMessage, "a streamed message must not write a log line")
	require.Equal(t, 2, perCall, "the call still logs its start and finish")
}

// serveOn serves an already built server on a loopback port and returns a
// connection to it.
func serveOn(t *testing.T, server *grpc.Server) *grpc.ClientConn {
	t.Helper()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}
