package streaming

import (
	"context"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem/filesystemconnect"
	filesystemconnectmocks "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem/filesystemconnect/mocks"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/process/processconnect"
)

func newFilesystemClient(t *testing.T, mockFS *filesystemconnectmocks.MockFilesystemHandler) filesystemconnect.FilesystemClient {
	t.Helper()

	_, handler := filesystemconnect.NewFilesystemHandler(mockFS, connect.WithInterceptors(DisableProxyBuffering()))
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return filesystemconnect.NewFilesystemClient(srv.Client(), srv.URL)
}

func TestServerStreamDisablesProxyBuffering(t *testing.T) {
	t.Parallel()

	mockFS := filesystemconnectmocks.NewMockFilesystemHandler(t)
	mockFS.EXPECT().
		WatchDir(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *connect.Request[filesystem.WatchDirRequest], stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
			return stream.Send(&filesystem.WatchDirResponse{Event: &filesystem.WatchDirResponse_Start{Start: &filesystem.WatchDirResponse_StartEvent{}}})
		})

	client := newFilesystemClient(t, mockFS)

	stream, err := client.WatchDir(t.Context(), connect.NewRequest(&filesystem.WatchDirRequest{Path: "/a"}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	require.True(t, stream.Receive(), stream.Err())
	assert.Equal(t, []string{"no"}, stream.ResponseHeader().Values(AccelBufferingHeader))
}

func TestServerStreamErrorStillDisablesProxyBuffering(t *testing.T) {
	t.Parallel()

	mockFS := filesystemconnectmocks.NewMockFilesystemHandler(t)
	mockFS.EXPECT().
		WatchDir(mock.Anything, mock.Anything, mock.Anything).
		Return(connect.NewError(connect.CodeNotFound, assert.AnError))

	client := newFilesystemClient(t, mockFS)

	stream, err := client.WatchDir(t.Context(), connect.NewRequest(&filesystem.WatchDirRequest{Path: "/missing"}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	assert.False(t, stream.Receive())
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(stream.Err()))
	assert.Equal(t, []string{"no"}, stream.ResponseHeader().Values(AccelBufferingHeader))
}

func TestUnaryLeavesProxyBufferingAlone(t *testing.T) {
	t.Parallel()

	mockFS := filesystemconnectmocks.NewMockFilesystemHandler(t)
	mockFS.EXPECT().
		ListDir(mock.Anything, mock.Anything).
		Return(connect.NewResponse(&filesystem.ListDirResponse{}), nil)

	client := newFilesystemClient(t, mockFS)

	resp, err := client.ListDir(t.Context(), connect.NewRequest(&filesystem.ListDirRequest{Path: "/a"}))
	require.NoError(t, err)
	assert.Empty(t, resp.Header().Values(AccelBufferingHeader))
}

// fakeProcess implements only the process RPCs these tests call.
type fakeProcess struct {
	processconnect.UnimplementedProcessHandler
}

func (fakeProcess) Start(_ context.Context, _ *connect.Request[process.StartRequest], stream *connect.ServerStream[process.StartResponse]) error {
	return stream.Send(&process.StartResponse{Event: &process.ProcessEvent{}})
}

func (fakeProcess) StreamInput(_ context.Context, stream *connect.ClientStream[process.StreamInputRequest]) (*connect.Response[process.StreamInputResponse], error) {
	for stream.Receive() {
	}

	return connect.NewResponse(&process.StreamInputResponse{}), stream.Err()
}

func newProcessClient(t *testing.T) processconnect.ProcessClient {
	t.Helper()

	_, handler := processconnect.NewProcessHandler(fakeProcess{}, connect.WithInterceptors(DisableProxyBuffering()))
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return processconnect.NewProcessClient(srv.Client(), srv.URL)
}

func TestProcessStartDisablesProxyBuffering(t *testing.T) {
	t.Parallel()

	client := newProcessClient(t)

	stream, err := client.Start(t.Context(), connect.NewRequest(&process.StartRequest{}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	require.True(t, stream.Receive(), stream.Err())
	assert.Equal(t, []string{"no"}, stream.ResponseHeader().Values(AccelBufferingHeader))
}

func TestClientStreamLeavesProxyBufferingAlone(t *testing.T) {
	t.Parallel()

	client := newProcessClient(t)

	stream := client.StreamInput(t.Context())
	require.NoError(t, stream.Send(&process.StreamInputRequest{}))

	resp, err := stream.CloseAndReceive()
	require.NoError(t, err)
	assert.Empty(t, resp.Header().Values(AccelBufferingHeader))
}
