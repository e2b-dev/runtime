package clusters

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

type fakeInfoServer struct {
	infogrpc.UnimplementedInfoServiceServer

	mu       sync.Mutex
	response *infogrpc.ServiceInfoResponse
	err      error
}

func (s *fakeInfoServer) ServiceInfo(context.Context, *emptypb.Empty) (*infogrpc.ServiceInfoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.response, s.err
}

func (s *fakeInfoServer) set(response *infogrpc.ServiceInfoResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.response, s.err = response, err
}

func startInfoServer(t *testing.T, response *infogrpc.ServiceInfoResponse) (*fakeInfoServer, string) {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fake := &fakeInfoServer{response: response}
	server := grpc.NewServer()
	infogrpc.RegisterInfoServiceServer(server, fake)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	return fake, lis.Addr().String()
}

func serveInfo(t *testing.T, response *infogrpc.ServiceInfoResponse) string {
	t.Helper()

	_, address := startInfoServer(t, response)

	return address
}

func callContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	return ctx
}
