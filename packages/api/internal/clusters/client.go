package clusters

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

type GRPCClient struct {
	Info     infogrpc.InfoServiceClient
	Sandbox  orchestratorgrpc.SandboxServiceClient
	Volumes  orchestratorgrpc.VolumeServiceClient
	Template templatemanagergrpc.TemplateServiceClient

	Connection *grpc.ClientConn

	observeTarget string

	// shared is set when Connection is also used by other clients: Close then
	// retires only this client, and whoever created Connection closes it.
	shared *sharedCalls
}

func NewGRPCClient(conn *grpc.ClientConn, observeTarget string) *GRPCClient {
	return newGRPCClient(conn, conn, observeTarget)
}

// newSharedGRPCClient issues its calls on a connection other clients also use,
// adding opts to every call. Closing it does not close conn; it calls release
// once instead.
func newSharedGRPCClient(conn *grpc.ClientConn, observeTarget string, release func(), opts ...grpc.CallOption) *GRPCClient {
	shared := &sharedCalls{conn: conn, opts: opts, release: release}
	client := newGRPCClient(conn, shared, observeTarget)
	client.shared = shared

	return client
}

func newGRPCClient(conn *grpc.ClientConn, calls grpc.ClientConnInterface, observeTarget string) *GRPCClient {
	return &GRPCClient{
		Connection:    conn,
		Info:          infogrpc.NewInfoServiceClient(calls),
		Sandbox:       orchestratorgrpc.NewSandboxServiceClient(calls),
		Volumes:       orchestratorgrpc.NewVolumeServiceClient(calls),
		Template:      templatemanagergrpc.NewTemplateServiceClient(calls),
		observeTarget: observeTarget,
	}
}

func (a *GRPCClient) Init(ctx context.Context) {
	// A shared connection is observed once, by its owner.
	if a.shared != nil {
		return
	}

	e2bgrpc.ObserveConnection(ctx, a.Connection, a.observeTarget)
}

// State is the connectivity state of this client: Shutdown once it is closed,
// even when its connection is shared and stays open for the other clients.
func (a *GRPCClient) State() connectivity.State {
	if a.shared != nil && a.shared.closed.Load() {
		return connectivity.Shutdown
	}

	return a.Connection.GetState()
}

func (a *GRPCClient) Close() error {
	if a.shared != nil {
		if a.shared.closed.CompareAndSwap(false, true) {
			a.shared.release()
		}

		return nil
	}

	err := a.Connection.Close()
	if err != nil {
		return fmt.Errorf("failed to close Connection: %w", err)
	}

	return nil
}

// errSharedClientClosed matches what a closed *grpc.ClientConn returns, so a
// closed shared client fails its calls the same way an owned one does.
var errSharedClientClosed = status.Error(codes.Canceled, "grpc: the client connection is closing")

// sharedCalls issues one client's calls on a shared connection.
type sharedCalls struct {
	conn    *grpc.ClientConn
	opts    []grpc.CallOption
	release func()
	closed  atomic.Bool
}

func (s *sharedCalls) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	if s.closed.Load() {
		return errSharedClientClosed
	}

	return s.conn.Invoke(ctx, method, args, reply, slices.Concat(s.opts, opts)...)
}

func (s *sharedCalls) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if s.closed.Load() {
		return nil, errSharedClientClosed
	}

	return s.conn.NewStream(ctx, desc, method, slices.Concat(s.opts, opts)...)
}
