package peerclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const peerConnectTimeout = 5 * time.Second

// Resolver looks up peer addresses for build IDs and manages gRPC connections
// to peer orchestrators. It is used by the routing provider to decide, per
// storage path, whether to read from a peer or from the base provider.
//
// The unexported resolve method restricts implementations to this package.
type Resolver interface {
	resolve(ctx context.Context, buildID string) (attribute.KeyValue, resolveResult)
	IsActive(buildID string) bool
	Purge(buildID string)
	Close()
}

type resolveResult struct {
	client   orchestrator.ChunkServiceClient
	uploaded *atomic.Bool
	addr     string
}

// NopResolver returns a Resolver that always falls back to the base provider.
func NopResolver() Resolver { return nopResolver{} }

type nopResolver struct{}

func (nopResolver) resolve(context.Context, string) (attribute.KeyValue, resolveResult) {
	return attrResolveNoPeer, resolveResult{}
}
func (nopResolver) IsActive(string) bool { return false }
func (nopResolver) Purge(string)         {}
func (nopResolver) Close()               {}

// peerResolver is the real implementation that looks up peers via the Registry.
type peerResolver struct {
	registry       Registry
	selfAddress    string
	creds          credentials.TransportCredentials
	peerConns      sync.Map // address → *grpc.ClientConn
	uploadedBuilds sync.Map // buildID → *atomic.Bool
	dialGroup      singleflight.Group
}

// NewResolver dials peers through creds.
func NewResolver(registry Registry, selfAddress string, creds credentials.TransportCredentials) Resolver {
	return &peerResolver{
		registry:    registry,
		selfAddress: selfAddress,
		creds:       creds,
	}
}

func (r *peerResolver) readPeerAddress(ctx context.Context, buildID string) (string, bool, error) {
	return r.registry.Lookup(ctx, buildID)
}

// getOrDialPeer deduplicates concurrent dials via singleflight.
func (r *peerResolver) getOrDialPeer(ctx context.Context, address string) (*grpc.ClientConn, error) {
	if conn, ok := r.peerConns.Load(address); ok {
		return conn.(*grpc.ClientConn), nil
	}

	v, err, _ := r.dialGroup.Do(address, func() (any, error) {
		if conn, ok := r.peerConns.Load(address); ok {
			return conn, nil
		}

		conn, err := grpc.NewClient(address,
			grpc.WithTransportCredentials(r.creds),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: peerConnectTimeout}),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to dial peer %s: %w", address, err)
		}

		e2bgrpc.ObserveConnection(ctx, conn, "peer-orchestrator")

		r.peerConns.Store(address, conn)

		return conn, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*grpc.ClientConn), nil
}

func (r *peerResolver) isSelfAddress(address string) bool {
	return address == r.selfAddress
}

// peerFlag returns the shared atomic "switched-to-storage" flag for buildID,
// creating it on first call. The presence of an entry in uploadedBuilds means
// "this build is/was peer-served on this orch"; the flag's value tracks
// whether a reader has observed the source switch to storage. Only resolve()
// creates entries (in the peer-found branch) so absence is meaningful: no
// peer ever existed for this build from this orch's perspective.
func (r *peerResolver) peerFlag(buildID string) *atomic.Bool {
	if v, ok := r.uploadedBuilds.Load(buildID); ok {
		return v.(*atomic.Bool)
	}

	flag := &atomic.Bool{}
	actual, _ := r.uploadedBuilds.LoadOrStore(buildID, flag)

	return actual.(*atomic.Bool)
}

// Purge removes the uploaded state for a build, called on template
// cache eviction so the entry doesn't accumulate forever.
func (r *peerResolver) Purge(buildID string) {
	r.uploadedBuilds.Delete(buildID)
}

// resolve looks up the peer for the given build and returns a gRPC client if
// a remote peer is found. Returns a nil client when the base provider should
// be used instead (uploaded, no peer, self, or error).
func (r *peerResolver) resolve(ctx context.Context, buildID string) (attribute.KeyValue, resolveResult) {
	// Fast path: a prior resolve flagged this build as peer-served and a
	// reader has since observed the switch to storage.
	if v, ok := r.uploadedBuilds.Load(buildID); ok && v.(*atomic.Bool).Load() {
		return attrResolveUploaded, resolveResult{}
	}

	addr, found, err := r.readPeerAddress(ctx, buildID)
	if err != nil {
		return attrResolveRedisError, resolveResult{}
	}

	if !found {
		return attrResolveNoPeer, resolveResult{}
	}

	if r.isSelfAddress(addr) {
		return attrResolveSelf, resolveResult{}
	}

	conn, err := r.getOrDialPeer(ctx, addr)
	if err != nil {
		return attrResolveDialError, resolveResult{}
	}

	// Peer found and dialable — register the flag now so IsActive and
	// future resolves can answer locally without touching Redis.
	uploaded := r.peerFlag(buildID)
	if uploaded.Load() {
		return attrResolveUploaded, resolveResult{}
	}

	return attrResolvePeer, resolveResult{
		client:   orchestrator.NewChunkServiceClient(conn),
		uploaded: uploaded,
		addr:     addr,
	}
}

// IsActive reports whether a peer is currently serving this build's
// chunks on this orch — i.e., resolve() found a peer and no reader has yet
// observed the switch to storage. Pure local read; no Redis, no dial.
//
// Absence of an entry means no peer was ever seen for this build, which
// implies the build is durable in storage.
func (r *peerResolver) IsActive(buildID string) bool {
	v, ok := r.uploadedBuilds.Load(buildID)

	return ok && !v.(*atomic.Bool).Load()
}

func (r *peerResolver) Close() {
	r.peerConns.Range(func(_, value any) bool {
		_ = value.(*grpc.ClientConn).Close()

		return true
	})
}
