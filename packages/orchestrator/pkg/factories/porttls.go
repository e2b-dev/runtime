//go:build linux

package factories

import (
	"net"
	"net/http"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

// The mode flags of the shared port and the peer hop; the env value under
// the same chart key is each one's fallback.
const (
	grpcModeFlag = "mtls-orchestrator-grpc-mode"
	peersHopFlag = "mtls-orchestrator-peers-client-mode"
)

// portTLS is the mutual TLS of the port gRPC and HTTP share through cmux.
// The listener sits in front of cmux and is created with the port, after
// the gRPC server, so the credentials and the guard ask it through
// connState; no connection reaches them before the port is served.
type portTLS struct {
	cfg         mtls.ServerConfig
	healthPaths []string
	listener    *mtls.Listener
}

// newPortTLS builds the shared port's configuration; no wildcard is
// admitted on it.
func newPortTLS(proc *mtls.Process, c cfg.TLSConfig) (*portTLS, error) {
	server, err := proc.Listener("grpc", grpcModeFlag, c.GRPC, mtls.WithoutWildcards())
	if err != nil {
		return nil, err
	}

	// newHTTPHandler's probe routes are always exempt; the env list adds to them.
	return &portTLS{cfg: server, healthPaths: append([]string{"/health", "/live"}, c.GRPC.HealthPaths...)}, nil
}

// wrap puts the mtls listener in front of the port: h2 for gRPC callers on
// TLS, http/1.1 for the probe and uploads.
func (p *portTLS) wrap(inner net.Listener) net.Listener {
	p.listener = mtls.NewListener(inner, p.cfg, mtls.WithNextProtos("h2", "http/1.1"))

	return p.listener
}

func (p *portTLS) connState(remoteAddr string) (mtls.ConnState, bool) {
	if p.listener == nil {
		return mtls.ConnState{}, false
	}

	return p.listener.ConnState(remoteAddr)
}

// grpcOptions are the gRPC server's credentials, which take the listener's
// verdict for each connection, and the per-call checks.
func (p *portTLS) grpcOptions() []e2bgrpc.ServerOption {
	return []e2bgrpc.ServerOption{
		e2bgrpc.WithTransportCredentials(mtls.NewServerCredentials(p.cfg, mtls.WithConnStateLookup(p.connState))),
		e2bgrpc.WithUnaryInterceptors(mtls.UnaryServerInterceptor(p.cfg)),
		e2bgrpc.WithStreamInterceptors(mtls.StreamServerInterceptor(p.cfg)),
	}
}

// guard puts the HTTP side's handler behind the health guard, asking the
// listener about each connection.
func (p *portTLS) guard(next http.Handler) http.Handler {
	return mtls.NewHTTPGuard(p.cfg, p.healthPaths, next, mtls.WithGuardConnStateLookup(p.connState))
}
