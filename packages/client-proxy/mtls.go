package main

import (
	"net/http"

	"google.golang.org/grpc/credentials"

	"github.com/e2b-dev/infra/packages/proxy/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

// apiHopModeFlag is the hop's mode flag; E2B_TLS_API_CLIENT_MODE is its fallback.
const apiHopModeFlag = "mtls-client-proxy-api-client-mode"

// routerHopModeFlag is the hop's mode flag; E2B_TLS_ROUTER_CLIENT_MODE is its fallback.
const routerHopModeFlag = "mtls-client-proxy-router-client-mode"

// newAPIHop is the credentials of the in-cluster hop to the api's internal
// gRPC port.
func newAPIHop(proc *mtls.Process, c cfg.TLSConfig) (credentials.TransportCredentials, error) {
	hop, err := proc.Hop("api", apiHopModeFlag, c.API)
	if err != nil {
		return nil, err
	}

	return mtls.NewClientCredentials(hop), nil
}

// newRouterHop returns the wrap that builds the transport of the hop to a
// Router's ingress endpoint, dialled by node IP, so the hop names no server
// DNS name.
func newRouterHop(proc *mtls.Process, c cfg.TLSConfig) (func(*http.Transport) *http.Transport, error) {
	hop, err := proc.Hop("router", routerHopModeFlag, c.Router)
	if err != nil {
		return nil, err
	}

	return func(base *http.Transport) *http.Transport { return mtls.NewHTTPTransport(hop, base) }, nil
}
