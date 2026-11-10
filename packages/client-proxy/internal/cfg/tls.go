package cfg

import "github.com/e2b-dev/infra/packages/shared/pkg/mtls"

// TLSConfig is the mutual TLS settings the chart renders for
// client-proxy's client hops to the api and to a Router's ingress.
type TLSConfig struct {
	API    mtls.HopSettings `envPrefix:"E2B_TLS_API_"`
	Router mtls.HopSettings `envPrefix:"E2B_TLS_ROUTER_"`
}
