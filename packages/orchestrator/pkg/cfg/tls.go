//go:build linux

package cfg

import "github.com/e2b-dev/infra/packages/shared/pkg/mtls"

// TLSConfig is the mutual TLS settings the chart renders for the shared
// port and the hop to peer orchestrators. An edition's own hops read
// theirs in the edition.
type TLSConfig struct {
	GRPC  mtls.ListenerSettings `envPrefix:"E2B_TLS_GRPC_"`
	Peers mtls.HopSettings      `envPrefix:"E2B_TLS_PEERS_"`
}
