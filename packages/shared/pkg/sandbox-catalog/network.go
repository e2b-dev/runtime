package sandbox_catalog

import "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"

// NetworkMode is the serialized network topology of a sandbox route.
type NetworkMode string

const (
	// NetworkModeLegacy is the per-worker network. A legacy route carries no
	// network placement at all, so this value never appears inside one.
	NetworkModeLegacy NetworkMode = "legacy"
	// NetworkModeEgressRouter sends egress through a router while ingress still
	// terminates on the worker.
	NetworkModeEgressRouter NetworkMode = "egress-router"
	// NetworkModeFullRouter sends both egress and ingress through a router.
	NetworkModeFullRouter NetworkMode = "full-router"
)

// NetworkPlacement is the routed-network placement of a running sandbox: where
// its traffic goes.
//
// It carries location only. The orchestrator owns the router configuration,
// which never travels through this record, so a reader only needs the record
// to route to the sandbox.
type NetworkPlacement struct {
	NetworkMode NetworkMode `json:"network_mode"`
	RouterID    string      `json:"router_id"`
	PortableIP  string      `json:"portable_ip"`
	// IngressAddress and IngressPort are the endpoint a caller sends sandbox
	// traffic to: the router for full-router, the worker for egress-router.
	IngressAddress string `json:"ingress_address,omitempty"`
	IngressPort    uint32 `json:"ingress_port,omitempty"`
}

// NetworkPlacementOf is the record form of a placement an orchestrator
// reported, nil for none. A reported placement carries no PortableIP.
func NetworkPlacementOf(placement *orchestrator.SandboxNetworkPlacement) *NetworkPlacement {
	if placement == nil {
		return nil
	}

	return &NetworkPlacement{
		NetworkMode:    NetworkMode(placement.GetNetworkMode()),
		RouterID:       placement.GetRouterId(),
		IngressAddress: placement.GetIngressAddress(),
		IngressPort:    placement.GetIngressPort(),
	}
}
