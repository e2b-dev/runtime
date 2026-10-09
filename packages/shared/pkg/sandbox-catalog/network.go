package sandbox_catalog

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
// It carries no configuration. The orchestrator hands the router its policy
// and workload identity over gRPC before the create call returns, and resends
// them when the router's process changes, so the record only has to let a
// reader route to the sandbox.
type NetworkPlacement struct {
	NetworkMode NetworkMode `json:"network_mode"`
	RouterID    string      `json:"router_id"`
	PortableIP  string      `json:"portable_ip"`
}
