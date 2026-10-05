package placement

import (
	"github.com/Masterminds/semver/v3"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// Feature is a sandbox capability that orchestrators implement only from a
// given release onwards. The floor travels with the capability so the two
// cannot be stated separately and drift apart.
type Feature struct {
	// name identifies the capability in placement errors.
	name string

	// minVersion is the first orchestrator release implementing the capability.
	minVersion *semver.Version
}

// featureGate pairs a capability with the test for a request wanting it.
type featureGate struct {
	feature Feature

	// requested reads the create request rather than the caller's intent, so
	// the gate and the bytes the orchestrator receives cannot disagree.
	requested func(req *orchestrator.SandboxCreateRequest) bool
}

// featureGates is every version-gated sandbox capability. An orchestrator that
// predates a request field drops it and starts the sandbox without the
// capability, which the caller cannot tell apart from success.
var featureGates = []featureGate{
	{
		// An orchestrator below 0.10.0 drops https_ports and serves the
		// declared port as plaintext HTTP, so the TLS-only service behind it
		// answers with the 502 this capability exists to remove — while the
		// API returns 201.
		feature: Feature{name: "https-backend-ports", minVersion: semver.New(0, 10, 0, "", "")},
		requested: func(req *orchestrator.SandboxCreateRequest) bool {
			return len(req.GetSandbox().GetNetwork().GetIngress().GetHttpsPorts()) > 0
		},
	},
	{
		// An older orchestrator drops egress_proxy_tls and dials the proxy in
		// the clear. The floor is the auto-deploy version that added it.
		feature: Feature{name: "egress-proxy-tls", minVersion: semver.New(0, 16, 202610011012, "", "")},
		requested: func(req *orchestrator.SandboxCreateRequest) bool {
			return req.GetSandbox().GetNetwork().GetEgress().GetEgressProxyTls().GetEnabled()
		},
	},
}

// EgressUpdateFeatures reports the capabilities a live network update needs
// from the node already running the sandbox. An update skips placement, so it
// is checked against the same gates on that node instead.
func EgressUpdateFeatures(egress *orchestrator.SandboxNetworkEgressConfig) FeatureRequirement {
	return requiredFeatures(&orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			Network: &orchestrator.SandboxNetworkConfig{Egress: egress},
		},
	})
}

// FilesystemOnlyCheckpoint is read from SandboxCheckpointRequest.filesystem_only
// by orchestrators from the release that introduced the field. An older one
// drops the field, takes a memory checkpoint and answers as if it had not, so
// the API refuses the request on such a node instead of sending it.
var FilesystemOnlyCheckpoint = Feature{name: "filesystem-only-checkpoint", minVersion: semver.New(0, 16, 202609301732, "", "")}

// Name identifies the capability in errors and logs.
func (f Feature) Name() string {
	return f.name
}

// Require builds the requirement for capabilities asked for outside sandbox
// creation, where there is no create request for a gate predicate to read.
func Require(features ...Feature) FeatureRequirement {
	var requirement FeatureRequirement
	for _, feature := range features {
		requirement.features = append(requirement.features, feature)
		if requirement.minVersion == nil || requirement.minVersion.LessThan(feature.minVersion) {
			requirement.minVersion = feature.minVersion
		}
	}

	return requirement
}

// FeatureRequirement is the orchestrator-capability constraint a sandbox puts
// on a candidate node. The zero value constrains nothing.
type FeatureRequirement struct {
	features []Feature

	// minVersion is the highest floor among features, so one comparison
	// answers for all of them.
	minVersion *semver.Version
}

// requiredFeatures reports the capabilities req needs from the node serving it.
func requiredFeatures(req *orchestrator.SandboxCreateRequest) FeatureRequirement {
	return featuresFrom(req, featureGates)
}

// featuresFrom takes the gate list as an argument so it can be evaluated
// against a list other than the package's own.
func featuresFrom(req *orchestrator.SandboxCreateRequest, gates []featureGate) FeatureRequirement {
	var features []Feature

	for _, gate := range gates {
		if gate.requested(req) {
			features = append(features, gate.feature)
		}
	}

	return Require(features...)
}

// FeatureNames lists the requested capabilities, for reporting.
func (r FeatureRequirement) FeatureNames() []string {
	names := make([]string, 0, len(r.features))
	for _, feature := range r.features {
		names = append(names, feature.name)
	}

	return names
}

// MinVersion is the lowest orchestrator release that satisfies the whole
// requirement, or "" when nothing is required.
func (r FeatureRequirement) MinVersion() string {
	if r.minVersion == nil {
		return ""
	}

	return r.minVersion.String()
}

// NodeSatisfiesFeatures reports whether node runs an orchestrator release that
// implements every capability req asks for. A version that does not parse fails
// the check.
func NodeSatisfiesFeatures(node *nodemanager.Node, req FeatureRequirement) bool {
	if req.minVersion == nil {
		return true
	}

	version, err := semver.NewVersion(node.Metadata().Version)
	if err != nil {
		return false
	}

	// Compared without the pre-release: distributions stamp a suffix onto the
	// shared release version, which semver sorts below the bare release.
	return !semver.New(version.Major(), version.Minor(), version.Patch(), "", "").LessThan(req.minVersion)
}

// anyNodeSatisfiesFeatures reports whether req could be served by any of nodes.
// Capacity, exclusions and health are ignored: it answers whether the fleet is
// new enough, not whether a node is free.
func anyNodeSatisfiesFeatures(nodes []*nodemanager.Node, req FeatureRequirement) bool {
	if req.minVersion == nil {
		return true
	}

	for _, node := range nodes {
		if NodeSatisfiesFeatures(node, req) {
			return true
		}
	}

	return false
}
