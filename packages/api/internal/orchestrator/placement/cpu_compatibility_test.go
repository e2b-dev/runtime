package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

func TestNodeSatisfiesCPU_NoBuildRequirements(t *testing.T) {
	t.Parallel()
	// When build has no CPU requirements, all nodes should be compatible
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4, nodemanager.WithCPUInfo("x86_64", "Intel", "6"))
	buildCPU := machineinfo.MachineInfo{} // Empty - no requirements

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.True(t, result, "Node should be compatible when build has no CPU requirements")
}

func TestNodeSatisfiesCPU_IdenticalCPU(t *testing.T) {
	t.Parallel()
	// Node and build have matching CPU info
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4, nodemanager.WithCPUInfo("x86_64", "Intel", "6"))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "Intel", CPUModel: "6"}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.True(t, result, "Node should be compatible when CPU info matches exactly")
}

func TestNodeSatisfiesCPU_ArchitectureMismatch(t *testing.T) {
	t.Parallel()
	// Different CPU architectures
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4, nodemanager.WithCPUInfo("aarch64", "ARM", "8"))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "Intel", CPUModel: "6"}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.False(t, result, "Node should be incompatible with different architecture")
}

func TestNodeSatisfiesCPU_UnlistedModelMismatch(t *testing.T) {
	t.Parallel()
	// A build whose model is not paired with the node's model in the compatible
	// model map: incompatible.
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", "79"))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: "85"}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.False(t, result, "Node with an unlisted different model should be incompatible")
}

func TestNodeSatisfiesCPU_NodeHasNoCPUInfo(t *testing.T) {
	t.Parallel()
	// Node without CPU info, build requires specific CPU
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4) // No CPU info
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "Intel", CPUModel: "6"}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.False(t, result, "Node without CPU info should be incompatible when build requires specific CPU")
}

func TestNodeSatisfiesCPU_BothEmpty(t *testing.T) {
	t.Parallel()
	// Both node and build have no CPU info
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4) // No CPU info
	buildCPU := machineinfo.MachineInfo{}                               // No requirements

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.True(t, result, "Node should be compatible when neither has CPU requirements")
}

func TestNodeSatisfiesCPU_OlderBuildOnNewerNode_Compatible(t *testing.T) {
	t.Parallel()
	// An Ice Lake build restored on a newer Emerald Rapids node: allowed because
	// the newer CPU is a superset of the older one.
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.EmeraldRapidsModel))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.IceLakeModel}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.True(t, result, "An older build should be allowed to run on a newer node")
}

func TestNodeSatisfiesCPU_NewerBuildOnOlderNode_Incompatible(t *testing.T) {
	t.Parallel()
	// An Emerald Rapids build restored on an older Ice Lake node: rejected because
	// the older CPU may lack instructions the newer build relies on. Compatibility
	// is directional (older build -> newer node only).
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.IceLakeModel))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.EmeraldRapidsModel}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.False(t, result, "A newer build should not be allowed to run on an older node")
}

func TestNodeSatisfiesCPU_FamilyMismatch(t *testing.T) {
	t.Parallel()
	// Same model number but different CPU family (e.g. Intel vs AMD): incompatible.
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "23", "85"))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: "85"}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.False(t, result, "Same model with a different family should be incompatible")
}

func TestNodeSatisfiesCPU_AllFieldsMatch(t *testing.T) {
	t.Parallel()
	// Complete match including architecture, family, and model
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4, nodemanager.WithCPUInfo("x86_64", "6", "85"))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: "85"}

	result := NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU})
	assert.True(t, result, "Node should be compatible when architecture, family, and model all match")
}

func TestNodeSatisfiesCPU_PinRejectsOtherModel(t *testing.T) {
	t.Parallel()
	// The cross-generation upgrade IsCompatibleWith permits is the one thing
	// the pin withholds.
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.EmeraldRapidsModel))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.IceLakeModel}

	assert.True(t, NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU}),
		"Without the pin the newer node stays eligible")
	assert.False(t, NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU, PinnedModel: machineinfo.IceLakeModel}),
		"The pin should reject a node reporting a different model")
}

func TestNodeSatisfiesCPU_PinAcceptsPinnedModel(t *testing.T) {
	t.Parallel()
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.IceLakeModel))
	buildCPU := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.IceLakeModel}

	assert.True(t, NodeSatisfiesCPU(node, CPURequirement{Build: buildCPU, PinnedModel: machineinfo.IceLakeModel}),
		"The pin should accept a node reporting the pinned model")
}

func TestNodeSatisfiesCPU_PinAppliesWithoutBuildCPU(t *testing.T) {
	t.Parallel()
	// The pin is a property of the node, not of the build, so a build predating
	// CPU recording is still confined to the pinned model.
	iceLake := nodemanager.NewTestNode("ice", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.IceLakeModel))
	emerald := nodemanager.NewTestNode("emerald", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.EmeraldRapidsModel))

	assert.True(t, NodeSatisfiesCPU(iceLake, CPURequirement{PinnedModel: machineinfo.IceLakeModel}))
	assert.False(t, NodeSatisfiesCPU(emerald, CPURequirement{PinnedModel: machineinfo.IceLakeModel}))
}

func TestNodeSatisfiesCPU_PinNarrowsRatherThanReplaces(t *testing.T) {
	t.Parallel()
	// Pinning to a model the build may not run on leaves no candidate, rather
	// than letting the pin override the build compatibility rule.
	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.IceLakeModel))
	newerBuild := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.EmeraldRapidsModel}

	assert.False(t, NodeSatisfiesCPU(node, CPURequirement{Build: newerBuild, PinnedModel: machineinfo.IceLakeModel}),
		"A newer build must not reach an older node just because the pin names it")
}

func TestNodeSatisfiesCPU_ExtraNodeModels(t *testing.T) {
	t.Parallel()

	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 2, 4,
		nodemanager.WithCPUInfo("x86_64", "6", machineinfo.GraniteRapidsModel))
	build := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: machineinfo.IceLakeModel}

	assert.False(t, NodeSatisfiesCPU(node, CPURequirement{Build: build}),
		"Ice Lake build should not be accepted on a Granite Rapids node by default")
	assert.True(t, NodeSatisfiesCPU(node, CPURequirement{
		Build:           build,
		ExtraNodeModels: map[string][]string{machineinfo.IceLakeModel: {machineinfo.GraniteRapidsModel}},
	}), "Ice Lake build should be accepted on a Granite Rapids node when the extra pair is listed")
}
