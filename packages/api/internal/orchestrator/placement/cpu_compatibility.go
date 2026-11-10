package placement

import (
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
)

// CPURequirement is the CPU constraint a sandbox puts on a candidate node.
type CPURequirement struct {
	// Build is the CPU the sandbox's build ran on. The zero value constrains
	// nothing: builds that predate CPU recording carry no machine info.
	Build machineinfo.MachineInfo

	// PinnedModel, when set, additionally holds the sandbox to nodes reporting
	// this CPU model. It narrows Build's rule rather than replacing it, so a
	// build the pinned model cannot run has no candidates at all.
	PinnedModel string

	// ExtraNodeModels adds one-way build model to node models pairs to the
	// hardcoded rule.
	ExtraNodeModels map[string][]string
}

// NodeSatisfiesCPU reports whether node meets req. A build with no recorded CPU
// accepts any node the pin allows; a node with no reported CPU satisfies only
// such a build.
func NodeSatisfiesCPU(node *nodemanager.Node, req CPURequirement) bool {
	nodeMachineInfo := node.MachineInfo()

	if req.PinnedModel != "" && nodeMachineInfo.CPUModel != req.PinnedModel {
		return false
	}

	if req.Build.CPUArchitecture == "" {
		return true
	}

	return req.Build.IsCompatibleWithExtra(nodeMachineInfo, req.ExtraNodeModels)
}
