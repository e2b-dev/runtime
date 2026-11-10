package machineinfo

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"go.uber.org/zap"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type MachineInfo struct {
	CPUArchitecture string   `json:"cpu_architecture"`
	CPUFamily       string   `json:"cpu_family"`
	CPUModel        string   `json:"cpu_model"`
	CPUModelName    string   `json:"cpu_model_name"`
	CPUFlags        []string `json:"cpu_flags"`
}

const (
	IceLakeModel       = "106"
	EmeraldRapidsModel = "207"
	GraniteRapidsModel = "173"
)

// compatibleNodeModels maps a build's CPU model (same architecture and family)
// to the set of newer node CPU models that a paused sandbox can be resumed on.
// Compatibility across generations is asymmetric.
//
// The key is the build model; the value is the set of node models that build may
// run on. The same model is always compatible and need not be listed.
var compatibleNodeModels = map[string]map[string]struct{}{
	// Intel: an n2 (Ice Lake, model 106) build may run on an n4
	// (Emerald Rapids, model 207) node, but not the reverse.
	IceLakeModel: {EmeraldRapidsModel: {}},
}

// IsCompatibleWith reports whether a sandbox whose build ran on this CPU (the
// receiver is the build CPU) can run on a node with nodeCPU. The same
// architecture+family+model is always compatible. Cross-model compatibility is
// asymmetric and restricted to the hardcoded compatibleNodeModels: a build on an
// older model can run on the newer node models listed for it, but not the other
// way around (e.g. an n2 build may restore on an n4 node, but an n4 build may not
// restore on an n2 node).
func (m MachineInfo) IsCompatibleWith(nodeCPU MachineInfo) bool {
	buildCPU := m

	if buildCPU.CPUArchitecture != nodeCPU.CPUArchitecture || buildCPU.CPUFamily != nodeCPU.CPUFamily {
		return false
	}

	if buildCPU.CPUModel == nodeCPU.CPUModel {
		return true
	}

	_, ok := compatibleNodeModels[buildCPU.CPUModel][nodeCPU.CPUModel]

	return ok
}

// IsCompatibleWithExtra is IsCompatibleWith plus the one-way build model to
// node models pairs in extra. Architecture and family must still match.
func (m MachineInfo) IsCompatibleWithExtra(nodeCPU MachineInfo, extra map[string][]string) bool {
	if m.IsCompatibleWith(nodeCPU) {
		return true
	}

	if m.CPUArchitecture != nodeCPU.CPUArchitecture || m.CPUFamily != nodeCPU.CPUFamily {
		return false
	}

	return slices.Contains(extra[m.CPUModel], nodeCPU.CPUModel)
}

// IsExactMatch reports whether m and other describe the same CPU
// (architecture, family, and model). Unlike IsCompatibleWith it does not allow
// cross-generation compatibility; it requires an identical CPU model.
func (m MachineInfo) IsExactMatch(other MachineInfo) bool {
	return m.CPUArchitecture == other.CPUArchitecture &&
		m.CPUFamily == other.CPUFamily &&
		m.CPUModel == other.CPUModel
}

func FromGRPCInfo(info *infogrpc.MachineInfo) MachineInfo {
	if info == nil {
		return MachineInfo{}
	}

	return MachineInfo{
		CPUArchitecture: info.GetCpuArchitecture(),
		CPUFamily:       info.GetCpuFamily(),
		CPUModel:        info.GetCpuModel(),
		CPUModelName:    info.GetCpuModelName(),
		CPUFlags:        info.GetCpuFlags(),
	}
}

func FromLDValue(ctx context.Context, value ldvalue.Value) MachineInfo {
	var info MachineInfo
	err := json.Unmarshal([]byte(value.JSONString()), &info)
	if err != nil {
		logger.L().Error(ctx, "failed to unmarshal machine info", zap.Error(err))

		return MachineInfo{}
	}

	return info
}
