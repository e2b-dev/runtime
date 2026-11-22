package builds

import (
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func ToMachineInfo(build queries.EnvBuild) machineinfo.MachineInfo {
	return machineinfo.MachineInfo{
		CPUArchitecture: utils.FromPtr(build.CpuArchitecture),
		CPUFamily:       utils.FromPtr(build.CpuFamily),
		CPUModel:        utils.FromPtr(build.CpuModel),
		CPUModelName:    utils.FromPtr(build.CpuModelName),
		CPUFlags:        build.CpuFlags,
	}
}
