package team

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/constants"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func LimitResources(limits *types.TeamLimits, cpuCount, memoryMB *int32) (int64, int64, *api.APIError) {
	cpu := constants.DefaultTemplateCPU
	ramMB := constants.DefaultTemplateMemory

	if cpuCount != nil {
		cpu = int64(*cpuCount)
		if apiErr := validateCPU(limits, cpu, "CPU count"); apiErr != nil {
			return 0, 0, apiErr
		}
	}

	if memoryMB != nil {
		ramMB = int64(*memoryMB)

		if ramMB < constants.MinTemplateMemory {
			return 0, 0, &api.APIError{
				Err:       fmt.Errorf("memory must be at least %d MiB", constants.MinTemplateMemory),
				ClientMsg: fmt.Sprintf("Memory must be at least %d MiB", constants.MinTemplateMemory),
				Code:      http.StatusBadRequest,
			}
		}

		if ramMB%2 != 0 {
			return 0, 0, &api.APIError{
				Err:       errors.New("user provided memory size isn't divisible by 2"),
				ClientMsg: "Memory must be divisible by 2",
				Code:      http.StatusBadRequest,
			}
		}

		if ramMB > limits.MaxRamMb {
			return 0, 0, &api.APIError{
				Err:       fmt.Errorf("memory exceeds team limits (%d MiB)", limits.MaxRamMb),
				ClientMsg: fmt.Sprintf("Memory can't be higher than %d MiB (if you need to increase this limit, please contact support)", limits.MaxRamMb),
				Code:      http.StatusBadRequest,
			}
		}
	}

	return cpu, ramMB, nil
}

// LimitMaxCPU validates a requested max CPU count against the resolved
// cpuCount and the team's allowance.
func LimitMaxCPU(limits *types.TeamLimits, cpuCount int64, maxCpuCount int32) (int64, *api.APIError) {
	maxCPU := int64(maxCpuCount)
	if apiErr := validateCPU(limits, maxCPU, "Max CPU count"); apiErr != nil {
		return 0, apiErr
	}

	if maxCPU < cpuCount {
		return 0, &api.APIError{
			Err:       fmt.Errorf("max CPU count %d is below CPU count %d", maxCPU, cpuCount),
			ClientMsg: fmt.Sprintf("Max CPU count must be at least the CPU count (%d)", cpuCount),
			Code:      http.StatusBadRequest,
		}
	}

	return maxCPU, nil
}

func validateCPU(limits *types.TeamLimits, cpu int64, field string) *api.APIError {
	var message string
	switch {
	case cpu < constants.MinTemplateCPU:
		message = fmt.Sprintf("%s must be at least %d", field, constants.MinTemplateCPU)
	case cpu > constants.MaxTemplateCPU:
		message = fmt.Sprintf("%s must be at most %d", field, constants.MaxTemplateCPU)
	case cpu != 1 && cpu%2 != 0:
		message = field + " must be 1 or an even number"
	case cpu > limits.MaxVcpu:
		message = fmt.Sprintf("%s can't be higher than %d (if you need to increase this limit, please contact support)", field, limits.MaxVcpu)
	default:
		return nil
	}

	return &api.APIError{
		Err:       errors.New(message),
		ClientMsg: message,
		Code:      http.StatusBadRequest,
	}
}
