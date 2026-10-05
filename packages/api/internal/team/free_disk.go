package team

import (
	"fmt"
	"net/http"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// LimitFreeDiskSize resolves the free-space growth target a build asked for
// against the team's allowance.
//
// A nil request is the team's default target. An explicit 0 disables requested
// growth and is kept as 0, so nothing here may treat the value as a flag.
func LimitFreeDiskSize(limits *types.TeamLimits, requestedMB *int32) (int64, *api.APIError) {
	if requestedMB == nil {
		return limits.DefaultFreeDiskSizeMb, nil
	}

	requested := int64(*requestedMB)

	if requested < 0 {
		return 0, &api.APIError{
			Err:       fmt.Errorf("minimum free disk must not be negative, got %d MiB", requested),
			ClientMsg: "Minimum free disk can't be negative",
			Code:      http.StatusBadRequest,
		}
	}

	if requested > limits.MaxFreeDiskSizeMb {
		return 0, &api.APIError{
			Err: fmt.Errorf("minimum free disk %d MiB exceeds team limits (%d MiB)",
				requested, limits.MaxFreeDiskSizeMb),
			ClientMsg: fmt.Sprintf(
				"Minimum free disk can't be higher than %d MiB (if you need to increase this limit, please contact support)",
				limits.MaxFreeDiskSizeMb),
			Code: http.StatusBadRequest,
		}
	}

	return requested, nil
}
