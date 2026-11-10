package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	typesteam "github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

const (
	defaultProxyAutoResumeTimeout = 5 * time.Minute
)

func validateAndParseTimeout(rawTimeout *int32, maxHours int64) (time.Duration, *api.APIError) {
	return validateAndParseTimeoutWithDefault(rawTimeout, maxHours, sandbox.SandboxTimeoutDefault)
}

func validateAndParseTimeoutWithDefault(rawTimeout *int32, maxHours int64, fallback time.Duration) (time.Duration, *api.APIError) {
	timeout := fallback
	if rawTimeout != nil {
		if *rawTimeout <= 0 {
			return 0, &api.APIError{
				Code:      http.StatusBadRequest,
				ClientMsg: "Timeout must be greater than 0",
			}
		}

		timeout = time.Duration(*rawTimeout) * time.Second

		if maxDuration := time.Duration(maxHours) * time.Hour; timeout > maxDuration {
			return 0, &api.APIError{
				Code:      http.StatusBadRequest,
				ClientMsg: fmt.Sprintf("Timeout cannot be greater than %d hours", maxHours),
			}
		}
	}

	return timeout, nil
}

func getTeamPlanLimit(team *typesteam.Team) time.Duration {
	if team == nil || team.Limits == nil {
		return 0
	}

	return time.Duration(team.Limits.MaxLengthHours) * time.Hour
}

func clampAutoResumeTimeout(requestedTimeout, teamPlanLimit, minAutoResumeTimeout time.Duration) time.Duration {
	timeout := requestedTimeout
	if teamPlanLimit > 0 && timeout > teamPlanLimit {
		timeout = teamPlanLimit
	}
	if timeout < minAutoResumeTimeout {
		timeout = minAutoResumeTimeout
	}

	return timeout
}

func calculateTimeoutSeconds(requestedTimeout, minAutoResumeTimeout time.Duration, team *typesteam.Team) uint64 {
	return uint64(clampAutoResumeTimeout(requestedTimeout, getTeamPlanLimit(team), minAutoResumeTimeout).Seconds())
}

func calculateAutoResumeTimeout(autoResume *dbtypes.SandboxAutoResumeConfig, minAutoResumeTimeout time.Duration, team *typesteam.Team) time.Duration {
	timeout := defaultProxyAutoResumeTimeout
	if autoResume != nil && autoResume.Timeout > 0 {
		timeout = time.Duration(autoResume.Timeout) * time.Second
	}

	return clampAutoResumeTimeout(timeout, getTeamPlanLimit(team), minAutoResumeTimeout)
}
