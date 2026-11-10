package handlers

import (
	"slices"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The builds routes report three statuses, coarser than the template build
// status route's: pending and in-progress builds are both building.

var defaultBuildStatusGroups = []types.BuildStatusGroup{
	types.BuildStatusGroupFailed,
	types.BuildStatusGroupInProgress,
	types.BuildStatusGroupReady,
	types.BuildStatusGroupPending,
}

// buildStatusGroupsFilter returns the status groups to list for the requested
// statuses, every group when none is requested.
func buildStatusGroupsFilter(statuses *api.BuildStatuses) []string {
	seen := make(map[types.BuildStatusGroup]struct{})
	groups := make([]string, 0, len(defaultBuildStatusGroups))

	for _, status := range sharedUtils.DerefOrDefault(statuses, nil) {
		for _, group := range buildStatusGroups(status) {
			if _, exists := seen[group]; exists {
				continue
			}

			seen[group] = struct{}{}
			groups = append(groups, string(group))
		}
	}

	if len(groups) == 0 {
		for _, group := range defaultBuildStatusGroups {
			groups = append(groups, string(group))
		}
	}

	return groups
}

func buildStatusGroups(status api.BuildStatus) []types.BuildStatusGroup {
	switch status {
	case api.BuildStatusFailed:
		return []types.BuildStatusGroup{types.BuildStatusGroupFailed}
	case api.BuildStatusSuccess:
		return []types.BuildStatusGroup{types.BuildStatusGroupReady}
	case api.BuildStatusBuilding:
		return []types.BuildStatusGroup{
			types.BuildStatusGroupPending,
			types.BuildStatusGroupInProgress,
		}
	default:
		return nil
	}
}

func isBuildingStatusGroups(groups []string) bool {
	building := buildStatusGroups(api.BuildStatusBuilding)
	if len(groups) != len(building) {
		return false
	}

	for _, group := range building {
		if !slices.Contains(groups, string(group)) {
			return false
		}
	}

	return true
}

func buildStatusFromStatusGroup(group types.BuildStatusGroup) api.BuildStatus {
	switch group {
	case types.BuildStatusGroupFailed:
		return api.BuildStatusFailed
	case types.BuildStatusGroupReady:
		return api.BuildStatusSuccess
	default:
		return api.BuildStatusBuilding
	}
}

// buildStatusMessage returns the failure message of a failed build, nil
// otherwise.
func buildStatusMessage(status api.BuildStatus, reason types.BuildReason) *string {
	if status != api.BuildStatusFailed || reason.Message == "" {
		return nil
	}

	return &reason.Message
}
