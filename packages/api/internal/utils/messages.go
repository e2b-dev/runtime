package utils

import (
	"fmt"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

func SandboxNotFoundMsg(sandboxID string) string {
	return fmt.Sprintf("Sandbox %q doesn't exist or you don't have access to it", sandboxID)
}

func SandboxLifecycleHeldMsg(sandboxID string) string {
	return fmt.Sprintf("Sandbox %q is busy with another operation, please retry later", sandboxID)
}

func SandboxChangingStateMsg(sandboxID string, state sandbox.State) string {
	switch state {
	case sandbox.StateKilling:
		return fmt.Sprintf("Sandbox %q is being killed", sandboxID)
	case sandbox.StatePausing:
		return fmt.Sprintf("Sandbox %q is being paused", sandboxID)
	case sandbox.StateSnapshotting:
		return fmt.Sprintf("Sandbox %q is being snapshotted", sandboxID)
	default:
		return fmt.Sprintf("Sandbox %q is in %s state", sandboxID, state)
	}
}
