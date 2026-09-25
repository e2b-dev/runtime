package redis

import (
	"strings"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	sandboxKeyPrefix    = "sandbox:storage"
	transitionKeyPrefix = "transition"
	lockKeyPrefix       = "lock"
	notifySuffix        = "notify"
	eidSuffix           = "eid"
	sandboxesKey        = "sandboxes"
	indexKey            = "index"
)

var (
	// globalStorageNotifyChannel is the single Redis PubSub channel used by
	// storage notifications. The per-event routing key is embedded in the message
	// payload so one connection per API pod is sufficient.
	globalStorageNotifyChannel = redis_utils.CreateKey(sandboxKeyPrefix, notifySuffix)

	globalTeamsSet      = redis_utils.CreateKey(sandboxKeyPrefix, "global:teams")
	globalExpirationSet = redis_utils.CreateKey(sandboxKeyPrefix, "global:expiration")
)

// expirationMember identifies one *execution* (incarnation) of a sandbox in
// the global expiration ZSET. Scoping the member to the execution makes every
// ZREM structurally safe: removing a dead execution's member can never
// unindex a live one, even when a lockless Add for the same sandbox ID races
// a Remove or the evictor's stale sweep.
func expirationMember(teamID, sandboxID, executionID string) string {
	return redis_utils.CreateKey(teamID, sandboxID, executionID)
}

// sandboxExpirationMember returns the expiration index member for a stored
// sandbox.
func sandboxExpirationMember(sbx sandboxtypes.Sandbox) string {
	return expirationMember(sbx.TeamID.String(), sbx.SandboxID, sbx.ExecutionID)
}

// parseExpirationMember parses "teamID:sandboxID:executionID" members.
// Sandbox IDs never contain ':' and execution IDs are always UUIDs.
func parseExpirationMember(member string) (teamID, sandboxID, executionID string, ok bool) {
	parts := strings.Split(member, ":")
	if len(parts) != 3 {
		return "", "", "", false
	}

	if _, err := uuid.Parse(parts[2]); err != nil {
		return "", "", "", false
	}

	return parts[0], parts[1], parts[2], true
}

// GetTeamPrefix returns the storage team prefix for external packages (e.g. reservations).
func GetTeamPrefix(teamID string) string {
	return redis_utils.CreateKey(sandboxKeyPrefix, redis_utils.SameSlot(teamID))
}

func getSandboxKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), sandboxesKey, sandboxID)
}

// getSandboxExecutionIDKey returns the key that stores the sandbox execution ID.
// It shares the same {teamID} hash tag as the sandbox key, so both keys land
// on the same Redis Cluster slot and can be read/written atomically in a single
// Lua script.
func getSandboxExecutionIDKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(getSandboxKey(teamID, sandboxID), eidSuffix)
}

// GetSandboxStorageTeamIndexKey returns the storage team index key for external packages (e.g. reservations).
func GetSandboxStorageTeamIndexKey(teamID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), indexKey)
}

func getTransitionKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), transitionKeyPrefix, sandboxID)
}

func getTransitionResultKey(teamID, sandboxID, transitionID string) string {
	return redis_utils.CreateKey(getTransitionKey(teamID, sandboxID), transitionID)
}

// getTransitionRoutingKey returns the per-transition routing key embedded in the
// payload of messages published to globalStorageNotifyChannel. Including
// transitionID ensures that notifications for one transition can never wake
// waiters subscribed to a different transition for the same sandbox.
func getTransitionRoutingKey(teamID, sandboxID, transitionID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), transitionKeyPrefix, sandboxID, transitionID, notifySuffix)
}

func getLockRoutingKey(lockKey string) string {
	return redis_utils.CreateKey(lockKeyPrefix, lockKey, notifySuffix)
}
