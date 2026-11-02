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

// parseExpirationMember parses "teamID:sandboxID:executionID" members. The
// team and execution parts are always UUIDs, written from uuid.UUID.String,
// and sandbox IDs never contain ':'. A member whose team or execution part
// is not a UUID, or whose team or sandbox part no key can be built from,
// does not parse, so it is swept as invalid instead of reaching a key
// builder, or a reader that would skip and defer it on every pass.
func parseExpirationMember(member string) (teamID, sandboxID, executionID string, ok bool) {
	parts := strings.Split(member, ":")
	if len(parts) != 3 || !validID(parts[0]) || !validID(parts[1]) {
		return "", "", "", false
	}

	for _, part := range []string{parts[0], parts[2]} {
		if _, err := uuid.Parse(part); err != nil {
			return "", "", "", false
		}
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

// GetSandboxStorageTeamIndexKey returns the storage team index key for external packages (e.g. reservations).
func GetSandboxStorageTeamIndexKey(teamID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), indexKey)
}

func getTransitionKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), transitionKeyPrefix, sandboxID)
}

// validID reports whether id can be a hash tag: an empty tag makes Redis
// hash the whole key, and a brace moves or ends the tag, so either splits a
// sandbox's keys across slots.
func validID(id string) bool {
	return id != "" && !strings.ContainsAny(id, "{}")
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
