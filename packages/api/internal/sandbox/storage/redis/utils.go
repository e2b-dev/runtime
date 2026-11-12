package redis

import (
	"fmt"
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
	indexKey            = "index"
	recordSuffix        = "record"
	metaSuffix          = "meta"
)

// metaFieldSep separates the packed meta fields. US (0x1f) cannot occur in a
// UUID execution ID or in a lifecycle-hold token, so a plain split is safe.
const metaFieldSep = "\x1f"

var (
	// globalStorageNotifyChannel is the single Redis PubSub channel used by
	// storage notifications. The per-event routing key is embedded in the message
	// payload so one connection per API pod is sufficient.
	globalStorageNotifyChannel = redis_utils.CreateKey(sandboxKeyPrefix, notifySuffix)

	globalTeamsSet      = redis_utils.CreateKey(sandboxKeyPrefix, "global:teams")
	globalExpirationSet = redis_utils.CreateKey(sandboxKeyPrefix, "global:expiration")

	// SchemeMarkerKey holds the oldest release that may run against the
	// store's keys; Start writes it when absent, and warns, since a release
	// older than the scheme kills sandbox-form records' sandboxes as orphans.
	SchemeMarkerKey = redis_utils.CreateKey(sandboxKeyPrefix, "scheme", "min-release")
)

// SchemeMinRelease is the release Start records under SchemeMarkerKey.
const SchemeMinRelease = "r1"

// expirationMember identifies one *execution* (incarnation) of a sandbox in
// the global expiration ZSET. Scoping the member to the execution makes every
// ZREM structurally safe: removing a dead execution's member can never
// unindex a live one, even when a resume of the same sandbox ID, admitted
// once a lock expired, races a Remove or the evictor's stale sweep.
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

// goneKeyFor names the gone set beside a record key, in the record's slot.
func goneKeyFor(recordKey string) string {
	return redis_utils.CreateKey(recordKey, "gone")
}

// metaKeyFor names the meta key beside a record key, in the record's slot. It
// holds packMeta's string — the execution ID and lifecycle hold of the record
// — so the hot path can read them with a single GET instead of decoding the
// record JSON on Redis's event loop (see packMeta and startTransitionScript).
func metaKeyFor(recordKey string) string {
	return redis_utils.CreateKey(recordKey, metaSuffix)
}

// packMeta serializes the fields the hot path needs from a record — its
// execution ID and lifecycle hold — into the meta key's value, so they can be
// read without decoding the record JSON. Layout: executionID, lifecycleHold,
// lifecycleHoldUntil (Unix ms), US-separated. Kept in sync with the record by
// every script that writes KEYS[1] (each writes the meta key in the same
// atomic script); removeSandboxScript deletes it with the record.
func packMeta(sbx sandboxtypes.Sandbox) string {
	return strings.Join([]string{
		sbx.ExecutionID,
		sbx.LifecycleHold,
		fmt.Sprintf("%d", sbx.LifecycleHoldUntil),
	}, metaFieldSep)
}

// GetSandboxStorageTeamIndexKey returns the storage team index key for external packages (e.g. reservations).
func GetSandboxStorageTeamIndexKey(teamID string) string {
	return redis_utils.CreateKey(GetTeamPrefix(teamID), indexKey)
}

// validID reports whether id can be a hash tag: an empty tag makes Redis
// hash the whole key, and a brace moves or ends the tag, so either splits a
// sandbox's keys across slots.
func validID(id string) bool {
	return id != "" && !strings.ContainsAny(id, "{}")
}

// checkID panics on an ID that cannot be a hash tag. IDs are generated by
// the server, so a bad one is a programming error; IDs read back from the
// store or reported by a node are checked by their readers before they reach
// a key builder.
func checkID(id string) {
	if !validID(id) {
		panic(fmt.Sprintf("sandbox store: invalid key ID %q", id))
	}
}

// sandboxPrefix returns the prefix of a sandbox's keys, tagged with the
// sandbox ID so they share one cluster slot.
func sandboxPrefix(sandboxID string) string {
	checkID(sandboxID)

	return redis_utils.CreateKey(sandboxKeyPrefix, redis_utils.SameSlot(sandboxID))
}

// recordKey returns the record key of a sandbox. The lock key is
// redis_utils.GetLockKey of the record key.
func recordKey(sandboxID string) string {
	return redis_utils.CreateKey(sandboxPrefix(sandboxID), recordSuffix)
}

// transitionKeyFor returns the transition marker key of a sandbox.
func transitionKeyFor(sandboxID string) string {
	return redis_utils.CreateKey(sandboxPrefix(sandboxID), transitionKeyPrefix)
}

// transitionResultKeyFor returns the result key of one transition of a
// sandbox. The transition ID follows the hash tag, so it cannot move the
// slot and is not checked; the wait reads it back from the store.
func transitionResultKeyFor(sandboxID, transitionID string) string {
	return redis_utils.CreateKey(transitionKeyFor(sandboxID), transitionID)
}

// transitionRoutingKey returns the routing key, embedded in the payload of
// messages published to globalStorageNotifyChannel, of the transition whose
// result key is given. Derived from the key, it names the transition so a
// notification can never wake the waiters of another transition of the same
// sandbox.
func transitionRoutingKey(resultKey string) string {
	return redis_utils.CreateKey(resultKey, notifySuffix)
}

func getLockRoutingKey(lockKey string) string {
	return redis_utils.CreateKey(lockKeyPrefix, lockKey, notifySuffix)
}
