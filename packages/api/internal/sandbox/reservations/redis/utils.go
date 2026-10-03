package redis

import (
	storage_redis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	reservationsKey = "reservations"
	pendingKey      = "pending"
	resultKey       = "result"
	killedKey       = "killed"
	notifySuffix    = "notify"
)

// getStorageIndexKey returns the existing storage team index key (read-only).
func getStorageIndexKey(teamID string) string {
	return storage_redis.GetSandboxStorageTeamIndexKey(teamID)
}

// getReservationPrefix returns the reservation prefix under the storage team key.
// e.g. sandbox:storage:{teamID}:reservations
func getReservationPrefix(teamID string) string {
	return redis_utils.CreateKey(storage_redis.GetTeamPrefix(teamID), reservationsKey)
}

// getPendingSetKey returns the key for the pending set of sandbox IDs being created.
// e.g. sandbox:storage:{teamID}:reservations:pending
func getPendingSetKey(teamID string) string {
	return redis_utils.CreateKey(getReservationPrefix(teamID), pendingKey)
}

// getResultKey returns the key for a sandbox creation result.
// e.g. sandbox:storage:{teamID}:reservations:sandboxID:result
func getResultKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(getReservationPrefix(teamID), sandboxID, resultKey)
}

// getReservationRoutingKey is PubSub routing key for reservation completion notifications.
// e.g. sandbox:storage:{teamID}:reservations:sandboxID:notify
func getReservationRoutingKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(getReservationPrefix(teamID), sandboxID, notifySuffix)
}

// getKillClaimKey returns the key holding a DELETE's claim over a sandbox ID,
// written by claimKillScript and checked by reserveScript.
// e.g. sandbox:storage:{teamID}:reservations:sandboxID:killed
func getKillClaimKey(teamID, sandboxID string) string {
	return redis_utils.CreateKey(getReservationPrefix(teamID), sandboxID, killedKey)
}
