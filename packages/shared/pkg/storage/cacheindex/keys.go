// Package cacheindex keeps a last-use index of the builds held in the shared
// NFS chunk cache, in Redis. Orchestrators record each build they read from or
// write to the cache; the cache cleaner evicts the least recently used builds
// from the index instead of listing the cache and sampling chunk atimes.
//
// The index is one sorted set per shard (member = build ID, score = last use in
// unix seconds), scoped by a cache ID that lives in a marker file at the cache
// root, so two caches never share an index even on one Redis.
package cacheindex

import (
	"hash/fnv"
	"strconv"
)

// NumShards spreads the index over many keys, so no single key (and no single
// cluster shard) takes every write.
const NumShards = 64

const keyPrefix = "nfsidx"

func shardOf(buildID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(buildID))

	return int(h.Sum32() % NumShards)
}

// lruKey is one shard's sorted set: entity, kind, then the hash-tagged ids. The
// hash tag ({...}) keeps it in one cluster slot, so the per-shard script is legal
// in cluster mode.
func lruKey(cacheID string, shard int) string {
	return keyPrefix + ":lru:{" + cacheID + ":" + strconv.Itoa(shard) + "}"
}

// buildKey marks a recent index build; it exists until the next one is due.
func buildKey(cacheID string) string {
	return keyPrefix + ":build:{" + cacheID + "}"
}
