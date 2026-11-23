package cacheindex

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// seedChunk bounds the members per ZADD sent while seeding.
const seedChunk = 1000

// removeIfNotNewer drops a build only while its score is still at or below
// the one the caller read, so a use recorded meanwhile keeps the entry.
var removeIfNotNewer = redis.NewScript(`
local s = redis.call('ZSCORE', KEYS[1], ARGV[1])
if s and tonumber(s) <= tonumber(ARGV[2]) then
  redis.call('ZREM', KEYS[1], ARGV[1])
  return 1
end
return 0
`)

// Entry is one build and its last use.
type Entry struct {
	BuildID string
	Score   int64
}

// Index is the cleaner's view of one cache's index.
type Index struct {
	client redis.UniversalClient
	id     string
}

func Open(client redis.UniversalClient, cacheID string) *Index {
	return &Index{client: client, id: cacheID}
}

func (x *Index) ID() string { return x.id }

// Seed records last uses observed on disk (a chunk atime, or a create time).
// Like a recorded use, it only moves a score forward: an atime is a lower bound
// on the last read, so it never replaces a newer recorded use.
func (x *Index) Seed(ctx context.Context, scores map[string]int64) error {
	shards := make(map[int][]redis.Z)
	for build, ts := range scores {
		s := shardOf(build)
		shards[s] = append(shards[s], redis.Z{Score: float64(ts), Member: build})
	}

	pipe := x.client.Pipeline()
	for s, members := range shards {
		for chunk := range slices.Chunk(members, seedChunk) {
			pipe.ZAddGT(ctx, lruKey(x.id, s), chunk...)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("seed index: %w", err)
	}

	return nil
}

// Score returns a build's last use, and false when the build is not indexed.
func (x *Index) Score(ctx context.Context, buildID string) (int64, bool, error) {
	v, err := x.client.ZScore(ctx, lruKey(x.id, shardOf(buildID)), buildID).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return int64(v), true, nil
}

// RemoveIfNotNewer removes a build whose score is still at most score.
func (x *Index) RemoveIfNotNewer(ctx context.Context, buildID string, score int64) (bool, error) {
	n, err := removeIfNotNewer.Run(ctx, x.client, []string{lruKey(x.id, shardOf(buildID))}, buildID, score).Int()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

// ColdIter yields the builds scored at most a maximum, coldest first across
// all shards (a k-way merge). Each shard is read in pages from a score cursor,
// so builds removed while iterating cannot hide others, and a page always ends
// on a whole group of equal scores.
type ColdIter struct {
	x        *Index
	maxScore string
	page     int64

	started bool
	bufs    [NumShards][]Entry
	next    [NumShards]string // ZRANGE min for the shard's next page; "" = shard done
}

// ColdestIter iterates the builds scored at most maxScore, reading page entries
// per shard at a time.
func (x *Index) ColdestIter(maxScore int64, page int) *ColdIter {
	it := &ColdIter{x: x, maxScore: strconv.FormatInt(maxScore, 10), page: int64(max(page, 1))}
	for s := range NumShards {
		it.next[s] = "-inf"
	}

	return it
}

// Next returns the coldest remaining build, or false when none is left.
func (it *ColdIter) Next(ctx context.Context) (Entry, bool, error) {
	if !it.started {
		it.started = true
		for s := range NumShards {
			if err := it.refill(ctx, s); err != nil {
				return Entry{}, false, err
			}
		}
	}

	best := -1
	for s := range NumShards {
		if len(it.bufs[s]) == 0 {
			continue
		}
		if best < 0 || less(it.bufs[s][0], it.bufs[best][0]) {
			best = s
		}
	}
	if best < 0 {
		return Entry{}, false, nil
	}
	e := it.bufs[best][0]
	it.bufs[best] = it.bufs[best][1:]
	if len(it.bufs[best]) == 0 {
		if err := it.refill(ctx, best); err != nil {
			return Entry{}, false, err
		}
	}

	return e, true, nil
}

func less(a, b Entry) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}

	return a.BuildID < b.BuildID
}

// refill reads shard s's next page. A full page is extended with the rest of
// its last score's members, and the cursor then moves past that score.
func (it *ColdIter) refill(ctx context.Context, s int) error {
	if it.next[s] == "" {
		return nil
	}
	key := lruKey(it.x.id, s)
	zs, err := it.x.client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key: key, Start: it.next[s], Stop: it.maxScore, ByScore: true, Count: it.page,
	}).Result()
	if err != nil {
		return fmt.Errorf("read coldest builds, shard %d: %w", s, err)
	}
	if int64(len(zs)) < it.page {
		it.next[s] = ""
		it.bufs[s] = entries(zs)
		slices.SortFunc(it.bufs[s], cmpEntry)

		return nil
	}

	last := strconv.FormatFloat(zs[len(zs)-1].Score, 'f', -1, 64)
	ties, err := it.x.client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key: key, Start: last, Stop: last, ByScore: true,
	}).Result()
	if err != nil {
		return fmt.Errorf("read coldest builds, shard %d: %w", s, err)
	}
	seen := make(map[string]struct{}, len(zs))
	for _, z := range zs {
		if b, ok := z.Member.(string); ok {
			seen[b] = struct{}{}
		}
	}
	for _, z := range ties {
		if b, ok := z.Member.(string); ok {
			if _, dup := seen[b]; !dup {
				zs = append(zs, z)
			}
		}
	}
	it.bufs[s] = entries(zs)
	slices.SortFunc(it.bufs[s], cmpEntry)
	it.next[s] = "(" + last

	return nil
}

func entries(zs []redis.Z) []Entry {
	out := make([]Entry, 0, len(zs))
	for _, z := range zs {
		if b, ok := z.Member.(string); ok {
			out = append(out, Entry{BuildID: b, Score: int64(z.Score)})
		}
	}

	return out
}

func cmpEntry(a, b Entry) int {
	switch {
	case less(a, b):
		return -1
	case less(b, a):
		return 1
	default:
		return 0
	}
}

// addBatch bounds the members sent in one pipeline by AddMissing.
const addBatch = 50_000

// AddMissing adds the given builds as the coldest, leaving builds the index
// already holds untouched (ZADD NX). Each gets its own score near 0 (1, 2, …,
// in the given order): older than any recorded use, and with no cache-wide tie
// for ColdestIter to read in one page. It returns how many it added.
func (x *Index) AddMissing(ctx context.Context, buildIDs []string) (int64, error) {
	var added int64
	for i, batch := range slices.Collect(slices.Chunk(buildIDs, addBatch)) {
		shards := make(map[int][]redis.Z)
		for j, b := range batch {
			s := shardOf(b)
			shards[s] = append(shards[s], redis.Z{Score: float64(i*addBatch + j + 1), Member: b})
		}
		pipe := x.client.Pipeline()
		var cmds []*redis.IntCmd
		for s, members := range shards {
			for chunk := range slices.Chunk(members, seedChunk) {
				cmds = append(cmds, pipe.ZAddNX(ctx, lruKey(x.id, s), chunk...))
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return added, fmt.Errorf("add missing builds to index: %w", err)
		}
		for _, cmd := range cmds {
			added += cmd.Val()
		}
	}

	return added, nil
}

// BuildDue reports whether the index is due a build: none was marked within
// the interval last passed to MarkBuilt.
func (x *Index) BuildDue(ctx context.Context) (bool, error) {
	n, err := x.client.Exists(ctx, buildKey(x.id)).Result()
	if err != nil {
		return false, fmt.Errorf("read index build mark: %w", err)
	}

	return n == 0, nil
}

// MarkBuilt records a finished build; the next one is due after every.
func (x *Index) MarkBuilt(ctx context.Context, at time.Time, every time.Duration) error {
	if err := x.client.Set(ctx, buildKey(x.id), at.Unix(), every).Err(); err != nil {
		return fmt.Errorf("mark index build: %w", err)
	}

	return nil
}
