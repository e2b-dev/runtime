package cacheindex

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestEnsureIDCreatesOnceUnderRace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()

	const callers = 16
	ids := make([]string, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			id, err := EnsureID(root)
			assert.NoError(t, err)
			ids[i] = id
		})
	}
	wg.Wait()

	for _, id := range ids {
		assert.Equal(t, ids[0], id)
	}
	_, err := uuid.Parse(ids[0])
	require.NoError(t, err)

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1, "temp files must be cleaned up")
	assert.Equal(t, MarkerName, entries[0].Name())
}

func TestReadIDRejectsGarbage(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, MarkerName), []byte("not-a-uuid\n"), 0o600))

	_, err := ReadID(root)
	require.Error(t, err)

	_, err = EnsureID(root)
	require.Error(t, err, "a corrupt marker must not be replaced silently")
}

func newWriter(t *testing.T, enabled bool) (*Writer, *Index, *time.Time) {
	t.Helper()
	client := redis_utils.SetupInstance(t)
	root := t.TempDir()
	id, err := EnsureID(root)
	require.NoError(t, err)

	now := time.Unix(1_700_000_000, 0)
	w := NewWriter(client, WriterConfig{
		Root:       root,
		Enabled:    func(context.Context) bool { return enabled },
		MaxPending: 3,
	})
	w.now = func() time.Time { return now }

	return w, Open(client, id), &now
}

func TestWriterFlushesLatestUse(t *testing.T) {
	t.Parallel()
	w, idx, now := newWriter(t, true)
	ctx := t.Context()

	w.Touch("a")
	*now = now.Add(time.Minute)
	w.Touch("a")
	w.Touch("b")
	w.flush(ctx)

	got, err := idx.members(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"a": now.Unix(), "b": now.Unix()}, got)

	// An older use (clock skew between nodes) never moves a score back.
	*now = now.Add(-time.Hour)
	w.Touch("a")
	w.flush(ctx)
	score, ok, err := idx.Score(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, now.Add(time.Hour).Unix(), score)
}

func TestWriterDropsNewBuildsPastCap(t *testing.T) {
	t.Parallel()
	w, idx, _ := newWriter(t, true)
	ctx := t.Context()

	for _, b := range []string{"a", "b", "c", "d"} {
		w.Touch(b)
	}
	w.Touch("a") // known builds still update when full
	w.flush(ctx)

	got, err := idx.members(ctx)
	require.NoError(t, err)
	assert.Len(t, got, 3)
	assert.NotContains(t, got, "d")
}

func TestWriterDisabledDiscards(t *testing.T) {
	t.Parallel()
	w, idx, _ := newWriter(t, false)
	ctx := t.Context()

	w.Touch("a")
	w.flush(ctx)

	got, err := idx.members(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)
	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Empty(t, w.pending, "a disabled flush must not let the pending set grow")
	assert.True(t, w.lastIDCheck.IsZero(), "with writes off the marker is never read")
}

func TestWriterWaitsForMarker(t *testing.T) {
	t.Parallel()
	client := redis_utils.SetupInstance(t)
	root := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	w := NewWriter(client, WriterConfig{Root: root})
	w.now = func() time.Time { return now }
	ctx := t.Context()

	w.Touch("a")
	w.flush(ctx) // no marker yet
	id, err := EnsureID(root)
	require.NoError(t, err)

	w.Touch("b")
	w.flush(ctx) // marker exists, but the re-check is paced
	idx := Open(client, id)
	got, err := idx.members(ctx)
	require.NoError(t, err)
	assert.Empty(t, got)

	now = now.Add(idRecheckInterval)
	w.Touch("c")
	w.flush(ctx)
	got, err = idx.members(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"c": now.Unix()}, got)
}

func TestWriterStartClose(t *testing.T) {
	t.Parallel()
	w, idx, now := newWriter(t, true)
	w.Start(t.Context())
	w.Touch("a")
	require.NoError(t, w.Close(t.Context()))

	score, ok, err := idx.Score(t.Context(), "a")
	require.NoError(t, err)
	require.True(t, ok, "Close must flush what is pending")
	assert.Equal(t, now.Unix(), score)
}

func drain(t *testing.T, it *ColdIter) []Entry {
	t.Helper()
	var out []Entry
	for {
		e, ok, err := it.Next(t.Context())
		require.NoError(t, err)
		if !ok {
			return out
		}
		out = append(out, e)
	}
}

func TestColdestIterMergesShardsInOrder(t *testing.T) {
	t.Parallel()
	client := redis_utils.SetupInstance(t)
	idx := Open(client, uuid.NewString())
	ctx := t.Context()

	seed := map[string]int64{}
	for i := range 300 {
		score := int64(1000 + i)
		if i%3 == 0 {
			score = 0 // many ties, as chunkless dirs have
		}
		seed[uuid.NewString()] = score
	}
	seed["warm"] = 5000
	require.NoError(t, idx.Seed(ctx, seed))

	got := drain(t, idx.ColdestIter(4000, 3)) // tiny pages: every shard refills and spans ties
	require.Len(t, got, 300, "every build at or below the maximum, once")
	assert.True(t, slices.IsSortedFunc(got, cmpEntry), "coldest first across shards")
	seen := map[string]struct{}{}
	for _, e := range got {
		seen[e.BuildID] = struct{}{}
	}
	assert.Len(t, seen, 300, "no build twice")
	assert.NotContains(t, seen, "warm")
}

func TestColdestIterSurvivesRemovalWhileIterating(t *testing.T) {
	t.Parallel()
	client := redis_utils.SetupInstance(t)
	idx := Open(client, uuid.NewString())
	ctx := t.Context()

	seed := map[string]int64{}
	for i := range 200 {
		seed[uuid.NewString()] = int64(i)
	}
	require.NoError(t, idx.Seed(ctx, seed))

	it := idx.ColdestIter(1000, 2)
	n := 0
	for {
		e, ok, err := it.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		n++
		removed, err := idx.RemoveIfNotNewer(ctx, e.BuildID, e.Score) // as the deleter does
		require.NoError(t, err)
		require.True(t, removed)
	}
	assert.Equal(t, 200, n, "removing what was read never hides the rest")
}

func TestKeysNameEntityThenKind(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "nfsidx:lru:{c:3}", lruKey("c", 3))
}

func TestWriterFollowsNewMarker(t *testing.T) {
	t.Parallel()
	w, oldIdx, now := newWriter(t, true)
	ctx := t.Context()
	w.Touch("a")
	w.flush(ctx)

	// The cache is recreated with a new marker.
	require.NoError(t, os.Remove(filepath.Join(w.cfg.Root, MarkerName)))
	newID, err := EnsureID(w.cfg.Root)
	require.NoError(t, err)
	require.NotEqual(t, oldIdx.ID(), newID)

	*now = now.Add(idRecheckInterval)
	w.Touch("b")
	w.flush(ctx)
	got, err := Open(w.client, newID).members(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"b": now.Unix()}, got)
}

func TestRemoveIfNotNewer(t *testing.T) {
	t.Parallel()
	client := redis_utils.SetupInstance(t)
	idx := Open(client, uuid.NewString())
	ctx := t.Context()
	require.NoError(t, idx.Seed(ctx, map[string]int64{"a": 100, "b": 100}))

	removed, err := idx.RemoveIfNotNewer(ctx, "a", 100)
	require.NoError(t, err)
	assert.True(t, removed)

	// b was used after the caller read its score.
	require.NoError(t, client.ZAddGT(ctx, lruKey(idx.ID(), shardOf("b")), redisZ("b", 900)).Err())
	removed, err = idx.RemoveIfNotNewer(ctx, "b", 100)
	require.NoError(t, err)
	assert.False(t, removed)

	removed, err = idx.RemoveIfNotNewer(ctx, "missing", 100)
	require.NoError(t, err)
	assert.False(t, removed)
}

func redisZ(member string, score float64) redis.Z {
	return redis.Z{Score: score, Member: member}
}

// Scores are float64 in Redis; whole unix seconds are exact up to 2^53.
func TestScoresRoundTripExactly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	idx := Open(redis_utils.SetupInstance(t), uuid.NewString())

	want := map[string]int64{
		uuid.NewString(): 1,               // smallest real score
		uuid.NewString(): 1_793_977_536,   // a 2026 timestamp
		uuid.NewString(): 1_793_977_537,   // one second later stays distinct
		uuid.NewString(): 253_402_300_799, // 9999-12-31T23:59:59Z
		uuid.NewString(): 1<<53 - 1,       // largest exact float64 integer, minus one
		uuid.NewString(): 1 << 53,         // largest exact float64 integer
	}
	require.NoError(t, idx.Seed(ctx, want))

	for b, s := range want {
		got, ok, err := idx.Score(ctx, b)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, s, got, "Score")
	}

	iterated := map[string]int64{}
	it := idx.ColdestIter(1<<53, 2) // a small page forces the tie/cursor path
	for {
		e, ok, err := it.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		iterated[e.BuildID] = e.Score
	}
	assert.Equal(t, want, iterated, "ColdestIter")
}

// A batch whose write fails is kept and lands with the next flush.
func TestWriterRetriesAFailedBatch(t *testing.T) {
	t.Parallel()
	w, idx, now := newWriter(t, true)
	ctx := t.Context()
	bad := lruKey(idx.ID(), shardOf("a"))
	require.NoError(t, w.client.Set(ctx, bad, "not a sorted set", 0).Err())

	used := now.Unix()
	w.Touch("a")
	w.flush(ctx) // fails: the use is kept, not dropped

	require.NoError(t, w.client.Del(ctx, bad).Err())
	*now = now.Add(time.Minute)
	w.flush(ctx)

	score, ok, err := idx.Score(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, used, score)
}

func TestRequeueKeepsTheNewerUseAndTheCap(t *testing.T) {
	t.Parallel()
	w, _, _ := newWriter(t, true) // MaxPending 3
	w.pending = map[string]int64{"a": 200, "b": 50}

	w.requeue(t.Context(), map[string]int64{"a": 100, "b": 60, "c": 1, "d": 1})

	assert.Equal(t, int64(200), w.pending["a"], "a newer pending use wins")
	assert.Equal(t, int64(60), w.pending["b"], "a newer failed use wins")
	assert.Len(t, w.pending, 3, "the pending cap still holds")
}

func TestWriterCloseReturnsWhenRedisStalls(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() }) // accepts, never answers
		}
	}()
	client := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), ReadTimeout: 5 * time.Second, MaxRetries: -1})
	t.Cleanup(func() { client.Close() })
	root := t.TempDir()
	_, err = EnsureID(root)
	require.NoError(t, err)
	w := NewWriter(client, WriterConfig{Root: root})
	w.Start(t.Context())
	w.Touch("a")

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.ErrorIs(t, w.Close(ctx), context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 4*time.Second) // under the 5 s Redis read timeout it must not wait for
}

func TestWriterCloseOnAForcedStopReturnsNil(t *testing.T) {
	t.Parallel()
	w, _, _ := newWriter(t, true)
	w.Start(t.Context())
	w.Touch("a")
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // the orchestrator's FORCE_STOP cancels the close context first

	assert.NoError(t, w.Close(ctx), "a forced stop is not a failed shutdown")
}

func TestAddMissingKeepsRecordedScores(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	idx := Open(redis_utils.SetupInstance(t), uuid.NewString())
	require.NoError(t, idx.Seed(ctx, map[string]int64{"used": 500}))

	added, err := idx.AddMissing(ctx, []string{"used", "old-1", "old-2"})

	require.NoError(t, err)
	assert.Equal(t, int64(2), added)
	got, err := idx.members(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"used": 500, "old-1": 2, "old-2": 3}, got, "own scores near 0, in the given order")
}

// A large build adds no cache-wide tie, so the first read stays one page per shard.
func TestAddMissingKeepsColdestIterPaged(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	idx := Open(redis_utils.SetupInstance(t), uuid.NewString())
	ids := make([]string, 3000)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	_, err := idx.AddMissing(ctx, ids)
	require.NoError(t, err)

	it := idx.ColdestIter(1<<53, 10)
	_, ok, err := it.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	buffered := 0
	for _, b := range it.bufs {
		buffered += len(b)
	}
	assert.LessOrEqual(t, buffered, NumShards*10)
}

func TestBuildDueUntilMarked(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	idx := Open(redis_utils.SetupInstance(t), uuid.NewString())

	due, err := idx.BuildDue(ctx)
	require.NoError(t, err)
	assert.True(t, due)

	require.NoError(t, idx.MarkBuilt(ctx, time.Now(), time.Hour))
	due, err = idx.BuildDue(ctx)
	require.NoError(t, err)
	assert.False(t, due)
	ttl, err := idx.client.TTL(ctx, buildKey(idx.ID())).Result()
	require.NoError(t, err)
	assert.InDelta(t, time.Hour.Seconds(), ttl.Seconds(), 60, "due again after the interval")
}

// members reads the whole index (tests only).
func (x *Index) members(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for s := range NumShards {
		zs, err := x.client.ZRangeWithScores(ctx, lruKey(x.id, s), 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, z := range zs {
			out[z.Member.(string)] = int64(z.Score)
		}
	}

	return out, nil
}
