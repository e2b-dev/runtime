package cleaner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/cacheindex"
)

const chunk = 4 << 20

// writeBuild writes a build of n 4 MiB chunks, all with the given atime.
func writeBuild(t *testing.T, root string, n int, atime time.Time) string {
	t.Helper()
	b := uuid.NewString()
	for i := range n {
		writeChunk(t, root, b, storage.MemfileName, i, chunk, atime)
	}

	return b
}

// testRedis returns a client on the shared test Redis server. It skips when none
// is configured: the public mirror's orchestrator shard runs without one.
func testRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	if os.Getenv(redis_utils.EnvTestDatabaseURL) == "" && !redis_utils.ClusterLane() {
		t.Skipf("needs a Redis server: set %s", redis_utils.EnvTestDatabaseURL)
	}

	return redis_utils.SetupInstance(t)
}

func newIndex(t *testing.T) *cacheindex.Index {
	t.Helper()

	return cacheindex.Open(testRedis(t), uuid.NewString())
}

func oldFolders(string) time.Duration { return 365 * 24 * time.Hour }

func indexCleaner(root string, idx *cacheindex.Index, mutate func(o *Options)) *Cleaner {
	c := newTestCleaner(root, mutate)
	c.SetIndex(idx)

	return c
}

// seededIndex is an index holding scores.
func seededIndex(t *testing.T, scores map[string]int64) *cacheindex.Index {
	t.Helper()

	return seededIndexOn(t, testRedis(t), scores)
}

// builtIndex is a seeded index whose weekly build already ran, so a run goes
// straight to the index pass.
func builtIndex(t *testing.T, scores map[string]int64) *cacheindex.Index {
	t.Helper()
	idx := seededIndex(t, scores)
	require.NoError(t, idx.MarkBuilt(t.Context(), time.Now(), time.Hour))

	return idx
}

func seededIndexOn(t *testing.T, client redis.UniversalClient, scores map[string]int64) *cacheindex.Index {
	t.Helper()
	idx := cacheindex.Open(client, uuid.NewString())
	require.NoError(t, idx.Seed(t.Context(), scores))

	return idx
}

// entryOf is b as the index walk proposes it, with its current score.
func entryOf(t *testing.T, idx *cacheindex.Index, b string) cacheindex.Entry {
	t.Helper()
	score, _, err := idx.Score(t.Context(), b)
	require.NoError(t, err)

	return cacheindex.Entry{BuildID: b, Score: score}
}

// indexMembers reads every entry of idx.
func indexMembers(t *testing.T, idx *cacheindex.Index) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	it := idx.ColdestIter(1<<53, 1000)
	for {
		e, ok, err := it.Next(t.Context())
		require.NoError(t, err)
		if !ok {
			return out
		}
		out[e.BuildID] = e.Score
	}
}

func TestIndexModeDeletesColdestFromIndex(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx := t.Context()
	// Every chunk shows the same old atime, so sampling sees five equally
	// cold builds. The index knows better.
	now := time.Now()
	read := now.Add(-40 * 24 * time.Hour)
	a := writeBuild(t, root, 4, read)
	b := writeBuild(t, root, 4, read)
	spare := writeBuild(t, root, 4, read)
	warm := writeBuild(t, root, 4, read)
	fresh := writeBuild(t, root, 4, read)
	idx := seededIndex(t, map[string]int64{
		a:     now.Add(-10 * 24 * time.Hour).Unix(),
		b:     now.Add(-5 * 24 * time.Hour).Unix(),
		spare: now.Add(-3 * 24 * time.Hour).Unix(), // cold, but the target is met first
		warm:  now.Add(-time.Hour).Unix(),
		fresh: now.Unix(),
	})

	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.Grace = GracePeriod
		o.TargetBytesToDelete = 20 << 20 // more than one build, less than two
		o.MaxConcurrentDelete = 1        // deterministic: exactly what the target needs
	})
	c.folderAge = oldFolders
	require.NoError(t, c.Clean(ctx))

	require.NoDirExists(t, filepath.Join(root, a))
	require.NoDirExists(t, filepath.Join(root, b))
	require.DirExists(t, filepath.Join(root, spare))
	require.DirExists(t, filepath.Join(root, warm))
	require.DirExists(t, filepath.Join(root, fresh))
	assert.Equal(t, int64(0), c.BuildsScanned.Load(), "index mode neither lists nor scans")
	assert.Equal(t, uint64(32<<20+2*otherFilesBytesEstimate), c.BytesFreed.Load(), "sized from chunk filenames")

	got := indexMembers(t, idx)
	assert.ElementsMatch(t, []string{spare, fresh, warm}, sortedKeys(got))
}

func TestIndexModeStopsAtGraceFloor(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx := t.Context()
	now := time.Now()
	read := now.Add(-40 * 24 * time.Hour)
	cold := writeBuild(t, root, 1, read)
	warm := writeBuild(t, root, 1, read)
	idx := seededIndex(t, map[string]int64{
		cold: now.Add(-10 * 24 * time.Hour).Unix(),
		warm: now.Add(-time.Hour).Unix(),
	})

	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.Grace = GracePeriod
	})
	c.folderAge = oldFolders
	assert.Equal(t, stopExhausted, c.cleanFromIndex(ctx, time.Time{}), "the warm build is never proposed")

	require.NoDirExists(t, filepath.Join(root, cold))
	require.DirExists(t, filepath.Join(root, warm))
}

func TestIndexModeReportsExhausted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := writeBuild(t, root, 1, time.Now())
	idx := seededIndex(t, map[string]int64{b: 1})

	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	assert.Equal(t, stopExhausted, c.cleanFromIndex(t.Context(), time.Time{}), "nothing warmer held back")
	require.NoDirExists(t, filepath.Join(root, b))
}

func TestIndexDeleteRejectsNonBuildNames(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	root := filepath.Join(parent, "cache")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub", "dir"), 0o755))
	sibling := filepath.Join(parent, "sibling")
	require.NoError(t, os.Mkdir(sibling, 0o755))

	names := []string{"", ".", "..", "../sibling", "sub/dir", "sub"}
	scores := map[string]int64{}
	for _, n := range names {
		scores[n] = 0
	}
	idx := seededIndex(t, scores)
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	for _, n := range names {
		c.indexDelete(t.Context(), cacheindex.Entry{BuildID: n}, time.Now().Unix())
	}

	require.DirExists(t, filepath.Join(root, "sub", "dir"))
	require.DirExists(t, sibling)
	assert.Equal(t, int64(0), c.Deleted.Load())
}

func TestIndexDeleteSkipsBuildUsedSincePicked(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx := t.Context()
	now := time.Now()
	b := writeBuild(t, root, 1, now)
	idx := seededIndex(t, map[string]int64{b: now.Unix()}) // used after it was queued

	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	c.indexDelete(ctx, entryOf(t, idx, b), now.Add(-GracePeriod).Unix())

	require.DirExists(t, filepath.Join(root, b))
	assert.Equal(t, int64(0), c.Deleted.Load())
}

func TestValidateIndexMode(t *testing.T) {
	t.Parallel()
	o := newTestCleaner("/x", func(o *Options) { o.IndexMode = "always" }).Options
	require.Error(t, o.Validate())
	o.IndexMode = "shadow" // removed: the index only proposes candidates now
	require.Error(t, o.Validate())
	for _, m := range []string{"", IndexOff, IndexOn} {
		o.IndexMode = m
		require.NoError(t, o.Validate(), m)
	}
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}

// A candidate is older than grace, so a younger folder means its entry outlived
// a failed cleanup and the build was recreated: keep it and re-seed it.
func TestIndexDeleteKeepsARecreatedFolder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	b := writeBuild(t, root, 1, old) // the folder itself is created now
	idx := newIndex(t)
	require.NoError(t, idx.Seed(ctx, map[string]int64{b: old.Unix()}))
	c := indexCleaner(root, idx, func(o *Options) { o.Grace = GracePeriod })

	c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix())

	require.DirExists(t, filepath.Join(root, b))
	score, ok, err := idx.Score(ctx, b)
	require.NoError(t, err)
	require.True(t, ok)
	assert.InDelta(t, time.Now().Unix(), score, 60)
}

func TestIndexDeleteRecreatedFolderInDryRunWritesNothing(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	b := writeBuild(t, root, 1, old)
	idx := newIndex(t)
	require.NoError(t, idx.Seed(ctx, map[string]int64{b: old.Unix()}))
	c := indexCleaner(root, idx, func(o *Options) {
		o.Grace = GracePeriod
		o.IndexMode = IndexOn
		o.DryRun = true
	})

	c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix())

	require.DirExists(t, filepath.Join(root, b))
	score, _, err := idx.Score(ctx, b)
	require.NoError(t, err)
	assert.Equal(t, old.Unix(), score)
}

func TestIndexDeleteOfAMissingFolderOnlyDropsTheEntry(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	idx := newIndex(t)
	b := uuid.NewString()
	old := time.Now().Add(-30 * 24 * time.Hour).Unix()
	require.NoError(t, idx.Seed(ctx, map[string]int64{b: old}))
	c := indexCleaner(t.TempDir(), idx, func(o *Options) {
		o.Grace = GracePeriod
		o.IndexMode = IndexOn
	})

	c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix())

	_, ok, err := idx.Score(ctx, b)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, int64(0), c.Deleted.Load())
}

func TestIndexDeleteOfAMissingFolderReportsARedisFailure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := testRedis(t)
	hook := &failCommand{name: "evalsha"} // the remove-if-not-newer script
	client.AddHook(hook)
	b := uuid.NewString()
	idx := seededIndexOn(t, client, map[string]int64{b: time.Now().Add(-30 * 24 * time.Hour).Unix()})
	c := indexCleaner(t.TempDir(), idx, func(o *Options) {
		o.Grace = GracePeriod
		o.IndexMode = IndexOn
	})
	hook.on.Store(true)

	assert.False(t, c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix()), "a failed Redis call must stop index deletion")
	assert.Positive(t, hook.fails.Load())
}

func TestIndexDeleteChecksAsManyChunksAsTheSamplingScan(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	b := writeBuild(t, root, 100, time.Now().Add(-30*24*time.Hour))
	c := indexCleaner(root, newIndex(t), func(o *Options) {
		o.SampleMinFiles = 8
		o.SamplePercent = 50
		o.SampleMaxFiles = 64
	})

	before := c.StatxC.Load()
	_, warmest := c.sizeAndWarmest(ctx, filepath.Join(root, b), time.Now().Add(-GracePeriod).Unix())
	recent := warmest > time.Now().Add(-GracePeriod).Unix()

	assert.False(t, recent)
	assert.Equal(t, int64(50), c.StatxC.Load()-before, "50 % of 100 chunks, as the sampling scan reads")
}

func TestIndexDeleteSeesAUseRecordedWhileInspectingTheFolder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	b := writeBuild(t, root, 1, old)
	idx := newIndex(t)
	require.NoError(t, idx.Seed(ctx, map[string]int64{b: old.Unix()}))
	c := indexCleaner(root, idx, func(o *Options) {
		o.Grace = GracePeriod
		o.IndexMode = IndexOn
	})
	c.folderAge = func(string) time.Duration { // a sandbox resumes the build mid-inspection
		require.NoError(t, idx.Seed(ctx, map[string]int64{b: time.Now().Unix()}))

		return oldFolders("")
	}

	c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix())

	require.DirExists(t, filepath.Join(root, b))
	assert.Equal(t, int64(0), c.Deleted.Load())
}

// failCommand fails one Redis command (e.g. zrange, zscore) once on: the health
// check (ZCARD, HGETALL) passes and index deletion then loses Valkey mid-run.
type failCommand struct {
	name  string
	on    atomic.Bool
	fails atomic.Int64
}

func (*failCommand) DialHook(next redis.DialHook) redis.DialHook { return next }

func (f *failCommand) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if f.on.Load() && strings.EqualFold(cmd.Name(), f.name) {
			f.fails.Add(1)

			return errors.New("injected " + f.name + " failure")
		}

		return next(ctx, cmd)
	}
}

func (*failCommand) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestIndexModeSamplesWhenValkeyFailsMidRun(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	scores := map[string]int64{}
	for range 3 {
		scores[writeBuild(t, root, 1, old)] = old.Unix()
	}
	client := testRedis(t)
	hook := &failCommand{name: "zrange"}
	client.AddHook(hook)
	idx := seededIndexOn(t, client, scores)
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	hook.on.Store(true)

	require.NoError(t, c.Clean(ctx))

	assert.Equal(t, stopIndexError, c.cleanFromIndex(ctx, time.Time{}))
	assert.Positive(t, c.BuildsScanned.Load(), "the run fell back to sampling")
	assert.GreaterOrEqual(t, c.Deleted.Load(), int64(2), "sampling deleted cold builds")
}

func TestIndexFailureLeavesUnderTargetToTheFallback(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	scores := map[string]int64{}
	for range 3 {
		scores[writeBuild(t, root, 1, old)] = old.Unix()
	}
	client := testRedis(t)
	hook := &failCommand{name: "zrange"}
	client.AddHook(hook)
	idx := seededIndexOn(t, client, scores)
	reader := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/cmd/clean-nfs-cache/cleaner"))
	require.NoError(t, err)
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.TargetBytesToDelete = 1 // one sampled delete meets it
	})
	c.metrics = m
	hook.on.Store(true)

	require.NoError(t, c.Clean(ctx))

	require.Positive(t, c.Deleted.Load(), "the sampling fallback met the target")
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == "nfsclean.run.under_target" {
				for _, dp := range mt.Data.(metricdata.Sum[int64]).DataPoints {
					assert.Zero(t, dp.Value, "a run whose fallback met the target is not under target")
				}
			}
		}
	}
}

func TestMidRunFallbackSamplesOnlyWhatIsLeft(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	scores := map[string]int64{}
	var one string
	for range 3 {
		one = writeBuild(t, root, 1, old)
		scores[one] = old.Unix()
	}
	client := testRedis(t)
	hook := &failCommand{name: "zrange"}
	client.AddHook(hook)
	idx := seededIndexOn(t, client, scores)
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.MaxConcurrentDelete = 1
	})
	size := c.sizeChunks(ctx, filepath.Join(root, one))
	c.TargetBytesToDelete = 3 * size
	c.BytesFreed.Store(2 * size) // freed from the index before Valkey failed
	hook.on.Store(true)

	require.NoError(t, c.Clean(ctx))

	assert.Equal(t, int64(1), c.Deleted.Load())
}

func TestIndexModeSamplesWhenScoreReadsFail(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	scores := map[string]int64{}
	for range 3 {
		scores[writeBuild(t, root, 1, old)] = old.Unix()
	}
	client := testRedis(t)
	hook := &failCommand{name: "zscore"}
	client.AddHook(hook)
	idx := seededIndexOn(t, client, scores)
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	hook.on.Store(true)

	require.NoError(t, c.Clean(ctx))

	assert.Positive(t, c.BuildsScanned.Load(), "the run fell back to sampling")
	assert.GreaterOrEqual(t, c.Deleted.Load(), int64(2))
}

// A writer that cannot reach Valkey leaves a stale score, but its reads still
// show in the chunks' access times: the build is kept and re-seeded.
func TestIndexDeleteKeepsABuildWithARecentChunkRead(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	b := writeBuild(t, root, 1, time.Now()) // read just now
	idx := newIndex(t)
	old := time.Now().Add(-30 * 24 * time.Hour).Unix()
	require.NoError(t, idx.Seed(ctx, map[string]int64{b: old}))
	c := indexCleaner(root, idx, func(o *Options) {
		o.Grace = GracePeriod
		o.IndexMode = IndexOn
	})
	c.folderAge = oldFolders

	require.True(t, c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix()))

	require.DirExists(t, filepath.Join(root, b))
	score, _, err := idx.Score(ctx, b)
	require.NoError(t, err)
	assert.Greater(t, score, old)
}

func TestIndexModeStopsQueueingAfterAReadFailure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	scores := map[string]int64{}
	for range 20 {
		scores[writeBuild(t, root, 1, old)] = old.Unix()
	}
	client := testRedis(t)
	hook := &failCommand{name: "zscore"}
	client.AddHook(hook)
	idx := seededIndexOn(t, client, scores)
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.MaxConcurrentDelete = 1
	})
	hook.on.Store(true)

	assert.Equal(t, stopIndexError, c.cleanFromIndex(ctx, time.Time{}))
	assert.LessOrEqual(t, hook.fails.Load(), int64(2), "no new deletes are queued once a read fails")
}

func TestIndexRunsOutThenSamplingFreesTheRest(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	known := writeBuild(t, root, 1, old)
	unknown1 := writeBuild(t, root, 1, old) // cached before the writers started
	unknown2 := writeBuild(t, root, 1, old)
	idx := seededIndex(t, map[string]int64{known: old.Unix()})
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.MaxConcurrentDelete = 1
	})
	c.TargetBytesToDelete = 3 * c.sizeChunks(ctx, filepath.Join(root, known))

	require.NoError(t, c.Clean(ctx))

	for _, b := range []string{known, unknown1, unknown2} {
		require.NoDirExists(t, filepath.Join(root, b))
	}
	assert.Positive(t, c.BuildsScanned.Load(), "the builds the index never saw leave through sampling")
	assert.Empty(t, indexMembers(t, idx), "the deleted build left the index")
}

func TestIndexModeDryRunLeavesTheIndexAlone(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	cold := writeBuild(t, root, 1, old)
	read := writeBuild(t, root, 1, time.Now())                                                   // a recent chunk read the index never heard of
	scores := map[string]int64{cold: old.Unix(), read: old.Unix(), uuid.NewString(): old.Unix()} // the last one has no folder
	idx := seededIndex(t, scores)
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.Grace = GracePeriod
		o.DryRun = true
	})
	c.folderAge = oldFolders

	c.cleanFromIndex(ctx, time.Time{})

	require.DirExists(t, filepath.Join(root, cold))
	assert.Equal(t, scores, indexMembers(t, idx), "no score raised, no entry dropped")
}

func TestIndexModeWithAByteBudgetSamples(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	b := writeBuild(t, root, 1, old)
	idx := seededIndex(t, map[string]int64{b: old.Unix()})
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.TargetBytesToDelete = 0
		o.MaxCacheBytes = 1 // sizing the cache needs a scan
	})

	require.NoError(t, c.Clean(ctx))

	assert.Positive(t, c.BuildsScanned.Load())
	assert.Equal(t, map[string]int64{b: old.Unix()}, indexMembers(t, idx), "the index walk never ran")
}

func TestIndexPassRemembersTheBuildsItKeptWarm(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	b := writeBuild(t, root, 1, time.Now()) // stale score, but read recently on disk
	idx := seededIndex(t, map[string]int64{b: old.Unix()})
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })

	require.True(t, c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix()))

	_, kept := c.handled.Load(b)
	assert.True(t, kept)
}

func TestSamplingAfterTheIndexSkipsBuildsTheIndexHandled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	kept := writeBuild(t, root, 1, old) // e.g. kept warm by this run's index pass
	other := writeBuild(t, root, 1, old)
	c := indexCleaner(root, seededIndex(t, map[string]int64{}), func(o *Options) {
		o.IndexMode = IndexOn
		o.TargetBytesToDelete = 1 << 40 // the empty index runs out at once; sampling takes everything else
	})
	c.handled.Store(kept, struct{}{})

	require.NoError(t, c.Clean(ctx))

	require.DirExists(t, filepath.Join(root, kept), "sampling must not undo the index pass's decision")
	require.NoDirExists(t, filepath.Join(root, other))
}

func TestDryRunSamplingAfterTheIndexDoesNotCountABuildTwice(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	b := writeBuild(t, root, 1, old)
	idx := seededIndex(t, map[string]int64{b: old.Unix()})
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.DryRun = true
		o.MaxConcurrentDelete = 1
	})
	size := c.sizeChunks(ctx, filepath.Join(root, b))
	c.TargetBytesToDelete = 2 * size

	require.NoError(t, c.Clean(ctx))

	assert.Equal(t, int64(1), c.Deleted.Load(), "one build, selected once")
	assert.Equal(t, size, c.BytesFreed.Load())
}

func TestNoSamplingAfterTheIndexWhenTheBudgetIsSpent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	b := writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour)) // unknown to the index
	c := indexCleaner(root, seededIndex(t, map[string]int64{}), func(o *Options) {
		o.IndexMode = IndexOn
		o.RunBudget = time.Nanosecond // spent before the empty index pass returns
	})

	require.NoError(t, c.Clean(ctx))

	assert.Zero(t, c.BuildsScanned.Load(), "no listing or scan past the budget")
	require.DirExists(t, filepath.Join(root, b))
}

func indexAddedCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == "nfsclean.index.added" {
				for _, dp := range mt.Data.(metricdata.Sum[int64]).DataPoints {
					n += dp.Value
				}
			}
		}
	}

	return n
}

// Builds cached before the writers started get an index entry from the build
// and leave through the index pass, with no sampling.
func TestIndexBuildLetsTheIndexDeleteBuildsItNeverSaw(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	unknown := writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour))
	require.NoError(t, os.Mkdir(filepath.Join(root, "lost+found"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, uuid.NewString()), nil, 0o600))
	idx := seededIndex(t, map[string]int64{})
	reader := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter(meterName))
	require.NoError(t, err)
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	c.metrics = m
	c.TargetBytesToDelete = c.sizeChunks(ctx, filepath.Join(root, unknown))
	c.buildIndexIfDue(ctx, time.Time{}) // the previous run's build

	require.NoError(t, c.Clean(ctx))

	require.NoDirExists(t, filepath.Join(root, unknown))
	assert.Zero(t, c.BuildsScanned.Load(), "the index pass deleted it; no sampling")
	assert.Equal(t, int64(1), indexAddedCount(t, reader), "only directories named by a build ID are added")
	due, err := idx.BuildDue(ctx)
	require.NoError(t, err)
	assert.False(t, due, "the next build waits for the interval")
}

func TestIndexBuildWaitsForTheNextInterval(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour)) // arrived after the last build
	idx := builtIndex(t, map[string]int64{})
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })

	c.buildIndexIfDue(t.Context(), time.Time{})

	assert.Empty(t, indexMembers(t, idx))
}

// The build follows the deletes, so its listing never takes their budget, and
// the builds it adds are candidates from the next run on.
func TestIndexBuildRunsAfterTheDeletes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	a := writeBuild(t, root, 1, old)
	b := writeBuild(t, root, 1, old)
	idx := seededIndex(t, map[string]int64{})
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.MaxConcurrentDelete = 1
	})
	c.TargetBytesToDelete = c.sizeChunks(ctx, filepath.Join(root, a)) // one of the two

	require.NoError(t, c.Clean(ctx))

	assert.Positive(t, c.BuildsScanned.Load(), "this run's empty index handed the target to sampling")
	left := a
	if _, err := os.Stat(filepath.Join(root, a)); errors.Is(err, os.ErrNotExist) {
		left = b
	}
	assert.Equal(t, []string{left}, sortedKeys(indexMembers(t, idx)), "the survivor is indexed for the next run")
}

// The run's completion record (log line and run duration) includes the build.
func TestIndexBuildRunsBeforeTheRunIsRecordedComplete(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour))
	core, logs := observer.New(zap.InfoLevel)
	c := indexCleaner(root, seededIndex(t, map[string]int64{}), func(o *Options) { o.IndexMode = IndexOn })
	c.Logger = logger.NewTracedLoggerFromCore(core)

	require.NoError(t, c.Clean(ctx))

	var order []string
	for _, e := range logs.All() {
		if e.Message == "cache index build" || e.Message == "clean complete" {
			order = append(order, e.Message)
		}
	}
	assert.Equal(t, []string{"cache index build", "clean complete"}, order)
}

func TestIndexBuildWaitsWhenTheBudgetIsSpent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour))
	idx := seededIndex(t, map[string]int64{})
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })

	c.buildIndexIfDue(ctx, time.Now().Add(-time.Second))

	assert.Empty(t, indexMembers(t, idx))
	due, err := idx.BuildDue(ctx)
	require.NoError(t, err)
	assert.True(t, due, "the next run builds instead")
}

// A build added at a near-0 score reports its sampled chunk atime as the
// deleted build's age, not 1970.
func TestIndexDeleteRecordsTheSampledAtimeAsDeleteAge(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	read := time.Now().Add(-3 * 24 * time.Hour)
	b := writeBuild(t, root, 1, read)
	idx := seededIndex(t, map[string]int64{b: 1})
	reader := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter(meterName))
	require.NoError(t, err)
	c := indexCleaner(root, idx, func(o *Options) { o.Grace = GracePeriod })
	c.metrics = m
	c.folderAge = oldFolders

	c.indexDelete(ctx, entryOf(t, idx, b), time.Now().Add(-GracePeriod).Unix())

	require.NoDirExists(t, filepath.Join(root, b))
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	var sum int64
	var count uint64
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == "nfsclean.delete.age" {
				for _, dp := range mt.Data.(metricdata.Histogram[int64]).DataPoints {
					sum += dp.Sum
					count += dp.Count
				}
			}
		}
	}
	require.Equal(t, uint64(1), count)
	assert.InDelta(t, (3 * 24 * time.Hour).Seconds(), float64(sum), 120)
}

func TestIndexBuildInADryRunWritesNothing(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour))
	idx := seededIndex(t, map[string]int64{})
	c := indexCleaner(root, idx, func(o *Options) {
		o.IndexMode = IndexOn
		o.DryRun = true
	})

	require.NoError(t, c.Clean(ctx))

	assert.Empty(t, indexMembers(t, idx))
	due, err := idx.BuildDue(ctx)
	require.NoError(t, err)
	assert.True(t, due)
}

// A build that fails (here: Redis rejects the mark) is retried next run, and
// this run still cleans.
func TestIndexBuildFailureLeavesTheRunGoing(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := t.TempDir()
	unknown := writeBuild(t, root, 1, time.Now().Add(-30*24*time.Hour))
	client := testRedis(t)
	hook := &failCommand{name: "set"}
	client.AddHook(hook)
	idx := seededIndexOn(t, client, map[string]int64{})
	hook.on.Store(true)
	reader := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter(meterName))
	require.NoError(t, err)
	c := indexCleaner(root, idx, func(o *Options) { o.IndexMode = IndexOn })
	c.metrics = m

	require.NoError(t, c.Clean(ctx))

	require.NoDirExists(t, filepath.Join(root, unknown))
	assert.Positive(t, hook.fails.Load())
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	var indexErrors int64
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name == "nfsclean.errors" {
				for _, dp := range mt.Data.(metricdata.Sum[int64]).DataPoints {
					if v, ok := dp.Attributes.Value(AttrOp); ok && v.AsString() == ValOpIndex {
						indexErrors += dp.Value
					}
				}
			}
		}
	}
	assert.Equal(t, int64(1), indexErrors, "the failed build is counted")
	due, err := idx.BuildDue(ctx)
	require.NoError(t, err)
	assert.True(t, due, "not marked: the next run builds again")
}
