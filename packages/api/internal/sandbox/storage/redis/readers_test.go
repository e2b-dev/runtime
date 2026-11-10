package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// intHistogram returns the point count and the sum of an integer histogram.
func intHistogram(t *testing.T, reader *sdkmetric.ManualReader, name string) (count uint64, sum int64) {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, point := range hist.DataPoints {
				count += point.Count
				sum += point.Sum
			}
		}
	}

	return count, sum
}

// A read of refs finds the records in one pipeline of GETs. A ref with no
// record is absent. A record that does not decode, or that belongs to
// another team, is in neither partition, and the team list does not show
// it. The GETs are recorded once per call.
func TestFetchRecordsClassifies(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	team := teamID.String()

	ids := []string{"sbx-s1", "sbx-t1", "sbx-s2", "sbx-t2", "sbx-t3"}
	want := make(map[recordRef]string, len(ids))
	for _, id := range ids {
		sbx := newRecord(teamID, id)
		addRecord(t, storage, sbx)
		want[recordRef{TeamID: team, SandboxID: id}] = sbx.ExecutionID
	}
	addRecord(t, storage, newRecord(uuid.New(), "sbx-other"))
	require.NoError(t, client.Set(t.Context(), recordKey("sbx-corrupt"), "not-json", 0).Err())
	require.NoError(t, client.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(team), "sbx-other", "sbx-corrupt").Err())

	var refs []recordRef
	for _, id := range []string{"sbx-s1", "sbx-t1", "sbx-gone", "sbx-s2", "sbx-t2", "sbx-other", "sbx-corrupt", "sbx-t3"} {
		refs = append(refs, recordRef{TeamID: team, SandboxID: id})
	}

	found, absent, err := storage.fetchRecords(t.Context(), refs)
	require.NoError(t, err)

	require.Len(t, found, len(want))
	for ref, executionID := range want {
		assert.Equal(t, executionID, found[ref].ExecutionID, ref.SandboxID)
	}
	assert.Equal(t, []recordRef{{TeamID: team, SandboxID: "sbx-gone"}}, absent)

	count, sum := intHistogram(t, reader, string(telemetry.ApiRedisStorageListGets))
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, int64(len(refs)), sum, "one GET per ref")

	items, err := storage.TeamItems(t.Context(), teamID, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sbx-s1", "sbx-t1", "sbx-s2", "sbx-t2", "sbx-t3"}, sandboxIDsOf(items))
}

// A record key of another type, which GET refuses, skips that sandbox
// alone: the rest of the batch is read, and the skipped one is neither
// found nor absent.
func TestFetchRecordsSkipsAKeyOfAnotherType(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	team := teamID.String()
	good := newRecord(teamID, "sbx-good")
	addRecord(t, storage, good)
	require.NoError(t, client.HSet(t.Context(), recordKey("sbx-hash"), "field", "value").Err())

	// The refused key first, so the pipeline's first error is its reply.
	found, absent, err := storage.fetchRecords(t.Context(), []recordRef{
		{TeamID: team, SandboxID: "sbx-hash"}, {TeamID: team, SandboxID: "sbx-good"}, {TeamID: team, SandboxID: "sbx-gone"},
	})

	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.Equal(t, good.ExecutionID, found[recordRef{TeamID: team, SandboxID: "sbx-good"}].ExecutionID)
	assert.Equal(t, []recordRef{{TeamID: team, SandboxID: "sbx-gone"}}, absent)
}

// A record the reader skips, one that does not decode or one of another
// team, is neither an orphan for the node reconcile nor a stale member for
// the evictor: only a ref with no record is. A bad record is
// logged and left running, never killed, and its expiration member leaves
// the expired window for the recheck delay instead of being read and logged
// again on the next tick.
func TestSkippedRecordsAreNotOrphans(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	team := teamID.String()
	addRecord(t, storage, newRecord(uuid.New(), "sbx-other"))
	require.NoError(t, client.Set(t.Context(), recordKey("sbx-corrupt"), "not-json", 0).Err())
	ids := []string{"sbx-other", "sbx-corrupt", "sbx-gone"}

	started := time.Now().Add(-2 * orphanGracePeriod)
	var node []sandboxtypes.NodeSandbox
	for _, id := range ids {
		node = append(node, sandboxtypes.NodeSandbox{SandboxID: id, TeamID: teamID, StartTime: started})
	}
	orphans := storage.Reconcile(t.Context(), node, "node-1")
	require.Len(t, orphans, 1)
	assert.Equal(t, "sbx-gone", orphans[0].SandboxID)

	score := float64(time.Now().Add(-time.Minute).UnixMilli())
	members := make(map[string]string, len(ids))
	for _, id := range ids {
		members[id] = expirationMember(team, id, uuid.NewString())
		require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{Score: score, Member: members[id]}).Err())
	}
	before := time.Now()
	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	assert.Empty(t, items)
	for _, id := range []string{"sbx-other", "sbx-corrupt"} {
		deferred, err := client.ZScore(t.Context(), globalExpirationSet, members[id]).Result()
		require.NoError(t, err, id)
		assert.GreaterOrEqual(t, deferred, float64(before.Add(transitionRecheckDelay).UnixMilli()), "%s leaves the expired window for the recheck delay", id)
		assert.LessOrEqual(t, deferred, float64(time.Now().Add(transitionRecheckDelay).UnixMilli()), id)
	}
	requireMemberAbsent(t, client, members["sbx-gone"])
	assert.Equal(t, int64(2), counterTotal(t, reader, string(telemetry.ApiRedisStorageExpirationIndexDeferred)))
}

// A record whose payload names another sandbox than its key, an empty or a
// braced ID included, is skipped: the evictor does not return it, since its
// removal would build keys from the payload's ID, its member is deferred,
// and the team list does not show it.
func TestReadersSkipRecordsWhosePayloadIDDiffers(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	team := teamID.String()
	index := GetSandboxStorageTeamIndexKey(team)
	addRecord(t, storage, newRecord(teamID, "sbx-live"))

	score := float64(time.Now().Add(-time.Minute).UnixMilli())
	members := make(map[string]string, 2)
	for key, payloadID := range map[string]string{"sbx-empty": "", "sbx-braced": "{sbx-1}"} {
		sbx := makeIndexedSandbox(teamID, payloadID, uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
		require.NoError(t, client.Set(t.Context(), recordKey(key), mustJSON(t, sbx), 0).Err())
		require.NoError(t, client.SAdd(t.Context(), index, key).Err())
		members[key] = expirationMember(team, key, sbx.ExecutionID)
		require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{Score: score, Member: members[key]}).Err())
	}

	before := time.Now()
	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	assert.Empty(t, items)
	for key, member := range members {
		deferred, err := client.ZScore(t.Context(), globalExpirationSet, member).Result()
		require.NoError(t, err, key)
		assert.GreaterOrEqual(t, deferred, float64(before.Add(transitionRecheckDelay).UnixMilli()), "%s leaves the expired window for the recheck delay", key)
	}

	listed, err := storage.TeamItems(t.Context(), teamID, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sbx-live"}, sandboxIDsOf(listed))
}

// Every failed command is counted on the errors counter by kind: the
// WRONGTYPE a record read skips, and the one a script raises, as other,
// and on a cluster a script whose keys span slots as crossslot. The
// counter lives on the global meter provider, whose delegate is wired once
// per process, so this test installs the provider for the whole binary and
// runs alone, ahead of the parallel tests.
//
//nolint:paralleltest // installs the process-wide meter provider; parallel tests would add to its counts
func TestRedisErrorsAreCounted(t *testing.T) {
	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	errorsOf := func(kind string) int64 {
		return counterByAttr(t, reader, string(telemetry.ApiRedisStorageErrors), attribute.String(errorKindAttr, kind))
	}

	// Start's zero records put every kind on the reader before any error.
	storage.initCounters(t.Context())
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	require.Len(t, counterPoints(t, collected, string(telemetry.ApiRedisStorageErrors)), 4)

	team := uuid.NewString()
	setKey := recordKey("sbx-set")
	require.NoError(t, client.SAdd(t.Context(), setKey, "member").Err())
	others := errorsOf(errorKindOther)
	_, _, err := storage.fetchRecords(t.Context(), []recordRef{{TeamID: team, SandboxID: "sbx-gone"}, {TeamID: team, SandboxID: "sbx-set"}})
	require.NoError(t, err)
	assert.Equal(t, others+1, errorsOf(errorKindOther), "the WRONGTYPE the read skipped")

	others = errorsOf(errorKindOther)
	err = RunScript(t.Context(), client, "get_set", redis.NewScript(`return redis.call('GET', KEYS[1])`), []string{setKey}).Err()
	require.Error(t, err)
	require.NotErrorIs(t, err, redis.Nil)
	assert.Equal(t, others+1, errorsOf(errorKindOther), "the script's WRONGTYPE")

	if redis_utils.ClusterLane() {
		crossSlots := errorsOf(errorKindCrossSlot)
		// Past RunScript's slot assertion: the server's refusal is what is counted.
		err := defaultScriptMetrics.run(t.Context(), client, "spans_slots", redis.NewScript(`return redis.call('GET', KEYS[1])`), []string{"{a}:1", "{b}:2"}).Err()
		require.True(t, redis.HasErrorPrefix(err, "CROSSSLOT"), "%v", err)
		assert.Equal(t, crossSlots+1, errorsOf(errorKindCrossSlot), "the script whose keys span slots")
	}
}

// A nil ahead of a refused reply in the pipeline hides neither: Exec reports
// only the first error, so every reply is read on its own. The refused
// reply, a GET of a key holding a set (WRONGTYPE), skips its sandbox alone;
// the nil one is absent.
func TestFetchRecordsNilThenRefusedReply(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	team := teamID.String()
	live := newRecord(teamID, "sbx-live")
	addRecord(t, storage, live)
	require.NoError(t, client.SAdd(t.Context(), recordKey("sbx-set"), "member").Err())

	found, absent, err := storage.fetchRecords(t.Context(), []recordRef{
		{TeamID: team, SandboxID: "sbx-gone"},
		{TeamID: team, SandboxID: "sbx-live"},
		{TeamID: team, SandboxID: "sbx-set"},
	})
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.Equal(t, live.ExecutionID, found[recordRef{TeamID: team, SandboxID: "sbx-live"}].ExecutionID)
	assert.Equal(t, []recordRef{{TeamID: team, SandboxID: "sbx-gone"}}, absent)
}

// A read whose pipeline fails aborts with that error and classifies
// nothing: a pipeline refused before it ran leaves its commands with no
// error and empty values, which must not pass for records that do not
// decode.
func TestFetchRecordsAbortsWhenThePipelineNeverRan(t *testing.T) {
	t.Parallel()

	errRefused := errors.New("refused")
	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	addRecord(t, storage, newRecord(teamID, "sbx-live"))
	client.AddHook(cmdHook{pipeline: func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error { return errRefused }})

	found, absent, err := storage.fetchRecords(t.Context(), []recordRef{
		{TeamID: teamID.String(), SandboxID: "sbx-gone"},
		{TeamID: teamID.String(), SandboxID: "sbx-live"},
	})
	require.ErrorIs(t, err, errRefused)
	assert.Empty(t, found)
	assert.Empty(t, absent)
}

// An ID no key can be built from, an index member for the team list or a
// node-reported sandbox for the reconcile, is skipped: the list still shows
// the team's other sandboxes, and the reconcile reports only the missing
// sandbox as an orphan, never the malformed ones.
func TestReadersSkipIDsNoKeyCanBeBuiltFrom(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	addRecord(t, storage, newRecord(teamID, "sbx-live"))
	require.NoError(t, client.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), "", "{sbx-braced}").Err())

	items, err := storage.TeamItems(t.Context(), teamID, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sbx-live"}, sandboxIDsOf(items))

	started := time.Now().Add(-2 * orphanGracePeriod)
	var node []sandboxtypes.NodeSandbox
	for _, id := range []string{"", "{sbx-braced}", "sbx-gone"} {
		node = append(node, sandboxtypes.NodeSandbox{SandboxID: id, TeamID: teamID, StartTime: started})
	}
	orphans := storage.Reconcile(t.Context(), node, "node-1")
	require.Len(t, orphans, 1)
	assert.Equal(t, "sbx-gone", orphans[0].SandboxID)
}

// fakeRedisError stands in for a server error the tests cannot provoke.
type fakeRedisError string

func (e fakeRedisError) Error() string { return string(e) }
func (fakeRedisError) RedisError()     {}

// A failed command is counted by the cluster error it returned, through
// wrapping; a nil reply and a cancelled context are not failures. A real
// CROSSSLOT is checked where the server is a cluster, a real WRONGTYPE on
// both.
func TestRedisErrorKind(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	require.NoError(t, client.SAdd(t.Context(), "{a}:set", "member").Err())
	wrongType := client.Get(t.Context(), "{a}:set").Err()
	require.Error(t, wrongType)

	type errorCase struct {
		err     error
		kind    string
		counted bool
	}
	cases := []errorCase{
		{fakeRedisError("CROSSSLOT Keys in request don't hash to the same slot"), errorKindCrossSlot, true},
		{fmt.Errorf("read: %w", fakeRedisError("MOVED 3999 127.0.0.1:6381")), errorKindMovedExhausted, true},
		{fakeRedisError("ASK 3999 127.0.0.1:6381"), errorKindMovedExhausted, true},
		{fakeRedisError("TRYAGAIN Multiple keys request during rehashing of slot"), errorKindTryAgain, true},
		{wrongType, errorKindOther, true},
		{errors.New("dial tcp: connection refused"), errorKindOther, true},
		{redis.Nil, "", false},
		{fmt.Errorf("read: %w", context.Canceled), "", false},
		{context.DeadlineExceeded, "", false},
	}
	if redis_utils.ClusterLane() {
		crossSlot := client.MGet(t.Context(), "{a}:1", "{b}:2").Err()
		require.Error(t, crossSlot)
		cases = append(cases, errorCase{crossSlot, errorKindCrossSlot, true})
	}

	for _, tc := range cases {
		kind, counted := redisErrorKind(tc.err)
		assert.Equal(t, tc.counted, counted, "%v", tc.err)
		assert.Equal(t, tc.kind, kind, "%v", tc.err)
	}
}
