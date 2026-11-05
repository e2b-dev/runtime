package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// healCount returns the count of healer repairs of one kind.
func healCount(t *testing.T, reader *sdkmetric.ManualReader, kind healKind) int64 {
	t.Helper()

	return counterByAttr(t, reader, string(telemetry.ApiRedisStorageIndexHeal), attribute.String(indexHealKindAttr, string(kind)))
}

// gaugePoints returns the values an integer gauge observed, by attribute set.
func gaugePoints(t *testing.T, reader *sdkmetric.ManualReader, name string) map[attribute.Distinct]int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	points := map[attribute.Distinct]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for _, point := range gauge.DataPoints {
				points[point.Attributes.Equivalent()] = point.Value
			}
		}
	}

	return points
}

// attrs returns the identity of an attribute set, which keys a metric's
// points.
func attrs(kvs ...attribute.KeyValue) attribute.Distinct {
	set := attribute.NewSet(kvs...)

	return set.Equivalent()
}

// repairPoints returns the points a sweep gauge observes for one report.
func repairPoints(r healReport) map[attribute.Distinct]int64 {
	return map[attribute.Distinct]int64{
		attrs(attribute.String(indexHealKindAttr, string(healReadded))):      int64(r.Readded),
		attrs(attribute.String(indexHealKindAttr, string(healRemoved))):      int64(r.Removed),
		attrs(attribute.String(indexHealKindAttr, string(healDeletedStale))): int64(r.DeletedStale),
		attrs(attribute.String(indexHealKindAttr, string(healTwinsDeleted))): int64(r.TwinsDeleted),
	}
}

// writeRecordWithMember stores sbx's record in the given form with its
// expiration member, past Add, which would delete the other form's record.
func writeRecordWithMember(t *testing.T, client redis.UniversalClient, form keyForm, sbx sandboxtypes.Sandbox) string {
	t.Helper()

	key := recordKey(form, sbx.TeamID.String(), sbx.SandboxID)
	writeRecord(t, client, key, "", sbx)
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{
		Score:  float64(sbx.EndTime.UnixMilli()),
		Member: sandboxExpirationMember(sbx),
	}).Err())

	return key
}

// registerTeam lists the team in the global teams index the team walk reads.
func registerTeam(t *testing.T, client redis.UniversalClient, teamID uuid.UUID) {
	t.Helper()

	require.NoError(t, client.ZAdd(t.Context(), globalTeamsSet, redis.Z{Score: float64(time.Now().Unix()), Member: teamID.String()}).Err())
}

// lockWatch records, each time the healer reaches its under-lock fault
// point, whether every key it watches is held; the test goroutine reads
// held once the sweep has ended.
type lockWatch struct {
	client redis.UniversalClient
	keys   []string
	held   []bool
}

func (w *lockWatch) hook(ctx context.Context) func(faultPoint) error {
	return func(p faultPoint) error {
		if p != faultHealerUnderLock {
			return nil
		}
		for _, key := range w.keys {
			n, err := w.client.Exists(ctx, key).Result()
			w.held = append(w.held, err == nil && n == 1)
		}

		return nil
	}
}

// runSweep runs one healer sweep on s as the loop does, the expiration
// index heal, whose walk feeds the index-consistency heal, and returns the
// expiration members it re-added and its report.
func runSweep(t *testing.T, s *Storage) (int, healReport) {
	t.Helper()

	healed, walk, err := s.healExpirationIndex(t.Context())
	require.NoError(t, err)
	report, err := s.healIndexConsistency(t.Context(), walk)
	require.NoError(t, err)

	return healed, report
}

// A Running record younger than the grace period is left unindexed: its Add
// may still be writing the entry.
func TestHealLeavesFreshUnindexed(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	sbx := makeIndexedSandbox(teamID, "sbx-fresh", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	addRecord(t, storage, formSandbox, sbx)
	require.NoError(t, client.SRem(t.Context(), index, sbx.SandboxID).Err())

	_, report := runSweep(t, storage)

	require.Zero(t, report.Readded)
	requireIndexed(t, client, index, sbx.SandboxID, false)
	require.Zero(t, healCount(t, reader, healReadded))
}

// A non-Running record without an index entry is never indexed again: once
// it is stale the record and its expiration member are deleted, in either
// form; until then it is left alone.
func TestHealDeletesStaleNonRunning(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, form := range []keyForm{formTeam, formSandbox} {
		stale := makeIndexedSandbox(teamID, fmt.Sprintf("sbx-stale-%d", form), uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-sandboxtypes.StaleCutoff-time.Minute))
		stale.State = sandboxtypes.StatePausing
		staleKey := addRecord(t, storage, form, stale)
		recent := makeIndexedSandbox(teamID, fmt.Sprintf("sbx-recent-%d", form), uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
		recent.State = sandboxtypes.StateKilling
		recentKey := addRecord(t, storage, form, recent)
		require.NoError(t, client.SRem(t.Context(), index, stale.SandboxID, recent.SandboxID).Err())

		_, report := runSweep(t, storage)

		require.Equal(t, 1, report.DeletedStale)
		require.Zero(t, report.Readded)
		requireNoRecord(t, client, staleKey)
		requireMemberAbsent(t, client, sandboxExpirationMember(stale))
		requireIndexed(t, client, index, stale.SandboxID, false)
		requireRecord(t, client, recentKey, mustJSON(t, recent))
		requireMemberScore(t, client, sandboxExpirationMember(recent), float64(recent.EndTime.UnixMilli()))
		requireIndexed(t, client, index, recent.SandboxID, false)
		require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(staleKey)))
	}
	require.Equal(t, int64(2), healCount(t, reader, healDeletedStale))
}

// An index entry whose record is absent in both forms is removed once it has
// been seen dangling for a grace period; one seen for the first time is left
// and remembered, and one that stops dangling is forgotten.
func TestHealRemovesDangling(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	registerTeam(t, client, teamID)
	old := recordRef{TeamID: teamID.String(), SandboxID: "sbx-old"}
	fresh := recordRef{TeamID: teamID.String(), SandboxID: "sbx-fresh"}
	require.NoError(t, client.SAdd(t.Context(), index, old.SandboxID, fresh.SandboxID).Err())
	storage.danglingSince = map[recordRef]time.Time{old: time.Now().Add(-2 * healGracePeriod)}

	_, report := runSweep(t, storage)

	require.Equal(t, 1, report.Removed)
	requireIndexed(t, client, index, old.SandboxID, false)
	requireIndexed(t, client, index, fresh.SandboxID, true)
	require.Contains(t, storage.danglingSince, fresh)
	require.WithinDuration(t, time.Now(), storage.danglingSince[fresh], 10*time.Second)
	require.Equal(t, int64(1), healCount(t, reader, healRemoved))

	addRecord(t, storage, formSandbox, newRecord(teamID, fresh.SandboxID))
	_, report = runSweep(t, storage)

	require.Zero(t, report.Removed)
	require.Empty(t, storage.danglingSince)
	requireIndexed(t, client, index, fresh.SandboxID, true)
}

// An index entry no key can be built from, which every list of the team
// skips, is removed by the first sweep that sees it, with no grace period
// and no lock: no writer can hold either for the ID. The team's other
// entries are untouched, a fresh dangling one included.
func TestHealRemovesUnusableEntries(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	live := newRecord(teamID, "sbx-live")
	addRecord(t, storage, formSandbox, live)
	require.NoError(t, client.SAdd(t.Context(), index, "", "{sbx-braced}", "sbx-dangling").Err())

	_, report := runSweep(t, storage)

	require.Equal(t, 2, report.Removed)
	members, err := client.SMembers(t.Context(), index).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{live.SandboxID, "sbx-dangling"}, members)
	require.Equal(t, int64(2), healCount(t, reader, healRemoved))
	require.Len(t, storage.danglingSince, 1)
	require.Contains(t, storage.danglingSince, recordRef{TeamID: teamID.String(), SandboxID: "sbx-dangling"})
}

// Of a sandbox recorded in both forms, the record with the older StartTime
// is deleted, whichever form holds it; the index entry stays for the other.
func TestHealDeletesOlderTwin(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	registerTeam(t, client, teamID)

	for _, olderForm := range []keyForm{formTeam, formSandbox} {
		id := fmt.Sprintf("sbx-twin-%d", olderForm)
		older := makeIndexedSandbox(teamID, id, uuid.NewString(), time.Now().Add(-2*time.Hour), time.Now().Add(time.Hour))
		newer := makeIndexedSandbox(teamID, id, uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		olderKey := writeRecordWithMember(t, client, olderForm, older)
		newerKey := writeRecordWithMember(t, client, olderForm.other(), newer)
		require.NoError(t, client.SAdd(t.Context(), index, id).Err())

		_, report := runSweep(t, storage)

		require.Equal(t, 1, report.TwinsDeleted)
		requireNoRecord(t, client, olderKey)
		requireRecord(t, client, newerKey, mustJSON(t, newer))
		requireIndexed(t, client, index, id, true)
		requireLocksFree(t, storage, teamID, id)
	}
	require.Equal(t, int64(2), healCount(t, reader, healTwinsDeleted))
}

// The walk only nominates: a record removed after the walk read it is not
// indexed again, and an index entry whose record appeared after the walk saw
// it dangling is kept, because the state is read again under the lock.
func TestHealRecheckUnderLock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	gone := makeIndexedSandbox(teamID, "sbx-gone", uuid.NewString(), time.Now().Add(-2*healGracePeriod), time.Now().Add(time.Hour))
	goneKey := addRecord(t, storage, formSandbox, gone)
	require.NoError(t, client.SRem(t.Context(), index, gone.SandboxID).Err())
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultHealerAfterNominate {
			require.NoError(t, client.Del(t.Context(), goneKey).Err())
		}

		return nil
	})

	_, report := runSweep(t, storage)

	require.Zero(t, report.Readded)
	requireIndexed(t, client, index, gone.SandboxID, false)
	require.Zero(t, healCount(t, reader, healReadded))
	requireLocksFree(t, storage, teamID, gone.SandboxID)

	resumed := newRecord(teamID, "sbx-resumed")
	require.NoError(t, client.SAdd(t.Context(), index, resumed.SandboxID).Err())
	storage.danglingSince = map[recordRef]time.Time{{TeamID: teamID.String(), SandboxID: resumed.SandboxID}: time.Now().Add(-2 * healGracePeriod)}
	var resumedKey string
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultHealerAfterNominate {
			resumedKey = writeRecordWithMember(t, client, formSandbox, resumed)
		}

		return nil
	})

	_, report = runSweep(t, storage)

	require.Zero(t, report.Removed)
	requireIndexed(t, client, index, resumed.SandboxID, true)
	requireRecord(t, client, resumedKey, mustJSON(t, resumed))
	require.Zero(t, healCount(t, reader, healRemoved))
}

// The census counts the records by form, is complete when every team was
// walked, and reaches the gauges; before the first sweep nothing is
// observed. Members no key can be built from count nowhere and are left to
// the evictor's sweep; such index entries are removed.
func TestHealCensus(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	require.Empty(t, gaugePoints(t, reader, string(telemetry.ApiRedisStorageRecordsForm)))
	require.Empty(t, gaugePoints(t, reader, string(telemetry.ApiRedisStorageCensusComplete)))

	for i := range 2 {
		addRecord(t, storage, formTeam, newRecord(teamID, fmt.Sprintf("sbx-team-%d", i)))
	}
	for i := range 3 {
		addRecord(t, storage, formSandbox, newRecord(teamID, fmt.Sprintf("sbx-sandbox-%d", i)))
	}
	braced := expirationMember(teamID.String(), "{braced}", uuid.NewString())
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet,
		redis.Z{Score: 1, Member: "garbage"},
		redis.Z{Score: 1, Member: braced},
	).Err())
	require.NoError(t, client.SAdd(t.Context(), index, "{braced}").Err())

	_, report := runSweep(t, storage)

	require.Equal(t, healReport{Removed: 1, TeamForm: 2, SandboxForm: 3, Complete: true}, report)
	requireIndexed(t, client, index, "{braced}", false)
	requireMemberScore(t, client, "garbage", 1)
	requireMemberScore(t, client, braced, 1)
	forms := gaugePoints(t, reader, string(telemetry.ApiRedisStorageRecordsForm))
	require.Equal(t, int64(2), forms[attrs(attribute.String(recordsFormAttr, "team"))])
	require.Equal(t, int64(3), forms[attrs(attribute.String(recordsFormAttr, "sandbox"))])
	require.Equal(t, map[attribute.Distinct]int64{attrs(): 1}, gaugePoints(t, reader, string(telemetry.ApiRedisStorageCensusComplete)))

	// A team whose index key is of another type is skipped by the team walk,
	// and its sandbox's index probe is refused, so the census is short; every
	// other sandbox is still probed and repaired.
	refused := uuid.New()
	require.NoError(t, client.Set(t.Context(), GetSandboxStorageTeamIndexKey(refused.String()), "not a set", 0).Err())
	registerTeam(t, client, refused)
	writeRecordWithMember(t, client, formTeam, newRecord(refused, "sbx-refused"))
	unindexed := makeIndexedSandbox(teamID, "sbx-unindexed", uuid.NewString(), time.Now().Add(-2*healGracePeriod), time.Now().Add(time.Hour))
	addRecord(t, storage, formSandbox, unindexed)
	require.NoError(t, client.SRem(t.Context(), index, unindexed.SandboxID).Err())

	_, report = runSweep(t, storage)

	require.Equal(t, healReport{Readded: 1, TeamForm: 2, SandboxForm: 4, Complete: false}, report)
	requireIndexed(t, client, index, unindexed.SandboxID, true)
	require.Equal(t, map[attribute.Distinct]int64{attrs(): 0}, gaugePoints(t, reader, string(telemetry.ApiRedisStorageCensusComplete)))
}

// The repairs of the last two sweeps reach the gauges by kind, so a rule
// can compare consecutive sweeps of one replica: nothing before the first
// sweep, only the last sweep's before the second, then both, the older
// sweep moving back a slot.
func TestHealSweepGauges(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	last, previous := string(telemetry.ApiRedisStorageIndexHealLastSweep), string(telemetry.ApiRedisStorageIndexHealPreviousSweep)
	require.Empty(t, gaugePoints(t, reader, last))
	require.Empty(t, gaugePoints(t, reader, previous))

	// One re-add, then nothing left to repair. The team was pruned from the
	// global teams index meanwhile, as the metrics callback does to a team
	// whose index is empty; the re-add lists it again, as Add would.
	unindexed := makeIndexedSandbox(teamID, "sbx-unindexed", uuid.NewString(), time.Now().Add(-2*healGracePeriod), time.Now().Add(time.Hour))
	addRecord(t, storage, formSandbox, unindexed)
	require.NoError(t, client.SRem(t.Context(), index, unindexed.SandboxID).Err())
	require.NoError(t, client.ZRem(t.Context(), globalTeamsSet, teamID.String()).Err())

	_, first := runSweep(t, storage)
	require.Equal(t, 1, first.Readded)
	require.Equal(t, int64(1), healCount(t, reader, healReadded))
	require.Equal(t, repairPoints(healReport{Readded: 1}), gaugePoints(t, reader, last))
	require.Empty(t, gaugePoints(t, reader, previous))
	require.NoError(t, client.ZScore(t.Context(), globalTeamsSet, teamID.String()).Err())

	_, second := runSweep(t, storage)
	require.Zero(t, second.repairs())
	require.Equal(t, repairPoints(healReport{}), gaugePoints(t, reader, last))
	require.Equal(t, repairPoints(healReport{Readded: 1}), gaugePoints(t, reader, previous))
	require.NoError(t, client.ZScore(t.Context(), globalTeamsSet, teamID.String()).Err())
}

// A repair waits for the sandbox's lock in the form its record is in: with
// that lock held elsewhere the sweep parks before touching anything, holds
// the lock while it reads under it, and reads a record removed meanwhile as
// gone, so nothing is indexed again.
func TestHealRepairWaitsForLock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, form := range []keyForm{formTeam, formSandbox} {
		sbx := makeIndexedSandbox(teamID, fmt.Sprintf("sbx-locked-%d", form), uuid.NewString(), time.Now().Add(-2*healGracePeriod), time.Now().Add(time.Hour))
		key := addRecord(t, storage, form, sbx)
		require.NoError(t, client.SRem(t.Context(), index, sbx.SandboxID).Err())
		_, walk, err := storage.healExpirationIndex(t.Context())
		require.NoError(t, err)

		lockKey := redis_utils.GetLockKey(key)
		held, err := storage.locker.Obtain(t.Context(), lockKey, lockTimeout)
		require.NoError(t, err)
		underLock := &lockWatch{client: client, keys: []string{lockKey}}
		storage.SetFaultHook(underLock.hook(t.Context()))

		var report healReport
		sweepDone := make(chan error, 1)
		go func() {
			var err error
			report, err = storage.healIndexConsistency(t.Context(), walk)
			sweepDone <- err
		}()

		waitForLockWaiter(t, storage.subManager, getLockRoutingKey(lockKey), sweepDone)
		requireIndexed(t, client, index, sbx.SandboxID, false)
		requireRecord(t, client, key, mustJSON(t, sbx))

		// Removed under the lock, as by a Remove that reached it first.
		require.NoError(t, client.Del(t.Context(), key).Err())
		require.NoError(t, held.Release(t.Context()))

		requireNoErrorFromChannel(t, sweepDone)
		require.Equal(t, []bool{true}, underLock.held, "the lock is held while the healer reads under it")
		require.Zero(t, report.Readded)
		requireIndexed(t, client, index, sbx.SandboxID, false)
		require.False(t, lockHeld(t, storage.locker, lockKey))
	}
	require.Zero(t, healCount(t, reader, healReadded))
}

// A twin repair waits for both forms' locks, the team form's first, and
// holds both while it reads: with the sandbox form's lock held elsewhere the
// sweep parks holding the team form's, and the newer twin removed meanwhile
// leaves no twin, so the older record stands, which a read made before the
// locks would have deleted.
func TestHealTwinRepairWaitsForLocks(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	startNotifications(t, storage)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())
	registerTeam(t, client, teamID)
	older := makeIndexedSandbox(teamID, "sbx-twin", uuid.NewString(), time.Now().Add(-2*time.Hour), time.Now().Add(time.Hour))
	newer := makeIndexedSandbox(teamID, "sbx-twin", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	olderKey := writeRecordWithMember(t, client, formTeam, older)
	newerKey := writeRecordWithMember(t, client, formSandbox, newer)
	require.NoError(t, client.SAdd(t.Context(), index, older.SandboxID).Err())
	_, walk, err := storage.healExpirationIndex(t.Context())
	require.NoError(t, err)

	teamLock, sandboxLock := lockKeys(teamID, older.SandboxID)
	held, err := storage.locker.Obtain(t.Context(), sandboxLock, lockTimeout)
	require.NoError(t, err)
	underLock := &lockWatch{client: client, keys: []string{teamLock, sandboxLock}}
	storage.SetFaultHook(underLock.hook(t.Context()))

	var report healReport
	sweepDone := make(chan error, 1)
	go func() {
		var err error
		report, err = storage.healIndexConsistency(t.Context(), walk)
		sweepDone <- err
	}()

	waitForLockWaiter(t, storage.subManager, getLockRoutingKey(sandboxLock), sweepDone)
	require.True(t, lockHeld(t, storage.locker, teamLock))
	requireRecord(t, client, olderKey, mustJSON(t, older))
	requireRecord(t, client, newerKey, mustJSON(t, newer))

	require.NoError(t, client.Del(t.Context(), newerKey).Err())
	require.NoError(t, held.Release(t.Context()))

	requireNoErrorFromChannel(t, sweepDone)
	require.Equal(t, []bool{true, true}, underLock.held, "both locks are held while the healer reads under them")
	require.Zero(t, report.TwinsDeleted)
	requireRecord(t, client, olderKey, mustJSON(t, older))
	requireIndexed(t, client, index, older.SandboxID, true)
	require.Zero(t, healCount(t, reader, healTwinsDeleted))
	requireLocksFree(t, storage, teamID, older.SandboxID)
}

// A probe pipeline that never ran, refused by a hook here, ends the sweep
// with the refusal and an incomplete census: its commands carry no error
// and empty replies, which must not pass for absent records.
func TestHealAbortsWhenProbesNeverRan(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	addRecord(t, storage, formTeam, newRecord(uuid.New(), "sbx-1"))
	errRefused := errors.New("pipeline refused")
	client.AddHook(cmdHook{pipeline: func(context.Context, []redis.Cmder, redis.ProcessPipelineHook) error { return errRefused }})

	report, err := storage.healIndexConsistency(t.Context(), teamWalk{complete: true})

	require.ErrorIs(t, err, errRefused)
	require.Equal(t, healReport{}, report)
}

// Start records every kind of the write anomaly and index heal counters at
// zero, so a kind's first event moves a series that already exists, and
// writes the scheme marker, with no expiry, before the healer runs, warning
// that the store had none; a later start finds it and does not warn.
//
//nolint:paralleltest // observes the global logger
func TestStart(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))
	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool {
		var collected metricdata.ResourceMetrics
		if reader.Collect(t.Context(), &collected) != nil {
			return false
		}
		marker, err := client.Get(t.Context(), SchemeMarkerKey).Result()

		return err == nil && marker == SchemeMinRelease && len(counterPoints(t, collected, string(telemetry.ApiRedisStorageIndexHeal))) == len(healKinds) &&
			logs.FilterMessageSnippet("scheme marker was absent").Len() == 1
	}, 5*time.Second, 10*time.Millisecond)
	for _, kind := range healKinds {
		require.Zero(t, healCount(t, reader, kind))
	}
	for _, kind := range []string{writeAnomalyRemoveStranded, writeAnomalyAddUnindexed, writeAnomalyAddWriteUnconfirmed} {
		require.Zero(t, writeAnomalies(t, reader, kind))
	}
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	require.Len(t, counterPoints(t, collected, string(telemetry.ApiRedisStorageWriteAnomalies)), 3)
	ttl, err := client.TTL(t.Context(), SchemeMarkerKey).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)

	storage.markScheme(t.Context())
	require.Equal(t, 1, logs.FilterMessageSnippet("scheme marker was absent").Len())
}
