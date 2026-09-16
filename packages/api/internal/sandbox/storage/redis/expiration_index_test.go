package redis

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// makeIndexedSandbox builds a running sandbox fixture with explicit lifecycle
// timestamps for expiration index tests.
func makeIndexedSandbox(teamID uuid.UUID, sandboxID, executionID string, startTime, endTime time.Time) sandboxtypes.Sandbox {
	return sandboxtypes.Sandbox{
		SandboxID:         sandboxID,
		TemplateID:        "test-template",
		ClientID:          "test-client",
		TeamID:            teamID,
		ExecutionID:       executionID,
		StartTime:         startTime,
		EndTime:           endTime,
		MaxInstanceLength: 24 * time.Hour,
		State:             sandboxtypes.StateRunning,
	}
}

func requireMemberScore(t *testing.T, client redis.UniversalClient, member string, wantScore float64) {
	t.Helper()

	score, err := client.ZScore(t.Context(), globalExpirationSet, member).Result()
	require.NoError(t, err)
	require.InDelta(t, wantScore, score, 0.5)
}

func requireMemberAbsent(t *testing.T, client redis.UniversalClient, member string) {
	t.Helper()

	err := client.ZScore(t.Context(), globalExpirationSet, member).Err()
	require.ErrorIs(t, err, redis.Nil)
}

func TestParseExpirationMember(t *testing.T) {
	t.Parallel()

	teamID := uuid.NewString()
	execID := uuid.NewString()

	tests := []struct {
		name       string
		member     string
		wantTeam   string
		wantSbx    string
		wantExec   string
		wantParsed bool
	}{
		{"execution scoped", teamID + ":sbx-1:" + execID, teamID, "sbx-1", execID, true},
		{"legacy two-part member", teamID + ":sbx-1", "", "", "", false},
		{"no separator", "garbage", "", "", "", false},
		{"empty rest", teamID + ":", "", "", "", false},
		{"non-uuid execution tag", teamID + ":sbx-1:not-a-uuid", "", "", "", false},
		{"too many parts", teamID + ":sbx-1:x:" + execID, "", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotTeam, gotSbx, gotExec, ok := parseExpirationMember(tt.member)
			require.Equal(t, tt.wantParsed, ok)
			require.Equal(t, tt.wantTeam, gotTeam)
			require.Equal(t, tt.wantSbx, gotSbx)
			require.Equal(t, tt.wantExec, gotExec)
		})
	}
}

func TestAddRemove_ExecutionScopedMember(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-roundtrip", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), sbx))

	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))

	require.NoError(t, storage.Remove(t.Context(), teamID, sbx.SandboxID, sbx.ExecutionID))
	requireMemberAbsent(t, client, member)

	err := client.Get(t.Context(), getSandboxKey(teamID.String(), sbx.SandboxID)).Err()
	require.ErrorIs(t, err, redis.Nil)
}

// TestRemove_DoesNotUnindexFreshExecution replays Race B: a lockless Add for
// a fresh execution of the same sandbox ID lands its index write while Remove
// of the old execution runs. The fresh execution's member must survive.
func TestRemove_DoesNotUnindexFreshExecution(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	const sandboxID = "sbx-race-b"

	old := makeIndexedSandbox(teamID, sandboxID, uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	require.NoError(t, storage.Add(t.Context(), old))

	// Simulate the concurrent Add's first phase (index write) for a fresh
	// execution landing before Remove's cleanup.
	fresh := makeIndexedSandbox(teamID, sandboxID, uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	freshMember := expirationMember(teamID.String(), sandboxID, fresh.ExecutionID)
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{
		Score:  float64(fresh.EndTime.UnixMilli()),
		Member: freshMember,
	}).Err())

	require.NoError(t, storage.Remove(t.Context(), teamID, sandboxID, old.ExecutionID))

	// Old execution's member removed, fresh execution's member intact.
	requireMemberAbsent(t, client, expirationMember(teamID.String(), sandboxID, old.ExecutionID))
	requireMemberScore(t, client, freshMember, float64(fresh.EndTime.UnixMilli()))
}

// TestExpiredItems_StaleSweepIsExecutionScoped replays Race A: the evictor's
// stale sweep processes an expired member of a dead execution while a fresh
// execution of the same sandbox ID is live. Only the dead execution's member
// may be removed.
func TestExpiredItems_StaleSweepIsExecutionScoped(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	const sandboxID = "sbx-race-a"

	// Fresh execution: live key, future member.
	fresh := makeIndexedSandbox(teamID, sandboxID, uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), fresh))

	// Dead execution: expired member left behind (e.g. failed Remove cleanup).
	deadExecutionID := uuid.NewString()
	deadMember := expirationMember(teamID.String(), sandboxID, deadExecutionID)
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: deadMember,
	}).Err())

	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Empty(t, items)

	requireMemberAbsent(t, client, deadMember)
	requireMemberScore(t, client, expirationMember(teamID.String(), sandboxID, fresh.ExecutionID), float64(fresh.EndTime.UnixMilli()))
}

func TestExpiredItems_RemovesOrphanMember(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	member := expirationMember(uuid.NewString(), "sbx-orphan", uuid.NewString())
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: member,
	}).Err())

	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Empty(t, items)

	requireMemberAbsent(t, client, member)
}

// TestExpiredItems_SweepsInvalidMember: unparseable members must be removed, not skipped,
// otherwise they permanently occupy the batch-limited expired scan window and starve
// eviction of valid sandboxes. A live sandbox indexed only by such a member
// is re-indexed in the current format by the healer.
func TestExpiredItems_SweepsInvalidMember(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-legacy-leftover", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), sbx))

	// Sandbox indexed only via an expired legacy two-part member.
	execMember := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	legacyMember := teamID.String() + ":" + sbx.SandboxID
	require.NoError(t, client.ZRem(t.Context(), globalExpirationSet, execMember).Err())
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: legacyMember,
	}).Err())

	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Empty(t, items)

	// Invalid member swept: it no longer occupies the scan window.
	requireMemberAbsent(t, client, legacyMember)

	// Healer restores eviction coverage in the current member format.
	healed, err := storage.healExpirationIndex(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, healed)
	requireMemberScore(t, client, execMember, float64(sbx.EndTime.UnixMilli()))
}

func TestExpiredItems_RescoresDriftedMember(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-drift", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), sbx))

	// Drift the member's score into the expired window while the stored
	// EndTime stays in the future.
	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: member,
	}).Err())

	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Empty(t, items)

	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))
}

func TestExpiredItems_ReturnsExpiredRunningSandbox(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-expired", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	require.NoError(t, storage.Add(t.Context(), sbx))

	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, sbx.SandboxID, items[0].SandboxID)
	require.Equal(t, sbx.ExecutionID, items[0].ExecutionID)
}

func TestExpiredItems_ResidualKillingAfterStaleCutoff(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	teamID := uuid.New()
	stale := makeIndexedSandbox(teamID, "sbx-stale-killing", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-sandboxtypes.StaleCutoff-time.Minute))
	stale.State = sandboxtypes.StateKilling
	require.NoError(t, storage.Add(t.Context(), stale))

	young := makeIndexedSandbox(teamID, "sbx-young-killing", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	young.State = sandboxtypes.StateKilling
	require.NoError(t, storage.Add(t.Context(), young))

	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, stale.SandboxID, items[0].SandboxID)
	require.Equal(t, sandboxtypes.StateKilling, items[0].State)
}

// TestHeal_RestoresMissingMember reproduces the production incident: a
// sandbox present in team storage but missing from the global expiration
// index is invisible to the evictor and would live forever.
func TestHeal_RestoresMissingMember(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-immortal", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	require.NoError(t, storage.Add(t.Context(), sbx))

	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	require.NoError(t, client.ZRem(t.Context(), globalExpirationSet, member).Err())

	// Immortal: the evictor cannot see it.
	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Empty(t, items)

	healed, err := storage.healExpirationIndex(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, healed)
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))

	items, err = storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, sbx.SandboxID, items[0].SandboxID)
}

func TestHeal_SkipsYoungSandbox(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-young", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), sbx))

	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	require.NoError(t, client.ZRem(t.Context(), globalExpirationSet, member).Err())

	healed, err := storage.healExpirationIndex(t.Context())
	require.NoError(t, err)
	require.Zero(t, healed)
	requireMemberAbsent(t, client, member)
}

// TestHeal_ManySandboxesMultipleBatches exercises the SSCAN pagination in the
// shared sandbox scanner: more sandboxes than sandboxScanBatchSize, with holes
// spread across the whole ID range.
func TestHeal_ManySandboxesMultipleBatches(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	total := sandboxScanBatchSize*2 + 37 // force >2 batches with a partial tail

	pipe := client.Pipeline()
	var missingMembers []string
	for i := range total {
		sbx := makeIndexedSandbox(teamID, fmt.Sprintf("sbx-batch-%04d", i), uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		data, err := json.Marshal(sbx)
		require.NoError(t, err)

		pipe.Set(t.Context(), getSandboxKey(teamID.String(), sbx.SandboxID), data, 0)
		pipe.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), sbx.SandboxID)

		member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
		if i%2 == 0 {
			pipe.ZAdd(t.Context(), globalExpirationSet, redis.Z{
				Score:  float64(sbx.EndTime.UnixMilli()),
				Member: member,
			})
		} else {
			missingMembers = append(missingMembers, member)
		}
	}
	// The shared scanner walks teams via the global teams index, so the team
	// has to be registered there.
	pipe.ZAdd(t.Context(), globalTeamsSet, redis.Z{Score: float64(time.Now().Unix()), Member: teamID.String()})
	_, err := pipe.Exec(t.Context())
	require.NoError(t, err)

	healed, err := storage.healExpirationIndex(t.Context())
	require.NoError(t, err)
	require.Equal(t, len(missingMembers), healed)

	scores, err := client.ZMScore(t.Context(), globalExpirationSet, missingMembers...).Result()
	require.NoError(t, err)
	for i, score := range scores {
		require.NotZero(t, score, "member %s not healed", missingMembers[i])
	}

	// Idempotent: a second pass heals nothing.
	healed, err = storage.healExpirationIndex(t.Context())
	require.NoError(t, err)
	require.Zero(t, healed)
}

func TestHeal_DoesNotClobberExistingScore(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-fresh-score", uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), sbx))

	// A concurrent SetTimeout moved the score past what the stored JSON says.
	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	freshScore := float64(time.Now().Add(2 * time.Hour).UnixMilli())
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{Score: freshScore, Member: member}).Err())

	healed, err := storage.healExpirationIndex(t.Context())
	require.NoError(t, err)
	require.Zero(t, healed)
	requireMemberScore(t, client, member, freshScore)
}

func TestUpdate_RescoresExecutionMember(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-update", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(t.Context(), sbx))

	newEndTime := time.Now().Add(2 * time.Hour)
	updated, err := storage.Update(t.Context(), teamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		cur.EndTime = newEndTime

		return cur, nil
	})
	require.NoError(t, err)

	requireMemberScore(t, client, expirationMember(teamID.String(), sbx.SandboxID, updated.ExecutionID), float64(newEndTime.UnixMilli()))
}
