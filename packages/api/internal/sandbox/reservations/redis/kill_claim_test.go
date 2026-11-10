package redis

import (
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
)

func TestClaimKill_NoResumeInFlight_ClaimsAndBlocksResume(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	_, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.True(t, claimed, "kill should be claimed when no resume is in flight")

	_, _, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.ErrorIs(t, err, sandboxtypes.ErrSandboxKilled)
}

func TestClaimKill_ResumePending_Refuses(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	finishStart, _, err := storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	require.NotNil(t, finishStart, "first reserve should win the reservation")

	_, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.False(t, claimed, "kill must be refused while a resume is pending")
}

func TestClaimKill_AlreadyRunning_Refuses(t *testing.T) {
	t.Parallel()
	storage, client := setupTestReservationStorage(t)

	teamID := uuid.New()

	// What storage.Add does when a resume publishes the sandbox.
	indexKey := storage_redis.GetSandboxStorageTeamIndexKey(teamID.String())
	require.NoError(t, client.SAdd(t.Context(), indexKey, testSandboxID).Err())

	_, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.False(t, claimed, "kill must be refused when the sandbox is already running")
}

func TestClaimKill_StalePending_Claims(t *testing.T) {
	t.Parallel()
	storage, client := setupTestReservationStorage(t)

	teamID := uuid.New()

	staleTimestamp := float64(time.Now().Add(-3 * time.Minute).Unix())
	require.NoError(t, client.ZAdd(t.Context(), getPendingSetKey(teamID.String()), goredis.Z{
		Score:  staleTimestamp,
		Member: testSandboxID,
	}).Err())

	_, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.True(t, claimed, "a stale pending entry must not block the kill")
}

func TestReleaseKillClaim_UnblocksResume(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	claimID, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	require.True(t, claimed)

	_, _, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.ErrorIs(t, err, sandboxtypes.ErrSandboxKilled)

	require.NoError(t, storage.ReleaseKillClaim(t.Context(), teamID, testSandboxID, claimID))

	finishStart, _, err := storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	assert.NotNil(t, finishStart)
}

func TestReleaseKillClaim_KeepsOverlappingClaim(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	firstID, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	require.True(t, claimed)

	secondID, claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, storage.ReleaseKillClaim(t.Context(), teamID, testSandboxID, firstID))

	_, _, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.ErrorIs(t, err, sandboxtypes.ErrSandboxKilled, "the second delete's claim must survive the first one's release")

	require.NoError(t, storage.ReleaseKillClaim(t.Context(), teamID, testSandboxID, secondID))

	finishStart, _, err := storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	assert.NotNil(t, finishStart)
}

func TestClaimKill_ConcurrentReserveAndClaim(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	for i := range 50 {
		teamID := uuid.New()
		sandboxID := "sbx-" + teamID.String()

		var (
			reserved bool
			claimed  bool
		)

		done := make(chan struct{}, 2)
		go func() {
			finishStart, _, err := storage.Reserve(t.Context(), teamID, sandboxID, 10)
			if err == nil && finishStart != nil {
				reserved = true
			}
			done <- struct{}{}
		}()
		go func() {
			_, c, err := storage.ClaimKill(t.Context(), teamID, sandboxID)
			if err == nil {
				claimed = c
			}
			done <- struct{}{}
		}()
		<-done
		<-done

		require.False(t, claimed && reserved, "iteration %d: both resume reserved and kill claimed for the same sandbox", i)
	}
}
