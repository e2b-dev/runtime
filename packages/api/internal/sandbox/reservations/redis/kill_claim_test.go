package redis

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
)

// TestClaimKill_NoResumeInFlight_ClaimsAndBlocksResume covers the common case:
// a paused sandbox with no in-flight resume. ClaimKill succeeds, and any resume
// that starts afterwards is refused so the accepted kill cannot be undone.
func TestClaimKill_NoResumeInFlight_ClaimsAndBlocksResume(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.True(t, claimed, "kill should be claimed when no resume is in flight")

	// A resume that starts after the claim must lose.
	_, _, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.ErrorIs(t, err, sandboxtypes.ErrSandboxKilled)
}

// TestClaimKill_ResumePending_Refuses covers the race the fix targets: a resume
// is already mid-flight (it holds the reservation) when the DELETE arrives.
// ClaimKill must refuse so the handler returns 409 and leaves the snapshot
// intact, rather than deleting it and letting the resume resurrect the sandbox.
func TestClaimKill_ResumePending_Refuses(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	finishStart, _, err := storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	require.NotNil(t, finishStart, "first reserve should win the reservation")

	claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.False(t, claimed, "kill must be refused while a resume is pending")
}

// TestClaimKill_AlreadyRunning_Refuses covers the resume that finished between
// StartRemoving finding no running record and the claim: the sandbox is back in
// the storage index. ClaimKill must refuse so the client retries the kill
// against the running sandbox via the normal locked path.
func TestClaimKill_AlreadyRunning_Refuses(t *testing.T) {
	t.Parallel()
	storage, client := setupTestReservationStorage(t)

	teamID := uuid.New()

	// Simulate a completed resume: the sandbox is present in the storage index,
	// which is what storage.Add does on publication.
	indexKey := storage_redis.GetSandboxStorageTeamIndexKey(teamID.String())
	require.NoError(t, client.SAdd(t.Context(), indexKey, testSandboxID).Err())

	claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	assert.False(t, claimed, "kill must be refused when the sandbox is already running")
}

// TestReleaseKillClaim_UnblocksResume covers the cleanup path: when the snapshot
// delete fails after a claim was taken, releasing the claim lets future resumes
// of that ID proceed instead of waiting out the claim TTL.
func TestReleaseKillClaim_UnblocksResume(t *testing.T) {
	t.Parallel()
	storage, _ := setupTestReservationStorage(t)

	teamID := uuid.New()

	claimed, err := storage.ClaimKill(t.Context(), teamID, testSandboxID)
	require.NoError(t, err)
	require.True(t, claimed)

	// While claimed, a resume is refused.
	_, _, err = storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.ErrorIs(t, err, sandboxtypes.ErrSandboxKilled)

	require.NoError(t, storage.ReleaseKillClaim(t.Context(), teamID, testSandboxID))

	// After release, the same ID can be reserved again.
	finishStart, _, err := storage.Reserve(t.Context(), teamID, testSandboxID, 10)
	require.NoError(t, err)
	assert.NotNil(t, finishStart)
}

// TestClaimKill_ConcurrentReserveAndClaim asserts the rendezvous is atomic:
// racing a resume's Reserve against a DELETE's ClaimKill, the two outcomes are
// always consistent — exactly one of "resume reserved" / "kill claimed" wins,
// never both, so a claimed kill is never resurrected and a reserved resume is
// never silently killed.
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
			c, err := storage.ClaimKill(t.Context(), teamID, sandboxID)
			if err == nil {
				claimed = c
			}
			done <- struct{}{}
		}()
		<-done
		<-done

		// If the kill was claimed, the resume must not have reserved (it either
		// lost the race and was refused, or has not started). If the resume
		// reserved first, the claim must have been refused.
		if claimed && reserved {
			t.Fatalf("iteration %d: both resume reserved and kill claimed for the same sandbox", i)
		}
	}
}
