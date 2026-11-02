package redis

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// A create's store write can land twice: the client retries a command that
// timed out, and a delayed segment of the first attempt can still reach Redis
// after later writes. The duplicate must not take the record back.
func TestAdd_ADuplicateOfTheSameExecutionKeepsTheStoredRecord(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	start := time.Now()
	sbx := makeIndexedSandbox(teamID, "sbx-duplicate-add", uuid.NewString(), start, start.Add(5*time.Minute))
	require.NoError(t, storage.Add(t.Context(), sbx))

	extended := start.Add(10 * time.Minute)
	_, err := storage.Update(t.Context(), teamID, sbx.SandboxID, func(s sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		s.EndTime = extended

		return s, nil
	})
	require.NoError(t, err)

	// The create-time copy, landing late.
	require.NoError(t, storage.Add(t.Context(), sbx))

	got, err := storage.Get(t.Context(), teamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, got.EndTime.Equal(extended), "the extended end time must survive the duplicate")
	requireMemberScore(t, client, expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID), float64(extended.UnixMilli()))
}

// A resume installs a new execution under the same sandbox ID; that write is
// not a duplicate and replaces the stored record.
func TestAdd_ANewExecutionReplacesTheStoredRecord(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	teamID := uuid.New()
	start := time.Now()
	old := makeIndexedSandbox(teamID, "sbx-new-execution", uuid.NewString(), start, start.Add(5*time.Minute))
	require.NoError(t, storage.Add(t.Context(), old))

	fresh := makeIndexedSandbox(teamID, "sbx-new-execution", uuid.NewString(), start.Add(time.Minute), start.Add(20*time.Minute))
	require.NoError(t, storage.Add(t.Context(), fresh))

	got, err := storage.Get(t.Context(), teamID, "sbx-new-execution")
	require.NoError(t, err)
	assert.Equal(t, fresh.ExecutionID, got.ExecutionID)
	assert.True(t, got.EndTime.Equal(fresh.EndTime))
}

// A re-add (a sync or reconcile, or a test installing a state) replaces the
// record of the same execution; only a create's write is idempotent.
func TestReplace_OverwritesTheSameExecution(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	start := time.Now()
	sbx := makeIndexedSandbox(teamID, "sbx-replace", uuid.NewString(), start, start.Add(5*time.Minute))
	require.NoError(t, storage.Add(t.Context(), sbx))

	changed := sbx
	changed.EndTime = start.Add(2 * time.Minute)
	require.NoError(t, storage.Replace(t.Context(), changed))

	got, err := storage.Get(t.Context(), teamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, got.EndTime.Equal(changed.EndTime))
	requireMemberScore(t, client, expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID), float64(changed.EndTime.UnixMilli()))
}

// A duplicate landing after the record was removed must not bring it back.
func TestAdd_AfterRemoveTheSameExecutionStaysGone(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	start := time.Now()
	sbx := makeIndexedSandbox(teamID, "sbx-gone", uuid.NewString(), start.Add(-2*time.Minute), start.Add(-time.Minute))
	require.NoError(t, storage.Add(t.Context(), sbx))
	require.NoError(t, storage.Remove(t.Context(), teamID, sbx.SandboxID))

	require.NoError(t, storage.Add(t.Context(), sbx))
	_, err := storage.Get(t.Context(), teamID, sbx.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)

	// The refused write's expiration member stays, as a removed execution's
	// leftover member does, and the sweep takes it once its score passes,
	// finding no record of its execution.
	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))
	items, err := storage.ExpiredItems(t.Context())
	require.NoError(t, err)
	require.Empty(t, items)
	requireMemberAbsent(t, client, member)
}

// A duplicate of an older execution landing after a resume installed a new
// one must not replace the live record.
func TestAdd_AnOlderExecutionNeverReplacesTheResumedOne(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	teamID := uuid.New()
	start := time.Now()
	old := makeIndexedSandbox(teamID, "sbx-resumed", uuid.NewString(), start, start.Add(5*time.Minute))
	require.NoError(t, storage.Add(t.Context(), old))
	fresh := makeIndexedSandbox(teamID, "sbx-resumed", uuid.NewString(), start.Add(time.Minute), start.Add(20*time.Minute))
	require.NoError(t, storage.Add(t.Context(), fresh))

	require.NoError(t, storage.Add(t.Context(), old))
	got, err := storage.Get(t.Context(), teamID, "sbx-resumed")
	require.NoError(t, err)
	assert.Equal(t, fresh.ExecutionID, got.ExecutionID)
}

// A sync re-add (Replace) of an execution the gone set holds reinstalls it:
// its record and member are back, and a late duplicate of its create is then
// a plain duplicate that keeps both, since a stored record of the execution
// is checked before the gone set.
func TestReplace_ReinstallsAGoneExecution(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	start := time.Now()
	sbx := makeIndexedSandbox(teamID, "sbx-reinstalled", uuid.NewString(), start, start.Add(5*time.Minute))
	require.NoError(t, storage.Add(t.Context(), sbx))
	require.NoError(t, storage.Remove(t.Context(), teamID, sbx.SandboxID))

	require.NoError(t, storage.Replace(t.Context(), sbx))
	got, err := storage.Get(t.Context(), teamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sbx.ExecutionID, got.ExecutionID)
	member := expirationMember(teamID.String(), sbx.SandboxID, sbx.ExecutionID)
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))

	require.NoError(t, storage.Add(t.Context(), sbx), "a late duplicate of the create")
	_, err = storage.Get(t.Context(), teamID, sbx.SandboxID)
	require.NoError(t, err, "the reinstalled record stays")
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))
}
