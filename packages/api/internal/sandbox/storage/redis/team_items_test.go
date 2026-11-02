package redis

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// TeamItems is on the sandbox-listing request path and had no coverage. These
// pin its observable behaviour — which records it returns, which it filters,
// and what it tolerates — so that sharing its decode step with the scanner can
// be shown to preserve it rather than merely asserted to.

// seedTeamSandbox writes one sandbox record and its team index entry directly,
// bypassing Storage.Add so the record's state can be set freely.
func seedTeamSandbox(t *testing.T, teamID uuid.UUID, sandboxID string, state sandboxtypes.State) sandboxtypes.Sandbox {
	t.Helper()

	sbx := makeIndexedSandbox(teamID, sandboxID, uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	sbx.State = state

	return sbx
}

func writeTeamSandbox(t *testing.T, storage *Storage, sbx sandboxtypes.Sandbox) {
	t.Helper()

	team := sbx.TeamID.String()
	writeRecord(t, storage.redisClient, recordKey(formTeam, team, sbx.SandboxID), GetSandboxStorageTeamIndexKey(team), sbx)
}

func sandboxIDsOf(sbxs []sandboxtypes.Sandbox) []string {
	ids := make([]string, 0, len(sbxs))
	for _, s := range sbxs {
		ids = append(ids, s.SandboxID)
	}

	return ids
}

func TestTeamItems_FiltersByState(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	teamID := uuid.New()

	running := seedTeamSandbox(t, teamID, "sbx-running", sandboxtypes.StateRunning)
	killing := seedTeamSandbox(t, teamID, "sbx-killing", sandboxtypes.StateKilling)
	pausing := seedTeamSandbox(t, teamID, "sbx-pausing", sandboxtypes.StatePausing)
	for _, sbx := range []sandboxtypes.Sandbox{running, killing, pausing} {
		writeTeamSandbox(t, storage, sbx)
	}

	t.Run("single state", func(t *testing.T) {
		t.Parallel()

		items, err := storage.TeamItems(t.Context(), teamID, []sandboxtypes.State{sandboxtypes.StateRunning})
		require.NoError(t, err)
		assert.Equal(t, []string{"sbx-running"}, sandboxIDsOf(items))
	})

	t.Run("several states", func(t *testing.T) {
		t.Parallel()

		items, err := storage.TeamItems(t.Context(), teamID, []sandboxtypes.State{sandboxtypes.StateRunning, sandboxtypes.StatePausing})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"sbx-running", "sbx-pausing"}, sandboxIDsOf(items))
	})

	t.Run("no states returns every record", func(t *testing.T) {
		t.Parallel()

		items, err := storage.TeamItems(t.Context(), teamID, nil)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"sbx-running", "sbx-killing", "sbx-pausing"}, sandboxIDsOf(items))
	})
}

// TestTeamItems_ToleratesStaleIndexEntries pins the tolerance the scanner's
// decode also relies on: an ID left in the team index after its record was
// deleted is skipped, not an error.
func TestTeamItems_ToleratesStaleIndexEntries(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	teamID := uuid.New()

	writeTeamSandbox(t, storage, seedTeamSandbox(t, teamID, "sbx-live", sandboxtypes.StateRunning))
	require.NoError(t, client.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), "sbx-deleted").Err())

	items, err := storage.TeamItems(t.Context(), teamID, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sbx-live"}, sandboxIDsOf(items))
}

// TestTeamItems_SkipsUndecodableRecord pins that one corrupt record does not
// fail the listing for the rest of the team.
func TestTeamItems_SkipsUndecodableRecord(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	teamID := uuid.New()

	writeTeamSandbox(t, storage, seedTeamSandbox(t, teamID, "sbx-live", sandboxtypes.StateRunning))

	require.NoError(t, client.Set(t.Context(), getSandboxKey(teamID.String(), "sbx-corrupt"), "not-json", 0).Err())
	require.NoError(t, client.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), "sbx-corrupt").Err())

	items, err := storage.TeamItems(t.Context(), teamID, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sbx-live"}, sandboxIDsOf(items))
}

func TestTeamItems_EmptyTeam(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	items, err := storage.TeamItems(t.Context(), uuid.New(), nil)
	require.NoError(t, err)
	assert.Empty(t, items)
}

// TestTeamItems_SpansSeveralBatches pins that a team larger than one scan
// batch is returned complete and without repeats. Sized so the scan needs
// more than two full batches plus a partial one.
func TestTeamItems_SpansSeveralBatches(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	teamID := uuid.New()

	running := sandboxScanBatchSize*2 + 37
	seedTeamSandboxes(t, client, teamID, running, sandboxtypes.StateRunning)
	seedTeamSandboxes(t, client, teamID, 5, sandboxtypes.StateKilling)

	items, err := storage.TeamItems(t.Context(), teamID, []sandboxtypes.State{sandboxtypes.StateRunning})
	require.NoError(t, err)

	ids := sandboxIDsOf(items)
	assert.Len(t, ids, running)
	assert.Len(t, slices.Compact(slices.Sorted(slices.Values(ids))), running, "no sandbox listed twice")
	for _, sbx := range items {
		assert.Equal(t, sandboxtypes.StateRunning, sbx.State)
	}
}

// TestTeamItems_IsScopedToOneTeam pins that the listing cannot leak another
// team's sandboxes — the reason the decode has to stay team-keyed.
func TestTeamItems_IsScopedToOneTeam(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	teamA, teamB := uuid.New(), uuid.New()

	writeTeamSandbox(t, storage, seedTeamSandbox(t, teamA, "sbx-a", sandboxtypes.StateRunning))
	writeTeamSandbox(t, storage, seedTeamSandbox(t, teamB, "sbx-b", sandboxtypes.StateRunning))

	items, err := storage.TeamItems(t.Context(), teamA, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sbx-a"}, sandboxIDsOf(items))
}
