package redis

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

func TestAllRunningItems_Empty(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)

	items, err := storage.AllRunningItems(t.Context())
	require.NoError(t, err)
	assert.Empty(t, items)
}

func TestAllRunningItems_MultipleBatchesAndStateFilter(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamA := uuid.New()
	teamB := uuid.New()

	// Force >2 scan batches with a partial tail for team A.
	totalA := sandboxScanBatchSize*2 + 37
	seedTeamSandboxes(t, client, teamA, totalA, sandboxtypes.StateRunning)

	// Team B: a few running plus non-running records that must be filtered.
	seedTeamSandboxes(t, client, teamB, 5, sandboxtypes.StateRunning)
	seedTeamSandboxes(t, client, teamB, 3, sandboxtypes.StateKilling)

	items, err := storage.AllRunningItems(t.Context())
	require.NoError(t, err)

	assert.Len(t, items, totalA+5, "must return all running sandboxes across teams and skip non-running states")

	perTeam := map[uuid.UUID]int{}
	for _, sbx := range items {
		assert.Equal(t, sandboxtypes.StateRunning, sbx.State)
		perTeam[sbx.TeamID]++
	}
	assert.Equal(t, totalA, perTeam[teamA])
	assert.Equal(t, 5, perTeam[teamB])
}

func TestAllRunningItems_ToleratesStaleIndexEntries(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	teamID := uuid.New()
	seedTeamSandboxes(t, client, teamID, 2, sandboxtypes.StateRunning)

	// Stale index entry: ID in the team set without a sandbox key.
	require.NoError(t, client.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), "sbx-deleted").Err())

	items, err := storage.AllRunningItems(t.Context())
	require.NoError(t, err)
	assert.Len(t, items, 2)
}

// TestAllRunningItems_SkipsUnreadableTeam pins the isolation contract the
// dead-node sweep depends on: one team whose records cannot be decoded must
// not hide the other teams' sandboxes.
func TestAllRunningItems_SkipsUnreadableTeam(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	good := uuid.New()
	bad := uuid.New()
	seedTeamSandboxes(t, client, good, 3, sandboxtypes.StateRunning)

	// The bad team's index key holds a string instead of a set, so SSCAN fails
	// with WRONGTYPE and the whole team is skipped.
	require.NoError(t, client.Set(t.Context(), GetSandboxStorageTeamIndexKey(bad.String()), "not-a-set", 0).Err())
	require.NoError(t, client.ZAdd(t.Context(), globalTeamsSet, redis.Z{Score: float64(time.Now().Unix()), Member: bad.String()}).Err())

	items, err := storage.AllRunningItems(t.Context())
	require.NoError(t, err)
	assert.Len(t, items, 3)
}

// TestScannedItems_DropsRepeatsAcrossBatches covers the SSCAN guarantee that
// bites here: members present for the whole iteration are returned *at least*
// once, so a team index that rehashes mid-scan can re-emit one on a later
// page. Without this, a caller acting per record acts on that sandbox twice.
//
// Driven against the accumulator rather than through Redis on purpose: a set
// cannot hold a duplicate member, so the repeat can only come from SSCAN's
// internal rehashing, which there is no deterministic way to provoke from a
// test. The batches below are what such a scan hands the callback.
func TestScannedItems_DropsRepeatsAcrossBatches(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	sbx := func(id string, state sandboxtypes.State) sandboxtypes.Sandbox {
		return sandboxtypes.Sandbox{SandboxID: id, TeamID: teamID, State: state}
	}

	items := newScannedItems([]sandboxtypes.State{sandboxtypes.StateRunning})
	items.add([]sandboxtypes.Sandbox{
		sbx("sbx-a", sandboxtypes.StateRunning),
		sbx("sbx-b", sandboxtypes.StateRunning),
		sbx("sbx-paused", sandboxtypes.StateKilling),
	})
	// The rehash re-emits sbx-a on a later page, alongside a new record.
	items.add([]sandboxtypes.Sandbox{
		sbx("sbx-a", sandboxtypes.StateRunning),
		sbx("sbx-c", sandboxtypes.StateRunning),
	})
	// And again, in the same page as itself.
	items.add([]sandboxtypes.Sandbox{
		sbx("sbx-b", sandboxtypes.StateRunning),
		sbx("sbx-b", sandboxtypes.StateRunning),
	})

	ids := make([]string, 0, len(items.out))
	for _, s := range items.out {
		ids = append(ids, s.SandboxID)
	}

	assert.Equal(t, []string{"sbx-a", "sbx-b", "sbx-c"}, ids,
		"each record once, in first-seen order, non-running filtered")
}

// TestScannedItems_SeparatesIdenticalIDsAcrossTeams pins the team scoping of
// the identity: records are keyed per team, so deduplication has to be too.
func TestScannedItems_SeparatesIdenticalIDsAcrossTeams(t *testing.T) {
	t.Parallel()

	teamA, teamB := uuid.New(), uuid.New()

	items := newScannedItems([]sandboxtypes.State{sandboxtypes.StateRunning})
	items.add([]sandboxtypes.Sandbox{
		{SandboxID: "sbx-1", TeamID: teamA, State: sandboxtypes.StateRunning},
		{SandboxID: "sbx-1", TeamID: teamB, State: sandboxtypes.StateRunning},
	})

	assert.Len(t, items.out, 2, "the same ID under two teams is two records")
}

// TestScannedItems_StateFilter pins the filter TeamItems relies on: an empty
// state list keeps every record, a non-empty one keeps only matching states.
func TestScannedItems_StateFilter(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	batch := []sandboxtypes.Sandbox{
		{SandboxID: "sbx-running", TeamID: teamID, State: sandboxtypes.StateRunning},
		{SandboxID: "sbx-pausing", TeamID: teamID, State: sandboxtypes.StatePausing},
		{SandboxID: "sbx-killing", TeamID: teamID, State: sandboxtypes.StateKilling},
	}

	all := newScannedItems(nil)
	all.add(batch)
	assert.Equal(t, []string{"sbx-running", "sbx-pausing", "sbx-killing"}, sandboxIDsOf(all.out))

	some := newScannedItems([]sandboxtypes.State{sandboxtypes.StateRunning, sandboxtypes.StatePausing})
	some.add(batch)
	assert.Equal(t, []string{"sbx-running", "sbx-pausing"}, sandboxIDsOf(some.out))
}

// seedTeamSandboxes writes sandbox records + index entries directly (bypassing
// Storage.Add) and registers the team in the global teams index.
func seedTeamSandboxes(t *testing.T, client redis.UniversalClient, teamID uuid.UUID, count int, state sandboxtypes.State) {
	t.Helper()

	pipe := client.Pipeline()
	for i := range count {
		sbx := makeIndexedSandbox(teamID, fmt.Sprintf("sbx-%s-%04d", state, i), uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		sbx.State = state
		sbx.NodeID = "test-node"

		data, err := json.Marshal(sbx)
		require.NoError(t, err)

		pipe.Set(t.Context(), getSandboxKey(teamID.String(), sbx.SandboxID), data, 0)
		pipe.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), sbx.SandboxID)
	}
	pipe.ZAdd(t.Context(), globalTeamsSet, redis.Z{Score: float64(time.Now().Unix()), Member: teamID.String()})
	_, err := pipe.Exec(t.Context())
	require.NoError(t, err)
}
