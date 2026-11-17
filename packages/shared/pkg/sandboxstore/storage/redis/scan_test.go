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

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

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
// Storage.Add), registers the team in the global teams index and returns the
// IDs written. The IDs carry the team: a record key carries the sandbox ID
// alone, so two teams' seeds must not share one.
func seedTeamSandboxes(t *testing.T, client redis.UniversalClient, teamID uuid.UUID, count int, state sandboxtypes.State) []string {
	t.Helper()

	ids := make([]string, 0, count)
	pipe := client.Pipeline()
	for i := range count {
		sbx := makeIndexedSandbox(teamID, fmt.Sprintf("sbx-%s-%s-%04d", teamID.String()[:8], state, i), uuid.NewString(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		sbx.State = state
		sbx.NodeID = "test-node"

		data, err := json.Marshal(sbx)
		require.NoError(t, err)

		pipe.Set(t.Context(), recordKey(sbx.SandboxID), data, 0)
		pipe.SAdd(t.Context(), GetSandboxStorageTeamIndexKey(teamID.String()), sbx.SandboxID)
		ids = append(ids, sbx.SandboxID)
	}
	pipe.ZAdd(t.Context(), globalTeamsSet, redis.Z{Score: float64(time.Now().Unix()), Member: teamID.String()})
	_, err := pipe.Exec(t.Context())
	require.NoError(t, err)

	return ids
}
