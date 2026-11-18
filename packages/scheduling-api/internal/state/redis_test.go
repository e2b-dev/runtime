package state_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/types/known/timestamppb"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

type redisSetup struct {
	records *storage_redis.Storage
	client  statepb.StateServiceClient
	remote  *remote.Storage
}

func setupRedis(t *testing.T) redisSetup {
	t.Helper()

	return setupRedisOn(t, redis_utils.SetupInstance(t))
}

func setupRedisOn(t *testing.T, client goredis.UniversalClient) redisSetup {
	t.Helper()

	records, err := storage_redis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go records.Start(t.Context())
	t.Cleanup(func() { records.Close(context.WithoutCancel(t.Context())) })

	conn := newConnWithWait(t, records, hangTimeout)

	return redisSetup{records: records, client: statepb.NewStateServiceClient(conn), remote: remote.NewStorage(conn)}
}

func (s redisSetup) add(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox) statepb.StateOutcome {
	t.Helper()

	resp, err := s.client.Add(ctx, statepb.AddRequest_builder{Record: encode(t, sbx)}.Build())
	require.NoError(t, err)

	return resp.GetOutcome()
}

func (s redisSetup) get(ctx context.Context, t *testing.T, teamID uuid.UUID, sandboxID string) *statepb.GetResponse {
	t.Helper()

	resp, err := s.client.Get(ctx, statepb.GetRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build())
	require.NoError(t, err)

	return resp
}

func (s redisSetup) remove(ctx context.Context, t *testing.T, teamID uuid.UUID, sandboxID, executionID string) statepb.StateOutcome {
	t.Helper()

	resp, err := s.client.Remove(ctx, statepb.RemoveRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, ExpectedExecutionId: executionID,
	}.Build())
	require.NoError(t, err)

	return resp.GetOutcome()
}

func (s redisSetup) hold(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox) {
	t.Helper()

	require.NoError(t, s.records.HoldExecution(ctx, sandboxtypes.Hold{
		TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: sbx.NodeID, ID: "hold-" + uuid.NewString(),
	}, time.Now().Add(time.Hour)))
}

func newSandboxID() string {
	return "sbx-" + uuid.NewString()
}

func TestRedisRecordFidelity(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)

	tests := map[string]sandboxtypes.Sandbox{
		"full":    fullRecord(uuid.New(), newSandboxID()),
		"minimal": minimalRecord(uuid.New(), newSandboxID()),
	}
	requireNoZeroField(t, tests["full"])

	for name, original := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := callContext(t)
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, original))

			resp := s.get(ctx, t, original.TeamID, original.SandboxID)
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
			assert.Equal(t, string(encode(t, original)), string(resp.GetRecord()), "the record's JSON survives Add and Get")

			got := decode(t, resp.GetRecord())
			assert.Equal(t, original, got, "the decoded record equals the original")
			for _, times := range [][2]time.Time{
				{original.StartTime, got.StartTime},
				{original.EndTime, got.EndTime},
				{original.RefusedUntil, got.RefusedUntil},
				{original.RefusedSince, got.RefusedSince},
				{original.EnvdRefusedSince, got.EnvdRefusedSince},
				{original.EnvdRefusedUntil, got.EnvdRefusedUntil},
			} {
				assert.True(t, times[0].Equal(times[1]), "time %v came back as %v", times[0], times[1])
			}

			stored, err := s.records.Get(ctx, original.TeamID, original.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, string(encode(t, stored)), string(resp.GetRecord()), "the RPC returns what the store's own Get returns")
			assert.Equal(t, stored, got)
		})
	}

	t.Run("minimal keeps nil", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		original := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, original))
		got := decode(t, s.get(ctx, t, original.TeamID, original.SandboxID).GetRecord())
		assert.Nil(t, got.Alias)
		assert.Nil(t, got.EnvdAccessToken)
		assert.Nil(t, got.TrafficAccessToken)
		assert.Nil(t, got.Network)
		assert.Nil(t, got.NetworkPlacement)
		assert.Nil(t, got.PausedTTL)
		assert.Nil(t, got.Metadata)
		assert.Nil(t, got.VolumeMounts)
	})
}

func TestRedisAdd(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)

	t.Run("same execution twice keeps the first", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		first := minimalRecord(uuid.New(), newSandboxID())
		first.TemplateID = "first"
		second := first
		second.TemplateID = "second"

		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, first))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, second))
		stored, err := s.records.Get(ctx, first.TeamID, first.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, "first", stored.TemplateID)
	})

	t.Run("held", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		held := minimalRecord(uuid.New(), newSandboxID())
		held.NodeID = "node"
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, held))
		s.hold(ctx, t, held)

		replacement := held
		replacement.ExecutionID = uuid.NewString()
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, s.add(ctx, t, replacement))
		stored, err := s.records.Get(ctx, held.TeamID, held.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, held.ExecutionID, stored.ExecutionID)
	})
}

func TestRedisGetNotFound(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)

	resp := s.get(ctx, t, uuid.New(), newSandboxID())
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, resp.GetOutcome())
	assert.Empty(t, resp.GetRecord())

	stored := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, stored))
	resp = s.get(ctx, t, uuid.New(), stored.SandboxID)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, resp.GetOutcome(), "another team's record is not found")
	assert.Empty(t, resp.GetRecord())
}

func TestRedisRemove(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)

	t.Run("stored", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.remove(ctx, t, sbx.TeamID, sbx.SandboxID, sbx.ExecutionID))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.get(ctx, t, sbx.TeamID, sbx.SandboxID).GetOutcome())
	})

	t.Run("any execution", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.remove(ctx, t, sbx.TeamID, sbx.SandboxID, ""))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.get(ctx, t, sbx.TeamID, sbx.SandboxID).GetOutcome())
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.remove(ctx, t, uuid.New(), newSandboxID(), ""))
	})

	t.Run("another execution expected", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, s.remove(ctx, t, sbx.TeamID, sbx.SandboxID, uuid.NewString()))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.get(ctx, t, sbx.TeamID, sbx.SandboxID).GetOutcome(), "the record stays")
	})

	t.Run("held", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		sbx.NodeID = "node"
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		s.hold(ctx, t, sbx)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, s.remove(ctx, t, sbx.TeamID, sbx.SandboxID, ""))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.get(ctx, t, sbx.TeamID, sbx.SandboxID).GetOutcome(), "the record stays")
	})
}

func teamItems(ctx context.Context, t *testing.T, client statepb.StateServiceClient, teamID uuid.UUID, states ...statepb.SandboxState) ([]string, int) {
	t.Helper()

	stream, err := client.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: teamID.String(), States: states}.Build())
	require.NoError(t, err)
	records, messages, err := readTeamItems(t, stream)
	require.NoError(t, err)

	ids := make([]string, 0, len(records))
	for _, record := range records {
		sbx := decode(t, record)
		assert.Equal(t, teamID, sbx.TeamID)
		ids = append(ids, sbx.SandboxID)
	}
	slices.Sort(ids)

	return ids, messages
}

func TestRedisTeamItems(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	team := uuid.New()
	other := uuid.New()

	cycle := []sandboxtypes.State{sandboxtypes.StateRunning, sandboxtypes.StatePausing, sandboxtypes.StateSnapshotting}
	var all, pausing []string
	for i := range 600 {
		sbx := minimalRecord(team, fmt.Sprintf("sbx-%03d-%s", i, uuid.NewString()))
		sbx.State = cycle[i%len(cycle)]
		require.NoError(t, s.records.Add(ctx, sbx))
		all = append(all, sbx.SandboxID)
		if sbx.State == sandboxtypes.StatePausing {
			pausing = append(pausing, sbx.SandboxID)
		}
	}
	for i := range 5 {
		require.NoError(t, s.records.Add(ctx, minimalRecord(other, fmt.Sprintf("other-%d", i))))
	}
	slices.Sort(all)
	slices.Sort(pausing)

	ids, messages := teamItems(ctx, t, s.client, team)
	assert.Equal(t, all, ids, "every record of the team, once")
	assert.GreaterOrEqual(t, messages, 3)

	ids, _ = teamItems(ctx, t, s.client, team, statepb.SandboxState_SANDBOX_STATE_PAUSING)
	assert.Len(t, ids, 200)
	assert.Equal(t, pausing, ids)

	ids, _ = teamItems(ctx, t, s.client, team,
		statepb.SandboxState_SANDBOX_STATE_RUNNING,
		statepb.SandboxState_SANDBOX_STATE_PAUSING,
		statepb.SandboxState_SANDBOX_STATE_KILLING,
		statepb.SandboxState_SANDBOX_STATE_SNAPSHOTTING,
	)
	assert.Equal(t, all, ids)

	ids, messages = teamItems(ctx, t, s.client, uuid.New())
	assert.Empty(t, ids)
	assert.Zero(t, messages, "a team without records gets no message")
}

func TestRedisExpiredItems(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)

	expired := minimalRecord(uuid.New(), newSandboxID())
	expired.EndTime = time.Now().Add(-time.Minute).UTC()
	live := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, expired))
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, live))

	stream, err := s.client.ExpiredItems(ctx, statepb.ExpiredItemsRequest_builder{}.Build())
	require.NoError(t, err)
	records, messages, err := readExpiredItems(t, stream)
	require.NoError(t, err)
	assert.Equal(t, 1, messages)
	require.Len(t, records, 1)
	assert.Equal(t, expired, decode(t, records[0]))
}

func TestRedisTeamsWithSandboxCount(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)

	first, second := uuid.New(), uuid.New()
	for _, team := range []uuid.UUID{first, first, second} {
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, minimalRecord(team, newSandboxID())))
	}

	resp, err := s.client.TeamsWithSandboxCount(ctx, statepb.TeamsWithSandboxCountRequest_builder{}.Build())
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{first.String(): 2, second.String(): 1}, resp.GetCounts())
}

func TestRedisReconcile(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	team := uuid.New()
	old := time.Now().Add(-time.Hour)

	stored := minimalRecord(team, newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, stored))
	orphan := newSandboxID()

	report := func(sandboxID string, startTime time.Time) *statepb.NodeSandbox {
		return statepb.NodeSandbox_builder{
			SandboxId: sandboxID,
			TeamId:    team.String(),
			NodeId:    "node",
			ClusterId: uuid.Nil.String(),
			StartTime: timestamppb.New(startTime),
		}.Build()
	}
	resp, err := s.client.Reconcile(ctx, statepb.ReconcileRequest_builder{
		NodeId: "node",
		Sandboxes: []*statepb.NodeSandbox{
			report(stored.SandboxID, old),
			report(orphan, old),
			report(newSandboxID(), time.Now()),
			report("", old),
			report("a{b", old),
		},
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetOrphans(), 1, "only the old sandbox the store does not know is an orphan; invalid IDs are skipped")
	assert.Equal(t, team.String(), resp.GetOrphans()[0].GetTeamId())
	assert.Equal(t, orphan, resp.GetOrphans()[0].GetSandboxId())
}
