package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

func (s redisSetup) begin(ctx context.Context, t *testing.T, teamID uuid.UUID, sandboxID string) *statepb.BeginUpdateResponse {
	t.Helper()

	resp, err := s.client.BeginUpdate(ctx, statepb.BeginUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build())
	require.NoError(t, err)

	return resp
}

func (s redisSetup) commit(ctx context.Context, t *testing.T, token []byte, sbx sandboxtypes.Sandbox) *statepb.CommitUpdateResponse {
	t.Helper()

	resp, err := s.client.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{
		TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, Token: token, Record: encode(t, sbx),
	}.Build())
	require.NoError(t, err)

	return resp
}

func (s redisSetup) abort(ctx context.Context, t *testing.T, teamID uuid.UUID, sandboxID string, token []byte) statepb.StateOutcome {
	t.Helper()

	resp, err := s.client.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, Token: token}.Build())
	require.NoError(t, err)

	return resp.GetOutcome()
}

func (s redisSetup) expired(ctx context.Context, t *testing.T) [][]byte {
	t.Helper()

	stream, err := s.client.ExpiredItems(ctx, statepb.ExpiredItemsRequest_builder{}.Build())
	require.NoError(t, err)
	records, _, err := readExpiredItems(t, stream)
	require.NoError(t, err)

	return records
}

func (s redisSetup) stored(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox) sandboxtypes.Sandbox {
	t.Helper()

	stored, err := s.records.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)

	return stored
}

func (s redisSetup) requireLockFree(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox) {
	t.Helper()

	resp := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, resp.GetToken()))
}

func (s redisSetup) addHeldRecord(ctx context.Context, t *testing.T) sandboxtypes.Sandbox {
	t.Helper()

	sbx := minimalRecord(uuid.New(), newSandboxID())
	sbx.NodeID = "node-a"
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

	return sbx
}

func holdOf(sbx sandboxtypes.Sandbox, id string) *statepb.Hold {
	return statepb.Hold_builder{
		TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, ExecutionId: sbx.ExecutionID, SourceNodeId: sbx.NodeID, Id: id,
	}.Build()
}

func (s redisSetup) holdRPC(ctx context.Context, t *testing.T, hold *statepb.Hold, until time.Time) statepb.StateOutcome {
	t.Helper()

	resp, err := s.client.HoldExecution(ctx, statepb.HoldExecutionRequest_builder{Hold: hold, Until: timestamppb.New(until)}.Build())
	require.NoError(t, err)

	return resp.GetOutcome()
}

func (s redisSetup) move(ctx context.Context, t *testing.T, hold *statepb.Hold) *statepb.MoveHeldExecutionResponse {
	t.Helper()

	resp, err := s.client.MoveHeldExecution(ctx, statepb.MoveHeldExecutionRequest_builder{Hold: hold, NodeId: "node-b"}.Build())
	require.NoError(t, err)

	return resp
}

func (s redisSetup) release(ctx context.Context, t *testing.T, hold *statepb.Hold) statepb.StateOutcome {
	t.Helper()

	resp, err := s.client.ReleaseHeldExecution(ctx, statepb.ReleaseHeldExecutionRequest_builder{Hold: hold}.Build())
	require.NoError(t, err)

	return resp.GetOutcome()
}

func (s redisSetup) removeHeld(ctx context.Context, t *testing.T, hold *statepb.Hold) *statepb.RemoveHeldExecutionResponse {
	t.Helper()

	resp, err := s.client.RemoveHeldExecution(ctx, statepb.RemoveHeldExecutionRequest_builder{Hold: hold}.Build())
	require.NoError(t, err)

	return resp
}

func TestRedisUpdateCommit(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

	require.Empty(t, s.expired(ctx, t))

	begun := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())
	require.NotEmpty(t, begun.GetToken())
	updated := decode(t, begun.GetRecord())
	assert.Equal(t, sbx, updated, "the record as stored")

	updated.TemplateID = "changed"
	updated.EndTime = time.Now().Add(-time.Minute).UTC()
	committed := s.commit(ctx, t, begun.GetToken(), updated)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, committed.GetOutcome())
	assert.Equal(t, updated, decode(t, committed.GetRecord()))
	assert.Equal(t, updated, s.stored(ctx, t, sbx))

	expired := s.expired(ctx, t)
	require.Len(t, expired, 1, "the commit re-scored the expiration")
	assert.Equal(t, updated, decode(t, expired[0]))

	s.requireLockFree(ctx, t, sbx)
}

func TestRedisUpdateAbort(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

	begun := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, begun.GetToken()))
	assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, begun.GetToken()), "a second abort finds the lock released")

	s.requireLockFree(ctx, t, sbx)
}

func TestRedisUpdateInvalidToken(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

	begun := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())
	updated := decode(t, begun.GetRecord())
	updated.TemplateID = "never"

	for _, invalid := range [][]byte{[]byte("malformed"), []byte(`{"lock":""}`), []byte(`{"lock":1}`)} {
		_, err := s.client.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{
			TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, Token: invalid, Record: encode(t, updated),
		}.Build())
		requireCode(t, err, codes.InvalidArgument)
		_, err = s.client.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, Token: invalid}.Build())
		requireCode(t, err, codes.InvalidArgument)
	}
	assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, begun.GetToken()), "the lock still holds the real token")
}

func TestRedisUpdateHoldsTheLock(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

	first := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, first.GetOutcome())

	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err := s.client.BeginUpdate(waitCtx, statepb.BeginUpdateRequest_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID}.Build())
	requireCode(t, err, codes.DeadlineExceeded)

	updated := decode(t, first.GetRecord())
	updated.TemplateID = "first"
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.commit(ctx, t, first.GetToken(), updated).GetOutcome())

	second := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, second.GetOutcome())
	assert.Equal(t, "first", decode(t, second.GetRecord()).TemplateID)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, second.GetToken()))
}

func TestRedisCommitAfterLockLost(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

	lost := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, lost.GetOutcome())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, lost.GetToken()))
	holder := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, holder.GetOutcome())

	updated := decode(t, lost.GetRecord())
	updated.TemplateID = "lost"
	resp := s.commit(ctx, t, lost.GetToken(), updated)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST, resp.GetOutcome())
	assert.Empty(t, resp.GetRecord())
	assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")

	updated.TemplateID = "holder"
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.commit(ctx, t, holder.GetToken(), updated).GetOutcome(), "the other holder's lock stands")
	assert.Equal(t, "holder", s.stored(ctx, t, sbx).TemplateID)
}

func TestRedisUpdateAcrossServers(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	first, second := setupRedisOn(t, client), setupRedisOn(t, client)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, first.add(ctx, t, sbx))

	begun := first.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, second.abort(ctx, t, sbx.TeamID, sbx.SandboxID, begun.GetToken()))
	assert.Equal(t, sbx, first.stored(ctx, t, sbx))

	begun = first.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())
	updated := decode(t, begun.GetRecord())
	updated.TemplateID = "second"
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, second.commit(ctx, t, begun.GetToken(), updated).GetOutcome())
	assert.Equal(t, updated, first.stored(ctx, t, sbx))

	first.requireLockFree(ctx, t, sbx)
}

func TestRedisBeginRefusals(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	held := s.addHeldRecord(ctx, t)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, holdOf(held, "hold"), time.Now().Add(time.Hour)))
	other := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, other))

	for range 2 {
		absent := s.begin(ctx, t, uuid.New(), newSandboxID())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, absent.GetOutcome())
		assert.Empty(t, absent.GetToken())

		refused := s.begin(ctx, t, held.TeamID, held.SandboxID)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, refused.GetOutcome())
		assert.Empty(t, refused.GetToken())
		assert.Empty(t, refused.GetRecord())
	}

	s.requireLockFree(ctx, t, other)
}

func TestRedisUpdateRecordThroughRPC(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	updates := s.remote

	t.Run("commits", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

		committed, err := sandboxtypes.UpdateRecord(ctx, updates, sbx.TeamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
			cur.TemplateID = "updated"

			return cur, nil
		})
		require.NoError(t, err)
		assert.Equal(t, "updated", committed.TemplateID)
		assert.Equal(t, committed, s.stored(ctx, t, sbx))
		s.requireLockFree(ctx, t, sbx)
	})

	t.Run("an error aborts", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

		failure := errors.New("update refused")
		_, err := sandboxtypes.UpdateRecord(ctx, updates, sbx.TeamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
			cur.TemplateID = "never"

			return cur, failure
		})
		require.ErrorIs(t, err, failure)
		assert.Equal(t, sbx, s.stored(ctx, t, sbx))
		s.requireLockFree(ctx, t, sbx)
	})

	t.Run("a panic aborts and propagates", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

		assert.PanicsWithValue(t, "update panicked", func() {
			_, _ = sandboxtypes.UpdateRecord(ctx, updates, sbx.TeamID, sbx.SandboxID, func(sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
				panic("update panicked")
			})
		})
		assert.Equal(t, sbx, s.stored(ctx, t, sbx))
		s.requireLockFree(ctx, t, sbx)
	})

	t.Run("a held record", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		held := s.addHeldRecord(ctx, t)
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, holdOf(held, "hold"), time.Now().Add(time.Hour)))

		_, err := sandboxtypes.UpdateRecord(ctx, updates, held.TeamID, held.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
			return cur, nil
		})
		require.ErrorIs(t, err, sandboxtypes.ErrLifecycleHeld)
	})
}

func TestRedisHolds(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	hour := time.Now().Add(time.Hour)

	t.Run("move", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := s.addHeldRecord(ctx, t)
		hold := holdOf(sbx, "hold")
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, hold, hour))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, hold, hour), "the same hold again is a no-op")

		resp := s.move(ctx, t, hold)
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.True(t, resp.GetMoved())
		stored := s.stored(ctx, t, sbx)
		assert.Equal(t, "node-b", stored.NodeID)
		assert.Empty(t, stored.LifecycleHold)
		assert.Zero(t, stored.LifecycleHoldUntil)

		resp = s.move(ctx, t, hold)
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.False(t, resp.GetMoved(), "a repeat writes nothing")
		assert.Equal(t, stored, s.stored(ctx, t, sbx))
	})

	t.Run("release", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := s.addHeldRecord(ctx, t)
		hold := holdOf(sbx, "hold")
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, hold, hour))
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.release(ctx, t, hold))

		stored := s.stored(ctx, t, sbx)
		assert.Equal(t, "node-a", stored.NodeID)
		assert.Empty(t, stored.LifecycleHold)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_HELD, s.release(ctx, t, hold), "released already")
		s.requireLockFree(ctx, t, sbx)
	})

	t.Run("remove", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := s.addHeldRecord(ctx, t)
		hold := holdOf(sbx, "hold")
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, hold, hour))
		before := s.stored(ctx, t, sbx)

		resp := s.removeHeld(ctx, t, hold)
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.Equal(t, before, decode(t, resp.GetRecord()), "the record as it was")
		assert.Equal(t, "hold", decode(t, resp.GetRecord()).LifecycleHold)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.get(ctx, t, sbx.TeamID, sbx.SandboxID).GetOutcome())

		again := s.removeHeld(ctx, t, hold)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, again.GetOutcome())
		assert.Empty(t, again.GetRecord())
	})

	t.Run("refusals", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := s.addHeldRecord(ctx, t)
		hold := holdOf(sbx, "hold")
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, hold, hour))

		otherHold := holdOf(sbx, "other")
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT, s.holdRPC(ctx, t, otherHold, hour), "a second holder")
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_HELD, s.move(ctx, t, otherHold).GetOutcome())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_HELD, s.release(ctx, t, otherHold))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_HELD, s.removeHeld(ctx, t, otherHold).GetOutcome())

		otherExecution := sbx
		otherExecution.ExecutionID = uuid.NewString()
		mismatch := holdOf(otherExecution, "hold")
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT, s.holdRPC(ctx, t, mismatch, hour))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, s.move(ctx, t, mismatch).GetOutcome())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, s.release(ctx, t, mismatch))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, s.removeHeld(ctx, t, mismatch).GetOutcome())

		replacement := sbx
		replacement.ExecutionID = uuid.NewString()
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, s.add(ctx, t, replacement))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, s.remove(ctx, t, sbx.TeamID, sbx.SandboxID, ""))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, s.begin(ctx, t, sbx.TeamID, sbx.SandboxID).GetOutcome())

		stored := s.stored(ctx, t, sbx)
		assert.Equal(t, sbx.ExecutionID, stored.ExecutionID)
		assert.Equal(t, "hold", stored.LifecycleHold)
		assert.Equal(t, "node-a", stored.NodeID)
	})

	t.Run("absent record", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		absent := holdOf(minimalRecord(uuid.New(), newSandboxID()), "hold")
		absent.SetSourceNodeId("node-a")
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.holdRPC(ctx, t, absent, hour))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.move(ctx, t, absent).GetOutcome())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.release(ctx, t, absent))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, s.removeHeld(ctx, t, absent).GetOutcome())
	})

	t.Run("deadline passed", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := s.addHeldRecord(ctx, t)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT, s.holdRPC(ctx, t, holdOf(sbx, "hold"), time.Now().Add(-time.Second)))
		assert.Empty(t, s.stored(ctx, t, sbx).LifecycleHold)
	})
}

func (s redisSetup) requireLockFreeWithin(t *testing.T, sbx sandboxtypes.Sandbox, within time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), within)
	defer cancel()
	resp, err := s.client.BeginUpdate(ctx, statepb.BeginUpdateRequest_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID}.Build())
	require.NoError(t, err, "the lock is free well before its one-minute expiry")
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(callContext(t), t, sbx.TeamID, sbx.SandboxID, resp.GetToken()))
}

func TestRedisCommitRejectedRecordReleasesTheLock(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	tests := map[string]func(sbx sandboxtypes.Sandbox) sandboxtypes.Sandbox{
		"not indexable": func(sbx sandboxtypes.Sandbox) sandboxtypes.Sandbox {
			sbx.ExecutionID = ""

			return sbx
		},
		"of another team": func(sbx sandboxtypes.Sandbox) sandboxtypes.Sandbox {
			sbx.TeamID = uuid.New()

			return sbx
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := callContext(t)
			sbx := minimalRecord(uuid.New(), newSandboxID())
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
			begun := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())

			_, err := s.client.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{
				TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, Token: begun.GetToken(), Record: encode(t, mutate(decode(t, begun.GetRecord()))),
			}.Build())
			requireCode(t, err, codes.InvalidArgument)

			s.requireLockFreeWithin(t, sbx, 10*time.Second)
			assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")
		})
	}

	t.Run("UpdateRecord with an unindexable record", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

		_, err := sandboxtypes.UpdateRecord(ctx, s.remote, sbx.TeamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
			cur.ExecutionID = "not-a-uuid"

			return cur, nil
		})
		requireCode(t, err, codes.InvalidArgument)

		s.requireLockFreeWithin(t, sbx, 10*time.Second)
		assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")
	})
}

func TestRedisUpdateTokenIsBoundToItsTeam(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
	begun := s.begin(ctx, t, sbx.TeamID, sbx.SandboxID)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())

	moved := decode(t, begun.GetRecord())
	moved.TeamID = uuid.New()
	_, err := s.client.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{
		TeamId: moved.TeamID.String(), SandboxId: sbx.SandboxID, Token: begun.GetToken(), Record: encode(t, moved),
	}.Build())
	requireCode(t, err, codes.InvalidArgument)
	_, err = s.client.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: moved.TeamID.String(), SandboxId: sbx.SandboxID, Token: begun.GetToken()}.Build())
	requireCode(t, err, codes.InvalidArgument)

	assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.abort(ctx, t, sbx.TeamID, sbx.SandboxID, begun.GetToken()), "the lock still holds the token: neither released nor broken")
}

func TestRedisMoveWithoutNodeKeepsTheHold(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	sbx := s.addHeldRecord(ctx, t)
	hold := holdOf(sbx, "hold")
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, hold, time.Now().Add(time.Hour)))

	_, err := s.client.MoveHeldExecution(ctx, statepb.MoveHeldExecutionRequest_builder{Hold: hold}.Build())
	requireCode(t, err, codes.InvalidArgument)

	stored := s.stored(ctx, t, sbx)
	assert.Equal(t, "node-a", stored.NodeID)
	assert.Equal(t, "hold", stored.LifecycleHold)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.release(ctx, t, hold), "the hold is still there")
}
