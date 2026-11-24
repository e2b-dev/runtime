package reservation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	reservations_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/reservations/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

var notUTF8 = []byte{0xff, 0x00, 0xfe, 'r', 0xc3}

func newRecordStorage(t *testing.T, client goredis.UniversalClient) *storage_redis.Storage {
	t.Helper()

	storage, err := storage_redis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return storage
}

type waitEntered struct {
	sandboxtypes.ReservationStorage

	entered chan struct{}
}

func (w *waitEntered) WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) ([]byte, error) {
	w.entered <- struct{}{}

	return w.ReservationStorage.WaitForStart(ctx, teamID, sandboxID)
}

func (w *waitEntered) awaitEntered(t *testing.T) {
	t.Helper()

	select {
	case <-w.entered:
	case <-time.After(hangTimeout):
		require.Fail(t, "WaitForStart never reached the backend")
	}
}

type redisSetup struct {
	records *storage_redis.Storage
	client  reservationpb.ReservationServiceClient
	waits   *waitEntered
}

func setupRedis(t *testing.T, client goredis.UniversalClient, maxWait time.Duration) redisSetup {
	t.Helper()

	records := newRecordStorage(t, client)
	waits := &waitEntered{
		ReservationStorage: reservations_redis.NewReservationStorage(client, records.Notifier()),
		entered:            make(chan struct{}, 16),
	}

	return redisSetup{records: records, client: newClient(t, waits, maxWait), waits: waits}
}

func TestRedisReserveOutcomes(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), hangTimeout)

	t.Run("reserved then already pending", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		id := newIDs()
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING, reserve(ctx, t, s.client, id, 10))
	})

	t.Run("limit reached", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		first := newIDs()
		second := ids{team: first.team, sandbox: "other-" + first.sandbox}
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, first, 1))
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED, reserve(ctx, t, s.client, second, 1))
	})

	t.Run("limit zero", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED, reserve(ctx, t, s.client, newIDs(), 0))
	})

	t.Run("negative limit is unlimited", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		team := uuid.NewString()
		for i := range 5 {
			id := ids{team: team, sandbox: fmt.Sprintf("unlimited-%d", i)}
			assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, -1))
		}
	})

	t.Run("stored record", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		id := newIDs()
		require.NoError(t, s.records.Add(ctx, sandboxtypes.Sandbox{
			SandboxID:         id.sandbox,
			TemplateID:        "test-template",
			ClientID:          "test-client",
			TeamID:            uuid.MustParse(id.team),
			ExecutionID:       uuid.NewString(),
			StartTime:         time.Now(),
			EndTime:           time.Now().Add(time.Hour),
			MaxInstanceLength: time.Hour,
			State:             sandboxtypes.StateRunning,
		}))
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_EXISTS, reserve(ctx, t, s.client, id, 10))
	})

	t.Run("kill claimed", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		id := newIDs()
		require.True(t, claimKill(ctx, t, s.client, id).GetClaimed())
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_KILLED, reserve(ctx, t, s.client, id, 10))
	})
}

func TestRedisFinishedResultIsOpaque(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), hangTimeout)
	ctx := callContext(t)
	id := newIDs()

	require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))
	finishStart(ctx, t, s.client, id, notUTF8)

	resp, err := waitForStart(ctx, s.client, id)
	require.NoError(t, err)
	assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, resp.GetOutcome())
	assert.Equal(t, notUTF8, resp.GetResult())
}

func TestRedisWaitingThenFinished(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), 50*time.Millisecond)
	ctx := callContext(t)
	id := newIDs()

	require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))

	resp, err := waitForStart(ctx, s.client, id)
	require.NoError(t, err)
	assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING, resp.GetOutcome())
	assert.Empty(t, resp.GetResult())

	finishStart(ctx, t, s.client, id, notUTF8)

	resp, err = waitForStart(ctx, s.client, id)
	require.NoError(t, err)
	assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, resp.GetOutcome())
	assert.Equal(t, notUTF8, resp.GetResult())
}

func TestRedisWaitInFlight(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)

	t.Run("finished", func(t *testing.T) {
		t.Parallel()

		s := setupRedis(t, client, hangTimeout)
		other := dial(t, serve(t, s.waits.ReservationStorage, hangTimeout))
		ctx := callContext(t)
		id := newIDs()
		require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))

		done := make(chan *reservationpb.WaitForStartResponse, 1)
		go func() {
			resp, err := waitForStart(ctx, s.client, id)
			assert.NoError(t, err)
			done <- resp
		}()
		s.waits.awaitEntered(t)
		finishStart(ctx, t, other, id, notUTF8)

		resp := <-done
		assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, resp.GetOutcome())
		assert.Equal(t, notUTF8, resp.GetResult())
	})

	t.Run("released", func(t *testing.T) {
		t.Parallel()

		s := setupRedis(t, client, hangTimeout)
		other := dial(t, serve(t, s.waits.ReservationStorage, hangTimeout))
		ctx := callContext(t)
		id := newIDs()
		require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))

		done := make(chan *reservationpb.WaitForStartResponse, 1)
		go func() {
			resp, err := waitForStart(ctx, s.client, id)
			assert.NoError(t, err)
			done <- resp
		}()
		s.waits.awaitEntered(t)
		_, err := other.Release(ctx, reservationpb.ReleaseRequest_builder{TeamId: id.team, SandboxId: id.sandbox}.Build())
		require.NoError(t, err)

		resp := <-done
		assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_NO_RESULT, resp.GetOutcome())
		assert.Empty(t, resp.GetResult())
	})
}

func TestRedisNothingPendingIsNoResult(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), hangTimeout)
	ctx := callContext(t)

	resp, err := waitForStart(ctx, s.client, newIDs())
	require.NoError(t, err)
	assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_NO_RESULT, resp.GetOutcome())
}

func TestRedisKillClaims(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), hangTimeout)

	t.Run("free ID", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		id := newIDs()
		claim := claimKill(ctx, t, s.client, id)
		require.True(t, claim.GetClaimed())
		require.NotEmpty(t, claim.GetClaimId())
		require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_KILLED, reserve(ctx, t, s.client, id, 10))

		_, err := s.client.ReleaseKillClaim(ctx, reservationpb.ReleaseKillClaimRequest_builder{
			TeamId: id.team, SandboxId: id.sandbox, ClaimId: claim.GetClaimId(),
		}.Build())
		require.NoError(t, err)
		assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))
	})

	t.Run("pending start", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		id := newIDs()
		require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, s.client, id, 10))
		claim := claimKill(ctx, t, s.client, id)
		assert.False(t, claim.GetClaimed())
		assert.Empty(t, claim.GetClaimId())
	})
}

func TestRedisCallerDeadline(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), time.Hour)
	id := newIDs()
	require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(callContext(t), t, s.client, id, 10))

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := waitForStart(ctx, s.client, id)
	requireCode(t, err, codes.DeadlineExceeded)
}

func TestRedisTwoServers(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	first := setupRedis(t, client, hangTimeout)
	second := setupRedis(t, client, hangTimeout)
	ctx := callContext(t)
	id := newIDs()

	require.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserve(ctx, t, first.client, id, 10))

	done := make(chan *reservationpb.WaitForStartResponse, 1)
	go func() {
		resp, err := waitForStart(ctx, first.client, id)
		assert.NoError(t, err)
		done <- resp
	}()
	first.waits.awaitEntered(t)
	finishStart(ctx, t, second.client, id, notUTF8)

	resp := <-done
	assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, resp.GetOutcome())
	assert.Equal(t, notUTF8, resp.GetResult())
}
