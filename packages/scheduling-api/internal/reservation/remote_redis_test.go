package reservation_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type env struct {
	team      uuid.UUID
	sandboxID string
}

func newEnv() env {
	return env{team: uuid.New(), sandboxID: "sbx-" + uuid.NewString()}
}

func (e env) normalize(t *testing.T, value any) string {
	t.Helper()

	data, err := json.Marshal(value)
	require.NoError(t, err)

	return strings.ReplaceAll(strings.ReplaceAll(string(data), e.team.String(), "TEAM"), e.sandboxID, "SANDBOX")
}

var storeSentinels = []error{
	sandboxtypes.ErrNotFound,
	sandboxtypes.ErrAlreadyExists,
	sandboxtypes.ErrSandboxKilled,
	sandboxtypes.ErrDraining,
	context.Canceled,
	context.DeadlineExceeded,
}

func requireSameTyped[E error](t *testing.T, direct, remote error, de, re env) {
	t.Helper()

	d, directOK := errors.AsType[E](direct)
	r, remoteOK := errors.AsType[E](remote)
	require.Equal(t, directOK, remoteOK, "both or neither are %T: direct %v, remote %v", d, direct, remote)
	if directOK {
		assert.Equal(t, de.normalize(t, d), re.normalize(t, r), "the fields of %T", d)
	}
}

func requireSameError(t *testing.T, direct, remote error, de, re env) {
	t.Helper()

	require.Equal(t, direct == nil, remote == nil, "both or neither fail: direct %v, remote %v", direct, remote)
	for _, sentinel := range storeSentinels {
		assert.Equal(t, errors.Is(direct, sentinel), errors.Is(remote, sentinel), "errors.Is %v: direct %v, remote %v", sentinel, direct, remote)
	}
	requireSameTyped[*sandboxtypes.LimitExceededError](t, direct, remote, de, re)
	requireSameTyped[*sandboxtypes.StartResultMissingError](t, direct, remote, de, re)
	if direct != nil {
		t.Logf("direct error: %s", de.normalize(t, direct.Error()))
		t.Logf("remote error: %s", re.normalize(t, remote.Error()))
	}
}

type reservationScenario func(ctx context.Context, t *testing.T, s redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error)

func (s redisSetup) storeRecord(ctx context.Context, t *testing.T, e env) {
	t.Helper()

	require.NoError(t, s.records.Add(ctx, sandboxtypes.Sandbox{
		SandboxID: e.sandboxID, TeamID: e.team, ExecutionID: uuid.NewString(), StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), State: sandboxtypes.StateRunning,
	}))
}

func TestDirectVersusRemote(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), hangTimeout)
	directStore := s.waits.ReservationStorage
	remoteStore := remote.NewReservationStorage(dialConn(t, serve(t, directStore, hangTimeout)))

	scenarios := map[string]reservationScenario{
		"Reserve reserved": func(ctx context.Context, _ *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			return store.Reserve(ctx, e.team, e.sandboxID, 10)
		},
		"Reserve already pending": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			reserved, err := directStore.Reserve(ctx, e.team, e.sandboxID, 10)
			require.NoError(t, err)
			require.True(t, reserved)

			return store.Reserve(ctx, e.team, e.sandboxID, 10)
		},
		"Reserve already exists": func(ctx context.Context, t *testing.T, s redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e)

			return store.Reserve(ctx, e.team, e.sandboxID, 10)
		},
		"Reserve limit exceeded": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			reserved, err := directStore.Reserve(ctx, e.team, e.sandboxID+"-first", 1)
			require.NoError(t, err)
			require.True(t, reserved)

			return store.Reserve(ctx, e.team, e.sandboxID, 1)
		},
		"Reserve killed": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			_, claimed, err := directStore.ClaimKill(ctx, e.team, e.sandboxID)
			require.NoError(t, err)
			require.True(t, claimed)

			return store.Reserve(ctx, e.team, e.sandboxID, 10)
		},
		"FinishStart then WaitForStart": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			reserved, err := store.Reserve(ctx, e.team, e.sandboxID, 10)
			require.NoError(t, err)
			require.True(t, reserved)
			require.NoError(t, store.FinishStart(ctx, e.team, e.sandboxID, notUTF8))

			return store.WaitForStart(ctx, e.team, e.sandboxID)
		},
		"FinishStart with an empty result then WaitForStart": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			reserved, err := store.Reserve(ctx, e.team, e.sandboxID, 10)
			require.NoError(t, err)
			require.True(t, reserved)
			require.NoError(t, store.FinishStart(ctx, e.team, e.sandboxID, []byte{}))
			result, err := store.WaitForStart(ctx, e.team, e.sandboxID)

			return []any{result, result == nil}, err
		},
		"Release then WaitForStart": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			reserved, err := store.Reserve(ctx, e.team, e.sandboxID, 10)
			require.NoError(t, err)
			require.True(t, reserved)
			require.NoError(t, store.Release(ctx, e.team, e.sandboxID))

			return store.WaitForStart(ctx, e.team, e.sandboxID)
		},
		"ClaimKill claimed": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			claimID, claimed, err := store.ClaimKill(ctx, e.team, e.sandboxID)
			require.NoError(t, err)
			_, reserveErr := directStore.Reserve(ctx, e.team, e.sandboxID, 10)

			return []any{claimID != "", claimed, errors.Is(reserveErr, sandboxtypes.ErrSandboxKilled)}, err
		},
		"ClaimKill of a pending start": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			reserved, err := directStore.Reserve(ctx, e.team, e.sandboxID, 10)
			require.NoError(t, err)
			require.True(t, reserved)
			claimID, claimed, err := store.ClaimKill(ctx, e.team, e.sandboxID)

			return []any{claimID, claimed}, err
		},
		"ReleaseKillClaim": func(ctx context.Context, t *testing.T, _ redisSetup, store sandboxtypes.ReservationStorage, e env) (any, error) {
			t.Helper()
			claimID, claimed, err := store.ClaimKill(ctx, e.team, e.sandboxID)
			require.NoError(t, err)
			require.True(t, claimed)
			err = store.ReleaseKillClaim(ctx, e.team, e.sandboxID, claimID)
			reserved, reserveErr := directStore.Reserve(ctx, e.team, e.sandboxID, 10)
			require.NoError(t, reserveErr)

			return reserved, err
		},
	}
	for name, run := range scenarios {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := callContext(t)
			de, re := newEnv(), newEnv()
			directValue, directErr := run(ctx, t, s, directStore, de)
			remoteValue, remoteErr := run(ctx, t, s, remoteStore, re)

			requireSameError(t, directErr, remoteErr, de, re)
			assert.Equal(t, de.normalize(t, directValue), re.normalize(t, remoteValue), "the remote store returns what the Redis store returns")
		})
	}
}

func TestRemoteWaitForStartAcrossPolls(t *testing.T) {
	t.Parallel()

	s := setupRedis(t, redis_utils.SetupInstance(t), hangTimeout)
	directStore := s.waits.ReservationStorage
	waiting := make(chan struct{}, 64)
	countWaiting := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if resp, ok := reply.(*reservationpb.WaitForStartResponse); ok && err == nil && resp.GetOutcome() == reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}

		return err
	})
	remoteStore := remote.NewReservationStorage(dialConn(t, serve(t, directStore, 50*time.Millisecond), countWaiting))
	ctx := callContext(t)
	e := newEnv()

	reserved, err := directStore.Reserve(ctx, e.team, e.sandboxID, 10)
	require.NoError(t, err)
	require.True(t, reserved)

	type waited struct {
		result []byte
		err    error
	}
	done := make(chan waited, 1)
	go func() {
		result, err := remoteStore.WaitForStart(ctx, e.team, e.sandboxID)
		done <- waited{result, err}
	}()
	for range 2 {
		select {
		case <-waiting:
		case <-time.After(hangTimeout):
			require.Fail(t, "the wait never crossed two polls")
		}
	}
	require.NoError(t, directStore.FinishStart(ctx, e.team, e.sandboxID, notUTF8))

	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, notUTF8, got.result, "the result of the start that finished after two WAITING answers")
	case <-time.After(hangTimeout):
		require.Fail(t, "the wait never returned")
	}
}
