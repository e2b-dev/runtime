package reservation_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/reservation"
	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type fakeStorage struct {
	mu    sync.Mutex
	calls int

	reserved bool
	claimID  string
	claimed  bool
	err      error
	wait     func(ctx context.Context) ([]byte, error)
}

var _ sandboxtypes.ReservationStorage = (*fakeStorage)(nil)

func (f *fakeStorage) call() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
}

func (f *fakeStorage) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func (f *fakeStorage) Reserve(context.Context, uuid.UUID, string, int) (bool, error) {
	f.call()

	return f.reserved, f.err
}

func (f *fakeStorage) FinishStart(context.Context, uuid.UUID, string, []byte) error {
	f.call()

	return f.err
}

func (f *fakeStorage) WaitForStart(ctx context.Context, _ uuid.UUID, _ string) ([]byte, error) {
	f.call()
	if f.wait != nil {
		return f.wait(ctx)
	}

	return nil, f.err
}

func (f *fakeStorage) Release(context.Context, uuid.UUID, string) error {
	f.call()

	return f.err
}

func (f *fakeStorage) ClaimKill(context.Context, uuid.UUID, string) (string, bool, error) {
	f.call()

	return f.claimID, f.claimed, f.err
}

func (f *fakeStorage) ReleaseKillClaim(context.Context, uuid.UUID, string, string) error {
	f.call()

	return f.err
}

type rpcCall struct {
	name string
	call func(ctx context.Context, client reservationpb.ReservationServiceClient, team, sandbox string) error
}

var rpcCalls = []rpcCall{
	{"Reserve", func(ctx context.Context, c reservationpb.ReservationServiceClient, team, sandbox string) error {
		_, err := c.Reserve(ctx, reservationpb.ReserveRequest_builder{TeamId: team, SandboxId: sandbox, Limit: 10}.Build())

		return err
	}},
	{"FinishStart", func(ctx context.Context, c reservationpb.ReservationServiceClient, team, sandbox string) error {
		_, err := c.FinishStart(ctx, reservationpb.FinishStartRequest_builder{TeamId: team, SandboxId: sandbox, Result: []byte("r")}.Build())

		return err
	}},
	{"WaitForStart", func(ctx context.Context, c reservationpb.ReservationServiceClient, team, sandbox string) error {
		_, err := c.WaitForStart(ctx, reservationpb.WaitForStartRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

		return err
	}},
	{"Release", func(ctx context.Context, c reservationpb.ReservationServiceClient, team, sandbox string) error {
		_, err := c.Release(ctx, reservationpb.ReleaseRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

		return err
	}},
	{"ClaimKill", func(ctx context.Context, c reservationpb.ReservationServiceClient, team, sandbox string) error {
		_, err := c.ClaimKill(ctx, reservationpb.ClaimKillRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

		return err
	}},
	{"ReleaseKillClaim", func(ctx context.Context, c reservationpb.ReservationServiceClient, team, sandbox string) error {
		_, err := c.ReleaseKillClaim(ctx, reservationpb.ReleaseKillClaimRequest_builder{TeamId: team, SandboxId: sandbox, ClaimId: "claim"}.Build())

		return err
	}},
}

func TestReserveOutcomeMapping(t *testing.T) {
	t.Parallel()

	limit := &sandboxtypes.LimitExceededError{TeamID: uuid.New()}
	tests := []struct {
		name     string
		reserved bool
		err      error
		want     reservationpb.ReserveOutcome
	}{
		{"reserved", true, nil, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED},
		{"already pending", false, nil, reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING},
		{"already exists", false, sandboxtypes.ErrAlreadyExists, reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_EXISTS},
		{"wrapped already exists", false, fmt.Errorf("x: %w", sandboxtypes.ErrAlreadyExists), reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_EXISTS},
		{"limit exceeded", false, limit, reservationpb.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED},
		{"wrapped limit exceeded", false, fmt.Errorf("x: %w", limit), reservationpb.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED},
		{"killed", false, sandboxtypes.ErrSandboxKilled, reservationpb.ReserveOutcome_RESERVE_OUTCOME_KILLED},
		{"wrapped killed", false, fmt.Errorf("x: %w", sandboxtypes.ErrSandboxKilled), reservationpb.ReserveOutcome_RESERVE_OUTCOME_KILLED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newClient(t, &fakeStorage{reserved: tt.reserved, err: tt.err}, time.Hour)
			assert.Equal(t, tt.want, reserve(callContext(t), t, client, newIDs(), 10))
		})
	}
}

func TestClaimKillMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		claimID string
		claimed bool
	}{
		{"claimed", "claim-1", true},
		{"not claimed", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newClient(t, &fakeStorage{claimID: tt.claimID, claimed: tt.claimed}, time.Hour)
			resp := claimKill(callContext(t), t, client, newIDs())
			assert.Equal(t, tt.claimed, resp.GetClaimed())
			assert.Equal(t, tt.claimID, resp.GetClaimId())
		})
	}
}

func TestBackendFailureIsInternal(t *testing.T) {
	t.Parallel()

	const cause = "backend exploded with secret detail"
	for _, rpcCall := range rpcCalls {
		t.Run(rpcCall.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeStorage{err: errors.New(cause)}
			client := newClient(t, fake, time.Hour)
			id := newIDs()
			st := requireCode(t, rpcCall.call(callContext(t), client, id.team, id.sandbox), codes.Internal)
			assert.Equal(t, "internal error", st.Message())
			assert.NotContains(t, st.Message(), cause)
			assert.Equal(t, 1, fake.callCount())
		})
	}
}

func TestCancelledCaller(t *testing.T) {
	t.Parallel()

	t.Run("over gRPC", func(t *testing.T) {
		t.Parallel()

		started := make(chan struct{})
		fake := &fakeStorage{wait: func(ctx context.Context) ([]byte, error) {
			close(started)
			<-ctx.Done()

			return nil, errors.New("backend gave up")
		}}
		client := newClient(t, fake, time.Hour)

		ctx, cancel := context.WithCancel(callContext(t))
		go func() {
			<-started
			cancel()
		}()
		_, err := waitForStart(ctx, client, newIDs())
		requireCode(t, err, codes.Canceled)
	})

	team, sandbox := uuid.NewString(), "sbx"
	handlerCalls := []struct {
		name string
		call func(ctx context.Context, s *reservation.Server) error
	}{
		{"Reserve", func(ctx context.Context, s *reservation.Server) error {
			_, err := s.Reserve(ctx, reservationpb.ReserveRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}},
		{"FinishStart", func(ctx context.Context, s *reservation.Server) error {
			_, err := s.FinishStart(ctx, reservationpb.FinishStartRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}},
		{"WaitForStart", func(ctx context.Context, s *reservation.Server) error {
			_, err := s.WaitForStart(ctx, reservationpb.WaitForStartRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}},
		{"Release", func(ctx context.Context, s *reservation.Server) error {
			_, err := s.Release(ctx, reservationpb.ReleaseRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}},
		{"ClaimKill", func(ctx context.Context, s *reservation.Server) error {
			_, err := s.ClaimKill(ctx, reservationpb.ClaimKillRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}},
		{"ReleaseKillClaim", func(ctx context.Context, s *reservation.Server) error {
			_, err := s.ReleaseKillClaim(ctx, reservationpb.ReleaseKillClaimRequest_builder{TeamId: team, SandboxId: sandbox, ClaimId: "claim"}.Build())

			return err
		}},
	}
	for _, hc := range handlerCalls {
		t.Run("handler "+hc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			server := reservation.NewServer(&fakeStorage{err: errors.New("backend gave up")}, time.Hour)
			st := requireCode(t, hc.call(ctx, server), codes.Canceled)
			assert.NotContains(t, st.Message(), "backend gave up")
		})
	}
}

func TestInvalidIDs(t *testing.T) {
	t.Parallel()

	valid := newIDs()
	tests := []struct {
		name    string
		team    string
		sandbox string
	}{
		{"empty team", "", valid.sandbox},
		{"team not a UUID", "team-1", valid.sandbox},
		{"empty sandbox", valid.team, ""},
		{"sandbox with opening brace", valid.team, "a{b"},
		{"sandbox with closing brace", valid.team, "a}b"},
	}
	for _, rpcCall := range rpcCalls {
		for _, tt := range tests {
			t.Run(rpcCall.name+" "+tt.name, func(t *testing.T) {
				t.Parallel()

				fake := &fakeStorage{}
				client := newClient(t, fake, time.Hour)
				requireCode(t, rpcCall.call(callContext(t), client, tt.team, tt.sandbox), codes.InvalidArgument)
				assert.Zero(t, fake.callCount())
			})
		}
	}

	t.Run("ReleaseKillClaim empty claim", func(t *testing.T) {
		t.Parallel()

		fake := &fakeStorage{}
		client := newClient(t, fake, time.Hour)
		_, err := client.ReleaseKillClaim(callContext(t), reservationpb.ReleaseKillClaimRequest_builder{
			TeamId: valid.team, SandboxId: valid.sandbox,
		}.Build())
		requireCode(t, err, codes.InvalidArgument)
		assert.Zero(t, fake.callCount())
	})
}

func TestMaxWaitIsWaiting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wait func(ctx context.Context) ([]byte, error)
	}{
		{"deadline error", func(ctx context.Context) ([]byte, error) {
			<-ctx.Done()

			return nil, fmt.Errorf("x: %w", ctx.Err())
		}},
		{"other error after the deadline", func(ctx context.Context) ([]byte, error) {
			<-ctx.Done()

			return nil, errors.New("i/o timeout")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hadDeadline := make(chan bool, 1)
			fake := &fakeStorage{wait: func(ctx context.Context) ([]byte, error) {
				_, ok := ctx.Deadline()
				hadDeadline <- ok

				return tt.wait(ctx)
			}}
			client := newClient(t, fake, 10*time.Millisecond)

			resp, err := waitForStart(t.Context(), client, newIDs())
			require.NoError(t, err)
			assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING, resp.GetOutcome())
			assert.True(t, <-hadDeadline)
		})
	}
}

func TestWaitForStartOutcomes(t *testing.T) {
	t.Parallel()

	t.Run("finished", func(t *testing.T) {
		t.Parallel()

		fake := &fakeStorage{wait: func(context.Context) ([]byte, error) { return notUTF8, nil }}
		resp, err := waitForStart(callContext(t), newClient(t, fake, time.Hour), newIDs())
		require.NoError(t, err)
		assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, resp.GetOutcome())
		assert.Equal(t, notUTF8, resp.GetResult())
	})

	t.Run("wrapped result missing", func(t *testing.T) {
		t.Parallel()

		fake := &fakeStorage{err: fmt.Errorf("x: %w", &sandboxtypes.StartResultMissingError{SandboxID: "sbx"})}
		resp, err := waitForStart(callContext(t), newClient(t, fake, time.Hour), newIDs())
		require.NoError(t, err)
		assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_NO_RESULT, resp.GetOutcome())
	})

	t.Run("caller ended during the wait", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()
		fake := &fakeStorage{wait: func(context.Context) ([]byte, error) {
			cancel()

			return nil, context.DeadlineExceeded
		}}
		server := reservation.NewServer(fake, time.Hour)
		_, err := server.WaitForStart(ctx, reservationpb.WaitForStartRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx"}.Build())
		requireCode(t, err, codes.Canceled)
	})
}
