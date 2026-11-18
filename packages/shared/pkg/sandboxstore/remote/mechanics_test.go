package remote_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type storeCall func(ctx context.Context, s *remote.Storage, r *remote.ReservationStorage) error

func everyCall() map[string]storeCall {
	sbx := testRecord()
	end := sbx.EndTime
	transition := sandboxtypes.StateTransition{Sandbox: sbx, OriginalEndTime: &end, ID: "transition-1"}
	hold := sandboxtypes.Hold{TeamID: teamID, SandboxID: sandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: "node", ID: "hold"}
	pause := sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause}

	return map[string]storeCall{
		"Add": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			return s.Add(ctx, sbx)
		},
		"Get": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.Get(ctx, teamID, sandboxID)

			return err
		},
		"Remove": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			return s.Remove(ctx, teamID, sandboxID, "")
		},
		"TeamItems": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.TeamItems(ctx, teamID, nil)

			return err
		},
		"ExpiredItems": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.ExpiredItems(ctx)

			return err
		},
		"TeamsWithSandboxCount": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.TeamsWithSandboxCount(ctx)

			return err
		},
		"BeginUpdate": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, _, err := s.BeginUpdate(ctx, teamID, sandboxID)

			return err
		},
		"CommitUpdate": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.CommitUpdate(ctx, teamID, sandboxID, "token", sbx)

			return err
		},
		"AbortUpdate": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			return s.AbortUpdate(ctx, teamID, sandboxID, "token")
		},
		"HoldExecution": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			return s.HoldExecution(ctx, hold, end)
		},
		"MoveHeldExecution": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.MoveHeldExecution(ctx, hold, "node-b")

			return err
		},
		"ReleaseHeldExecution": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			return s.ReleaseHeldExecution(ctx, hold)
		},
		"RemoveHeldExecution": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.RemoveHeldExecution(ctx, hold)

			return err
		},
		"BeginRemoving": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, _, err := s.BeginRemoving(ctx, teamID, sandboxID, pause)

			return err
		},
		"FinishTransition": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			return s.FinishTransition(ctx, teamID, sandboxID, "transition-1", sandboxtypes.StateActionPause, sandboxtypes.TransitionOutcome{})
		},
		"RestoreRunning": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.RestoreRunning(ctx, transition, sandboxtypes.RestoreOpts{})

			return err
		},
		"RestoreCheckpointed": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.RestoreCheckpointed(ctx, transition, &orchestrator.SandboxNetworkPlacement{})

			return err
		},
		"WaitForTransition": func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) error {
			_, err := s.WaitForTransition(ctx, teamID, sandboxID, "transition-1")

			return err
		},
		"Reserve": func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) error {
			_, err := r.Reserve(ctx, teamID, sandboxID, 1)

			return err
		},
		"FinishStart": func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) error {
			return r.FinishStart(ctx, teamID, sandboxID, []byte("result"))
		},
		"WaitForStart": func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) error {
			_, err := r.WaitForStart(ctx, teamID, sandboxID)

			return err
		},
		"Release": func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) error {
			return r.Release(ctx, teamID, sandboxID)
		},
		"ClaimKill": func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) error {
			_, _, err := r.ClaimKill(ctx, teamID, sandboxID)

			return err
		},
		"ReleaseKillClaim": func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) error {
			return r.ReleaseKillClaim(ctx, teamID, sandboxID, "claim")
		},
	}
}

func TestEveryCallIsCovered(t *testing.T) {
	t.Parallel()

	state := len(statepb.StateService_ServiceDesc.Methods) + len(statepb.StateService_ServiceDesc.Streams)
	reservation := len(reservationpb.ReservationService_ServiceDesc.Methods)
	assert.Len(t, everyCall(), state-1+reservation, "every RPC but Reconcile, which returns no error")
}

func TestTransportErrors(t *testing.T) {
	t.Parallel()

	for _, code := range []codes.Code{codes.Unavailable, codes.Internal, codes.DeadlineExceeded, codes.Canceled} {
		for name, call := range everyCall() {
			t.Run(name+" "+code.String(), func(t *testing.T) {
				t.Parallel()

				s, r := newStores(dial(t, respond(nil, status.Error(code, "failure"))))
				err := call(callContext(t), s, r)
				requireNoStoreError(t, err)
				assert.Equal(t, code, status.Code(err), "the status is kept: %v", err)
				assert.Contains(t, err.Error(), "/"+name+":", "the error names the method")
			})
		}

		t.Run("Reconcile "+code.String(), func(t *testing.T) {
			t.Parallel()

			s, _ := newStores(dial(t, respond(nil, status.Error(code, "failure"))))
			orphans := s.Reconcile(callContext(t), []sandboxtypes.NodeSandbox{{SandboxID: sandboxID, TeamID: teamID}}, "node")
			assert.Nil(t, orphans, "a failed call answers no orphans")
		})
	}
}

func blockUntilDone(entered chan<- string) handler {
	return func(ctx context.Context, method string, _ proto.Message, _ func(proto.Message) error) (proto.Message, error) {
		entered <- method
		<-ctx.Done()

		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func TestCallerContextEnds(t *testing.T) {
	t.Parallel()

	calls := map[string]struct {
		call func(ctx context.Context, s *remote.Storage, r *remote.ReservationStorage) (any, error)
		want any
	}{
		"unary Get": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) (any, error) {
				return s.Get(ctx, teamID, sandboxID)
			},
			want: sandboxtypes.Sandbox{},
		},
		"long poll WaitForStart": {
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) (any, error) {
				return r.WaitForStart(ctx, teamID, sandboxID)
			},
			want: []byte(nil),
		},
		"long poll WaitForTransition": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) (any, error) {
				return s.WaitForTransition(ctx, teamID, sandboxID, "transition-1")
			},
			want: "transition-1",
		},
		"stream TeamItems": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) (any, error) {
				return s.TeamItems(ctx, teamID, nil)
			},
			want: []sandboxtypes.Sandbox(nil),
		},
	}
	for name, tt := range calls {
		t.Run(name+" cancelled", func(t *testing.T) {
			t.Parallel()

			entered := make(chan string, 1)
			s, r := newStores(dial(t, blockUntilDone(entered)))
			ctx, cancel := context.WithCancel(callContext(t))
			type result struct {
				value any
				err   error
			}
			done := make(chan result, 1)
			go func() {
				value, err := tt.call(ctx, s, r)
				done <- result{value, err}
			}()

			select {
			case <-entered:
			case <-time.After(hangTimeout):
				require.Fail(t, "the call never reached the server")
			}
			cancel()

			select {
			case got := <-done:
				require.ErrorIs(t, got.err, context.Canceled, "the caller's cancellation")
				require.NotErrorIs(t, got.err, context.DeadlineExceeded)
				assert.Equal(t, tt.want, got.value)
			case <-time.After(hangTimeout):
				require.Fail(t, "the call did not end after the caller cancelled")
			}
		})

		t.Run(name+" deadline", func(t *testing.T) {
			t.Parallel()

			entered := make(chan string, 1)
			s, r := newStores(dial(t, blockUntilDone(entered)))
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()

			value, err := tt.call(ctx, s, r)
			require.ErrorIs(t, err, context.DeadlineExceeded, "the caller's deadline")
			require.NotErrorIs(t, err, context.Canceled)
			assert.Equal(t, tt.want, value)
		})
	}
}

type deadlineSeen struct {
	deadline time.Time
	ok       bool
}

func recordDeadline(seen chan<- deadlineSeen, resp proto.Message) handler {
	return func(ctx context.Context, _ string, _ proto.Message, _ func(proto.Message) error) (proto.Message, error) {
		deadline, ok := ctx.Deadline()
		seen <- deadlineSeen{deadline: deadline, ok: ok}

		return resp, nil
	}
}

func TestDeadlines(t *testing.T) {
	t.Parallel()

	const slack = time.Millisecond
	finished := reservationpb.WaitForStartResponse_builder{Outcome: reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED}.Build()
	notFound := statepb.GetResponse_builder{Outcome: statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND}.Build()
	get := func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
		_, _ = s.Get(ctx, teamID, sandboxID)
	}
	teamItems := func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
		_, _ = s.TeamItems(ctx, teamID, nil)
	}
	waitForStart := func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
		_, _ = r.WaitForStart(ctx, teamID, sandboxID)
	}

	tests := map[string]struct {
		resp     proto.Message
		call     func(ctx context.Context, s *remote.Storage, r *remote.ReservationStorage)
		caller   time.Duration
		expected time.Duration
	}{
		"unary without a caller deadline":            {resp: notFound, call: get, expected: remote.DefaultDeadline},
		"unary with a caller deadline":               {resp: notFound, call: get, caller: 2 * time.Minute, expected: 2 * time.Minute},
		"stream without a caller deadline":           {resp: nil, call: teamItems, expected: remote.DefaultDeadline},
		"stream with a caller deadline":              {resp: nil, call: teamItems, caller: hangTimeout, expected: hangTimeout},
		"long poll without a caller deadline":        {resp: finished, call: waitForStart, expected: remote.LongPollDeadline},
		"long poll with an earlier caller deadline":  {resp: finished, call: waitForStart, caller: 2 * time.Second, expected: 2 * time.Second},
		"long poll with a later caller deadline":     {resp: finished, call: waitForStart, caller: hangTimeout, expected: remote.LongPollDeadline},
		"long poll with a caller deadline far ahead": {resp: finished, call: waitForStart, caller: time.Hour, expected: remote.LongPollDeadline},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			seen := make(chan deadlineSeen, 1)
			s, r := newStores(dial(t, recordDeadline(seen, tt.resp)))
			ctx := context.WithoutCancel(t.Context())
			before := time.Now()
			if tt.caller > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.caller)
				defer cancel()
			}
			tt.call(ctx, s, r)
			after := time.Now()

			var got deadlineSeen
			select {
			case got = <-seen:
			default:
				require.Fail(t, "the server never saw the call")
			}
			require.True(t, got.ok, "the server sees a deadline")
			assert.False(t, got.deadline.Before(before.Add(tt.expected-slack)), "deadline %s is at least %s after the call", got.deadline, tt.expected)
			assert.False(t, got.deadline.After(after.Add(tt.expected+slack)), "deadline %s is at most %s after the call", got.deadline, tt.expected)
		})
	}

	assert.Less(t, remote.LongPollDeadline, remote.DefaultDeadline)
}

func TestLongPolls(t *testing.T) {
	t.Parallel()

	t.Run("WaitForStart polls while waiting", func(t *testing.T) {
		t.Parallel()

		rec := &recorder{}
		_, r := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
			rec.record(req)
			if len(rec.all()) < 4 {
				return reservationpb.WaitForStartResponse_builder{Outcome: reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING}.Build(), nil
			}

			return reservationpb.WaitForStartResponse_builder{Outcome: reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, Result: []byte("result")}.Build(), nil
		}))

		result, err := r.WaitForStart(callContext(t), teamID, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, []byte("result"), result)
		want := reservationpb.WaitForStartRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build()
		requests := rec.all()
		require.Len(t, requests, 4, "three WAITING answers, then the result")
		for _, req := range requests {
			requireProtoEqual(t, want, req)
		}
	})

	t.Run("WaitForTransition sends the returned ID", func(t *testing.T) {
		t.Parallel()

		rec := &recorder{}
		s, _ := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
			rec.record(req)
			if len(rec.all()) < 4 {
				return statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, TransitionId: "transition-1"}.Build(), nil
			}

			return statepb.WaitForTransitionResponse_builder{
				Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED, TransitionId: "transition-1", Failure: "boom",
			}.Build(), nil
		}))

		id, err := s.WaitForTransition(callContext(t), teamID, sandboxID, "")
		assert.Equal(t, "transition-1", id)
		assert.Equal(t, &sandboxtypes.TransitionFailedError{Message: "boom"}, err)
		var sent []string
		for _, req := range rec.all() {
			wait := req.(*statepb.WaitForTransitionRequest)
			assert.Equal(t, teamID.String(), wait.GetTeamId())
			assert.Equal(t, sandboxID, wait.GetSandboxId())
			sent = append(sent, wait.GetTransitionId())
		}
		assert.Equal(t, []string{"", "transition-1", "transition-1", "transition-1"}, sent, "every poll after the first sends the ID the server resolved")
	})

	waiting := map[string]struct {
		resp proto.Message
		call func(ctx context.Context, s *remote.Storage, r *remote.ReservationStorage) (string, error)
	}{
		"WaitForStart": {
			resp: reservationpb.WaitForStartResponse_builder{Outcome: reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING}.Build(),
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) (string, error) {
				result, err := r.WaitForStart(ctx, teamID, sandboxID)

				return string(result), err
			},
		},
		"WaitForTransition": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, TransitionId: "transition-1"}.Build(),
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) (string, error) {
				return s.WaitForTransition(ctx, teamID, sandboxID, "")
			},
		},
	}
	for name, tt := range waiting {
		t.Run(name+" ends when the caller's context ends between polls", func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(callContext(t))
			defer cancel()
			rec := &recorder{}
			cancelAfterAnswer := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				err := invoker(ctx, method, req, reply, cc, opts...)
				cancel()

				return err
			})
			s, r := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
				rec.record(req)

				return tt.resp, nil
			}, cancelAfterAnswer))

			got, err := tt.call(ctx, s, r)
			require.ErrorIs(t, err, context.Canceled)
			assert.Len(t, rec.all(), 1, "the server answered WAITING once")
			if name == "WaitForTransition" {
				assert.Equal(t, "transition-1", got, "the ID the server returned, to resume the wait with")
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestStreams(t *testing.T) {
	t.Parallel()

	records := make([]sandboxtypes.Sandbox, 5)
	for i := range records {
		records[i] = testRecord()
		records[i].SandboxID = uuid.NewString()
	}
	streams := map[string]struct {
		message func(records [][]byte) proto.Message
		call    func(ctx context.Context, s *remote.Storage) ([]sandboxtypes.Sandbox, error)
		empty   []sandboxtypes.Sandbox
	}{
		"TeamItems": {
			message: func(records [][]byte) proto.Message {
				return statepb.TeamItemsResponse_builder{Records: records}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) ([]sandboxtypes.Sandbox, error) {
				return s.TeamItems(ctx, teamID, nil)
			},
			empty: []sandboxtypes.Sandbox{},
		},
		"ExpiredItems": {
			message: func(records [][]byte) proto.Message {
				return statepb.ExpiredItemsResponse_builder{Records: records}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) ([]sandboxtypes.Sandbox, error) {
				return s.ExpiredItems(ctx)
			},
			empty: nil,
		},
	}
	for name, tt := range streams {
		send := func(failAfter int) handler {
			return func(_ context.Context, _ string, _ proto.Message, send func(proto.Message) error) (proto.Message, error) {
				for i, chunk := range [][]sandboxtypes.Sandbox{records[:2], records[2:3], records[3:]} {
					if i == failAfter {
						return nil, status.Error(codes.Internal, "failure")
					}
					var encoded [][]byte
					for _, sbx := range chunk {
						encoded = append(encoded, encode(t, sbx))
					}
					if err := send(tt.message(encoded)); err != nil {
						return nil, err
					}
				}

				return nil, nil
			}
		}

		t.Run(name+" joins the messages in order", func(t *testing.T) {
			t.Parallel()

			s, _ := newStores(dial(t, send(-1)))
			got, err := tt.call(callContext(t), s)
			require.NoError(t, err)
			assert.Equal(t, records, got)
		})

		t.Run(name+" returns no records with an error", func(t *testing.T) {
			t.Parallel()

			s, _ := newStores(dial(t, send(2)))
			got, err := tt.call(callContext(t), s)
			requireNoStoreError(t, err)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Nil(t, got, "no partial result")
		})

		t.Run(name+" with an undecodable record returns no records", func(t *testing.T) {
			t.Parallel()

			s, _ := newStores(dial(t, func(_ context.Context, _ string, _ proto.Message, send func(proto.Message) error) (proto.Message, error) {
				return nil, send(tt.message([][]byte{encode(t, records[0]), []byte("{")}))
			}))
			got, err := tt.call(callContext(t), s)
			requireNoStoreError(t, err)
			assert.Nil(t, got, "no partial result")
		})

		t.Run(name+" empty", func(t *testing.T) {
			t.Parallel()

			s, _ := newStores(dial(t, respond(nil, nil)))
			got, err := tt.call(callContext(t), s)
			require.NoError(t, err)
			assert.Equal(t, tt.empty, got, "nothing, as the store answers it")
		})
	}

	t.Run("TeamItems with an unknown state", func(t *testing.T) {
		t.Parallel()

		rec := &recorder{}
		s, _ := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
			rec.record(req)

			return nil, nil
		}))
		got, err := s.TeamItems(callContext(t), teamID, []sandboxtypes.State{"unknown"})
		requireNoStoreError(t, err)
		assert.Nil(t, got)
		assert.Empty(t, rec.all(), "nothing is sent")
	})
}

func TestReconcile(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 13, 10, 11, 12, 0, time.UTC)
	first := sandboxtypes.NodeSandbox{
		SandboxID: "sbx-1", ExecutionID: uuid.NewString(), TeamID: teamID, NodeID: "node", ClusterID: uuid.New(), StartTime: start,
		VCpu: 2, RamMB: 512, HugePages: true, Ended: true, GuestEvent: "poweroff", DiskKept: true,
	}
	requireNoZeroField(t, first)
	second := first
	second.SandboxID = "sbx-2"
	second.ExecutionID = uuid.NewString()
	second.TeamID = uuid.New()
	second.VCpu = 4
	second.GuestEvent = "crash"

	rec := &recorder{}
	s, _ := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
		rec.record(req)

		return statepb.ReconcileResponse_builder{Orphans: []*statepb.SandboxReference{
			statepb.SandboxReference_builder{TeamId: second.TeamID.String(), SandboxId: second.SandboxID}.Build(),
			statepb.SandboxReference_builder{TeamId: teamID.String(), SandboxId: "unknown"}.Build(),
			statepb.SandboxReference_builder{TeamId: second.TeamID.String(), SandboxId: first.SandboxID}.Build(),
		}}.Build(), nil
	}))

	orphans := s.Reconcile(callContext(t), []sandboxtypes.NodeSandbox{first, second}, "node-1")
	assert.Equal(t, []sandboxtypes.NodeSandbox{second}, orphans, "the caller's own value for each reference it sent")

	var wire []*statepb.NodeSandbox
	for _, sbx := range []sandboxtypes.NodeSandbox{first, second} {
		wire = append(wire, statepb.NodeSandbox_builder{
			SandboxId: sbx.SandboxID, ExecutionId: sbx.ExecutionID, TeamId: sbx.TeamID.String(), NodeId: sbx.NodeID, ClusterId: sbx.ClusterID.String(),
			StartTime: timestamppb.New(sbx.StartTime), Vcpu: sbx.VCpu, RamMb: sbx.RamMB, HugePages: sbx.HugePages, Ended: sbx.Ended,
			GuestEvent: sbx.GuestEvent, DiskKept: sbx.DiskKept,
		}.Build())
	}
	requireProtoEqual(t, statepb.ReconcileRequest_builder{NodeId: "node-1", Sandboxes: wire}.Build(), rec.last(t))

	none := s.Reconcile(callContext(t), []sandboxtypes.NodeSandbox{first}, "node-1")
	assert.Nil(t, none, "no orphans: a reference to a sandbox the caller did not send is ignored")
}
