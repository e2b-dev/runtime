package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/state"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

var (
	updateTeam      = uuid.New()
	updateSandbox   = "sbx-update"
	updateExecution = uuid.NewString()
	errLockNotHeld  = fmt.Errorf("%w: %w", errors.New("redislock: lock not held"), sandboxtypes.ErrLockLost)
)

const storeToken sandboxtypes.UpdateToken = "store-token-secret-lock-value"

func issuedToken(t *testing.T, teamID uuid.UUID, sandboxID string, token sandboxtypes.UpdateToken) []byte {
	t.Helper()

	fake := newFake()
	fake.token = token
	resp, err := newClient(t, fake).BeginUpdate(callContext(t), statepb.BeginUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
	require.NotEmpty(t, resp.GetToken())

	return resp.GetToken()
}

func updateToken(t *testing.T) []byte {
	t.Helper()

	return issuedToken(t, updateTeam, updateSandbox, storeToken)
}

func updateHold() *statepb.Hold {
	return statepb.Hold_builder{
		TeamId: updateTeam.String(), SandboxId: updateSandbox, ExecutionId: updateExecution, SourceNodeId: "node-a", Id: "hold",
	}.Build()
}

type updateCall struct {
	call      func(ctx context.Context, client statepb.StateServiceClient) (statepb.StateOutcome, error)
	sentinels []error
}

func updateCalls(t *testing.T) map[string]updateCall {
	t.Helper()

	record := encode(t, minimalRecord(updateTeam, updateSandbox))
	held := []error{sandboxtypes.ErrNotHeld, sandboxtypes.ErrExecutionMismatch, sandboxtypes.ErrNotFound, sandboxtypes.ErrLockLost}

	token := updateToken(t)

	return map[string]updateCall{
		"BeginUpdate": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.BeginUpdate(ctx, statepb.BeginUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: []error{sandboxtypes.ErrNotFound, sandboxtypes.ErrLifecycleHeld},
		},
		"CommitUpdate": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{
					TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: token, Record: record,
				}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: []error{sandboxtypes.ErrLockLost},
		},
		"AbortUpdate": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: token}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: []error{sandboxtypes.ErrLockLost, errLockNotHeld},
		},
		"HoldExecution": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.HoldExecution(ctx, statepb.HoldExecutionRequest_builder{Hold: updateHold(), Until: timestamppb.New(time.Now().Add(time.Hour))}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: []error{sandboxtypes.ErrHoldConflict, sandboxtypes.ErrNotFound, sandboxtypes.ErrLockLost},
		},
		"MoveHeldExecution": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.MoveHeldExecution(ctx, statepb.MoveHeldExecutionRequest_builder{Hold: updateHold(), NodeId: "node-b"}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: held,
		},
		"ReleaseHeldExecution": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.ReleaseHeldExecution(ctx, statepb.ReleaseHeldExecutionRequest_builder{Hold: updateHold()}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: held,
		},
		"RemoveHeldExecution": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
				resp, err := c.RemoveHeldExecution(ctx, statepb.RemoveHeldExecutionRequest_builder{Hold: updateHold()}.Build())

				return resp.GetOutcome(), err
			},
			sentinels: append([]error{sandboxtypes.ErrLifecycleHeld}, held...),
		},
	}
}

var sentinelOutcomes = map[error]statepb.StateOutcome{
	sandboxtypes.ErrNotFound:          statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND,
	sandboxtypes.ErrLifecycleHeld:     statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD,
	sandboxtypes.ErrExecutionMismatch: statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH,
	sandboxtypes.ErrLockLost:          statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST,
	sandboxtypes.ErrHoldConflict:      statepb.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT,
	sandboxtypes.ErrNotHeld:           statepb.StateOutcome_STATE_OUTCOME_NOT_HELD,
	errLockNotHeld:                    statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST,
}

func TestUpdateOutcomeMapping(t *testing.T) {
	t.Parallel()

	for name, rpc := range updateCalls(t) {
		for _, sentinel := range rpc.sentinels {
			for _, wrapped := range []bool{false, true} {
				err := sentinel
				if wrapped {
					err = fmt.Errorf("sandbox %q: %w", updateSandbox, sentinel)
				}
				t.Run(fmt.Sprintf("%s %v wrapped=%v", name, sentinel, wrapped), func(t *testing.T) {
					t.Parallel()

					fake := newFake()
					fake.err = err
					outcome, callErr := rpc.call(callContext(t), newClient(t, fake))
					require.NoError(t, callErr)
					assert.Equal(t, sentinelOutcomes[sentinel], outcome)
				})
			}
		}

		t.Run(name+" nil is OK", func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.token = "token"
			outcome, err := rpc.call(callContext(t), newClient(t, fake))
			require.NoError(t, err)
			assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, outcome)
		})
	}
}

func TestEverySentinelHasAnOutcome(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		sandboxtypes.ErrNotFound,
		sandboxtypes.ErrLifecycleHeld,
		sandboxtypes.ErrExecutionMismatch,
		sandboxtypes.ErrLockLost,
		sandboxtypes.ErrHoldConflict,
		sandboxtypes.ErrNotHeld,
		sandboxtypes.ErrEvictionInProgress,
		sandboxtypes.ErrEvictionNotNeeded,
		&sandboxtypes.InvalidStateTransitionError{CurrentState: sandboxtypes.StateKilling, TargetState: sandboxtypes.StatePausing},
		sandboxtypes.ErrRestoreConflict,
	}
	require.Len(t, statepb.StateOutcome_name, len(sentinels)+2, "every outcome besides UNSPECIFIED and OK has its sentinel")

	seen := map[statepb.StateOutcome]error{}
	for _, sentinel := range sentinels {
		fake := newFake()
		fake.err = sentinel
		resp, err := newClient(t, fake).Remove(callContext(t), statepb.RemoveRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox}.Build())
		require.NoError(t, err, "%v has no outcome", sentinel)
		outcome := resp.GetOutcome()
		assert.NotEqual(t, statepb.StateOutcome_STATE_OUTCOME_UNSPECIFIED, outcome, sentinel.Error())
		assert.NotEqual(t, statepb.StateOutcome_STATE_OUTCOME_OK, outcome, sentinel.Error())
		assert.NotContains(t, seen, outcome, "%v shares its outcome with %v", sentinel, seen[outcome])
		seen[outcome] = sentinel
	}
}

func TestUpdateBackendFailureIsInternal(t *testing.T) {
	t.Parallel()

	const cause = "backend exploded with secret detail"
	for name, rpc := range updateCalls(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.err = errors.New(cause)
			_, err := rpc.call(callContext(t), newClient(t, fake))
			st := requireCode(t, err, codes.Internal)
			assert.Equal(t, "internal error", st.Message())
			assert.NotContains(t, st.Message(), cause)
			assert.Equal(t, 1, fake.callCount())
		})
	}
}

func TestUpdateCancelledCaller(t *testing.T) {
	t.Parallel()

	record := encode(t, minimalRecord(updateTeam, updateSandbox))
	until := timestamppb.New(time.Now().Add(time.Hour))
	token := updateToken(t)
	handlerCalls := map[string]func(ctx context.Context, s *state.Server) error{
		"BeginUpdate": func(ctx context.Context, s *state.Server) error {
			_, err := s.BeginUpdate(ctx, statepb.BeginUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox}.Build())

			return err
		},
		"CommitUpdate": func(ctx context.Context, s *state.Server) error {
			_, err := s.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: token, Record: record}.Build())

			return err
		},
		"AbortUpdate": func(ctx context.Context, s *state.Server) error {
			_, err := s.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: token}.Build())

			return err
		},
		"HoldExecution": func(ctx context.Context, s *state.Server) error {
			_, err := s.HoldExecution(ctx, statepb.HoldExecutionRequest_builder{Hold: updateHold(), Until: until}.Build())

			return err
		},
		"MoveHeldExecution": func(ctx context.Context, s *state.Server) error {
			_, err := s.MoveHeldExecution(ctx, statepb.MoveHeldExecutionRequest_builder{Hold: updateHold(), NodeId: "node-b"}.Build())

			return err
		},
		"ReleaseHeldExecution": func(ctx context.Context, s *state.Server) error {
			_, err := s.ReleaseHeldExecution(ctx, statepb.ReleaseHeldExecutionRequest_builder{Hold: updateHold()}.Build())

			return err
		},
		"RemoveHeldExecution": func(ctx context.Context, s *state.Server) error {
			_, err := s.RemoveHeldExecution(ctx, statepb.RemoveHeldExecutionRequest_builder{Hold: updateHold()}.Build())

			return err
		},
	}
	for name, call := range handlerCalls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			fake := newFake()
			fake.err = errors.New("backend gave up")
			st := requireCode(t, call(ctx, state.NewServer(fake, hangTimeout)), codes.Canceled)
			assert.NotContains(t, st.Message(), "backend gave up")
		})
	}

	t.Run("BeginUpdate the caller cannot receive is aborted", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		fake := newFake()
		fake.token = "begun-token"
		fake.block = func(context.Context) { cancel() }
		_, err := state.NewServer(fake, hangTimeout).BeginUpdate(ctx, statepb.BeginUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox}.Build())
		requireCode(t, err, codes.Canceled)
		assert.Equal(t, []sandboxtypes.UpdateToken{"begun-token"}, fake.snapshot().aborted)
	})
}

func TestUpdateInvalidRequests(t *testing.T) {
	t.Parallel()

	type rpcCall = func(ctx context.Context, client statepb.StateServiceClient) error
	commit := func(team, sandbox string, token, record []byte) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{TeamId: team, SandboxId: sandbox, Token: token, Record: record}.Build())

			return err
		}
	}
	abort := func(team, sandbox string, token []byte) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: team, SandboxId: sandbox, Token: token}.Build())

			return err
		}
	}
	begin := func(team, sandbox string) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.BeginUpdate(ctx, statepb.BeginUpdateRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}
	}
	holdRequest := func(req *statepb.HoldExecutionRequest) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.HoldExecution(ctx, req)

			return err
		}
	}
	hold := func(mutate func(*statepb.Hold), until *timestamppb.Timestamp) rpcCall {
		h := updateHold()
		mutate(h)

		return holdRequest(statepb.HoldExecutionRequest_builder{Hold: h, Until: until}.Build())
	}
	heldCalls := func(h *statepb.Hold) map[string]rpcCall {
		return map[string]rpcCall{
			"MoveHeldExecution": func(ctx context.Context, c statepb.StateServiceClient) error {
				_, err := c.MoveHeldExecution(ctx, statepb.MoveHeldExecutionRequest_builder{Hold: h, NodeId: "node-b"}.Build())

				return err
			},
			"ReleaseHeldExecution": func(ctx context.Context, c statepb.StateServiceClient) error {
				_, err := c.ReleaseHeldExecution(ctx, statepb.ReleaseHeldExecutionRequest_builder{Hold: h}.Build())

				return err
			},
			"RemoveHeldExecution": func(ctx context.Context, c statepb.StateServiceClient) error {
				_, err := c.RemoveHeldExecution(ctx, statepb.RemoveHeldExecutionRequest_builder{Hold: h}.Build())

				return err
			},
		}
	}
	mutatedHold := func(mutate func(*statepb.Hold)) *statepb.Hold {
		h := updateHold()
		mutate(h)

		return h
	}

	team := updateTeam.String()
	valid := encode(t, minimalRecord(updateTeam, updateSandbox))
	later := timestamppb.New(time.Now().Add(time.Hour))
	tests := map[string]rpcCall{
		"BeginUpdate team not a UUID":  begin("team-1", updateSandbox),
		"BeginUpdate braced sandbox":   begin(team, "a{b"),
		"CommitUpdate empty token":     commit(team, updateSandbox, nil, valid),
		"CommitUpdate team not a UUID": commit("team-1", updateSandbox, updateToken(t), valid),
		"CommitUpdate empty sandbox":   commit(team, "", updateToken(t), valid),
		"CommitUpdate token not JSON":  commit(team, updateSandbox, []byte("malformed-secret"), valid),
		"AbortUpdate token not JSON":   abort(team, updateSandbox, []byte("malformed-secret")),
		"MoveHeldExecution empty node": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.MoveHeldExecution(ctx, statepb.MoveHeldExecutionRequest_builder{Hold: updateHold()}.Build())

			return err
		},
		"AbortUpdate empty token":         abort(team, updateSandbox, nil),
		"AbortUpdate team not a UUID":     abort("team-1", updateSandbox, updateToken(t)),
		"HoldExecution no hold":           holdRequest(statepb.HoldExecutionRequest_builder{Until: later}.Build()),
		"HoldExecution empty hold ID":     hold(func(h *statepb.Hold) { h.SetId("") }, later),
		"HoldExecution empty source node": hold(func(h *statepb.Hold) { h.SetSourceNodeId("") }, later),
		"HoldExecution team not a UUID":   hold(func(h *statepb.Hold) { h.SetTeamId("team-1") }, later),
		"HoldExecution braced sandbox":    hold(func(h *statepb.Hold) { h.SetSandboxId("a}b") }, later),
		"HoldExecution missing until":     hold(func(*statepb.Hold) {}, nil),
		"HoldExecution invalid until":     hold(func(*statepb.Hold) {}, &timestamppb.Timestamp{Seconds: 1, Nanos: -1}),
	}
	for name, call := range heldCalls(nil) {
		tests[name+" no hold"] = call
	}
	for name, call := range heldCalls(mutatedHold(func(h *statepb.Hold) { h.SetId("") })) {
		tests[name+" empty hold ID"] = call
	}
	for name, call := range heldCalls(mutatedHold(func(h *statepb.Hold) { h.SetSandboxId("") })) {
		tests[name+" empty sandbox"] = call
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			requireCode(t, call(callContext(t), newClient(t, fake)), codes.InvalidArgument)
			assert.Zero(t, fake.callCount(), "the store is not called")
		})
	}

	refusedByStore := issuedToken(t, updateTeam, updateSandbox, "malformed-secret")
	malformed := map[string]rpcCall{
		"CommitUpdate": commit(team, updateSandbox, refusedByStore, valid),
		"AbortUpdate":  abort(team, updateSandbox, refusedByStore),
	}
	for name, call := range malformed {
		t.Run(name+" store token the store refuses", func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.err = fmt.Errorf("%w: invalid character 'm' looking for beginning of value", sandboxtypes.ErrInvalidUpdateToken)
			st := requireCode(t, call(callContext(t), newClient(t, fake)), codes.InvalidArgument)
			assert.NotContains(t, st.Message(), "malformed-secret")
			assert.NotContains(t, st.Message(), "invalid character")
			assert.Equal(t, 1, fake.callCount())
		})
	}
}

func TestUpdateStoreReceivesTheCallersValues(t *testing.T) {
	t.Parallel()

	t.Run("BeginUpdate", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		fake.record = fullRecord(updateTeam, updateSandbox)
		fake.token = "token-from-store"
		resp, err := newClient(t, fake).BeginUpdate(callContext(t), statepb.BeginUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.Equal(t, fake.record, decode(t, resp.GetRecord()))
		assert.NotEqual(t, []byte("token-from-store"), resp.GetToken(), "the wire token is not the store's token as is")
		_, err = newClient(t, fake).AbortUpdate(callContext(t), statepb.AbortUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: resp.GetToken()}.Build())
		require.NoError(t, err)
		assert.Equal(t, sandboxtypes.UpdateToken("token-from-store"), fake.snapshot().token, "the wire token carries the store's token")
	})

	t.Run("CommitUpdate", func(t *testing.T) {
		t.Parallel()

		full := fullRecord(updateTeam, updateSandbox)
		fake := newFake()
		resp, err := newClient(t, fake).CommitUpdate(callContext(t), statepb.CommitUpdateRequest_builder{
			TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: updateToken(t), Record: encode(t, full),
		}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.Equal(t, full, decode(t, resp.GetRecord()))
		seen := fake.snapshot()
		assert.Equal(t, storeToken, seen.token)
		assert.Equal(t, full, seen.updated)
	})

	t.Run("AbortUpdate", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		_, err := newClient(t, fake).AbortUpdate(callContext(t), statepb.AbortUpdateRequest_builder{TeamId: updateTeam.String(), SandboxId: updateSandbox, Token: updateToken(t)}.Build())
		require.NoError(t, err)
		assert.Equal(t, storeToken, fake.snapshot().token)
	})

	want := sandboxtypes.Hold{TeamID: updateTeam, SandboxID: updateSandbox, ExecutionID: updateExecution, SourceNodeID: "node-a", ID: "hold"}

	t.Run("HoldExecution", func(t *testing.T) {
		t.Parallel()

		until := time.Date(2026, 11, 13, 10, 11, 12, 123456789, time.UTC)
		fake := newFake()
		_, err := newClient(t, fake).HoldExecution(callContext(t), statepb.HoldExecutionRequest_builder{Hold: updateHold(), Until: timestamppb.New(until)}.Build())
		require.NoError(t, err)
		seen := fake.snapshot()
		assert.Equal(t, want, seen.hold)
		assert.True(t, until.Equal(seen.until), "until %v arrived as %v", until, seen.until)
	})

	t.Run("MoveHeldExecution", func(t *testing.T) {
		t.Parallel()

		for _, moved := range []bool{true, false} {
			fake := newFake()
			fake.moved = moved
			resp, err := newClient(t, fake).MoveHeldExecution(callContext(t), statepb.MoveHeldExecutionRequest_builder{Hold: updateHold(), NodeId: "node-b"}.Build())
			require.NoError(t, err)
			assert.Equal(t, moved, resp.GetMoved())
			seen := fake.snapshot()
			assert.Equal(t, want, seen.hold)
			assert.Equal(t, "node-b", seen.nodeID)
		}
	})

	t.Run("ReleaseHeldExecution", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		_, err := newClient(t, fake).ReleaseHeldExecution(callContext(t), statepb.ReleaseHeldExecutionRequest_builder{Hold: updateHold()}.Build())
		require.NoError(t, err)
		assert.Equal(t, want, fake.snapshot().hold)
	})

	t.Run("RemoveHeldExecution", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		fake.record = fullRecord(updateTeam, updateSandbox)
		resp, err := newClient(t, fake).RemoveHeldExecution(callContext(t), statepb.RemoveHeldExecutionRequest_builder{Hold: updateHold()}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.Equal(t, fake.record, decode(t, resp.GetRecord()))
		assert.Equal(t, want, fake.snapshot().hold)
	})
}

func (f *fakeStorage) callNames() []string {
	var names []string
	for {
		select {
		case name := <-f.calls:
			names = append(names, name)
		default:
			return names
		}
	}
}

func TestCommitRejectedRecordReleasesTheLock(t *testing.T) {
	t.Parallel()

	withRecord := func(mutate func(*sandboxtypes.Sandbox)) []byte {
		sbx := minimalRecord(updateTeam, updateSandbox)
		mutate(&sbx)

		return encode(t, sbx)
	}
	records := map[string][]byte{
		"not JSON":             []byte("not json"),
		"without IDs":          []byte("{}"),
		"of another team":      withRecord(func(s *sandboxtypes.Sandbox) { s.TeamID = uuid.New() }),
		"of another sandbox":   withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "other" }),
		"empty execution ID":   withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "" }),
		"upper-case execution": withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = strings.ToUpper(uuid.NewString()) }),
		"execution not a UUID": withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "execution-1" }),
		"colon in sandbox ID":  encode(t, minimalRecord(updateTeam, "a:b")),
	}
	for name, record := range records {
		for _, abortFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s abort fails=%v", name, abortFails), func(t *testing.T) {
				t.Parallel()

				sandboxID := updateSandbox
				if name == "colon in sandbox ID" {
					sandboxID = "a:b"
				}
				fake := newFake()
				if abortFails {
					fake.err = errors.New("abort failed")
				}
				_, err := newClient(t, fake).CommitUpdate(callContext(t), statepb.CommitUpdateRequest_builder{
					TeamId: updateTeam.String(), SandboxId: sandboxID, Token: issuedToken(t, updateTeam, sandboxID, storeToken), Record: record,
				}.Build())
				st := requireCode(t, err, codes.InvalidArgument)
				assert.NotContains(t, st.Message(), "abort failed")
				assert.Equal(t, []string{"AbortUpdate"}, fake.callNames(), "the store's commit is not called and the lock is released once")
				assert.Equal(t, []sandboxtypes.UpdateToken{storeToken}, fake.snapshot().aborted, "with the store's own token")
			})
		}
	}
}

func TestUpdateTokenIsBoundToTeamAndSandbox(t *testing.T) {
	t.Parallel()

	token := updateToken(t)
	otherTeam := uuid.New()
	type call func(ctx context.Context, c statepb.StateServiceClient) error
	commit := func(teamID uuid.UUID, sandboxID string, token []byte) call {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.CommitUpdate(ctx, statepb.CommitUpdateRequest_builder{
				TeamId: teamID.String(), SandboxId: sandboxID, Token: token, Record: encode(t, minimalRecord(teamID, sandboxID)),
			}.Build())

			return err
		}
	}
	abort := func(teamID uuid.UUID, sandboxID string, token []byte) call {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.AbortUpdate(ctx, statepb.AbortUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, Token: token}.Build())

			return err
		}
	}

	var fields map[string]any
	require.NoError(t, json.Unmarshal(token, &fields))
	malformed := map[string][]byte{
		"not JSON":              []byte("not json"),
		"JSON of another type":  []byte(`["x"]`),
		"the store's raw token": []byte(`{"lock":"secret-lock-value"}`),
		"an empty object":       []byte("{}"),
	}
	for field := range fields {
		without := map[string]any{}
		for k, v := range fields {
			if k != field {
				without[k] = v
			}
		}
		data, err := json.Marshal(without)
		require.NoError(t, err)
		malformed["without "+field] = data
	}

	tests := map[string]call{
		"commit for another team":    commit(otherTeam, updateSandbox, token),
		"abort for another team":     abort(otherTeam, updateSandbox, token),
		"commit for another sandbox": commit(updateTeam, "sbx-other", token),
		"abort for another sandbox":  abort(updateTeam, "sbx-other", token),
	}
	for name, data := range malformed {
		tests["commit with "+name] = commit(updateTeam, updateSandbox, data)
		tests["abort with "+name] = abort(updateTeam, updateSandbox, data)
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			st := requireCode(t, call(callContext(t), newClient(t, fake)), codes.InvalidArgument)
			assert.NotContains(t, st.Message(), "secret-lock-value")
			assert.Empty(t, fake.callNames(), "the store is not called and nothing is released")
		})
	}

	t.Run("the issuing team and sandbox", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		client := newClient(t, fake)
		require.NoError(t, commit(updateTeam, updateSandbox, token)(callContext(t), client))
		require.NoError(t, abort(updateTeam, updateSandbox, token)(callContext(t), client))
		assert.Equal(t, []string{"CommitUpdate", "AbortUpdate"}, fake.callNames())
		assert.Equal(t, []sandboxtypes.UpdateToken{storeToken}, fake.snapshot().aborted)
	})
}
