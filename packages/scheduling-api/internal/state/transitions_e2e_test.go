package state_test

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
	"google.golang.org/grpc"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type observedStorage struct {
	sandboxtypes.Storage

	waits chan struct{}

	mu    sync.Mutex
	opts  []sandboxtypes.RemoveOpts
	begun []sandboxtypes.StateTransition
}

func newObservedStorage(storage sandboxtypes.Storage) *observedStorage {
	return &observedStorage{Storage: storage, waits: make(chan struct{}, 64)}
}

func (o *observedStorage) BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, error) {
	transition, alreadyDone, err := o.Storage.BeginRemoving(ctx, teamID, sandboxID, opts)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opts = append(o.opts, opts)
	o.begun = append(o.begun, transition)

	return transition, alreadyDone, err
}

func (o *observedStorage) WaitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error) {
	select {
	case o.waits <- struct{}{}:
	default:
	}

	return o.Storage.WaitForTransition(ctx, teamID, sandboxID, transitionID)
}

func (o *observedStorage) awaitWaits(t *testing.T, n int) {
	t.Helper()

	for range n {
		select {
		case <-o.waits:
		case <-time.After(hangTimeout):
			require.Fail(t, "the wait never reached the store")
		}
	}
}

func (o *observedStorage) removeOpts() []sandboxtypes.RemoveOpts {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]sandboxtypes.RemoveOpts(nil), o.opts...)
}

func countWaiting(waiting chan<- struct{}) grpc.DialOption {
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if resp, ok := reply.(*statepb.WaitForTransitionResponse); ok && err == nil && resp.GetOutcome() == statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}

		return err
	})
}

type removeResult struct {
	alreadyDone bool
	errType     string
	errText     string
	state       sandboxtypes.State
	started     bool
	inFlightID  string
	hasOrigEnd  bool
	opts        []sandboxtypes.RemoveOpts
}

type joinScenario struct {
	starter     sandboxtypes.StateAction
	joiner      func(sbx sandboxtypes.Sandbox) sandboxtypes.RemoveOpts
	finish      sandboxtypes.TransitionOutcome
	cancel      bool
	check       func(t *testing.T, err error)
	alreadyDone bool
	started     bool
}

func runJoin(t *testing.T, s redisSetup, path string, tt joinScenario) removeResult {
	t.Helper()

	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
	starter, _, err := s.records.BeginRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: tt.starter})
	require.NoError(t, err)
	require.NotEmpty(t, starter.ID)

	observed := newObservedStorage(s.records)
	var transitions sandboxtypes.StateTransitions = observed
	awaitWait := func() { observed.awaitWaits(t, 1) }
	if path == "rpc" {
		waiting := make(chan struct{}, 64)
		transitions = remote.NewStorage(newConnWithWait(t, observed, shortWait, countWaiting(waiting)))
		awaitWait = func() {
			for range 2 {
				select {
				case <-waiting:
				case <-time.After(hangTimeout):
					require.Fail(t, "the join never crossed two polls")
				}
			}
		}
	}

	joinCtx, cancelJoin := context.WithCancel(ctx)
	defer cancelJoin()
	opts := tt.joiner(sbx)
	type joined struct {
		transition  sandboxtypes.StateTransition
		alreadyDone bool
		err         error
	}
	done := make(chan joined, 1)
	go func() {
		transition, alreadyDone, err := sandboxtypes.StartRemoving(joinCtx, transitions, sbx.TeamID, sbx.SandboxID, opts)
		done <- joined{transition, alreadyDone, err}
	}()
	awaitWait()

	if tt.cancel {
		cancelJoin()
	} else {
		require.NoError(t, s.records.FinishTransition(ctx, sbx.TeamID, sbx.SandboxID, starter.ID, tt.starter, tt.finish))
	}

	var got joined
	select {
	case got = <-done:
	case <-time.After(hangTimeout):
		require.Fail(t, "the join never returned")
	}
	if tt.cancel {
		require.NoError(t, s.records.FinishTransition(ctx, sbx.TeamID, sbx.SandboxID, starter.ID, tt.starter, sandboxtypes.TransitionOutcome{}))
	}
	if got.transition.ID != "" {
		require.NoError(t, s.records.FinishTransition(ctx, sbx.TeamID, sbx.SandboxID, got.transition.ID, opts.Action, sandboxtypes.TransitionOutcome{}))
	}

	assert.Equal(t, tt.alreadyDone, got.alreadyDone, path)
	assert.Equal(t, tt.started, got.transition.ID != "", path)
	tt.check(t, got.err)
	for _, seen := range observed.removeOpts() {
		assert.Equal(t, opts, seen, "%s: every begin gets the caller's options", path)
	}

	result := removeResult{
		alreadyDone: got.alreadyDone,
		state:       got.transition.Sandbox.State,
		started:     got.transition.ID != "",
		inFlightID:  got.transition.InFlightID,
		hasOrigEnd:  got.transition.OriginalEndTime != nil,
		opts:        observed.removeOpts(),
	}
	for i := range result.opts {
		result.opts[i].ExpectExecutionID = ""
	}
	if got.err != nil {
		result.errType = fmt.Sprintf("%T", got.err)
		result.errText = got.err.Error()
	}
	if tt.cancel {
		result.errText = ""
	}

	return result
}

func TestStartRemovingThroughRPC(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	pause := func(sandboxtypes.Sandbox) sandboxtypes.RemoveOpts {
		return sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause}
	}
	fsPause := func(sandboxtypes.Sandbox) sandboxtypes.RemoveOpts {
		return sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause, FilesystemOnly: true}
	}
	restoredAs := func(refusal error) sandboxtypes.TransitionOutcome {
		return sandboxtypes.TransitionOutcomeOf(&sandboxtypes.RestoredTransitionError{Refusal: refusal})
	}
	refused := func(want error) func(t *testing.T, err error) {
		return func(t *testing.T, err error) {
			t.Helper()
			require.Error(t, err)
			assert.Equal(t, want, err, "the joiner gets the refusal the starter got")
		}
	}

	tests := map[string]joinScenario{
		"same target joins": {
			starter: sandboxtypes.StateActionPause, joiner: pause, alreadyDone: true,
			check: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		"restored busy node": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: restoredAs(sandboxtypes.PauseQueueExhaustedError{}),
			check: refused(sandboxtypes.PauseQueueExhaustedError{}),
		},
		"restored envd unresponsive": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: restoredAs(sandboxtypes.PauseRefusedEnvdUnresponsiveError{}),
			check: refused(sandboxtypes.PauseRefusedEnvdUnresponsiveError{}),
		},
		"restored unsupported, filesystem joiner": {
			starter: sandboxtypes.StateActionPause, joiner: fsPause, finish: restoredAs(sandboxtypes.PauseUnsupportedError{}),
			check: refused(sandboxtypes.PauseUnsupportedError{}),
		},
		"restored unsupported, memory joiner": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: restoredAs(sandboxtypes.PauseUnsupportedError{}),
			check: refused(sandboxtypes.PauseQueueExhaustedError{}),
		},
		"starter failed": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: sandboxtypes.TransitionOutcomeOf(errors.New("boom")),
			check: func(t *testing.T, err error) {
				t.Helper()
				require.EqualError(t, err, "failed waiting for transition: transition failed: boom")
				failed, ok := errors.AsType[*sandboxtypes.TransitionFailedError](err)
				require.True(t, ok, "the failure is typed")
				assert.Equal(t, "boom", failed.Message)
			},
		},
		"another target begins again": {
			starter: sandboxtypes.StateActionPause,
			joiner: func(sbx sandboxtypes.Sandbox) sandboxtypes.RemoveOpts {
				return sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill, Reason: sandboxtypes.KillReasonRequest, ExpectExecutionID: sbx.ExecutionID}
			},
			started: true,
			check: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		"joiner's context ends": {
			starter: sandboxtypes.StateActionPause, joiner: pause, cancel: true,
			check: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, context.Canceled)
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			direct := runJoin(t, s, "direct", tt)
			rpc := runJoin(t, s, "rpc", tt)
			assert.Equal(t, direct, rpc, "the RPC path returns what the in-process path returns")
		})
	}
}
