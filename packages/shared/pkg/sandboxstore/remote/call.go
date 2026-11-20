package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const (
	defaultDeadline = 70 * time.Second
	longPollBound   = 5 * time.Second
	longPollMargin  = 5 * time.Second
	maxMessageSize  = 32 << 20
)

var stateErrors = map[scheduling.StateOutcome]error{
	scheduling.StateOutcome_STATE_OUTCOME_OK:                   nil,
	scheduling.StateOutcome_STATE_OUTCOME_NOT_FOUND:            sandboxtypes.ErrNotFound,
	scheduling.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD:       sandboxtypes.ErrLifecycleHeld,
	scheduling.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH:   sandboxtypes.ErrExecutionMismatch,
	scheduling.StateOutcome_STATE_OUTCOME_LOCK_LOST:            sandboxtypes.ErrLockLost,
	scheduling.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT:        sandboxtypes.ErrHoldConflict,
	scheduling.StateOutcome_STATE_OUTCOME_NOT_HELD:             sandboxtypes.ErrNotHeld,
	scheduling.StateOutcome_STATE_OUTCOME_EVICTION_IN_PROGRESS: sandboxtypes.ErrEvictionInProgress,
	scheduling.StateOutcome_STATE_OUTCOME_EVICTION_NOT_NEEDED:  sandboxtypes.ErrEvictionNotNeeded,
	scheduling.StateOutcome_STATE_OUTCOME_RESTORE_CONFLICT:     sandboxtypes.ErrRestoreConflict,
}

var states = map[scheduling.SandboxState]sandboxtypes.State{
	scheduling.SandboxState_SANDBOX_STATE_RUNNING:      sandboxtypes.StateRunning,
	scheduling.SandboxState_SANDBOX_STATE_PAUSING:      sandboxtypes.StatePausing,
	scheduling.SandboxState_SANDBOX_STATE_KILLING:      sandboxtypes.StateKilling,
	scheduling.SandboxState_SANDBOX_STATE_SNAPSHOTTING: sandboxtypes.StateSnapshotting,
}

var wireStates = map[sandboxtypes.State]scheduling.SandboxState{
	sandboxtypes.StateRunning:      scheduling.SandboxState_SANDBOX_STATE_RUNNING,
	sandboxtypes.StatePausing:      scheduling.SandboxState_SANDBOX_STATE_PAUSING,
	sandboxtypes.StateKilling:      scheduling.SandboxState_SANDBOX_STATE_KILLING,
	sandboxtypes.StateSnapshotting: scheduling.SandboxState_SANDBOX_STATE_SNAPSHOTTING,
}

type rpc[Req, Resp any] func(context.Context, Req, ...grpc.CallOption) (Resp, error)

func unary[Req, Resp any](ctx context.Context, method string, call rpc[Req, Resp], req Req) (Resp, error) {
	callCtx, cancel := withDeadline(ctx)
	defer cancel()

	return invoke(ctx, callCtx, method, call, req)
}

// Every call runs with a deadline: callers pass contexts without one (context.WithoutCancel).
func withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, defaultDeadline)
}

func longPoll[Req, Resp any](ctx context.Context, method string, call rpc[Req, Resp], req Req) (Resp, error) {
	callCtx, cancel := context.WithTimeout(ctx, longPollBound+longPollMargin)
	defer cancel()

	return invoke(ctx, callCtx, method, call, req)
}

func invoke[Req, Resp any](ctx, callCtx context.Context, method string, call rpc[Req, Resp], req Req) (Resp, error) {
	resp, err := call(callCtx, req, grpc.MaxCallRecvMsgSize(maxMessageSize))
	if err != nil {
		var zero Resp

		return zero, failed(ctx, method, err)
	}

	return resp, nil
}

// A partial result is never returned: a caller would act on records missing from it.
func readRecords[Req, T any, P interface {
	*T
	GetRecords() [][]byte
}](ctx context.Context, method string, open func(context.Context, Req, ...grpc.CallOption) (grpc.ServerStreamingClient[T], error), req Req, items []sandboxtypes.Sandbox) ([]sandboxtypes.Sandbox, error) {
	callCtx, cancel := withDeadline(ctx)
	defer cancel()

	stream, err := open(callCtx, req, grpc.MaxCallRecvMsgSize(maxMessageSize))
	if err != nil {
		return nil, failed(ctx, method, err)
	}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return items, nil
		}
		if err != nil {
			return nil, failed(ctx, method, err)
		}
		for _, record := range P(resp).GetRecords() {
			sbx, err := decodeRecord(method, record)
			if err != nil {
				return nil, err
			}
			items = append(items, sbx)
		}
	}
}

// When the caller's context has ended, the error must match ctx.Err(): a gRPC status does not.
func failed(ctx context.Context, method string, err error) error {
	if ctxErr := callerError(ctx); ctxErr != nil {
		return fmt.Errorf("%s: %w: %w", method, ctxErr, err)
	}

	return fmt.Errorf("%s: %w", method, err)
}

// A call can fail at the caller's deadline before the context's timer marks it done.
func callerError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}

	return nil
}

func outcomeError(method string, outcome scheduling.StateOutcome) error {
	err, ok := stateErrors[outcome]
	if !ok {
		return unknownOutcome(method, outcome)
	}

	return err
}

func transitionError(method string, outcome scheduling.StateOutcome, current scheduling.SandboxState, target sandboxtypes.State) error {
	if outcome != scheduling.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION {
		return outcomeError(method, outcome)
	}
	state, ok := states[current]
	if !ok {
		return fmt.Errorf("%s: unknown current state %v", method, current)
	}

	return &sandboxtypes.InvalidStateTransitionError{CurrentState: state, TargetState: target}
}

func unknownOutcome(method string, outcome fmt.Stringer) error {
	return fmt.Errorf("%s: unknown outcome %v", method, outcome)
}

// Records carry access tokens: an error never quotes one.
func encodeRecord(method string, sbx sandboxtypes.Sandbox) ([]byte, error) {
	record, err := json.Marshal(sbx)
	if err != nil {
		return nil, fmt.Errorf("%s: encode the record: %w", method, err)
	}

	return record, nil
}

func decodeRecord(method string, record []byte) (sandboxtypes.Sandbox, error) {
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(record, &sbx); err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("%s: decode the record: %w", method, err)
	}

	return sbx, nil
}
