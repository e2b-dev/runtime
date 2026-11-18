package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type fakeState struct {
	sandboxtypes.Storage

	calls atomic.Int32

	mu      sync.Mutex
	records map[string]sandboxtypes.Sandbox

	get       func(ctx context.Context) (sandboxtypes.Sandbox, error)
	teamItems func(ctx context.Context) ([]sandboxtypes.Sandbox, error)
	wait      func(ctx context.Context) (string, error)
}

func (f *fakeState) Add(_ context.Context, sbx sandboxtypes.Sandbox) error {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.records == nil {
		f.records = map[string]sandboxtypes.Sandbox{}
	}
	f.records[sbx.SandboxID] = sbx

	return nil
}

func (f *fakeState) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandboxtypes.Sandbox, error) {
	f.calls.Add(1)
	if f.get != nil {
		return f.get(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	sbx, ok := f.records[sandboxID]
	if !ok || sbx.TeamID != teamID {
		return sandboxtypes.Sandbox{}, sandboxtypes.ErrNotFound
	}

	return sbx, nil
}

func (f *fakeState) TeamItems(ctx context.Context, teamID uuid.UUID, _ []sandboxtypes.State) ([]sandboxtypes.Sandbox, error) {
	f.calls.Add(1)
	if f.teamItems != nil {
		return f.teamItems(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var items []sandboxtypes.Sandbox
	for _, sbx := range f.records {
		if sbx.TeamID == teamID {
			items = append(items, sbx)
		}
	}

	return items, nil
}

func (f *fakeState) HoldExecution(context.Context, sandboxtypes.Hold, time.Time) error {
	f.calls.Add(1)

	return nil
}

func (f *fakeState) WaitForTransition(ctx context.Context, _ uuid.UUID, _ string, _ string) (string, error) {
	f.calls.Add(1)

	return f.wait(ctx)
}

func testRecord(teamID uuid.UUID) sandboxtypes.Sandbox {
	start := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	return sandboxtypes.Sandbox{
		SandboxID:   "sbx",
		TeamID:      teamID,
		ExecutionID: uuid.NewString(),
		StartTime:   start,
		EndTime:     start.Add(time.Hour),
		State:       sandboxtypes.StateRunning,
	}
}

func encodeRecord(t *testing.T, sbx sandboxtypes.Sandbox) []byte {
	t.Helper()

	record, err := json.Marshal(sbx)
	require.NoError(t, err)

	return record
}

func violationIDs(t *testing.T, err error) []string {
	t.Helper()

	var ids []string
	for _, detail := range status.Convert(err).Details() {
		violations, ok := detail.(*validate.Violations)
		require.True(t, ok, "detail %T", detail)
		for _, violation := range violations.GetViolations() {
			id := protovalidate.FieldPathString(violation.GetField())
			if id == "" {
				id = violation.GetRuleId()
			}
			ids = append(ids, id)
		}
	}

	return ids
}

func TestStreamValidation(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		req   *statepb.TeamItemsRequest
		field string
	}{
		"invalid team_id": {statepb.TeamItemsRequest_builder{TeamId: "team"}.Build(), "team_id"},
		"unspecified state": {statepb.TeamItemsRequest_builder{
			TeamId: uuid.NewString(),
			States: []statepb.SandboxState{statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED},
		}.Build(), "states[0]"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeState{}

			stream, err := newStateClient(t, fake).TeamItems(callContext(t), tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Equal(t, codes.InvalidArgument, status.Code(err), err)
			assert.Equal(t, []string{tc.field}, violationIDs(t, err), "the stream validation interceptor reports the field")
			assert.Zero(t, fake.calls.Load(), "the store is not called")
		})
	}
}

func TestStateUnaryValidation(t *testing.T) {
	t.Parallel()
	fake := &fakeState{}

	_, err := newStateClient(t, fake).HoldExecution(callContext(t), statepb.HoldExecutionRequest_builder{
		Hold: statepb.Hold_builder{
			TeamId:      uuid.NewString(),
			SandboxId:   "sbx",
			ExecutionId: uuid.NewString(),
			Id:          "hold",
		}.Build(),
		Until: timestamppb.New(time.Now().Add(time.Minute)),
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), err)
	assert.Equal(t, []string{"hold.source_node_id"}, violationIDs(t, err), "the validation interceptor reports the message rule")
	assert.Zero(t, fake.calls.Load(), "the store is not called")
}

func TestStateRecords(t *testing.T) {
	t.Parallel()
	team := uuid.New()
	sbx := testRecord(team)
	client := newStateClient(t, &fakeState{records: map[string]sandboxtypes.Sandbox{"sbx": sbx}})
	ctx := callContext(t)

	got, err := client.Get(ctx, statepb.GetRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())
	require.NoError(t, err)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, got.GetOutcome())
	assert.JSONEq(t, string(encodeRecord(t, sbx)), string(got.GetRecord()), "Get returns the store's record")

	stream, err := client.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: team.String()}.Build())
	require.NoError(t, err)
	var records [][]byte
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		records = append(records, resp.GetRecords()...)
	}
	require.Len(t, records, 1, "TeamItems streams the store's records")
	assert.JSONEq(t, string(encodeRecord(t, sbx)), string(records[0]))
}

func TestStatePanicIsInternal(t *testing.T) {
	t.Parallel()
	fake := &fakeState{get: func(context.Context) (sandboxtypes.Sandbox, error) { panic("backend bug") }}

	_, err := newStateClient(t, fake).Get(callContext(t), statepb.GetRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx"}.Build())
	require.Equal(t, codes.Internal, status.Code(err), err)
	assert.Equal(t, "internal error", status.Convert(err).Message())
}

func TestStreamPanicIsInternal(t *testing.T) {
	t.Parallel()
	team := uuid.New()
	var panicked atomic.Bool
	fake := &fakeState{teamItems: func(context.Context) ([]sandboxtypes.Sandbox, error) {
		if panicked.CompareAndSwap(false, true) {
			panic("stream backend bug")
		}

		return []sandboxtypes.Sandbox{testRecord(team)}, nil
	}}
	client := newStateClient(t, fake)
	ctx := callContext(t)

	stream, err := client.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: team.String()}.Build())
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Internal, status.Code(err), err)
	assert.Equal(t, "internal error", status.Convert(err).Message())

	stream, err = client.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: team.String()}.Build())
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err, "the server still serves after a stream panic")
	assert.Len(t, resp.GetRecords(), 1)
	assert.Equal(t, int32(2), fake.calls.Load(), "both calls reached the store")
}

func TestStateUnaryDeadline(t *testing.T) {
	t.Parallel()
	assert.Greater(t, server.UnaryDeadline, time.Minute, "the unary deadline must cover the store's one-minute wait for a record's lock")
	deadlines := make(chan time.Time, 1)
	fake := &fakeState{get: func(ctx context.Context) (sandboxtypes.Sandbox, error) {
		deadline, _ := ctx.Deadline()
		deadlines <- deadline

		return sandboxtypes.Sandbox{}, sandboxtypes.ErrNotFound
	}}

	before := time.Now()
	resp, err := newStateClient(t, fake).Get(t.Context(), statepb.GetRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx"}.Build())
	after := time.Now()
	require.NoError(t, err)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, resp.GetOutcome())
	deadline := <-deadlines
	require.False(t, deadline.IsZero(), "the handler has a deadline")
	assert.False(t, deadline.Before(before.Add(server.UnaryDeadline)), "deadline %s is the server's unary deadline", deadline)
	assert.False(t, deadline.After(after.Add(server.UnaryDeadline)), "deadline %s is the server's unary deadline", deadline)
}

func TestWaitForTransitionWaiting(t *testing.T) {
	t.Parallel()
	assert.Less(t, server.MaxWait, server.UnaryDeadline)
	remaining := make(chan time.Duration, 1)
	fake := &fakeState{wait: func(ctx context.Context) (string, error) {
		var left time.Duration
		if deadline, ok := ctx.Deadline(); ok {
			left = time.Until(deadline)
		}
		remaining <- left

		return "transition", context.DeadlineExceeded
	}}

	resp, err := newStateClient(t, fake).WaitForTransition(callContext(t), statepb.WaitForTransitionRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx"}.Build())
	require.NoError(t, err)
	assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, resp.GetOutcome())
	left := <-remaining
	assert.Positive(t, left, "the wait has a deadline")
	assert.LessOrEqual(t, left, server.MaxWait, "the wait ends at most maxWait after it starts")
}

func TestMessageSize(t *testing.T) {
	t.Parallel()

	t.Run("a large record round-trips", func(t *testing.T) {
		t.Parallel()
		team := uuid.New()
		sbx := testRecord(team)
		sbx.Metadata = map[string]string{"pad": strings.Repeat("a", 5<<20)}
		record := encodeRecord(t, sbx)
		client := newStateClient(t, &fakeState{}, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(server.MaxMessageSize)))
		ctx := callContext(t)

		added, err := client.Add(ctx, statepb.AddRequest_builder{Record: record}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, added.GetOutcome())
		got, err := client.Get(ctx, statepb.GetRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())
		require.NoError(t, err)
		assert.True(t, bytes.Equal(record, got.GetRecord()), "the record comes back unchanged: sent %d bytes, got %d", len(record), len(got.GetRecord()))
	})

	t.Run("a request above the limit is refused", func(t *testing.T) {
		t.Parallel()
		sbx := testRecord(uuid.New())
		sbx.Metadata = map[string]string{"pad": strings.Repeat("a", server.MaxMessageSize)}
		fake := &fakeState{}

		_, err := newStateClient(t, fake).Add(callContext(t), statepb.AddRequest_builder{Record: encodeRecord(t, sbx)}.Build())
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		assert.Zero(t, fake.calls.Load(), "the store is not called")
	})
}
