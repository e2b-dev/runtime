package state_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/state"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type observed struct {
	added    sandboxtypes.Sandbox
	states   []sandboxtypes.State
	reported []sandboxtypes.NodeSandbox
	nodeID   string
	token    sandboxtypes.UpdateToken
	updated  sandboxtypes.Sandbox
	hold     sandboxtypes.Hold
	until    time.Time
	aborted  []sandboxtypes.UpdateToken

	removeOpts   sandboxtypes.RemoveOpts
	action       sandboxtypes.StateAction
	transitionID string
	result       sandboxtypes.TransitionOutcome
	restoring    sandboxtypes.StateTransition
	restoreOpts  sandboxtypes.RestoreOpts
	placement    *orchestrator.SandboxNetworkPlacement
	waitDeadline bool
}

type fakeStorage struct {
	sandboxtypes.Storage

	calls chan string

	err     error
	record  sandboxtypes.Sandbox
	items   []sandboxtypes.Sandbox
	counts  map[uuid.UUID]int64
	orphans []sandboxtypes.NodeSandbox
	token   sandboxtypes.UpdateToken
	moved   bool
	block   func(ctx context.Context)

	transition  sandboxtypes.StateTransition
	alreadyDone bool
	waitID      string
	wait        func(ctx context.Context) (string, error)

	mu   sync.Mutex
	seen observed
}

func newFake() *fakeStorage {
	return &fakeStorage{calls: make(chan string, 16)}
}

func (f *fakeStorage) call(ctx context.Context, name string) {
	f.calls <- name
	if f.block != nil {
		f.block(ctx)
	}
}

func (f *fakeStorage) callCount() int {
	return len(f.calls)
}

func (f *fakeStorage) observe(update func(*observed)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	update(&f.seen)
}

func (f *fakeStorage) snapshot() observed {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.seen
}

func (f *fakeStorage) Add(ctx context.Context, sbx sandboxtypes.Sandbox) error {
	f.call(ctx, "Add")
	f.observe(func(o *observed) { o.added = sbx })

	return f.err
}

func (f *fakeStorage) Get(ctx context.Context, _ uuid.UUID, _ string) (sandboxtypes.Sandbox, error) {
	f.call(ctx, "Get")

	return f.record, f.err
}

func (f *fakeStorage) Remove(ctx context.Context, _ uuid.UUID, _, _ string) error {
	f.call(ctx, "Remove")

	return f.err
}

func (f *fakeStorage) TeamItems(ctx context.Context, _ uuid.UUID, states []sandboxtypes.State) ([]sandboxtypes.Sandbox, error) {
	f.call(ctx, "TeamItems")
	f.observe(func(o *observed) { o.states = states })

	return f.items, f.err
}

func (f *fakeStorage) ExpiredItems(ctx context.Context) ([]sandboxtypes.Sandbox, error) {
	f.call(ctx, "ExpiredItems")

	return f.items, f.err
}

func (f *fakeStorage) TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error) {
	f.call(ctx, "TeamsWithSandboxCount")

	return f.counts, f.err
}

func (f *fakeStorage) Reconcile(ctx context.Context, sandboxes []sandboxtypes.NodeSandbox, nodeID string) []sandboxtypes.NodeSandbox {
	f.call(ctx, "Reconcile")
	f.observe(func(o *observed) { o.reported, o.nodeID = sandboxes, nodeID })

	return f.orphans
}

func (f *fakeStorage) BeginUpdate(ctx context.Context, _ uuid.UUID, _ string) (sandboxtypes.Sandbox, sandboxtypes.UpdateToken, error) {
	f.call(ctx, "BeginUpdate")
	if f.err != nil {
		return sandboxtypes.Sandbox{}, "", f.err
	}

	return f.record, f.token, nil
}

func (f *fakeStorage) CommitUpdate(ctx context.Context, _ uuid.UUID, _ string, token sandboxtypes.UpdateToken, updated sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
	f.call(ctx, "CommitUpdate")
	f.observe(func(o *observed) { o.token, o.updated = token, updated })
	if f.err != nil {
		return sandboxtypes.Sandbox{}, f.err
	}

	return updated, nil
}

func (f *fakeStorage) AbortUpdate(ctx context.Context, _ uuid.UUID, _ string, token sandboxtypes.UpdateToken) error {
	f.call(ctx, "AbortUpdate")
	f.observe(func(o *observed) { o.token, o.aborted = token, append(o.aborted, token) })

	return f.err
}

func (f *fakeStorage) HoldExecution(ctx context.Context, hold sandboxtypes.Hold, until time.Time) error {
	f.call(ctx, "HoldExecution")
	f.observe(func(o *observed) { o.hold, o.until = hold, until })

	return f.err
}

func (f *fakeStorage) MoveHeldExecution(ctx context.Context, hold sandboxtypes.Hold, nodeID string) (bool, error) {
	f.call(ctx, "MoveHeldExecution")
	f.observe(func(o *observed) { o.hold, o.nodeID = hold, nodeID })
	if f.err != nil {
		return false, f.err
	}

	return f.moved, nil
}

func (f *fakeStorage) ReleaseHeldExecution(ctx context.Context, hold sandboxtypes.Hold) error {
	f.call(ctx, "ReleaseHeldExecution")
	f.observe(func(o *observed) { o.hold = hold })

	return f.err
}

func (f *fakeStorage) RemoveHeldExecution(ctx context.Context, hold sandboxtypes.Hold) (sandboxtypes.Sandbox, error) {
	f.call(ctx, "RemoveHeldExecution")
	f.observe(func(o *observed) { o.hold = hold })
	if f.err != nil {
		return sandboxtypes.Sandbox{}, f.err
	}

	return f.record, nil
}

func (f *fakeStorage) BeginRemoving(ctx context.Context, _ uuid.UUID, _ string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, error) {
	f.call(ctx, "BeginRemoving")
	f.observe(func(o *observed) { o.removeOpts = opts })

	return f.transition, f.alreadyDone, f.err
}

func (f *fakeStorage) FinishTransition(ctx context.Context, _ uuid.UUID, _ string, transitionID string, action sandboxtypes.StateAction, outcome sandboxtypes.TransitionOutcome) error {
	f.call(ctx, "FinishTransition")
	f.observe(func(o *observed) { o.transitionID, o.action, o.result = transitionID, action, outcome })

	return f.err
}

func (f *fakeStorage) RestoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, opts sandboxtypes.RestoreOpts) (sandboxtypes.Sandbox, error) {
	f.call(ctx, "RestoreRunning")
	f.observe(func(o *observed) { o.restoring, o.restoreOpts = transition, opts })
	if f.err != nil {
		return sandboxtypes.Sandbox{}, f.err
	}

	return f.record, nil
}

func (f *fakeStorage) RestoreCheckpointed(ctx context.Context, transition sandboxtypes.StateTransition, placement *orchestrator.SandboxNetworkPlacement) (sandboxtypes.Sandbox, error) {
	f.call(ctx, "RestoreCheckpointed")
	f.observe(func(o *observed) { o.restoring, o.placement = transition, placement })
	if f.err != nil {
		return sandboxtypes.Sandbox{}, f.err
	}

	return f.record, nil
}

func (f *fakeStorage) WaitForTransition(ctx context.Context, _ uuid.UUID, _ string, transitionID string) (string, error) {
	f.call(ctx, "WaitForTransition")
	_, hasDeadline := ctx.Deadline()
	f.observe(func(o *observed) { o.transitionID, o.waitDeadline = transitionID, hasDeadline })
	if f.wait != nil {
		return f.wait(ctx)
	}

	return f.waitID, f.err
}

type teamItemsStream struct {
	grpc.ServerStreamingServer[statepb.TeamItemsResponse]

	context func() context.Context
}

func (s teamItemsStream) Context() context.Context {
	return s.context()
}

type expiredItemsStream struct {
	grpc.ServerStreamingServer[statepb.ExpiredItemsResponse]

	context func() context.Context
}

func (s expiredItemsStream) Context() context.Context {
	return s.context()
}

type outcomeCall func(ctx context.Context, client statepb.StateServiceClient) (statepb.StateOutcome, error)

func outcomeCalls(t *testing.T) map[string]outcomeCall {
	t.Helper()

	team := uuid.New()
	record := encode(t, minimalRecord(team, "sbx"))

	return map[string]outcomeCall{
		"Add": func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
			resp, err := c.Add(ctx, statepb.AddRequest_builder{Record: record}.Build())

			return resp.GetOutcome(), err
		},
		"Get": func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
			resp, err := c.Get(ctx, statepb.GetRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())

			return resp.GetOutcome(), err
		},
		"Remove": func(ctx context.Context, c statepb.StateServiceClient) (statepb.StateOutcome, error) {
			resp, err := c.Remove(ctx, statepb.RemoveRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())

			return resp.GetOutcome(), err
		},
	}
}

type rpcCall func(ctx context.Context, client statepb.StateServiceClient) error

func errorCalls(t *testing.T) map[string]rpcCall {
	t.Helper()

	calls := map[string]rpcCall{
		"TeamItems": func(ctx context.Context, c statepb.StateServiceClient) error {
			stream, err := c.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: uuid.NewString()}.Build())
			if err != nil {
				return err
			}
			_, _, err = readTeamItems(t, stream)

			return err
		},
		"ExpiredItems": func(ctx context.Context, c statepb.StateServiceClient) error {
			stream, err := c.ExpiredItems(ctx, statepb.ExpiredItemsRequest_builder{}.Build())
			if err != nil {
				return err
			}
			_, _, err = readExpiredItems(t, stream)

			return err
		},
		"TeamsWithSandboxCount": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.TeamsWithSandboxCount(ctx, statepb.TeamsWithSandboxCountRequest_builder{}.Build())

			return err
		},
	}
	for name, call := range outcomeCalls(t) {
		calls[name] = func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := call(ctx, c)

			return err
		}
	}

	return calls
}

func TestOutcomeMapping(t *testing.T) {
	t.Parallel()

	sentinels := []struct {
		err  error
		want statepb.StateOutcome
	}{
		{sandboxtypes.ErrNotFound, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND},
		{sandboxtypes.ErrLifecycleHeld, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD},
		{sandboxtypes.ErrExecutionMismatch, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH},
		{sandboxtypes.ErrLockLost, statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST},
	}
	for name, call := range outcomeCalls(t) {
		for _, sentinel := range sentinels {
			for _, wrapped := range []bool{false, true} {
				err := sentinel.err
				if wrapped {
					err = fmt.Errorf("sandbox %q: %w", "sbx", sentinel.err)
				}
				t.Run(fmt.Sprintf("%s %v wrapped=%v", name, sentinel.want, wrapped), func(t *testing.T) {
					t.Parallel()

					fake := newFake()
					fake.err = err
					outcome, callErr := call(callContext(t), newClient(t, fake))
					require.NoError(t, callErr)
					assert.Equal(t, sentinel.want, outcome)
				})
			}
		}
	}

	t.Run("nil is OK", func(t *testing.T) {
		t.Parallel()

		for name, call := range outcomeCalls(t) {
			outcome, err := call(callContext(t), newClient(t, newFake()))
			require.NoError(t, err, name)
			assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, outcome, name)
		}
	})
}

func TestGetReturnsRecordOnlyWhenOK(t *testing.T) {
	t.Parallel()

	team := uuid.New()
	full := fullRecord(team, "sbx")

	fake := newFake()
	fake.record = full
	resp, err := newClient(t, fake).Get(callContext(t), statepb.GetRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())
	require.NoError(t, err)
	assert.Equal(t, encode(t, full), resp.GetRecord())
	assert.Equal(t, full, decode(t, resp.GetRecord()))

	missing := newFake()
	missing.err = sandboxtypes.ErrNotFound
	resp, err = newClient(t, missing).Get(callContext(t), statepb.GetRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())
	require.NoError(t, err)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, resp.GetOutcome())
	assert.Empty(t, resp.GetRecord())
}

func TestAddDecodesRecord(t *testing.T) {
	t.Parallel()

	full := fullRecord(uuid.New(), "sbx")
	requireNoZeroField(t, full)

	fake := newFake()
	resp, err := newClient(t, fake).Add(callContext(t), statepb.AddRequest_builder{Record: encode(t, full)}.Build())
	require.NoError(t, err)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
	assert.Equal(t, full, fake.snapshot().added, "the store receives the caller's record")

	withUnknown := []byte(`{"sandboxID":"sbx","teamID":"` + full.TeamID.String() + `","executionID":"` + uuid.NewString() + `","futureField":{"x":1}}`)
	fake = newFake()
	resp, err = newClient(t, fake).Add(callContext(t), statepb.AddRequest_builder{Record: withUnknown}.Build())
	require.NoError(t, err)
	assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome(), "unknown fields are ignored")
	assert.Equal(t, "sbx", fake.snapshot().added.SandboxID)
}

func TestBackendFailureIsInternal(t *testing.T) {
	t.Parallel()

	const cause = "backend exploded with secret detail"
	for name, call := range errorCalls(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.err = errors.New(cause)
			st := requireCode(t, call(callContext(t), newClient(t, fake)), codes.Internal)
			assert.Equal(t, "internal error", st.Message())
			assert.NotContains(t, st.Message(), cause)
			assert.Equal(t, 1, fake.callCount())
		})
	}
}

func TestCancelledCaller(t *testing.T) {
	t.Parallel()

	t.Run("TeamItems over gRPC", func(t *testing.T) {
		t.Parallel()

		started := make(chan struct{})
		fake := newFake()
		fake.err = errors.New("backend gave up")
		fake.block = func(ctx context.Context) {
			close(started)
			<-ctx.Done()
		}
		client := newClient(t, fake)

		ctx, cancel := context.WithCancel(callContext(t))
		go func() {
			<-started
			cancel()
		}()
		stream, err := client.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: uuid.NewString()}.Build())
		require.NoError(t, err)
		_, _, err = readTeamItems(t, stream)
		requireCode(t, err, codes.Canceled)
	})

	team := uuid.New()
	record := encode(t, minimalRecord(team, "sbx"))
	handlerCalls := map[string]func(ctx context.Context, s *state.Server) error{
		"Add": func(ctx context.Context, s *state.Server) error {
			_, err := s.Add(ctx, statepb.AddRequest_builder{Record: record}.Build())

			return err
		},
		"Get": func(ctx context.Context, s *state.Server) error {
			_, err := s.Get(ctx, statepb.GetRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())

			return err
		},
		"Remove": func(ctx context.Context, s *state.Server) error {
			_, err := s.Remove(ctx, statepb.RemoveRequest_builder{TeamId: team.String(), SandboxId: "sbx"}.Build())

			return err
		},
		"TeamsWithSandboxCount": func(ctx context.Context, s *state.Server) error {
			_, err := s.TeamsWithSandboxCount(ctx, statepb.TeamsWithSandboxCountRequest_builder{}.Build())

			return err
		},
	}
	for name, call := range handlerCalls {
		t.Run("handler "+name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			fake := newFake()
			fake.err = errors.New("backend gave up")
			st := requireCode(t, call(ctx, state.NewServer(fake, hangTimeout)), codes.Canceled)
			assert.NotContains(t, st.Message(), "backend gave up")
		})
	}

	t.Run("handler TeamItems", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		fake := newFake()
		fake.err = errors.New("backend gave up")
		stream := teamItemsStream{context: func() context.Context { return ctx }}
		err := state.NewServer(fake, hangTimeout).TeamItems(statepb.TeamItemsRequest_builder{TeamId: team.String()}.Build(), stream)
		st := requireCode(t, err, codes.Canceled)
		assert.NotContains(t, st.Message(), "backend gave up")
	})

	t.Run("handler ExpiredItems", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		fake := newFake()
		fake.err = errors.New("backend gave up")
		stream := expiredItemsStream{context: func() context.Context { return ctx }}
		err := state.NewServer(fake, hangTimeout).ExpiredItems(statepb.ExpiredItemsRequest_builder{}.Build(), stream)
		st := requireCode(t, err, codes.Canceled)
		assert.NotContains(t, st.Message(), "backend gave up")
	})
}

func TestInvalidRequests(t *testing.T) {
	t.Parallel()

	team := uuid.New()
	withRecord := func(mutate func(*sandboxtypes.Sandbox)) []byte {
		sbx := minimalRecord(team, "sbx")
		mutate(&sbx)

		return encode(t, sbx)
	}
	add := func(record []byte) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.Add(ctx, statepb.AddRequest_builder{Record: record}.Build())

			return err
		}
	}
	get := func(teamID, sandboxID string) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.Get(ctx, statepb.GetRequest_builder{TeamId: teamID, SandboxId: sandboxID}.Build())

			return err
		}
	}
	remove := func(teamID, sandboxID string) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.Remove(ctx, statepb.RemoveRequest_builder{TeamId: teamID, SandboxId: sandboxID}.Build())

			return err
		}
	}
	teamItems := func(teamID string, states ...statepb.SandboxState) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			stream, err := c.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: teamID, States: states}.Build())
			if err != nil {
				return err
			}
			_, _, err = readTeamItems(t, stream)

			return err
		}
	}
	reconcile := func(mutate func(*statepb.NodeSandbox)) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			sbx := statepb.NodeSandbox_builder{SandboxId: "sbx", TeamId: team.String(), ClusterId: uuid.NewString()}.Build()
			mutate(sbx)
			_, err := c.Reconcile(ctx, statepb.ReconcileRequest_builder{Sandboxes: []*statepb.NodeSandbox{sbx}}.Build())

			return err
		}
	}

	tests := map[string]rpcCall{
		"Add not JSON":             add([]byte("not json")),
		"Add empty":                add(nil),
		"Add wrong type":           add([]byte(`{"sandboxID":1}`)),
		"Add null":                 add([]byte("null")),
		"Add empty sandbox ID":     add(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "" })),
		"Add opening brace":        add(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "a{b" })),
		"Add closing brace":        add(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "a}b" })),
		"Add nil team":             add(withRecord(func(s *sandboxtypes.Sandbox) { s.TeamID = uuid.Nil })),
		"Add colon in sandbox ID":  add(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "a:b" })),
		"Add empty execution ID":   add(withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "" })),
		"Add execution not a UUID": add(withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "execution-1" })),
		"Add upper-case execution": add(withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = strings.ToUpper(uuid.NewString()) })),
		"Add braced execution":     add(withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "{" + uuid.NewString() + "}" })),
		"Add URN execution":        add(withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "urn:uuid:" + uuid.NewString() })),
		"Add unhyphenated execution": add(withRecord(func(s *sandboxtypes.Sandbox) {
			s.ExecutionID = strings.ReplaceAll(uuid.NewString(), "-", "")
		})),
		"Get empty team":            get("", "sbx"),
		"Get team not a UUID":       get("team-1", "sbx"),
		"Get empty sandbox":         get(team.String(), ""),
		"Get braced sandbox":        get(team.String(), "a{b"),
		"Remove team not a UUID":    remove("team-1", "sbx"),
		"Remove empty sandbox":      remove(team.String(), ""),
		"Remove braced sandbox":     remove(team.String(), "a}b"),
		"TeamItems team not a UUID": teamItems("team-1"),
		"TeamItems unknown state":   teamItems(team.String(), statepb.SandboxState_SANDBOX_STATE_RUNNING, statepb.SandboxState(99)),
		"TeamItems unspecified":     teamItems(team.String(), statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED),
		"Reconcile bad team":        reconcile(func(s *statepb.NodeSandbox) { s.SetTeamId("team-1") }),
		"Reconcile bad cluster":     reconcile(func(s *statepb.NodeSandbox) { s.SetClusterId("") }),
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			requireCode(t, call(callContext(t), newClient(t, fake)), codes.InvalidArgument)
			assert.Zero(t, fake.callCount(), "the store is not called")
		})
	}
}

func TestStatesMapping(t *testing.T) {
	t.Parallel()

	known := map[sandboxtypes.State]statepb.SandboxState{
		sandboxtypes.StateRunning:      statepb.SandboxState_SANDBOX_STATE_RUNNING,
		sandboxtypes.StatePausing:      statepb.SandboxState_SANDBOX_STATE_PAUSING,
		sandboxtypes.StateKilling:      statepb.SandboxState_SANDBOX_STATE_KILLING,
		sandboxtypes.StateSnapshotting: statepb.SandboxState_SANDBOX_STATE_SNAPSHOTTING,
	}
	require.Len(t, statepb.SandboxState_name, len(known)+1, "every sandboxtypes.State has one enum value besides UNSPECIFIED")

	for want, value := range known {
		fake := newFake()
		stream, err := newClient(t, fake).TeamItems(callContext(t), statepb.TeamItemsRequest_builder{
			TeamId: uuid.NewString(), States: []statepb.SandboxState{value},
		}.Build())
		require.NoError(t, err)
		_, _, err = readTeamItems(t, stream)
		require.NoError(t, err)
		assert.Equal(t, []sandboxtypes.State{want}, fake.snapshot().states, value.String())
	}

	fake := newFake()
	stream, err := newClient(t, fake).TeamItems(callContext(t), statepb.TeamItemsRequest_builder{TeamId: uuid.NewString()}.Build())
	require.NoError(t, err)
	_, _, err = readTeamItems(t, stream)
	require.NoError(t, err)
	assert.Empty(t, fake.snapshot().states, "no states asks the store for every state")
}

func TestTeamItemsChunks(t *testing.T) {
	t.Parallel()

	team := uuid.New()
	tests := []struct {
		items    int
		messages int
	}{
		{0, 0},
		{1, 1},
		{256, 1},
		{257, 2},
		{600, 3},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.items), func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			for i := range tt.items {
				fake.items = append(fake.items, minimalRecord(team, fmt.Sprintf("sbx-%d", i)))
			}
			stream, err := newClient(t, fake).TeamItems(callContext(t), statepb.TeamItemsRequest_builder{TeamId: team.String()}.Build())
			require.NoError(t, err)
			records, messages, err := readTeamItems(t, stream)
			require.NoError(t, err)
			assert.Equal(t, tt.messages, messages)
			require.Len(t, records, tt.items)
			for i, record := range records {
				assert.Equal(t, fake.items[i], decode(t, record), "records keep the store's order")
			}
		})
	}
}

func TestReconcile(t *testing.T) {
	t.Parallel()

	t.Run("nil orphans", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		resp, err := newClient(t, fake).Reconcile(callContext(t), statepb.ReconcileRequest_builder{NodeId: "node"}.Build())
		require.NoError(t, err)
		assert.Empty(t, resp.GetOrphans())
		assert.Equal(t, 1, fake.callCount())
	})

	t.Run("caller's value reaches the store", func(t *testing.T) {
		t.Parallel()

		reported := []sandboxtypes.NodeSandbox{
			{
				SandboxID:   "sbx-1",
				ExecutionID: "execution",
				TeamID:      uuid.New(),
				NodeID:      "node",
				ClusterID:   uuid.New(),
				StartTime:   time.Date(2026, 11, 13, 10, 11, 12, 123456789, time.UTC),
				VCpu:        2,
				RamMB:       512,
				HugePages:   true,
				Ended:       true,
				GuestEvent:  "poweroff",
				DiskKept:    true,
			},
			{SandboxID: "a{b", TeamID: uuid.New()},
		}
		requireNoZeroField(t, reported[0])

		wire := make([]*statepb.NodeSandbox, 0, len(reported))
		for _, sbx := range reported {
			wire = append(wire, statepb.NodeSandbox_builder{
				SandboxId:   sbx.SandboxID,
				ExecutionId: sbx.ExecutionID,
				TeamId:      sbx.TeamID.String(),
				NodeId:      sbx.NodeID,
				ClusterId:   sbx.ClusterID.String(),
				StartTime:   timestamppb.New(sbx.StartTime),
				Vcpu:        sbx.VCpu,
				RamMb:       sbx.RamMB,
				HugePages:   sbx.HugePages,
				Ended:       sbx.Ended,
				GuestEvent:  sbx.GuestEvent,
				DiskKept:    sbx.DiskKept,
			}.Build())
		}
		wire = append(wire, statepb.NodeSandbox_builder{SandboxId: "no-start", TeamId: uuid.NewString(), ClusterId: uuid.Nil.String()}.Build())

		fake := newFake()
		fake.orphans = []sandboxtypes.NodeSandbox{reported[0]}
		resp, err := newClient(t, fake).Reconcile(callContext(t), statepb.ReconcileRequest_builder{NodeId: "node-1", Sandboxes: wire}.Build())
		require.NoError(t, err)

		seen := fake.snapshot()
		require.Len(t, seen.reported, 3)
		assert.Equal(t, reported, seen.reported[:2])
		assert.True(t, seen.reported[2].StartTime.IsZero(), "an absent start time is the zero time")
		assert.Equal(t, "node-1", seen.nodeID)
		require.Len(t, resp.GetOrphans(), 1)
		assert.Equal(t, reported[0].TeamID.String(), resp.GetOrphans()[0].GetTeamId())
		assert.Equal(t, reported[0].SandboxID, resp.GetOrphans()[0].GetSandboxId())
	})
}

func recordOfSize(t *testing.T, team uuid.UUID, i, size int) sandboxtypes.Sandbox {
	t.Helper()

	sbx := minimalRecord(team, fmt.Sprintf("sbx-%04d", i))
	sbx.Metadata = map[string]string{"pad": ""}
	base := len(encode(t, sbx))
	require.GreaterOrEqual(t, size, base)
	sbx.Metadata["pad"] = strings.Repeat("x", size-base)
	require.Len(t, encode(t, sbx), size)

	return sbx
}

func TestRecordMessageBoundaries(t *testing.T) {
	t.Parallel()

	const (
		small   = 1000
		quarter = maxMessageRecordLen / 4
		third   = maxMessageRecordLen/3 + 1
		big     = maxMessageRecordLen + 1
	)
	tests := []struct {
		name     string
		sizes    []int
		messages []int
	}{
		{"count rule", slices.Repeat([]int{small}, 600), []int{256, 256, 88}},
		{"byte rule at the cap", slices.Repeat([]int{quarter}, 9), []int{4, 4, 1}},
		{"byte rule above the cap", slices.Repeat([]int{third}, 7), []int{2, 2, 2, 1}},
		{"a record above the cap goes alone", []int{small, small, big, small, small}, []int{2, 1, 2}},
		{"records above the cap in a row", []int{big, big, small}, []int{1, 1, 1}},
		{"a record above the cap after a full count", append(slices.Repeat([]int{small}, 256), big), []int{256, 1}},
	}
	streams := map[string]func(ctx context.Context, t *testing.T, client statepb.StateServiceClient) [][][]byte{
		"TeamItems": func(ctx context.Context, t *testing.T, client statepb.StateServiceClient) [][][]byte {
			t.Helper()

			stream, err := client.TeamItems(ctx, statepb.TeamItemsRequest_builder{TeamId: uuid.NewString()}.Build())
			require.NoError(t, err)
			messages, err := readMessages(t, stream)
			require.NoError(t, err)

			return messages
		},
		"ExpiredItems": func(ctx context.Context, t *testing.T, client statepb.StateServiceClient) [][][]byte {
			t.Helper()

			stream, err := client.ExpiredItems(ctx, statepb.ExpiredItemsRequest_builder{}.Build())
			require.NoError(t, err)
			messages, err := readMessages(t, stream)
			require.NoError(t, err)

			return messages
		},
	}
	for rpc, read := range streams {
		for _, tt := range tests {
			t.Run(rpc+" "+tt.name, func(t *testing.T) {
				t.Parallel()

				team := uuid.New()
				fake := newFake()
				for i, size := range tt.sizes {
					fake.items = append(fake.items, recordOfSize(t, team, i, size))
				}

				messages := read(callContext(t), t, newClient(t, fake))
				counts := make([]int, 0, len(messages))
				for _, message := range messages {
					counts = append(counts, len(message))
				}
				assert.Equal(t, tt.messages, counts, "message boundaries")

				var sizes []int
				var ids, want []string
				for _, record := range flatten(messages) {
					sizes = append(sizes, len(record))
					ids = append(ids, decode(t, record).SandboxID)
				}
				for _, sbx := range fake.items {
					want = append(want, sbx.SandboxID)
				}
				assert.Equal(t, tt.sizes, sizes, "every record is delivered whole")
				assert.Equal(t, want, ids, "records keep the store's order")
			})
		}
	}
}
