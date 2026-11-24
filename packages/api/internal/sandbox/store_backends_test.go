package sandbox

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type backendCall struct {
	method string
	span   trace.SpanContext
}

type fakeBackend struct {
	token      sandboxtypes.UpdateToken
	reserved   bool
	reserveErr error

	mu     sync.Mutex
	calls  []backendCall
	onCall func(method string)
	// held is how many more begins a lifecycle hold refuses.
	held int
}

var (
	_ Storage            = (*fakeBackend)(nil)
	_ ReservationStorage = (*fakeBackend)(nil)
)

func (f *fakeBackend) record(ctx context.Context, method string) {
	f.mu.Lock()
	f.calls = append(f.calls, backendCall{method: method, span: trace.SpanContextFromContext(ctx)})
	hook := f.onCall
	f.mu.Unlock()

	if hook != nil {
		hook(method)
	}
}

func (f *fakeBackend) setOnCall(hook func(method string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onCall = hook
}

func (f *fakeBackend) recorded() []backendCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls)
}

func (f *fakeBackend) methods() []string {
	var methods []string
	for _, call := range f.recorded() {
		methods = append(methods, call.method)
	}

	return methods
}

func (f *fakeBackend) Add(ctx context.Context, _ Sandbox) error {
	f.record(ctx, "Add")

	return nil
}

func (f *fakeBackend) Get(ctx context.Context, _ uuid.UUID, _ string) (Sandbox, error) {
	f.record(ctx, "Get")

	return Sandbox{}, nil
}

func (f *fakeBackend) Remove(ctx context.Context, _ uuid.UUID, _, _ string) error {
	f.record(ctx, "Remove")

	return nil
}

func (f *fakeBackend) TeamItems(ctx context.Context, _ uuid.UUID, _ []State) ([]Sandbox, error) {
	f.record(ctx, "TeamItems")

	return nil, nil
}

func (f *fakeBackend) ExpiredItems(ctx context.Context) ([]Sandbox, error) {
	f.record(ctx, "ExpiredItems")

	return nil, nil
}

func (f *fakeBackend) TeamsWithSandboxCount(ctx context.Context) (map[uuid.UUID]int64, error) {
	f.record(ctx, "TeamsWithSandboxCount")

	return map[uuid.UUID]int64{}, nil
}

// refuseHeld reports whether a lifecycle hold refuses this begin.
func (f *fakeBackend) refuseHeld() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held == 0 {
		return false
	}
	f.held--

	return true
}

func (f *fakeBackend) BeginUpdate(ctx context.Context, _ uuid.UUID, _ string) (Sandbox, sandboxtypes.UpdateToken, error) {
	f.record(ctx, "BeginUpdate")
	if f.refuseHeld() {
		return Sandbox{}, "", ErrLifecycleHeld
	}

	return Sandbox{}, f.token, nil
}

func (f *fakeBackend) CommitUpdate(ctx context.Context, _ uuid.UUID, _ string, token sandboxtypes.UpdateToken, updated Sandbox) (Sandbox, error) {
	f.record(ctx, "CommitUpdate")
	if token != f.token {
		return Sandbox{}, ErrLockLost
	}

	return updated, nil
}

func (f *fakeBackend) AbortUpdate(ctx context.Context, _ uuid.UUID, _ string, token sandboxtypes.UpdateToken) error {
	f.record(ctx, "AbortUpdate")
	if token != f.token {
		return ErrLockLost
	}

	return nil
}

func (f *fakeBackend) BeginRemoving(ctx context.Context, _ uuid.UUID, _ string, _ RemoveOpts) (StateTransition, bool, error) {
	f.record(ctx, "BeginRemoving")
	if f.refuseHeld() {
		return StateTransition{}, false, ErrLifecycleHeld
	}

	return StateTransition{ID: "transition"}, false, nil
}

func (f *fakeBackend) FinishTransition(ctx context.Context, _ uuid.UUID, _ string, _ string, _ StateAction, _ sandboxtypes.TransitionOutcome) error {
	f.record(ctx, "FinishTransition")

	return nil
}

func (f *fakeBackend) RestoreRunning(ctx context.Context, _ StateTransition, _ RestoreOpts) (Sandbox, error) {
	f.record(ctx, "RestoreRunning")

	return Sandbox{}, nil
}

func (f *fakeBackend) RestoreCheckpointed(ctx context.Context, _ StateTransition, _ *orchestrator.SandboxNetworkPlacement) (Sandbox, error) {
	f.record(ctx, "RestoreCheckpointed")

	return Sandbox{}, nil
}

func (f *fakeBackend) WaitForTransition(ctx context.Context, _ uuid.UUID, _ string, transitionID string) (string, error) {
	f.record(ctx, "WaitForTransition")

	return transitionID, nil
}

func (f *fakeBackend) HoldExecution(ctx context.Context, _ Hold, _ time.Time) error {
	f.record(ctx, "HoldExecution")

	return nil
}

func (f *fakeBackend) MoveHeldExecution(ctx context.Context, _ Hold, _ string) (bool, error) {
	f.record(ctx, "MoveHeldExecution")

	return true, nil
}

func (f *fakeBackend) ReleaseHeldExecution(ctx context.Context, _ Hold) error {
	f.record(ctx, "ReleaseHeldExecution")

	return nil
}

func (f *fakeBackend) RemoveHeldExecution(ctx context.Context, _ Hold) (Sandbox, error) {
	f.record(ctx, "RemoveHeldExecution")

	return Sandbox{}, nil
}

func (f *fakeBackend) Reconcile(ctx context.Context, sandboxes []NodeSandbox, _ string) []NodeSandbox {
	f.record(ctx, "Reconcile")

	return sandboxes
}

func (f *fakeBackend) Reserve(ctx context.Context, _ uuid.UUID, _ string, _ int) (bool, error) {
	f.record(ctx, "Reserve")

	return f.reserved, f.reserveErr
}

func (f *fakeBackend) FinishStart(ctx context.Context, _ uuid.UUID, _ string, _ []byte) error {
	f.record(ctx, "FinishStart")

	return nil
}

func (f *fakeBackend) WaitForStart(ctx context.Context, _ uuid.UUID, _ string) ([]byte, error) {
	f.record(ctx, "WaitForStart")

	return encodeResult(Sandbox{}, nil)
}

func (f *fakeBackend) Release(ctx context.Context, _ uuid.UUID, _ string) error {
	f.record(ctx, "Release")

	return nil
}

func (f *fakeBackend) ClaimKill(ctx context.Context, _ uuid.UUID, _ string) (string, bool, error) {
	f.record(ctx, "ClaimKill")

	return "claim", true, nil
}

func (f *fakeBackend) ReleaseKillClaim(ctx context.Context, _ uuid.UUID, _, _ string) error {
	f.record(ctx, "ReleaseKillClaim")

	return nil
}

type fixture struct {
	td        *ldtestdata.TestDataSource
	local     *fakeBackend
	remote    *fakeBackend
	store     *Store
	teamID    uuid.UUID
	sandboxID string
}

func newFixture(t *testing.T, withRemote bool, callbacks Callbacks) *fixture {
	t.Helper()

	f := &fixture{
		td:        ldtestdata.DataSource(),
		local:     &fakeBackend{token: "local-token", reserved: true},
		remote:    &fakeBackend{token: "remote-token", reserved: true},
		teamID:    uuid.New(),
		sandboxID: "sbx" + uuid.NewString()[:8],
	}
	var opts []Option
	if withRemote {
		flags, err := featureflags.NewClientWithDatasource(f.td)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
		opts = append(opts, WithRemoteBackend(f.remote, f.remote, flags))
	}
	f.store = NewStore(f.local, f.local, callbacks, opts...)

	return f
}

func (f *fixture) setFlag(on bool) {
	f.td.Update(f.td.Flag(featureflags.SandboxStateSchedulingAPIFlag.Key()).VariationForAll(on))
}

func (f *fixture) record() Sandbox {
	return Sandbox{TeamID: f.teamID, SandboxID: f.sandboxID, ExecutionID: uuid.NewString()}
}

func (f *fixture) hold() Hold {
	return Hold{TeamID: f.teamID, SandboxID: f.sandboxID, ExecutionID: uuid.NewString(), SourceNodeID: "node", ID: "op"}
}

func (f *fixture) pair(remote bool) *fakeBackend {
	if remote {
		return f.remote
	}

	return f.local
}

func noopCallbacks() Callbacks {
	return Callbacks{
		AsyncNewlyCreatedSandbox: func(context.Context, Sandbox, CreationMetadata) {},
		KillOrphanSandbox:        func(context.Context, NodeSandbox) {},
		EndPoweredOffSandbox:     func(context.Context, NodeSandbox) {},
		EndEndedSandbox:          func(context.Context, NodeSandbox) {},
	}
}

type storeMethod struct {
	name   string
	span   string
	team   bool
	inside []string
	after  []string
	run    func(t *testing.T, ctx context.Context, f *fixture) func(ctx context.Context)
}

func storeMethods() []storeMethod {
	return []storeMethod{
		{
			name: "Add", span: "sandbox-store add", team: true, inside: []string{"Add"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				require.NoError(t, f.store.Add(ctx, f.record(), nil))

				return nil
			},
		},
		{
			name: "Get", span: "sandbox-store get", team: true, inside: []string{"Get"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.Get(ctx, f.teamID, f.sandboxID)
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "Remove", span: "sandbox-store remove", team: true, inside: []string{"Remove", "Release"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				f.store.Remove(ctx, f.teamID, f.sandboxID, "")

				return nil
			},
		},
		{
			name: "TeamItems", span: "sandbox-store team-items", team: true, inside: []string{"TeamItems"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.TeamItems(ctx, f.teamID, []State{StateRunning})
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "ExpiredItems", span: "sandbox-store expired-items", inside: []string{"ExpiredItems"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.ExpiredItems(ctx)
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "TeamsWithSandboxes", span: "sandbox-store teams-with-sandboxes", inside: []string{"TeamsWithSandboxCount"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.TeamsWithSandboxes(ctx)
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "Update", span: "sandbox-store update", team: true, inside: []string{"BeginUpdate", "CommitUpdate"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.Update(ctx, f.teamID, f.sandboxID, func(sbx Sandbox) (Sandbox, error) { return sbx, nil })
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "HoldExecution", span: "sandbox-store hold-execution", team: true, inside: []string{"HoldExecution"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				require.NoError(t, f.store.HoldExecution(ctx, f.hold(), time.Now().Add(time.Minute)))

				return nil
			},
		},
		{
			name: "MoveHeldExecution", span: "sandbox-store move-held-execution", team: true, inside: []string{"MoveHeldExecution"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.MoveHeldExecution(ctx, f.hold(), "target")
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "ReleaseHeldExecution", span: "sandbox-store release-held-execution", team: true, inside: []string{"ReleaseHeldExecution"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				require.NoError(t, f.store.ReleaseHeldExecution(ctx, f.hold()))

				return nil
			},
		},
		{
			name: "RemoveHeldExecution", span: "sandbox-store remove-held-execution", team: true, inside: []string{"RemoveHeldExecution", "Release"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.RemoveHeldExecution(ctx, f.hold())
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "StartRemoving", span: "sandbox-store start-removing", team: true, inside: []string{"BeginRemoving"}, after: []string{"FinishTransition"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, _, finish, err := f.store.StartRemoving(ctx, f.teamID, f.sandboxID, RemoveOpts{Action: StateActionKill})
				require.NoError(t, err)

				return func(ctx context.Context) { finish(ctx, nil) }
			},
		},
		{
			name: "RestoreRunning", span: "sandbox-store restore-running", team: true, inside: []string{"RestoreRunning"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.RestoreRunning(ctx, StateTransition{Sandbox: f.record(), ID: "transition"}, RestoreOpts{})
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "RestoreCheckpointed", span: "sandbox-store restore-checkpointed", team: true, inside: []string{"RestoreCheckpointed"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, err := f.store.RestoreCheckpointed(ctx, StateTransition{Sandbox: f.record(), ID: "transition"}, nil)
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "WaitForStateChange", span: "sandbox-store wait-for-state-change", team: true, inside: []string{"WaitForTransition"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				require.NoError(t, f.store.WaitForStateChange(ctx, f.teamID, f.sandboxID))

				return nil
			},
		},
		{
			name: "Reconcile", span: "sandbox-store reconcile", inside: []string{"Reconcile"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				f.store.Reconcile(ctx, []NodeSandbox{{TeamID: f.teamID, SandboxID: f.sandboxID}}, "node")

				return nil
			},
		},
		{
			name: "Reserve", span: "sandbox-store reserve", team: true, inside: []string{"Reserve"}, after: []string{"FinishStart"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				finishStart, waitForStart, err := f.store.Reserve(ctx, f.teamID, f.sandboxID, 10)
				require.NoError(t, err)
				require.NotNil(t, finishStart)
				require.Nil(t, waitForStart)

				return func(context.Context) { finishStart(f.record(), nil) }
			},
		},
		{
			name: "ClaimKill", span: "sandbox-store claim-kill", team: true, inside: []string{"ClaimKill"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				_, _, err := f.store.ClaimKill(ctx, f.teamID, f.sandboxID)
				require.NoError(t, err)

				return nil
			},
		},
		{
			name: "ReleaseKillClaim", span: "sandbox-store release-kill-claim", team: true, inside: []string{"ReleaseKillClaim"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				require.NoError(t, f.store.ReleaseKillClaim(ctx, f.teamID, f.sandboxID, "claim"))

				return nil
			},
		},
		{
			name: "WaitForStart", span: "sandbox-store wait-for-start", team: true, inside: []string{"WaitForStart"},
			run: func(t *testing.T, ctx context.Context, f *fixture) func(context.Context) {
				t.Helper()
				require.NoError(t, f.store.WaitForStart(ctx, f.teamID, f.sandboxID))

				return nil
			},
		},
	}
}

func runMethod(t *testing.T, ctx context.Context, f *fixture, m storeMethod) {
	t.Helper()

	if then := m.run(t, ctx, f); then != nil {
		then(ctx)
	}
}

func TestStoreMethodsCoverEveryExportedMethod(t *testing.T) {
	t.Parallel()

	var listed []string
	for _, m := range storeMethods() {
		listed = append(listed, m.name)
	}

	var exported []string
	for method := range reflect.TypeFor[*Store]().Methods() {
		exported = append(exported, method.Name)
	}

	assert.ElementsMatch(t, exported, listed)
}

func TestSandboxStateSchedulingAPIFlagDefaultsOff(t *testing.T) {
	t.Parallel()

	if _, set := os.LookupEnv("SANDBOX_STATE_SCHEDULING_API"); set {
		t.Skip("SANDBOX_STATE_SCHEDULING_API is set")
	}

	assert.False(t, featureflags.SandboxStateSchedulingAPIFlag.Fallback())
}

func TestStoreRoutesEachMethodByFlag(t *testing.T) {
	t.Parallel()

	for _, m := range storeMethods() {
		for _, state := range []string{"unset", "off", "on"} {
			t.Run(m.name+"/"+state, func(t *testing.T) {
				t.Parallel()

				f := newFixture(t, true, noopCallbacks())
				if state != "unset" {
					f.setFlag(state == "on")
				}

				runMethod(t, t.Context(), f, m)

				on := state == "on" || (state == "unset" && featureflags.SandboxStateSchedulingAPIFlag.Fallback())
				remote := m.team && on
				assert.Equal(t, slices.Concat(m.inside, m.after), f.pair(remote).methods())
				assert.Empty(t, f.pair(!remote).methods())
			})
		}
	}
}

func TestStoreWithoutRemoteBackendStaysLocal(t *testing.T) {
	t.Parallel()

	for _, m := range storeMethods() {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, false, noopCallbacks())
			f.setFlag(true)

			runMethod(t, t.Context(), f, m)

			assert.Equal(t, slices.Concat(m.inside, m.after), f.local.methods())
			assert.Empty(t, f.remote.methods())
		})
	}
}

func TestStoreTargetsTeam(t *testing.T) {
	t.Parallel()

	f := newFixture(t, true, noopCallbacks())
	other := uuid.New()
	f.td.Update(f.td.Flag(featureflags.SandboxStateSchedulingAPIFlag.Key()).
		BooleanFlag().ClearRules().ClearTargets().FallthroughVariation(false).
		VariationForKey(featureflags.TeamKind, f.teamID.String(), true))

	_, err := f.store.Get(t.Context(), f.teamID, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.Get(t.Context(), other, f.sandboxID)
	require.NoError(t, err)

	assert.Equal(t, []string{"Get"}, f.remote.methods())
	assert.Equal(t, []string{"Get"}, f.local.methods())
}

func TestStoreTargetsSandbox(t *testing.T) {
	t.Parallel()

	f := newFixture(t, true, noopCallbacks())
	f.td.Update(f.td.Flag(featureflags.SandboxStateSchedulingAPIFlag.Key()).
		BooleanFlag().ClearRules().ClearTargets().FallthroughVariation(false).
		VariationForKey(featureflags.SandboxKind, f.sandboxID, true))

	_, err := f.store.Get(t.Context(), f.teamID, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.Get(t.Context(), f.teamID, "other"+f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.TeamItems(t.Context(), f.teamID, nil)
	require.NoError(t, err)

	assert.Equal(t, []string{"Get"}, f.remote.methods())
	assert.Equal(t, []string{"Get", "TeamItems"}, f.local.methods())
}

func TestStoreKeepsOnePairWhenTheFlagFlips(t *testing.T) {
	t.Parallel()

	flipOn := func(f *fixture, startOn bool, method string) {
		for _, b := range []*fakeBackend{f.local, f.remote} {
			b.setOnCall(func(called string) {
				if called == method {
					f.setFlag(!startOn)
				}
			})
		}
	}

	tests := []struct {
		name   string
		want   []string
		run    func(t *testing.T, f *fixture, startOn bool)
		before func(f *fixture)
	}{
		{
			name: "update commit",
			want: []string{"BeginUpdate", "CommitUpdate"},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				_, err := f.store.Update(t.Context(), f.teamID, f.sandboxID, func(sbx Sandbox) (Sandbox, error) {
					f.setFlag(!startOn)

					return sbx, nil
				})
				require.NoError(t, err)
			},
		},
		{
			name: "update abort",
			want: []string{"BeginUpdate", "AbortUpdate"},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				refused := errors.New("refused")
				_, err := f.store.Update(t.Context(), f.teamID, f.sandboxID, func(Sandbox) (Sandbox, error) {
					f.setFlag(!startOn)

					return Sandbox{}, refused
				})
				require.ErrorIs(t, err, refused)
			},
		},
		{
			name: "update after a hold",
			want: []string{"BeginUpdate", "BeginUpdate", "CommitUpdate"},
			before: func(f *fixture) {
				f.local.held, f.remote.held = 1, 1
			},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				flipOn(f, startOn, "BeginUpdate")
				_, err := f.store.Update(t.Context(), f.teamID, f.sandboxID, func(sbx Sandbox) (Sandbox, error) { return sbx, nil })
				require.NoError(t, err)
			},
		},
		{
			name: "remove",
			want: []string{"Remove", "Release"},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				flipOn(f, startOn, "Remove")
				f.store.Remove(t.Context(), f.teamID, f.sandboxID, "")
			},
		},
		{
			name: "remove held execution",
			want: []string{"RemoveHeldExecution", "Release"},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				flipOn(f, startOn, "RemoveHeldExecution")
				_, err := f.store.RemoveHeldExecution(t.Context(), f.hold())
				require.NoError(t, err)
			},
		},
		{
			name: "start removing",
			want: []string{"BeginRemoving", "FinishTransition"},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				_, _, finish, err := f.store.StartRemoving(t.Context(), f.teamID, f.sandboxID, RemoveOpts{Action: StateActionKill})
				require.NoError(t, err)
				f.setFlag(!startOn)
				finish(t.Context(), nil)
			},
		},
		{
			name: "start removing after a hold",
			want: []string{"BeginRemoving", "BeginRemoving", "FinishTransition"},
			before: func(f *fixture) {
				f.local.held, f.remote.held = 1, 1
			},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				flipOn(f, startOn, "BeginRemoving")
				_, _, finish, err := f.store.StartRemoving(t.Context(), f.teamID, f.sandboxID, RemoveOpts{Action: StateActionKill, WaitForLifecycleHold: true})
				require.NoError(t, err)
				finish(t.Context(), nil)
			},
		},
		{
			name: "reserve reserved",
			want: []string{"Reserve", "FinishStart"},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				finishStart, _, err := f.store.Reserve(t.Context(), f.teamID, f.sandboxID, 10)
				require.NoError(t, err)
				require.NotNil(t, finishStart)
				f.setFlag(!startOn)
				finishStart(f.record(), nil)
			},
		},
		{
			name: "reserve already pending",
			want: []string{"Reserve", "WaitForStart"},
			before: func(f *fixture) {
				f.local.reserved, f.remote.reserved = false, false
			},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				_, waitForStart, err := f.store.Reserve(t.Context(), f.teamID, f.sandboxID, 10)
				require.NoError(t, err)
				require.NotNil(t, waitForStart)
				f.setFlag(!startOn)
				_, err = waitForStart(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name: "reserve already exists",
			want: []string{"Reserve", "Get"},
			before: func(f *fixture) {
				f.local.reserveErr, f.remote.reserveErr = ErrAlreadyExists, ErrAlreadyExists
			},
			run: func(t *testing.T, f *fixture, startOn bool) {
				t.Helper()
				_, waitForStart, err := f.store.Reserve(t.Context(), f.teamID, f.sandboxID, 10)
				require.NoError(t, err)
				require.NotNil(t, waitForStart)
				f.setFlag(!startOn)
				_, err = waitForStart(t.Context())
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		for _, startOn := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{false: "/from off", true: "/from on"}[startOn], func(t *testing.T) {
				t.Parallel()

				f := newFixture(t, true, noopCallbacks())
				if tt.before != nil {
					tt.before(f)
				}
				f.setFlag(startOn)

				tt.run(t, f, startOn)

				assert.Equal(t, tt.want, f.pair(startOn).methods())
				assert.Empty(t, f.pair(!startOn).methods())
			})
		}
	}
}

func storeSpans(traceID trace.TraceID) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, span := range testSpanRecorder.Ended() {
		if span.SpanContext().TraceID() == traceID && span.InstrumentationScope().Name == "github.com/e2b-dev/infra/packages/api/internal/sandbox" {
			spans = append(spans, span)
		}
	}

	return spans
}

func backendAttribute(span sdktrace.ReadOnlySpan) attribute.Value {
	for _, attr := range span.Attributes() {
		if attr.Key == "sandbox.store.backend" {
			return attr.Value
		}
	}

	return attribute.Value{}
}

func TestStoreTracesEachCall(t *testing.T) {
	t.Parallel()

	for _, m := range storeMethods() {
		for _, on := range []bool{false, true} {
			t.Run(m.name+map[bool]string{false: "/off", true: "/on"}[on], func(t *testing.T) {
				t.Parallel()

				f := newFixture(t, true, noopCallbacks())
				f.setFlag(on)
				remote := m.team && on
				slug := map[bool]string{false: "redis", true: "scheduling-api"}[remote]
				ctx, parent := sdktrace.NewTracerProvider().Tracer("github.com/e2b-dev/infra/packages/api/internal/sandbox").Start(t.Context(), "parent")
				defer parent.End()

				then := m.run(t, ctx, f)

				spans := storeSpans(parent.SpanContext().TraceID())
				require.Len(t, spans, 1)
				span := spans[0]
				assert.Equal(t, m.span, span.Name())
				assert.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
				assert.Equal(t, parent.SpanContext().TraceID(), span.SpanContext().TraceID())
				assert.Equal(t, attribute.StringValue(slug), backendAttribute(span))

				if then != nil {
					then(ctx)
				}
				assert.Len(t, storeSpans(parent.SpanContext().TraceID()), 1)

				calls := f.pair(remote).recorded()
				require.Len(t, calls, len(m.inside)+len(m.after))
				for _, call := range calls[:len(m.inside)] {
					assert.Equal(t, span.SpanContext().SpanID(), call.span.SpanID(), call.method)
				}
				for _, call := range calls[len(m.inside):] {
					assert.Equal(t, parent.SpanContext().SpanID(), call.span.SpanID(), call.method)
				}
			})
		}
	}
}

func TestStoreStartsNoSpanWithoutParent(t *testing.T) {
	t.Parallel()

	for _, m := range storeMethods() {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, true, noopCallbacks())
			f.setFlag(true)

			runMethod(t, t.Context(), f, m)

			calls := slices.Concat(f.local.recorded(), f.remote.recorded())
			require.Len(t, calls, len(m.inside)+len(m.after))
			for _, call := range calls {
				assert.False(t, call.span.IsValid(), call.method)
			}
		})
	}
}

func TestStoreCallbacksKeepCallerContext(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	seen := map[string]trace.SpanContext{}
	record := func(name string) OrphanCallback {
		return func(ctx context.Context, _ NodeSandbox) {
			mu.Lock()
			defer mu.Unlock()
			seen[name] = trace.SpanContextFromContext(ctx)
		}
	}
	created := make(chan trace.SpanContext, 1)
	f := newFixture(t, true, Callbacks{
		AsyncNewlyCreatedSandbox: func(ctx context.Context, _ Sandbox, _ CreationMetadata) {
			created <- trace.SpanContextFromContext(ctx)
		},
		KillOrphanSandbox:    record("orphan"),
		EndPoweredOffSandbox: record("powered off"),
		EndEndedSandbox:      record("ended"),
	})
	ctx, parent := sdktrace.NewTracerProvider().Tracer("github.com/e2b-dev/infra/packages/api/internal/sandbox").Start(t.Context(), "parent")
	defer parent.End()

	require.NoError(t, f.store.Add(ctx, f.record(), &CreationMetadata{}))
	f.store.Reconcile(ctx, []NodeSandbox{
		{TeamID: f.teamID, SandboxID: "running"},
		{TeamID: f.teamID, SandboxID: "powered-off", DiskKept: true},
		{TeamID: f.teamID, SandboxID: "ended", Ended: true},
	}, "node")

	select {
	case sc := <-created:
		assert.Equal(t, parent.SpanContext().SpanID(), sc.SpanID())
	case <-time.After(10 * time.Second):
		require.Fail(t, "the creation callback did not run")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 3)
	for name, sc := range seen {
		assert.Equal(t, parent.SpanContext().SpanID(), sc.SpanID(), name)
	}
	assert.Len(t, storeSpans(parent.SpanContext().TraceID()), 2)
}
