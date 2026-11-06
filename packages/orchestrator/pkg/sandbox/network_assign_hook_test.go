//go:build linux

package sandbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type networkAssignHookFunc func(context.Context, *Sandbox, NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error)

func (f networkAssignHookFunc) OnNetworkAssign(ctx context.Context, sbx *Sandbox, reason NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
	return f(ctx, sbx, reason)
}

func testNetworkAssignHookSandbox() *Sandbox {
	return &Sandbox{
		Metadata:    &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "sandbox-id"}},
		LifecycleID: "lifecycle-id",
	}
}

func TestFactoryRunNetworkAssignHookSuccess(t *testing.T) {
	t.Parallel()

	sbx := testNetworkAssignHookSandbox()
	callerCtx := t.Context()
	var gotSandbox *Sandbox
	var gotReason NetworkAssignReason
	var ctxUnchanged bool
	want := &orchestrator.SandboxNetworkPlacement{ExecutionId: "execution"}
	factory := &Factory{networkAssignHook: networkAssignHookFunc(func(ctx context.Context, got *Sandbox, reason NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
		gotSandbox = got
		gotReason = reason
		ctxUnchanged = ctx == callerCtx

		return want, nil
	})}

	placement, err := factory.runNetworkAssignHook(callerCtx, sbx, NetworkAssignReasonResume)
	require.NoError(t, err)
	require.Same(t, want, placement)
	require.Same(t, sbx, gotSandbox)
	require.Equal(t, NetworkAssignReasonResume, gotReason)
	require.True(t, ctxUnchanged, "the caller's ctx is passed through unchanged, with no derived timeout")
}

// An error fails the start, one the hook marked optional is logged and the
// start goes on, and a panic fails the start without escaping.
func TestFactoryRunNetworkAssignHookFailures(t *testing.T) {
	t.Parallel()
	failure := errors.New("hook failed")
	for name, tc := range map[string]struct {
		hook  networkAssignHookFunc
		fatal bool
	}{
		"error": {fatal: true, hook: func(context.Context, *Sandbox, NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
			return nil, failure
		}},
		"optional error": {hook: func(context.Context, *Sandbox, NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
			return nil, OptionalNetworkAssignError(failure)
		}},
		"panic": {fatal: true, hook: func(context.Context, *Sandbox, NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
			panic("hook panicked")
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			factory := &Factory{networkAssignHook: tc.hook}
			var placement *orchestrator.SandboxNetworkPlacement
			var err error
			require.NotPanics(t, func() {
				placement, err = factory.runNetworkAssignHook(t.Context(), testNetworkAssignHookSandbox(), NetworkAssignReasonResume)
			})
			require.Nil(t, placement)
			if tc.fatal {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNewFactoryDefaultsNilNetworkAssignHook(t *testing.T) {
	t.Parallel()

	factory := NewFactory(t.Context(), cfg.BuilderConfig{}, nil, nil, nil, nil, nil, nil, nil, nil)

	require.IsType(t, NoopNetworkAssignHook{}, factory.networkAssignHook)
}

func TestNetworkAssignReasonString(t *testing.T) {
	t.Parallel()

	cases := map[NetworkAssignReason]string{
		NetworkAssignReasonUnknown:         "unknown",
		NetworkAssignReasonCreate:          "create",
		NetworkAssignReasonResume:          "resume",
		NetworkAssignReasonReboot:          "reboot",
		NetworkAssignReasonThrowawayResume: "throwaway_resume",
		NetworkAssignReason(255):           "unknown",
	}
	for reason, want := range cases {
		require.Equal(t, want, reason.String())
	}
}

// BenchmarkFactoryRunNetworkAssignHook measures the overhead of this
// extension point on the no-op path (no edition configures a real hook).
func BenchmarkFactoryRunNetworkAssignHook(b *testing.B) {
	factory := &Factory{networkAssignHook: NoopNetworkAssignHook{}}
	sbx := testNetworkAssignHookSandbox()
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		_, _ = factory.runNetworkAssignHook(ctx, sbx, NetworkAssignReasonResume)
	}
}

func (networkAssignHookFunc) OnNetworkUpdate(context.Context, *Sandbox) {}

func (networkAssignHookFunc) OnEnvdReady(context.Context, *Sandbox, string) error { return nil }

// updateRecorder records the sandboxes whose committed updates reached the hook.
type updateRecorder struct {
	NoopNetworkAssignHook

	updated []*Sandbox
}

func (r *updateRecorder) OnNetworkUpdate(_ context.Context, sbx *Sandbox) {
	r.updated = append(r.updated, sbx)
}

// A sandbox the hook gave a placement is refused an egress proxy before its
// update runs or commits, takes egress without one, and the hook hears of
// each committed update but not of a rolled-back one. A sandbox on the
// worker's own network takes a proxy, and the hook never hears of it.
func TestFactoryRunUpdate(t *testing.T) {
	t.Parallel()
	hook := &updateRecorder{}
	factory := &Factory{networkAssignHook: hook}
	ordinary, placed := testNetworkAssignHookSandbox(), testNetworkAssignHookSandbox()
	ordinary.Config, placed.Config = NewConfig(Config{}), NewConfig(Config{})
	placed.NetworkPlacement = &orchestrator.SandboxNetworkPlacement{ExecutionId: "execution"}
	committed := func(sbx *Sandbox) uint64 {
		var n uint64
		sbx.ReadCommitted(func(committedUpdates uint64) { n = committedUpdates })

		return n
	}
	proxied := &orchestrator.SandboxNetworkEgressConfig{EgressProxyAddress: "proxy.example:1080"}
	apply := func(sbx *Sandbox, egress *orchestrator.SandboxNetworkEgressConfig) error {
		return factory.RunUpdate(t.Context(), sbx, egress, func() error {
			sbx.Config.SetNetworkEgress(egress)

			return nil
		})
	}

	require.ErrorIs(t, apply(placed, proxied), ErrEgressProxyUnsupported)
	require.Nil(t, placed.Config.GetNetworkEgress(), "the rejected proxy never became desired")
	require.Zero(t, committed(placed))
	require.Empty(t, hook.updated)

	require.NoError(t, apply(placed, &orchestrator.SandboxNetworkEgressConfig{AllowedCidrs: []string{"192.0.2.0/24"}}))
	require.Equal(t, []string{"192.0.2.0/24"}, placed.Config.GetNetworkEgress().GetAllowedCidrs())
	require.Equal(t, uint64(1), committed(placed))
	require.Equal(t, []*Sandbox{placed}, hook.updated)

	failure := errors.New("the kernel refused the update")
	require.ErrorIs(t, factory.RunUpdate(t.Context(), placed, nil, func() error { return failure }), failure)
	require.Equal(t, uint64(1), committed(placed))
	require.Len(t, hook.updated, 1, "a rolled-back update is not told")

	require.NoError(t, apply(ordinary, proxied))
	require.Equal(t, proxied.GetEgressProxyAddress(), ordinary.Config.GetNetworkEgress().GetEgressProxyAddress())
	require.Equal(t, uint64(1), committed(ordinary))
	require.NoError(t, apply(ordinary, nil))
	require.Nil(t, ordinary.Config.GetNetworkEgress(), "ordinary BYOP removal still commits")
	require.Equal(t, uint64(2), committed(ordinary))
	require.Len(t, hook.updated, 1)
}
