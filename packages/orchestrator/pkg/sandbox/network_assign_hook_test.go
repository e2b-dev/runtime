//go:build linux

package sandbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type networkAssignHookFunc func(context.Context, *Sandbox, NetworkAssignReason) error

func (f networkAssignHookFunc) OnNetworkAssign(ctx context.Context, sbx *Sandbox, reason NetworkAssignReason) error {
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
	factory := &Factory{networkAssignHook: networkAssignHookFunc(func(ctx context.Context, got *Sandbox, reason NetworkAssignReason) error {
		gotSandbox = got
		gotReason = reason
		ctxUnchanged = ctx == callerCtx

		return nil
	})}

	factory.runNetworkAssignHook(callerCtx, sbx, NetworkAssignReasonResume)

	require.Same(t, sbx, gotSandbox)
	require.Equal(t, NetworkAssignReasonResume, gotReason)
	require.True(t, ctxUnchanged, "the caller's ctx is passed through unchanged, with no derived timeout")
}

func TestFactoryRunNetworkAssignHookSwallowsError(t *testing.T) {
	t.Parallel()

	factory := &Factory{networkAssignHook: networkAssignHookFunc(func(context.Context, *Sandbox, NetworkAssignReason) error {
		return errors.New("hook failed")
	})}

	require.NotPanics(t, func() {
		factory.runNetworkAssignHook(t.Context(), testNetworkAssignHookSandbox(), NetworkAssignReasonResume)
	})
}

func TestFactoryRunNetworkAssignHookRecoversPanic(t *testing.T) {
	t.Parallel()

	factory := &Factory{networkAssignHook: networkAssignHookFunc(func(context.Context, *Sandbox, NetworkAssignReason) error {
		panic("hook panicked")
	})}

	require.NotPanics(t, func() {
		factory.runNetworkAssignHook(t.Context(), testNetworkAssignHookSandbox(), NetworkAssignReasonResume)
	})
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
		factory.runNetworkAssignHook(ctx, sbx, NetworkAssignReasonResume)
	}
}
