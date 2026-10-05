//go:build linux

package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func TestDrainSandboxesReturnsWhenEmpty(t *testing.T) {
	t.Parallel()

	s := drainTestServer()

	require.NoError(t, s.DrainSandboxes(t.Context()))
}

func TestDrainSandboxesBlocksWhileLiveAndReturnsOnCancel(t *testing.T) {
	t.Parallel()

	s := drainTestServer()
	sbx := drainTestSandbox(t, "lifecycle-1")
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- s.DrainSandboxes(ctx)
	}()

	// A live sandbox keeps the drain blocked.
	select {
	case err := <-done:
		require.Failf(t, "DrainSandboxes returned while a sandbox was live", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("DrainSandboxes did not return after context cancellation")
	}
}

func TestDrainSandboxesCompletesAfterSandboxLeaves(t *testing.T) {
	t.Parallel()

	s := drainTestServer()
	sbx := drainTestSandbox(t, "lifecycle-1")
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	// Remove the sandbox before draining so the first poll observes an empty
	// node and the drain completes without waiting on the poll interval.
	require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))
	s.sandboxFactory.Sandboxes.MarkStopped(t.Context(), sbx)

	done := make(chan error, 1)
	go func() {
		done <- s.DrainSandboxes(t.Context())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DrainSandboxes did not complete after the node emptied")
	}
}

func drainTestServer() *Server {
	return &Server{
		sandboxFactory: &sandbox.Factory{
			Sandboxes: sandbox.NewSandboxesMap(),
		},
	}
}

func drainTestSandbox(t *testing.T, lifecycleID string) *sandbox.Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	return &sandbox.Sandbox{
		LifecycleID: lifecycleID,
		Metadata: &sandbox.Metadata{
			Config:  sandbox.NewConfig(sandbox.Config{}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "sandbox-1"},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
}
