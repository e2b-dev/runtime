//go:build linux

package server

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
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

// A resume-fresh checkpoint takes its sandbox out of the live map and lets
// the old lifecycle close before the resumed one is registered, so the live
// and lifecycle counts both read 0 mid-checkpoint. The drain must still wait:
// finishing there let the shutdown tear down the node under a resumed sandbox
// that went live after the drain, orphaning it.
func TestDrainSandboxesWaitsForCheckpointReplacement(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s := drainTestServer()
		sandboxes := s.sandboxFactory.Sandboxes
		old := drainTestSandbox(t, "lifecycle-old")
		require.NoError(t, sandboxes.MarkRunning(t.Context(), old))
		releaseOldLifecycle := s.info.TrackWork() // setupSandboxLifecycle's hold

		done := make(chan error, 1)
		go func() { done <- s.DrainSandboxes(t.Context()) }()

		// The steps of checkpointResumeFreshWith up to the resume: the RPC
		// holds work for its whole run, takes the sandbox out of the live map
		// for a reservation, and the snapshot ends the old VM, whose lifecycle
		// closes.
		releaseCheckpoint := s.info.TrackWork()
		reservation, err := sandboxes.MarkStoppingReserved(t.Context(), old.Runtime.SandboxID, old.LifecycleID)
		require.NoError(t, err)
		sandboxes.MarkStopped(t.Context(), old)
		releaseOldLifecycle()
		require.Zero(t, sandboxes.Count())
		require.Empty(t, sandboxes.LifecycleItems())

		requireDrainBlocked(t, done, "a checkpoint was replacing its sandbox")

		// The resumed sandbox's lifecycle is set up, then it goes live and the
		// RPC returns.
		resumed := drainTestSandbox(t, "lifecycle-resumed")
		releaseResumedLifecycle := s.info.TrackWork()
		require.NoError(t, reservation.MarkRunning(t.Context(), resumed))
		reservation.Release()
		releaseCheckpoint()

		requireDrainBlocked(t, done, "the resumed sandbox was live")

		require.True(t, sandboxes.MarkStopping(t.Context(), resumed.Runtime.SandboxID, resumed.LifecycleID))
		sandboxes.MarkStopped(t.Context(), resumed)
		releaseResumedLifecycle()

		time.Sleep(sandboxDrainPollInterval)
		require.NoError(t, <-done)
	})
}

// A create is not live until its VM is up; one in flight when the drain looks
// would otherwise go live after the drain finished.
func TestDrainSandboxesWaitsForInFlightCreate(t *testing.T) {
	t.Parallel()

	s := admissionTestServer(t, nil)
	s.info.MaxSandboxes.Store(int64(featureflags.MaxSandboxesPerNode.Fallback()))
	s.sandboxCreateDuration = noop.Int64Histogram{}

	synctest.Test(t, func(t *testing.T) {
		// The create waits for a starting slot, past its reservation. The slot
		// is never given back, so the create can only end by cancellation.
		s.startingSandboxes = utils.Must(utils.NewAdjustableSemaphore(1))
		require.True(t, s.startingSandboxes.TryAcquire(1))

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		created := make(chan error, 1)
		go func() {
			_, err := s.Create(ctx, &orchestrator.SandboxCreateRequest{
				Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-create", Snapshot: true, Vcpu: 1},
			})
			created <- err
		}()
		synctest.Wait()
		require.Zero(t, s.sandboxFactory.Sandboxes.Count())

		done := make(chan error, 1)
		go func() { done <- s.DrainSandboxes(t.Context()) }()

		requireDrainBlocked(t, done, "a create was in flight")

		cancel()
		require.Equal(t, codes.ResourceExhausted, status.Code(<-created))

		time.Sleep(sandboxDrainPollInterval)
		require.NoError(t, <-done)
	})
}

// requireDrainBlocked advances the bubble's clock past a drain poll and
// checks the drain is still waiting.
func requireDrainBlocked(t *testing.T, done <-chan error, why string) {
	t.Helper()

	time.Sleep(sandboxDrainPollInterval)
	synctest.Wait()
	select {
	case err := <-done:
		require.Failf(t, "DrainSandboxes returned while "+why, "err: %v", err)
	default:
	}
}

func drainTestServer() *Server {
	return &Server{
		info: &service.ServiceInfo{},
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
