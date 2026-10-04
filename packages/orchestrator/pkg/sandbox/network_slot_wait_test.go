//go:build linux

package sandbox

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

const poolWaitAssertion = "With NETWORK_VERSION=1, default slot capacities, DSCP 0 and no custom egress, a live getNetworkSlot caller blocked after an empty-queue reuse miss receives the exact slot successfully returned through normal ReturnAsync before the live producer publishes a new slot."

const poolWaitAssertionFailureMarker = "POOL_WAIT_ASSERTION_FAILED: "

type blockedNetworkSlotStorage struct {
	started   chan struct{}
	startOnce sync.Once
	finished  atomic.Bool
}

func (s *blockedNetworkSlotStorage) Acquire(ctx context.Context) (*network.Slot, error) {
	s.startOnce.Do(func() { close(s.started) })
	<-ctx.Done()
	s.finished.Store(true)

	return nil, ctx.Err()
}

func (*blockedNetworkSlotStorage) Release(*network.Slot) error { return nil }

type poolWaitReleaseRecorder struct {
	released chan *Sandbox
}

func (r *poolWaitReleaseRecorder) OnInsert(context.Context, *Sandbox)   {}
func (r *poolWaitReleaseRecorder) OnStopping(context.Context, *Sandbox) {}
func (r *poolWaitReleaseRecorder) OnNetworkRelease(_ context.Context, sbx *Sandbox) error {
	r.released <- sbx

	return nil
}

// This is the native CreateSandbox slot-acquisition gate: Factory.CreateSandbox
// starts getNetworkSlot before rootfs work and waits on its Promise. The fixture
// stops at that gate; it does not create a namespace or start Firecracker.
func TestGetNetworkSlotUsesReturnedSlotBeforeProducerPublishes(t *testing.T) { //nolint:paralleltest // controls process environment and a live producer
	t.Setenv("NETWORK_VERSION", "1")
	t.Setenv("SANDBOX_EGRESS_DSCP", "0")
	t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", "")

	config, err := network.ParseConfig()
	require.NoError(t, err)
	require.Equal(t, 1, config.NetworkVersion)
	require.Zero(t, config.SandboxEgressDSCP)
	require.Nil(t, config.BuildSandboxEgressDSCP)

	synctest.Test(t, func(t *testing.T) {
		storage := &blockedNetworkSlotStorage{started: make(chan struct{})}
		pool := network.NewPool(network.NewSlotsPoolSize, network.ReusedSlotsPoolSize, storage, config)
		producerCtx, cancelProducer := context.WithCancel(t.Context())
		getCtx, cancelGet := context.WithCancel(t.Context())
		defer func() {
			cancelGet()
			cancelProducer()
			synctest.Wait()
		}()

		go pool.Populate(producerCtx)
		synctest.Wait()
		select {
		case <-storage.started:
		default:
			t.Fatal("serial producer did not enter Storage.Acquire")
		}

		sandboxes := NewSandboxesMap()
		releaseEvents := &poolWaitReleaseRecorder{released: make(chan *Sandbox, 1)}
		sandboxes.Subscribe(releaseEvents)

		const donorIndex = 29999
		donor, err := network.NewSlot("pool-wait-donor", donorIndex, config, network.NewNoopEgressProxy())
		require.NoError(t, err)
		donorSandbox := &Sandbox{
			Resources:   &Resources{Slot: donor},
			Metadata:    &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "pool-wait-donor", ExecutionID: "pool-wait-execution"}},
			LifecycleID: "pool-wait-lifecycle",
		}
		sandboxes.AssignNetwork(t.Context(), donorSandbox)
		_, err = sandboxes.GetByHostPort(net.JoinHostPort(donor.HostIPString(), "80"))
		require.NoError(t, err, "donor must be mapped before normal release")

		released := sandboxes.NetworkReleased
		donorCleanup := NewCleanup()
		donorCleanup.Add(t.Context(), func(ctx context.Context) error {
			return pool.ReturnAsync(ctx, donor, released, network.ReturnDelay)
		})

		getCleanup := NewCleanup()
		getPromise := getNetworkSlot(
			getCtx,
			pool,
			getCleanup,
			&orchestrator.SandboxNetworkConfig{},
			released,
			sandboxtypes.EgressClassSandbox,
		)
		synctest.Wait()
		select {
		case <-getPromise.Done():
			t.Fatal("create gate completed before a slot was published")
		default:
		}
		// NewPool starts with both queues empty. Populate is live but blocked
		// before Storage.Acquire returns, and no return has started. Pool.Get's
		// first select has a default, so this durable wait is its second select.
		require.NoError(t, producerCtx.Err())
		require.NoError(t, getCtx.Err())
		require.False(t, storage.finished.Load())

		require.NoError(t, donorCleanup.Run(t.Context()))
		time.Sleep(network.ReturnDelay)
		synctest.Wait()
		select {
		case releasedSandbox := <-releaseEvents.released:
			require.Same(t, donorSandbox, releasedSandbox)
		default:
			t.Fatal("normal release callback did not notify the mapped donor")
		}
		_, err = sandboxes.GetByHostPort(net.JoinHostPort(donor.HostIPString(), "80"))
		require.Error(t, err, "successful release must remove the donor's network map entry")
		require.NoError(t, producerCtx.Err(), "producer must still be live")
		require.NoError(t, getCtx.Err(), "caller must still be live")
		require.False(t, storage.finished.Load(), "new-slot production must still be blocked")

		var (
			got    *network.Slot
			getErr error
		)
		select {
		case <-getPromise.Done():
			got, getErr = getPromise.Result()
		default:
		}
		if getErr != nil || got == nil || got.Idx != donorIndex {
			t.Errorf("%s%s; caller slot=%v donor=%d err=%v", poolWaitAssertionFailureMarker, poolWaitAssertion, slotIndex(got), donorIndex, getErr)
		}

		// The donor is a metadata-only fixture. No netns was created, so the
		// test stops at the acquisition gate and has no OS resource to tear down.
		cancelGet()
		cancelProducer()
		synctest.Wait()
	})
}

func slotIndex(slot *network.Slot) any {
	if slot == nil {
		return "<nil>"
	}

	return slot.Idx
}
