//go:build linux

package factories

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	networkv2 "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network/v2"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type recordingNetworkPool struct {
	closeCtxErr error
	closeDone   chan struct{}
}

func (p *recordingNetworkPool) Get(context.Context, *orchestrator.SandboxNetworkConfig, sandboxtypes.EgressClass) (*network.Slot, error) {
	return nil, nil
}

func (p *recordingNetworkPool) ReturnAsync(context.Context, *network.Slot, network.ReleaseNotify, time.Duration) error {
	return nil
}

func (p *recordingNetworkPool) Close(ctx context.Context) error {
	p.closeCtxErr = ctx.Err()
	close(p.closeDone)

	return nil
}

func TestNetworkPoolCloserDetachesForceStopCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	p := &recordingNetworkPool{closeDone: make(chan struct{})}
	registered := networkPoolCloser(p)
	require.Equal(t, "network pool", registered.name)
	require.NoError(t, registered.close(ctx))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	select {
	case <-p.closeDone:
	default:
		t.Fatal("the registered closer did not call pool.Close")
	}
	require.NoError(t, p.closeCtxErr)
}

// No slot is allocated: Acquire parks at a controlled barrier, then reports
// exhaustion. This runs the real pools' producer/close/get/join behavior without
// namespace or firewall setup; it does not cover the full-newSlots send path.
type shutdownSlotStorage struct {
	entered chan struct{}
	resume  chan struct{}
}

func (s *shutdownSlotStorage) Acquire(ctx context.Context) (*network.Slot, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.resume:
		return nil, errors.New("test slot storage exhausted")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*shutdownSlotStorage) Release(*network.Slot) error {
	panic("shutdown test must not allocate or release a network slot")
}

type networkPoolProducer interface {
	network.PoolInterface
	Populate(ctx context.Context)
}

func TestNetworkPoolCloserStopsRealProducersAndJoins(t *testing.T) {
	t.Parallel()

	for _, implementation := range []struct {
		name string
		new  func(network.Storage) networkPoolProducer
	}{
		{"v1", func(storage network.Storage) networkPoolProducer {
			return network.NewPool(2, 1, storage, network.Config{NetworkVersion: 1})
		}},
		{"v2", func(storage network.Storage) networkPoolProducer {
			return networkv2.NewV2Pool(storage, network.Config{NetworkVersion: 2}, nil, nil)
		}},
	} {
		for _, mode := range []struct {
			name      string
			forceStop bool
		}{{"graceful", false}, {"force-stop", true}} {
			t.Run(implementation.name+"/"+mode.name, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					storage := &shutdownSlotStorage{entered: make(chan struct{}, 1), resume: make(chan struct{})}
					pool := implementation.new(storage)
					producerCtx, cancelProducer := context.WithCancel(context.WithoutCancel(t.Context()))
					defer cancelProducer()
					var services errgroup.Group
					services.Go(func() error {
						pool.Populate(producerCtx)

						return nil
					})
					synctest.Wait()
					select {
					case <-storage.entered:
					default:
						t.Fatal("Populate did not reach the storage barrier")
					}

					closeCtx, cancelClose := context.WithCancel(t.Context())
					defer cancelClose()
					if mode.forceStop {
						cancelClose()
					}
					registered := networkPoolCloser(pool)
					closeResult := make(chan error, 1)
					joinResult := make(chan error, 1)
					go func() {
						// One close call only, then the same service-group join used by run.
						closeResult <- registered.close(closeCtx)
						joinResult <- services.Wait()
					}()
					synctest.Wait()
					// These channels are buffered: a premature return is observable.
					assert.Empty(t, closeResult, "Close returned before Populate settled")
					assert.Empty(t, joinResult, "service join returned before Populate settled")

					type getResult struct {
						slot *network.Slot
						err  error
					}
					got := make(chan getResult, 1)
					go func() {
						slot, err := pool.Get(producerCtx, new(orchestrator.SandboxNetworkConfig), sandboxtypes.EgressClassSandbox)
						got <- getResult{slot, err}
					}()
					synctest.Wait()
					select {
					case result := <-got:
						assert.Nil(t, result.slot)
						require.ErrorIs(t, result.err, network.ErrClosed, "the registered closer did not publish closed")
					default:
						t.Error("Get stayed blocked: the registered closer did not publish closed")
					}

					storage.resume <- struct{}{}
					synctest.Wait()
					require.NoError(t, producerCtx.Err(), "producer cancellation must not mask the close behavior")
					select {
					case err := <-joinResult:
						require.NoError(t, err)
					default:
						t.Error("Populate did not stop; service-group join stayed blocked")
					}
					select {
					case err := <-closeResult:
						require.NoError(t, err)
					default:
						t.Error("pool closer did not finish after releasing the producer barrier")
					}
					// Explicit cancellation follows the successful checks. Deferred
					// cancellation rescues failures, never supplies asserted completion.
					cancelProducer()
					synctest.Wait()
				})
			})
		}
	}
}
