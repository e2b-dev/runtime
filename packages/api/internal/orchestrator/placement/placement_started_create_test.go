package placement

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// heldCreateClient holds the create until released, answers with fail when
// set, and reports whether the create's context ended first, as the real node
// client would.
type heldCreateClient struct {
	orchestrator.SandboxServiceClient

	release chan struct{}
	started chan struct{}
	fail    error
	calls   atomic.Int32
	cut     atomic.Bool
}

func newHeldCreateClient(fail error) *heldCreateClient {
	return &heldCreateClient{release: make(chan struct{}), started: make(chan struct{}), fail: fail}
}

func (c *heldCreateClient) Create(ctx context.Context, _ *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	if c.calls.Add(1) == 1 {
		close(c.started)
	}
	select {
	case <-c.release:
		if c.fail != nil {
			return nil, c.fail
		}

		return &orchestrator.SandboxCreateResponse{}, nil
	case <-ctx.Done():
		c.cut.Store(true)

		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

type placementOutcome struct {
	result PlacementResult
	err    error
}

func placeHeld(ctx context.Context, algorithm Algorithm, nodes []*nodemanager.Node, preferred *nodemanager.Node, until time.Time) <-chan placementOutcome {
	out := make(chan placementOutcome, 1)
	go func() {
		result, err := PlaceSandboxCompletingStartedCreate(ctx, algorithm, nodes, preferred, testSbxRequest("sbx"), Requirements{}, false, nil, until)
		out <- placementOutcome{result: result, err: err}
	}()

	return out
}

// A create under way when the caller leaves completes under the deadline and
// the placement is reported, so the control plane's work is never cancelled
// half way; without a deadline the create is cut with the caller, as the REST
// path expects.
func TestPlaceSandbox_StartedCreateCompletesForACallerThatLeft(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		until time.Time
		cut   bool
	}{
		{"completes under the deadline", time.Now().Add(30 * time.Second), false},
		{"cut without a deadline", time.Time{}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newHeldCreateClient(nil)
			node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
			node.SetSandboxClient(client)

			callerCtx, leave := context.WithCancel(t.Context())
			outcome := placeHeld(callerCtx, &mockAlgorithm{}, []*nodemanager.Node{node}, node, tt.until)

			<-client.started
			leave()
			if tt.cut {
				o := <-outcome
				require.Error(t, o.err)
				require.True(t, o.result.TimedOut, "a create cut with the caller is a timeout")
				require.True(t, client.cut.Load(), "the create outlived the caller without a deadline")

				return
			}

			select {
			case o := <-outcome:
				require.FailNow(t, "placement ended before the create did", "%v", o.err)
			case <-time.After(200 * time.Millisecond):
			}
			close(client.release)
			o := <-outcome
			require.NoError(t, o.err)
			require.Same(t, node, o.result.Node)
			require.False(t, client.cut.Load(), "the caller's departure cut the create")
		})
	}
}

// A node that answers a detached create with its own error after the caller
// left gets that error reported: not a timeout, no warming node for a remap,
// and no further create on another node for a caller who is gone.
func TestPlaceSandbox_NodeAnswerAfterTheCallerLeftIsNotATimeout(t *testing.T) {
	t.Parallel()

	first := newHeldCreateClient(status.Error(codes.Internal, "boom"))
	node1 := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	node1.SetSandboxClient(first)
	second := newHeldCreateClient(nil)
	node2 := nodemanager.NewTestNode("node-2", api.NodeStatusReady, 0, 8)
	node2.SetSandboxClient(second)

	callerCtx, leave := context.WithCancel(t.Context())
	outcome := placeHeld(callerCtx, &mockAlgorithm{}, []*nodemanager.Node{node1, node2}, node1, time.Now().Add(30*time.Second))

	<-first.started
	leave()
	close(first.release)
	o := <-outcome

	var createErr SandboxCreateError
	require.ErrorAs(t, o.err, &createErr)
	require.Equal(t, codes.Internal, status.Code(createErr.LastErr))
	require.False(t, o.result.TimedOut, "the node's answer was reported as a timeout")
	require.Nil(t, o.result.WarmedNode, "a node that refused must not be pinned for the next resume")
	require.False(t, first.cut.Load())
	require.Zero(t, second.calls.Load(), "the loop went on to another node after the caller left")
}

// A create that runs on past its caller is cut at the instant, and that is a
// timeout with the warming node, as the caller's own deadline would report;
// a caller that stays is not cut at the instant. The clock is synctest's, so
// the instant passes only while every goroutine is blocked: the create has
// started before it, and the stay past it is exact rather than a window.
func TestPlaceSandbox_InstantCutsAStartedCreateOnlyForACallerThatLeft(t *testing.T) {
	t.Parallel()

	// Built outside the bubble: the test node's gRPC connection runs goroutines of its own.
	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	synctest.Test(t, func(t *testing.T) {
		client := newHeldCreateClient(nil)
		node.SetSandboxClient(client)

		callerCtx, leave := context.WithCancel(t.Context())
		until := time.Now().Add(time.Second)
		outcome := placeHeld(callerCtx, &mockAlgorithm{}, []*nodemanager.Node{node}, node, until)
		<-client.started
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case o := <-outcome:
			require.FailNow(t, "the instant cut a create the caller still waits for", "%v", o.err)
		default:
		}
		leave()
		o := <-outcome

		var timeoutErr PlacementTimeoutError
		require.ErrorAs(t, o.err, &timeoutErr)
		require.True(t, o.result.TimedOut)
		require.Same(t, node, o.result.WarmedNode)
		require.True(t, client.cut.Load())
		require.EqualValues(t, 1, client.calls.Load(), "a create cut at the deadline was retried")
	})
}

// A capacity refusal that arrives after the caller left is reported as what
// the nodes said, not as a timeout that would pin the node that hard-failed
// before it.
func TestPlaceSandbox_RefusalAfterTheCallerLeftIsNotATimeout(t *testing.T) {
	t.Parallel()

	first := nodemanager.NewTestNode("node-first", api.NodeStatusReady, 0, 8)
	first.SetSandboxClient(erroringClient(nil, status.Error(codes.Internal, "boom")))
	held := newHeldCreateClient(status.Error(codes.ResourceExhausted, "full"))
	second := nodemanager.NewTestNode("node-second", api.NodeStatusReady, 0, 8)
	second.SetSandboxClient(held)
	algo := stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		return second, nil
	}}

	callerCtx, leave := context.WithCancel(t.Context())
	outcome := placeHeld(callerCtx, algo, []*nodemanager.Node{first, second}, first, time.Now().Add(30*time.Second))

	<-held.started
	leave()
	close(held.release)
	o := <-outcome

	var createErr SandboxCreateError
	require.ErrorAs(t, o.err, &createErr)
	require.Equal(t, 1, createErr.Attempts)
	require.False(t, o.result.TimedOut, "the refusal was reported as a timeout")
	require.Nil(t, o.result.WarmedNode, "the node that hard-failed must not be pinned")
	require.EqualValues(t, 1, held.calls.Load())
}

// A create its caller left runs on only until the instant: cut there, it is
// a timeout with the warming node. On synctest's clock the cut lands on the
// instant itself.
func TestPlaceSandbox_InstantEndsACreateItsCallerLeft(t *testing.T) {
	t.Parallel()

	// Built outside the bubble: the test node's gRPC connection runs goroutines of its own.
	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	synctest.Test(t, func(t *testing.T) {
		client := newHeldCreateClient(nil)
		node.SetSandboxClient(client)

		callerCtx, leave := context.WithCancel(t.Context())
		until := time.Now().Add(time.Second)
		outcome := placeHeld(callerCtx, &mockAlgorithm{}, []*nodemanager.Node{node}, node, until)
		<-client.started
		leave()
		o := <-outcome

		var timeoutErr PlacementTimeoutError
		require.ErrorAs(t, o.err, &timeoutErr)
		require.True(t, o.result.TimedOut)
		require.Same(t, node, o.result.WarmedNode)
		require.True(t, client.cut.Load())
		require.Equal(t, until, time.Now(), "cut with the caller, not at the instant")
	})
}
