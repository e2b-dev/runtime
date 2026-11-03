package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// endedNodeClient answers Delete as a node that only lists the sandbox as
// ended: NotFound.
type endedNodeClient struct {
	orchestrator.SandboxServiceClient

	mu          sync.Mutex
	killReasons []string
}

func (c *endedNodeClient) Delete(_ context.Context, in *orchestrator.SandboxDeleteRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.killReasons = append(c.killReasons, in.GetKillReason())

	return nil, status.Errorf(codes.NotFound, "sandbox '%s' already ended", in.GetSandboxId())
}

func (c *endedNodeClient) deletes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.killReasons...)
}

func newEndedFixture(t *testing.T) (refusalFixture, *endedNodeClient) {
	t.Helper()

	f := newRefusalFixture(t, false, consts.LocalClusterID, nil)
	client := &endedNodeClient{}
	f.o.GetNode(consts.LocalClusterID, f.sbx.NodeID).SetSandboxClient(client)

	return f, client
}

func listedEnded(sbx sandbox.Sandbox) sandbox.NodeSandbox {
	node := sbx.ToNodeSandbox()
	node.Ended = true
	node.GuestEvent = "crash_no_restart"

	return node
}

// A sync that finds the sandbox ended on its node ends it in the store as a
// requested kill does, instead of leaving it running until its end time.
func TestEndEndedSandbox(t *testing.T) {
	t.Parallel()

	t.Run("the listed execution is killed in the store", func(t *testing.T) {
		t.Parallel()

		f, client := newEndedFixture(t)

		f.o.endEndedSandbox(t.Context(), listedEnded(f.sbx))

		_, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.ErrorIs(t, err, sandbox.ErrNotFound)
		assert.Equal(t, []string{sandbox.KillReasonEndedOnNode.String()}, client.deletes())
		require.Eventually(t, func() bool { return f.recorder.stoppedCount() == 1 },
			3*time.Second, 10*time.Millisecond)
	})

	// Every later sync, and every other API instance, lists it again.
	t.Run("a repeat does nothing", func(t *testing.T) {
		t.Parallel()

		f, client := newEndedFixture(t)

		f.o.endEndedSandbox(t.Context(), listedEnded(f.sbx))
		f.o.endEndedSandbox(t.Context(), listedEnded(f.sbx))

		assert.Len(t, client.deletes(), 1)
		require.Eventually(t, func() bool { return f.recorder.stoppedCount() == 1 },
			3*time.Second, 10*time.Millisecond)
		assert.Never(t, func() bool { return f.recorder.stoppedCount() > 1 },
			200*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("a record of another execution is left alone", func(t *testing.T) {
		t.Parallel()

		f, client := newEndedFixture(t)
		listed := listedEnded(f.sbx)
		listed.ExecutionID = uuid.NewString()

		f.o.endEndedSandbox(t.Context(), listed)

		got, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StateRunning, got.State)
		assert.Empty(t, client.deletes())
	})

	// The listing must come from the node the record places the sandbox on.
	t.Run("a listing from another node is left alone", func(t *testing.T) {
		t.Parallel()

		f, client := newEndedFixture(t)
		listed := listedEnded(f.sbx)
		listed.NodeID = "other-node"

		f.o.endEndedSandbox(t.Context(), listed)

		got, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StateRunning, got.State)
		assert.Empty(t, client.deletes())
	})

	// A pause or kill in flight ends the record its own way.
	t.Run("a record that is not running is left alone", func(t *testing.T) {
		t.Parallel()

		f, client := newEndedFixture(t)
		_, _, finish, err := f.o.sandboxStore.StartRemoving(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
		require.NoError(t, err)
		t.Cleanup(func() { finish(context.WithoutCancel(t.Context()), nil) })

		f.o.endEndedSandbox(t.Context(), listedEnded(f.sbx))

		got, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StatePausing, got.State)
		assert.Empty(t, client.deletes())
	})
}
