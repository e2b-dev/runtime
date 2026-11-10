package nodemanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
)

func TestNodeCanAcceptNewRequests(t *testing.T) {
	t.Parallel()

	cases := map[api.NodeStatus]bool{
		api.NodeStatusReady:        true,
		api.NodeStatusConnecting:   false,
		api.NodeStatusDraining:     false,
		api.NodeStatusShuttingDown: false,
		api.NodeStatusStandby:      false,
		api.NodeStatusUnhealthy:    false,
		api.NodeStatusNotReady:     false,
		api.NodeStatus("nonsense"): false,
	}

	for status, expected := range cases {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			node := NewTestNode("test-node", status, 0, 1)

			assert.Equal(t, expected, node.CanAcceptNewRequests())
		})
	}
}

// Only the node enters its own startup and shutdown statuses.
func TestSendStatusChangeRejectsProcessStatuses(t *testing.T) {
	t.Parallel()

	node := &Node{ID: "test-node"}
	for status, message := range map[api.NodeStatus]string{
		api.NodeStatusShuttingDown: "shutting_down can only be entered during process shutdown",
		api.NodeStatusNotReady:     "not_ready can only be entered during process startup",
	} {
		err := node.SendStatusChange(t.Context(), StatusChange{Status: status})
		require.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
		require.Equal(t, message, grpcstatus.Convert(err).Message())
	}
}

// The override carries the owner and force to the orchestrator, and an
// acknowledged change updates this replica's node at once: a Standby keeps its
// owner, a release clears it, a refusal leaves the node as it was.
func TestSendStatusChange_ForwardsOwnerAndForceAndUpdatesLocally(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node-1", api.NodeStatusReady, 0, 1)
	client := &RecordingInfoClient{}
	node.SetInfoClient(client)

	require.NoError(t, node.SendStatusChange(t.Context(), StatusChange{Status: api.NodeStatusStandby, Owner: "x", Force: true}))
	require.Len(t, client.Requests, 1)
	assert.Equal(t, orchestratorinfo.ServiceInfoStatus_Standby, client.Requests[0].GetServiceStatus())
	assert.Equal(t, "x", client.Requests[0].GetOwner())
	assert.True(t, client.Requests[0].GetForce())
	assert.Equal(t, api.NodeStatusStandby, node.StatusInfo().Status)
	assert.Equal(t, "x", node.StatusInfo().Owner)

	parkedAt := node.StatusInfo().ChangedAt
	require.NoError(t, node.SendStatusChange(t.Context(), StatusChange{Status: api.NodeStatusStandby, Owner: "x"}))
	assert.Equal(t, parkedAt, node.StatusInfo().ChangedAt, "a repeated change keeps its time")

	require.NoError(t, node.SendStatusChange(t.Context(), StatusChange{Status: api.NodeStatusReady, Owner: "x"}))
	require.Len(t, client.Requests, 3)
	assert.Equal(t, "x", client.Requests[2].GetOwner())
	assert.False(t, client.Requests[2].GetForce())
	assert.Equal(t, api.NodeStatusReady, node.StatusInfo().Status)
	assert.Empty(t, node.StatusInfo().Owner)

	client.Err = grpcstatus.Error(codes.FailedPrecondition, "standby owned by x")
	err := node.SendStatusChange(t.Context(), StatusChange{Status: api.NodeStatusStandby, Owner: "y"})
	require.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
	assert.Equal(t, api.NodeStatusReady, node.StatusInfo().Status)
	assert.Empty(t, node.StatusInfo().Owner)
}

// Every status the orchestrator can report needs an api.NodeStatus counterpart.
// Node construction and sync fall back to api.NodeStatusUnhealthy for statuses
// they don't recognize, so a value added to the proto enum without a mapping
// here would silently make every node reporting it look broken — which is how
// standby nodes were once misread as unhealthy. Driving the test off
// ServiceInfoStatus_name means a new proto value fails this test rather than
// slipping through.
func TestOrchestratorToApiNodeStateMapperCoversEveryProtoStatus(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, orchestratorinfo.ServiceInfoStatus_name)

	expectedWireValues := map[orchestratorinfo.ServiceInfoStatus]string{
		orchestratorinfo.ServiceInfoStatus_Healthy:      "ready",
		orchestratorinfo.ServiceInfoStatus_Draining:     "draining",
		orchestratorinfo.ServiceInfoStatus_Unhealthy:    "unhealthy",
		orchestratorinfo.ServiceInfoStatus_Standby:      "standby",
		orchestratorinfo.ServiceInfoStatus_ShuttingDown: "shutting_down",
		orchestratorinfo.ServiceInfoStatus_NotReady:     "not_ready",
	}

	for value, name := range orchestratorinfo.ServiceInfoStatus_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			orchStatus := orchestratorinfo.ServiceInfoStatus(value)

			apiStatus, ok := OrchestratorToApiNodeStateMapper[orchStatus]
			require.Truef(t, ok, "no api.NodeStatus mapped for orchestrator status %s", name)
			assert.Truef(t, apiStatus.Valid(), "orchestrator status %s maps to %q, which is not a member of the api.NodeStatus enum", name, apiStatus)
			expected, ok := expectedWireValues[orchStatus]
			require.Truef(t, ok, "no expected wire value for orchestrator status %s", name)
			assert.Equal(t, expected, string(apiStatus))
		})
	}
}

// The two mappers are used in opposite directions on the same wire values:
// OrchestratorToApiNodeStateMapper translates what a node reports, and
// ApiNodeToOrchestratorStateMapper translates a status override back before
// sending it. They therefore have to be exact inverses, or an override would
// land the node in a status other than the one that was asked for.
func TestStatusMappersAreInverses(t *testing.T) {
	t.Parallel()

	t.Run("orchestrator status round-trips through its api status", func(t *testing.T) {
		t.Parallel()

		for orchStatus, apiStatus := range OrchestratorToApiNodeStateMapper {
			back, ok := ApiNodeToOrchestratorStateMapper[apiStatus]
			require.Truef(t, ok, "%s maps to %q, which maps back to nothing", orchStatus, apiStatus)
			assert.Equalf(t, orchStatus, back, "%s does not round-trip: it maps to %q, which maps back to %s", orchStatus, apiStatus, back)
		}
	})

	t.Run("api status round-trips through its orchestrator status", func(t *testing.T) {
		t.Parallel()

		for apiStatus, orchStatus := range ApiNodeToOrchestratorStateMapper {
			back, ok := OrchestratorToApiNodeStateMapper[orchStatus]
			require.Truef(t, ok, "%q maps to %s, which maps back to nothing", apiStatus, orchStatus)
			assert.Equalf(t, apiStatus, back, "%q does not round-trip: it maps to %s, which maps back to %q", apiStatus, orchStatus, back)
		}
	})

	// api.NodeStatusConnecting is the one asymmetry, and it is deliberate:
	// StatusInfo derives it from the local gRPC channel state, no orchestrator
	// ever reports it, and SendStatusChange must reject an attempt to push it
	// rather than translate it into some neighbouring status.
	t.Run("connecting has no orchestrator counterpart", func(t *testing.T) {
		t.Parallel()

		_, ok := ApiNodeToOrchestratorStateMapper[api.NodeStatusConnecting]
		assert.False(t, ok, "api.NodeStatusConnecting is derived locally and must not be sent to an orchestrator")
	})
}

// The metric half of the tag is asserted in the orchestrator package. Both
// emitters of this line need pinning: setStatus is the one the orchestrator's
// own reported transitions go through, markUnhealthyLocal only the local
// sync-failure path.
//
//nolint:paralleltest // ReplaceGlobals mutates process state
func TestStatusChangeLogsTheDiscoveryBackend(t *testing.T) {
	transitions := map[string]func(*Node, context.Context){
		"reported by the orchestrator": func(n *Node, ctx context.Context) {
			n.setStatus(ctx, api.NodeStatusDraining, "", time.Now())
		},
		"observed locally": func(n *Node, ctx context.Context) { n.markUnhealthyLocal(ctx) },
	}

	for name, transition := range transitions {
		t.Run(name, func(t *testing.T) {
			core, logs := observer.New(zapcore.InfoLevel)
			t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

			node := &Node{ID: "node-1", Backend: servicediscovery.BackendKubernetes}
			transition(node, t.Context())

			entries := logs.FilterMessage("NodeID status changed").All()
			require.Len(t, entries, 1)
			assert.Equal(t, string(servicediscovery.BackendKubernetes), entries[0].ContextMap()["backend"])
		})
	}
}
