package handlers

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// The admin status route forwards the owner and force to the node, answers
// 204 once the node took the change, and maps the node's refusals: an
// ownership refusal to 409 with the node's message, a bad owner to 400.
func TestPostNodesNodeID_ForwardsOwnerAndForceAndMaps409(t *testing.T) {
	t.Parallel()

	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 1)
	client := &nodemanager.RecordingInfoClient{}
	node.SetInfoClient(client)
	store := &APIStore{orchestrator: orchestrator.NewTestOrchestrator(node)}
	clusterID := node.ClusterID
	owner, force := "x", true

	response := callAdmin(t, store, http.MethodPost, "/nodes/node-1", mustJSON(t, api.NodeStatusChange{
		ClusterID: &clusterID, Status: api.NodeStatusStandby, Owner: &owner, Force: &force,
	}))
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.Len(t, client.Requests, 1)
	require.Equal(t, "x", client.Requests[0].GetOwner())
	require.True(t, client.Requests[0].GetForce())
	require.Equal(t, api.NodeStatusStandby, node.StatusInfo().Status)
	require.Equal(t, "x", node.StatusInfo().Owner)

	client.Err = status.Error(codes.FailedPrecondition, "standby owned by x")
	response = callAdmin(t, store, http.MethodPost, "/nodes/node-1", mustJSON(t, api.NodeStatusChange{ClusterID: &clusterID, Status: api.NodeStatusReady}))
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "standby owned by x")

	client.Err = status.Error(codes.InvalidArgument, "owner must be at most 63 characters")
	response = callAdmin(t, store, http.MethodPost, "/nodes/node-1", mustJSON(t, api.NodeStatusChange{ClusterID: &clusterID, Status: api.NodeStatusStandby, Owner: &owner}))
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())

	// The schema refuses an owner outside the label character set before the handler runs.
	badOwner := "Not_Valid"
	response = callAdmin(t, store, http.MethodPost, "/nodes/node-1", mustJSON(t, api.NodeStatusChange{ClusterID: &clusterID, Status: api.NodeStatusStandby, Owner: &badOwner}))
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Len(t, client.Requests, 3, "a request the schema refused never reaches the node")

	response = callAdmin(t, store, http.MethodPost, "/nodes/missing", mustJSON(t, api.NodeStatusChange{ClusterID: &clusterID, Status: api.NodeStatusReady}))
	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
}
