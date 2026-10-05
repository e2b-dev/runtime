package orchestrator

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

func TestAdminNodesOutstandingWork(t *testing.T) {
	t.Parallel()

	n := nodemanager.NewTestNode("node", api.NodeStatusReady, 0, 4)
	client, _ := n.GetClient(t.Context())
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	o := &Orchestrator{nodes: smap.New[*nodemanager.Node]()}
	o.nodes.Insert(o.scopedNodeID(n.ClusterID, n.ID), n)

	for _, work := range []uint64{0, 7, math.MaxUint64, 0} {
		n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{OutstandingWork: work})
		nodes, err := o.AdminNodes(n.ClusterID)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		detail, err := o.AdminNodeDetail(n.ClusterID, n.ID)
		require.NoError(t, err)
		require.Equal(t, work, nodes[0].OutstandingWork)
		require.Equal(t, work, detail.OutstandingWork)

		data, err := json.Marshal(nodes)
		require.NoError(t, err)
		var listJSON []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &listJSON))
		data, err = json.Marshal(detail)
		require.NoError(t, err)
		var detailJSON map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &detailJSON))
		for _, response := range []map[string]json.RawMessage{listJSON[0], detailJSON} {
			require.Equal(t, strconv.FormatUint(work, 10), string(response["outstandingWork"]))
		}
	}
}

func TestAdminNodeOutstandingWorkSchema(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	for _, name := range []string{"Node", "NodeDetail"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			node := spec.Components.Schemas[name].Value
			require.Contains(t, node.Required, "outstandingWork")
			work := node.Properties["outstandingWork"].Value
			require.True(t, work.Type.Is("integer"))
			require.Equal(t, "uint64", work.Format)
			require.Equal(t, new(float64(0)), work.Min)
			require.Nil(t, work.Default)
			require.False(t, work.Nullable)
		})
	}
}

func TestAdminNodesMaxSandboxes(t *testing.T) {
	t.Parallel()

	n := nodemanager.NewTestNode("node", api.NodeStatusReady, 0, 4)
	client, _ := n.GetClient(t.Context())
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	o := &Orchestrator{nodes: smap.New[*nodemanager.Node]()}
	o.nodes.Insert(o.scopedNodeID(n.ClusterID, n.ID), n)

	for _, limit := range []int64{0, 200, 320, 0, -1, 0} {
		n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{MaxSandboxes: limit})
		nodes, err := o.AdminNodes(n.ClusterID)
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		detail, err := o.AdminNodeDetail(n.ClusterID, n.ID)
		require.NoError(t, err)

		for _, response := range []any{nodes[0], detail} {
			data, err := json.Marshal(response)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &fields))
			require.Equal(t, strconv.FormatInt(limit, 10), string(fields["maxSandboxes"]))
		}
	}
}

func TestAdminNodeMaxSandboxesSchema(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	for _, name := range []string{"Node", "NodeDetail"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			node := spec.Components.Schemas[name].Value
			require.Contains(t, node.Required, "maxSandboxes")
			limit := node.Properties["maxSandboxes"].Value
			require.True(t, limit.Type.Is("integer"))
			require.Equal(t, "int64", limit.Format)
			require.Nil(t, limit.Default)
			require.False(t, limit.Nullable)
		})
	}
}
