package nomad

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	nomadapi "github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/clusters/discovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
)

func newNomadAllocationsMock(t *testing.T, allocations []map[string]any) *nomadapi.Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []any{}
		if r.URL.Path == "/v1/allocations" {
			for _, a := range allocations {
				body = append(body, a)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(body), "encoding nomad stub response")
	}))
	t.Cleanup(srv.Close)

	client, err := nomadapi.NewClient(&nomadapi.Config{Address: srv.URL})
	require.NoError(t, err)

	return client
}

func nomadAllocation(allocID, nodeName, ip string) map[string]any {
	return map[string]any{
		"ID":       allocID,
		"NodeName": nodeName,
		"JobID":    "template-manager",
		"AllocatedResources": map[string]any{
			"Shared": map[string]any{"Networks": []map[string]any{{"IP": ip}}},
		},
	}
}

// The allocation is the identity: two runs of a process on one machine are two
// instances, which is the distinction the node-based backends cannot make and
// the reason this backend exists.
func TestNomadAllocationDiscovery_IdentifiesTheAllocationAndTheMachineSeparately(t *testing.T) {
	t.Parallel()

	client := newNomadAllocationsMock(t, []map[string]any{
		nomadAllocation("alloc-1", "nomad-node-1", "10.1.0.1"),
		nomadAllocation("alloc-2", "nomad-node-1", "10.1.0.2"),
	})

	instances, err := NewAllocations(client, discovery.FilterTemplateBuilders).ListInstances(t.Context())
	require.NoError(t, err)
	require.Len(t, instances, 2)

	assert.Equal(t, []servicediscovery.Instance{
		{WorkloadID: "alloc-1", NodeID: "nomad-node-1", IPAddress: "10.1.0.1", Port: consts.OrchestratorAPIPort, Backend: servicediscovery.BackendNomad},
		{WorkloadID: "alloc-2", NodeID: "nomad-node-1", IPAddress: "10.1.0.2", Port: consts.OrchestratorAPIPort, Backend: servicediscovery.BackendNomad},
	}, instances, "same machine, two allocations, two instances")
}

func TestNomadAllocationDiscovery_PropagatesAListingFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	client, err := nomadapi.NewClient(&nomadapi.Config{Address: srv.URL})
	require.NoError(t, err)

	_, err = NewAllocations(client, discovery.FilterTemplateBuilders).ListInstances(t.Context())
	require.Error(t, err, "a failed listing must not read as an empty fleet")
}
