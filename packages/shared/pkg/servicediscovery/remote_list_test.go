package servicediscovery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	api "github.com/e2b-dev/infra/packages/shared/pkg/http/edge"
)

func remoteEdgeStub(t *testing.T, status int, payload any) *api.ClientWithResponses {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if payload == nil {
			return
		}
		// assert, not require: FailNow from a handler goroutine is illegal.
		assert.NoError(t, json.NewEncoder(w).Encode(payload), "encoding edge stub response")
	}))
	t.Cleanup(srv.Close)

	client, err := api.NewClientWithResponses(srv.URL)
	require.NoError(t, err)

	return client
}

// The remote side already reports a per-run identity, so it maps straight onto
// ID; the machine it names maps onto NodeID. Getting these the wrong way round
// would make every remote instance look like it never restarts.
func TestRemoteDiscovery_MapsTheEdgeResponseOntoBothFacets(t *testing.T) {
	t.Parallel()

	client := remoteEdgeStub(t, http.StatusOK, map[string]any{
		"orchestrators": []map[string]any{
			{"nodeID": "remote-node-1", "serviceInstanceID": "svc-aaa", "serviceHost": "10.9.0.1:5008"},
			{"nodeID": "remote-node-2", "serviceInstanceID": "svc-bbb", "serviceHost": "10.9.0.2"},
		},
	})

	instances, err := NewRemote(client).ListInstances(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []Instance{
		{WorkloadID: "svc-aaa", NodeID: "remote-node-1", IPAddress: "10.9.0.1", Backend: BackendRemote},
		{WorkloadID: "svc-bbb", NodeID: "remote-node-2", IPAddress: "10.9.0.2", Backend: BackendRemote},
	}, instances)
}

// A remote cluster that answers with an error must not read as a cluster with
// no instances: the caller would deregister everything it has there.
func TestRemoteDiscovery_PropagatesAnEdgeFailure(t *testing.T) {
	t.Parallel()

	_, err := NewRemote(remoteEdgeStub(t, http.StatusBadGateway, nil)).ListInstances(t.Context())
	require.Error(t, err)
}
