package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
)

const (
	testNamespace                  = "e2b"
	testOrchestratorLabelSelector  = "app.kubernetes.io/name=orchestrator"
	testTemplateManagerLabelSelect = "app.kubernetes.io/name=template-manager"
)

// A fleet with no allocations serves an empty template-builder plane on the
// Nomad side, so anything that plane returns came from Kubernetes.
type nomadFleet struct {
	registrations []map[string]any
	poolNodes     []map[string]any
	allocations   []map[string]any
}

func nomadStub(t *testing.T, fleet nomadFleet) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := []any{}
		switch r.URL.Path {
		case "/v1/service/orchestrator":
			for _, reg := range fleet.registrations {
				body = append(body, reg)
			}
		case "/v1/nodes":
			for _, node := range fleet.poolNodes {
				body = append(body, node)
			}
		case "/v1/allocations":
			for _, alloc := range fleet.allocations {
				body = append(body, alloc)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(body))
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

func nomadBuilderAllocation(allocID, nodeName, address string) map[string]any {
	return map[string]any{
		"ID":       allocID,
		"NodeName": nodeName,
		"JobID":    "template-manager",
		"AllocatedResources": map[string]any{
			"Shared": map[string]any{
				"Networks": []map[string]any{{"IP": address}},
			},
		},
	}
}

func nomadPoolNode(nodeID, address string) map[string]any {
	return map[string]any{
		"ID":       nodeID,
		"Address":  address,
		"Status":   "ready",
		"NodePool": "default",
	}
}

func nomadRegistration(nodeID, address string) map[string]any {
	return map[string]any{
		"ID":          "_nomad-task-" + nodeID,
		"ServiceName": "orchestrator",
		"NodeID":      nodeID,
		"Address":     address,
		"Port":        5008,
	}
}

func discoveredPod(name, role, hostIP string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			UID:       types.UID(name + "-uid"),
			Labels:    map[string]string{"app.kubernetes.io/name": role},
		},
		// No Ready condition: discovery must not consult one, and a fixture that
		// sets it hides the gate coming back.
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			HostIP: hostIP,
			PodIP:  hostIP,
		},
	}
}

func discoveryConfig(provider, nomadAddress string) cfg.Config {
	return cfg.Config{
		ServiceDiscoveryProvider:                provider,
		NomadAddress:                            nomadAddress,
		NomadOrchestratorServiceNames:           []string{"orchestrator"},
		NomadOrchestratorLegacyDiscoveryEnabled: true,
		K8sNamespace:                            testNamespace,
		K8sOrchestratorPodLabelSelector:         testOrchestratorLabelSelector,
		K8sTemplateManagerPodLabelSelector:      testTemplateManagerLabelSelect,
	}
}

func fixedKubeClient(objects ...runtime.Object) kubeClientFactory {
	client := fake.NewSimpleClientset(objects...)

	return func(context.Context, string) (kubernetes.Interface, error) { return client, nil }
}

func recordingKubeClient(endpoint *string) kubeClientFactory {
	client := fake.NewSimpleClientset()

	return func(_ context.Context, e string) (kubernetes.Interface, error) {
		*endpoint = e

		return client, nil
	}
}

// A failing pod listing is what a revoked RBAC binding or an unreachable API
// server looks like.
func failingKubeClient(listErr error) kubeClientFactory {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, listErr
	})

	return func(context.Context, string) (kubernetes.Interface, error) { return client, nil }
}

func planeIPs(t *testing.T, plane servicediscovery.Discoverer) []string {
	t.Helper()

	instances, err := plane.ListInstances(t.Context())
	require.NoError(t, err)

	ips := make([]string, 0, len(instances))
	for _, i := range instances {
		ips = append(ips, i.IPAddress)
	}

	return ips
}

func TestServiceDiscoveryProvider_OnlyAnUnsetProviderDefaults(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		configured string
		localEnv   bool
		want       string
	}{
		"unset in a local environment has no Nomad to reach": {
			configured: "", localEnv: true, want: cfg.ServiceDiscoveryProviderLocal,
		},
		"unset anywhere else is Nomad": {
			configured: "", localEnv: false, want: cfg.ServiceDiscoveryProviderNomad,
		},
		"nomad named explicitly survives a local environment": {
			configured: cfg.ServiceDiscoveryProviderNomad, localEnv: true, want: cfg.ServiceDiscoveryProviderNomad,
		},
		"kubernetes named explicitly survives too": {
			configured: cfg.ServiceDiscoveryProviderKubernetes, localEnv: true, want: cfg.ServiceDiscoveryProviderKubernetes,
		},
		"the composed mode survives too": {
			configured: cfg.ServiceDiscoveryProviderNomadKubernetes, localEnv: true, want: cfg.ServiceDiscoveryProviderNomadKubernetes,
		},
		"local stays local": {
			configured: cfg.ServiceDiscoveryProviderLocal, localEnv: false, want: cfg.ServiceDiscoveryProviderLocal,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := serviceDiscoveryProvider(cfg.Config{ServiceDiscoveryProvider: tt.configured}, tt.localEnv)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Both planes have to resolve without a Nomad agent: the template-builder plane
// feeds the local cluster, which is the only source of orchestrator nodes when
// the Nomad node sync is off, so a plane that errors here leaves the API
// permanently unhealthy.
func TestNewServiceDiscovery_AnUnsetProviderResolvesBothPlanesLocally(t *testing.T) {
	t.Parallel()

	config := cfg.Config{LocalOrchestratorAddress: "127.0.0.1:5008"}

	sd, err := newServiceDiscovery(t.Context(), config, nil, serviceDiscoveryProvider(config, true))
	require.NoError(t, err)

	for name, plane := range map[string]servicediscovery.Discoverer{
		"nodes":            sd.nodes,
		"templateBuilders": sd.templateBuilders,
	} {
		instances, err := plane.ListInstances(t.Context())
		require.NoErrorf(t, err, "the %s plane must resolve without a Nomad agent", name)
		require.Lenf(t, instances, 1, "the %s plane must report the local orchestrator", name)
		assert.Equal(t, "127.0.0.1:5008", instances[0].Address())
	}
}

func TestNewServiceDiscovery_ExplicitProviderSurvivesALocalEnvironment(t *testing.T) {
	t.Parallel()

	kubeErr := errors.New("no kubeconfig")
	config := cfg.Config{ServiceDiscoveryProvider: cfg.ServiceDiscoveryProviderKubernetes}

	_, err := newServiceDiscovery(
		t.Context(),
		config,
		func(context.Context, string) (kubernetes.Interface, error) { return nil, kubeErr },
		serviceDiscoveryProvider(config, true),
	)
	require.ErrorIs(t, err, kubeErr)
}

// The clusters registry owns orchestrator nodes only when the resolved provider
// is the static local one — there both planes are the same single instance.
// Deriving it from the environment instead builds a Nomad or Kubernetes node
// plane that neither the periodic loop nor on-demand discovery ever consults,
// so an orchestrator that is not also a template builder never registers.
func TestLocalRegistryOwnsOrchestrators_FollowsTheProviderNotTheEnvironment(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		configured string
		localEnv   bool
		wantOwns   bool
	}{
		"unset in a local environment resolves to local, so the registry owns them": {
			configured: "", localEnv: true, wantOwns: true,
		},
		"nomad named explicitly in a local environment keeps its node plane": {
			configured: cfg.ServiceDiscoveryProviderNomad, localEnv: true, wantOwns: false,
		},
		"kubernetes named explicitly in a local environment keeps its node plane": {
			configured: cfg.ServiceDiscoveryProviderKubernetes, localEnv: true, wantOwns: false,
		},
		"the composed mode keeps its node plane": {
			configured: cfg.ServiceDiscoveryProviderNomadKubernetes, localEnv: true, wantOwns: false,
		},
		"nomad anywhere else keeps its node plane": {
			configured: "", localEnv: false, wantOwns: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			provider := serviceDiscoveryProvider(cfg.Config{ServiceDiscoveryProvider: tt.configured}, tt.localEnv)
			assert.Equal(t, tt.wantOwns, provider == cfg.ServiceDiscoveryProviderLocal)
		})
	}
}

// The nomad-only control pins that the pod reaches the catalog through the
// union and not through some other path.
//
//nolint:dupl // the nomad-first test asserts ordering on the same fixture
func TestServiceDiscovery_ComposedProviderUnionsBothPlatforms(t *testing.T) {
	t.Parallel()

	nomad := nomadStub(t, nomadFleet{
		registrations: []map[string]any{nomadRegistration("11111111-2222-3333-4444-555555555555", "10.1.0.1")},
		allocations:   []map[string]any{nomadBuilderAllocation("alloc-1", "nomad-node-1", "10.1.0.2")},
	})
	newKube := fixedKubeClient(
		discoveredPod("orchestrator-abcde-fghij", "orchestrator", "10.2.0.1"),
		discoveredPod("template-manager-abcde-fghij", "template-manager", "10.2.0.2"),
	)

	composed, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), newKube, cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"10.1.0.1", "10.2.0.1"}, planeIPs(t, composed.nodes))
	assert.ElementsMatch(t, []string{"10.1.0.2", "10.2.0.2"}, planeIPs(t, composed.templateBuilders))

	nomadOnly, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomad, nomad), newKube, cfg.ServiceDiscoveryProviderNomad)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.1.0.1"}, planeIPs(t, nomadOnly.nodes))
	assert.Equal(t, []string{"10.1.0.2"}, planeIPs(t, nomadOnly.templateBuilders))
}

// Each side keeps the tag of the backend it came from, which is the whole point
// of carrying one through a union.
func TestServiceDiscovery_ComposedProviderTagsEachSideWithItsBackend(t *testing.T) {
	t.Parallel()

	nomad := nomadStub(t, nomadFleet{
		registrations: []map[string]any{nomadRegistration("11111111-2222-3333-4444-555555555555", "10.1.0.1")},
	})
	newKube := fixedKubeClient(discoveredPod("orchestrator-abcde-fghij", "orchestrator", "10.2.0.1"))

	composed, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), newKube, cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)

	instances, err := composed.nodes.ListInstances(t.Context())
	require.NoError(t, err)

	backends := map[string]string{}
	for _, i := range instances {
		backends[i.IPAddress] = i.Backend
	}
	assert.Equal(t, map[string]string{
		"10.1.0.1": servicediscovery.BackendNomad,
		"10.2.0.1": servicediscovery.BackendKubernetes,
	}, backends)
}

// The property that makes the mode safe to enable before the second platform
// has anything on it.
func TestServiceDiscovery_ComposedProviderWithEmptyKubernetesSideIsANoOp(t *testing.T) {
	t.Parallel()

	nomad := nomadStub(t, nomadFleet{
		registrations: []map[string]any{
			nomadRegistration("11111111-2222-3333-4444-555555555555", "10.1.0.1"),
			nomadRegistration("66666666-7777-8888-9999-000000000000", "10.1.0.2"),
		},
		allocations: []map[string]any{nomadBuilderAllocation("alloc-1", "nomad-node-1", "10.1.0.3")},
	})
	newKube := fixedKubeClient()

	composed, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), newKube, cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)

	nomadOnly, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomad, nomad), newKube, cfg.ServiceDiscoveryProviderNomad)
	require.NoError(t, err)

	// Both sides of this comparison route the node plane through the same
	// combinator, so pin the expected set too — otherwise a combinator
	// regression cancels out and the assertion passes.
	assert.Equal(t, []string{"10.1.0.1", "10.1.0.2"}, planeIPs(t, composed.nodes))
	assert.Equal(t, []string{"10.1.0.3"}, planeIPs(t, composed.templateBuilders))
	assert.Equal(t, planeIPs(t, nomadOnly.nodes), planeIPs(t, composed.nodes))
	assert.Equal(t, planeIPs(t, nomadOnly.templateBuilders), planeIPs(t, composed.templateBuilders))
}

// Orchestrators from jobspecs that predate the service port-label fix register
// with an empty address and are visible only through the node listing.
func TestServiceDiscovery_ComposedProviderKeepsTheLegacyNomadUnion(t *testing.T) {
	t.Parallel()

	nomad := nomadStub(t, nomadFleet{
		registrations: []map[string]any{nomadRegistration("11111111-2222-3333-4444-555555555555", "10.1.0.1")},
		poolNodes:     []map[string]any{nomadPoolNode("99999999-8888-7777-6666-555555555555", "10.1.0.9")},
	})
	newKube := fixedKubeClient(discoveredPod("orchestrator-abcde-fghij", "orchestrator", "10.2.0.1"))

	composed, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), newKube, cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"10.1.0.1", "10.1.0.9", "10.2.0.1"}, planeIPs(t, composed.nodes))
}

// The caller reads a discovery error as keep-last-known, so a partial union
// would deregister every node the failing side owns.
func TestServiceDiscovery_ComposedProviderFailsTheListingWhenKubernetesFails(t *testing.T) {
	t.Parallel()

	listErr := errors.New("kubernetes api unreachable")
	nomad := nomadStub(t, nomadFleet{
		registrations: []map[string]any{nomadRegistration("11111111-2222-3333-4444-555555555555", "10.1.0.1")},
	})

	composed, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), failingKubeClient(listErr), cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)

	_, err = composed.nodes.ListInstances(t.Context())
	require.ErrorIs(t, err, listErr, "a Kubernetes failure must not degrade to the Nomad-only node list")

	_, err = composed.templateBuilders.ListInstances(t.Context())
	require.ErrorIs(t, err, listErr, "a Kubernetes failure must not degrade to the Nomad-only builder list")
}

// Returning a zero serviceDiscovery would hand a nil Discoverer to the
// combinator and panic on the first sync tick instead of at startup.
func TestServiceDiscovery_ComposedProviderFailsWhenTheKubernetesClientFails(t *testing.T) {
	t.Parallel()

	clientErr := errors.New("no application default credentials")
	nomad := nomadStub(t, nomadFleet{})

	newKube := func(context.Context, string) (kubernetes.Interface, error) { return nil, clientErr }

	_, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), newKube, cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.ErrorIs(t, err, clientErr)
}

func TestServiceDiscovery_PassesTheConfiguredEndpointToTheClient(t *testing.T) {
	t.Parallel()

	var got string
	config := discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomadStub(t, nomadFleet{}))
	config.K8sAPIEndpoint = "https://dns-endpoint.example"

	_, err := newServiceDiscovery(t.Context(), config, recordingKubeClient(&got), cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)

	assert.Equal(t, "https://dns-endpoint.example", got)
}

func TestServiceDiscovery_KubernetesProviderSeparatesThePlanesByLabel(t *testing.T) {
	t.Parallel()

	newKube := fixedKubeClient(
		discoveredPod("orchestrator-abcde-fghij", "orchestrator", "10.2.0.1"),
		discoveredPod("template-manager-abcde-fghij", "template-manager", "10.2.0.2"),
	)

	sd, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderKubernetes, nomadStub(t, nomadFleet{})), newKube, cfg.ServiceDiscoveryProviderKubernetes)
	require.NoError(t, err)

	assert.Equal(t, []string{"10.2.0.1"}, planeIPs(t, sd.nodes), "the node plane must select on the orchestrator label")
	assert.Equal(t, []string{"10.2.0.2"}, planeIPs(t, sd.templateBuilders), "the builder plane must select on the template-manager label")
}

func TestServiceDiscovery_LocalProviderPointsBothPlanesAtTheConfiguredAddress(t *testing.T) {
	t.Parallel()

	config := discoveryConfig(cfg.ServiceDiscoveryProviderLocal, nomadStub(t, nomadFleet{}))
	config.LocalOrchestratorAddress = "127.0.0.1:6123"

	sd, err := newServiceDiscovery(t.Context(), config, fixedKubeClient(), cfg.ServiceDiscoveryProviderLocal)
	require.NoError(t, err)

	assert.Equal(t, []string{"127.0.0.1"}, planeIPs(t, sd.nodes))
	assert.Equal(t, []string{"127.0.0.1"}, planeIPs(t, sd.templateBuilders))
}

// store.go states that Nomad is the primary so its entries win a dedup conflict.
func TestServiceDiscovery_ComposedProviderPutsNomadFirst(t *testing.T) {
	t.Parallel()

	nomad := nomadStub(t, nomadFleet{
		registrations: []map[string]any{nomadRegistration("11111111-2222-3333-4444-555555555555", "10.1.0.1")},
		allocations:   []map[string]any{nomadBuilderAllocation("alloc-1", "nomad-node-1", "10.1.0.2")},
	})
	newKube := fixedKubeClient(
		discoveredPod("orchestrator-abcde-fghij", "orchestrator", "10.2.0.1"),
		discoveredPod("template-manager-abcde-fghij", "template-manager", "10.2.0.2"),
	)

	composed, err := newServiceDiscovery(t.Context(), discoveryConfig(cfg.ServiceDiscoveryProviderNomadKubernetes, nomad), newKube, cfg.ServiceDiscoveryProviderNomadKubernetes)
	require.NoError(t, err)

	assert.Equal(t, []string{"10.1.0.1", "10.2.0.1"}, planeIPs(t, composed.nodes))
	assert.Equal(t, []string{"10.1.0.2", "10.2.0.2"}, planeIPs(t, composed.templateBuilders))
}
