package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
)

const (
	testNamespace           = "e2b"
	testPort                = 5008
	labelOrchestrator       = "e2b.dev/orchestrator"
	labelTemplateManager    = "e2b.dev/template-manager"
	selectorOrchestrator    = labelOrchestrator + "=true"
	selectorTemplateManager = labelTemplateManager + "=true"
)

func newPod(name, hostIP string, labels ...string) *corev1.Pod {
	podLabels := map[string]string{}
	for _, label := range labels {
		podLabels[label] = "true"
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Labels: podLabels},
		Spec:       corev1.PodSpec{NodeName: "machine-" + name},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			HostIP: hostIP,
			PodIP:  "10.99.0.1",
		},
	}
}

func instance(name, hostIP string) servicediscovery.Instance {
	return servicediscovery.Instance{
		WorkloadID: name,
		NodeID:     "machine-" + name,
		IPAddress:  hostIP,
		Port:       testPort,
		Backend:    servicediscovery.BackendKubernetes,
	}
}

func listInstances(t *testing.T, templateManagerSelector string, pods ...*corev1.Pod) []servicediscovery.Instance {
	t.Helper()

	client := fake.NewClientset()
	for _, pod := range pods {
		_, err := client.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	discovery, err := NewDiscovery(client, Config{
		Namespace:               testNamespace,
		OrchestratorSelector:    selectorOrchestrator,
		TemplateManagerSelector: templateManagerSelector,
		Port:                    testPort,
	})
	require.NoError(t, err)

	instances, err := discovery.ListInstances(t.Context())
	require.NoError(t, err)

	return instances
}

func TestListInstancesReturnsOrchestratorAndTemplateManagerPods(t *testing.T) {
	t.Parallel()

	instances := listInstances(t, selectorTemplateManager,
		newPod("orchestrator-a", "10.0.0.1", labelOrchestrator),
		newPod("template-manager-a", "10.0.0.2", labelTemplateManager),
		newPod("unrelated", "10.0.0.3", "app"),
	)

	assert.ElementsMatch(t, []servicediscovery.Instance{
		instance("orchestrator-a", "10.0.0.1"),
		instance("template-manager-a", "10.0.0.2"),
	}, instances)
}

func TestListInstancesReturnsAPodMatchedByBothSelectorsOnce(t *testing.T) {
	t.Parallel()

	instances := listInstances(t, selectorTemplateManager,
		newPod("both", "10.0.0.1", labelOrchestrator, labelTemplateManager),
	)

	assert.Equal(t, []servicediscovery.Instance{instance("both", "10.0.0.1")}, instances)
}

func TestListInstancesWithoutTemplateManagerSelectorReturnsOnlyOrchestrators(t *testing.T) {
	t.Parallel()

	instances := listInstances(t, "",
		newPod("orchestrator-a", "10.0.0.1", labelOrchestrator),
		newPod("template-manager-a", "10.0.0.2", labelTemplateManager),
	)

	assert.Equal(t, []servicediscovery.Instance{instance("orchestrator-a", "10.0.0.1")}, instances)
}

func TestNewDiscoveryRejectsAnEmptyOrchestratorSelector(t *testing.T) {
	t.Parallel()

	_, err := NewDiscovery(fake.NewClientset(), Config{
		Namespace:               testNamespace,
		TemplateManagerSelector: selectorTemplateManager,
	})

	require.Error(t, err)
}

func TestNewDiscoveryRejectsAnEmptyNamespace(t *testing.T) {
	t.Parallel()

	_, err := NewDiscovery(fake.NewClientset(), Config{
		OrchestratorSelector:    selectorOrchestrator,
		TemplateManagerSelector: selectorTemplateManager,
	})

	require.Error(t, err)
}
