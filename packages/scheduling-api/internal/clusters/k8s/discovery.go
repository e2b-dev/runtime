package k8s

import (
	"context"
	"errors"

	"k8s.io/client-go/kubernetes"

	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery/kube"
)

// Config selects the orchestrator and template-manager pods of one
// Kubernetes cluster.
type Config struct {
	// Namespace is required: an empty namespace lists pods in every namespace.
	Namespace string
	// OrchestratorSelector is required: an empty label selector matches every
	// pod in the namespace.
	OrchestratorSelector string
	// TemplateManagerSelector may be empty; then only orchestrator pods are listed.
	TemplateManagerSelector string
	// Port is the node gRPC port, dialed on each pod's host IP.
	Port uint16
}

var _ servicediscovery.Discoverer = (*Discovery)(nil)

// Discovery lists the union of the orchestrator and template-manager pods,
// one instance per pod.
type Discovery struct {
	inner servicediscovery.Discoverer
}

func NewDiscovery(client kubernetes.Interface, config Config) (*Discovery, error) {
	if config.Namespace == "" {
		return nil, errors.New("pod namespace is empty")
	}

	if config.OrchestratorSelector == "" {
		return nil, errors.New("orchestrator pod label selector is empty")
	}

	inner := kube.NewPodsOnPort(client, config.Namespace, config.OrchestratorSelector, config.Port, true)
	if config.TemplateManagerSelector != "" {
		inner = servicediscovery.NewMerged(inner,
			kube.NewPodsOnPort(client, config.Namespace, config.TemplateManagerSelector, config.Port, true))
	}

	return &Discovery{inner: inner}, nil
}

func (d *Discovery) ListInstances(ctx context.Context) ([]servicediscovery.Instance, error) {
	return d.inner.ListInstances(ctx)
}

func (d *Discovery) Start(ctx context.Context) {
	d.inner.Start(ctx)
}

func (d *Discovery) Stop(ctx context.Context) {
	d.inner.Stop(ctx)
}
