package placement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

type mockAlgorithm struct {
	mock.Mock
}

func (m *mockAlgorithm) chooseNode(ctx context.Context, nodes []*nodemanager.Node, nodesExcluded map[string]struct{}, requested nodemanager.SandboxResources, requirements Requirements, filterByLabels bool, requiredLabels []string) (*nodemanager.Node, error) {
	args := m.Called(ctx, nodes, nodesExcluded, requested, requirements, filterByLabels, requiredLabels)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*nodemanager.Node), args.Error(1)
}

func TestPlaceSandbox_SuccessfulPlacement(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create test nodes
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4)
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 5, 4)
	node3 := nodemanager.NewTestNode("node3", api.NodeStatusReady, 7, 4)
	nodes := []*nodemanager.Node{node1, node2, node3}

	// Create a mock algorithm that returns node2
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node2, nil)

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, Requirements{}, false, nil)

	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node2, resultNode.Node)
	algorithm.AssertExpectations(t)
}

func TestPlaceSandbox_WithPreferredNode(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create test nodes
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4)
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 5, 4)
	node3 := nodemanager.NewTestNode("node3", api.NodeStatusReady, 7, 4)
	nodes := []*nodemanager.Node{node1, node2, node3}

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	// Test without preferred node - algorithm should be called
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node1, nil).Once()

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, Requirements{}, false, nil)
	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node1, resultNode.Node)
	algorithm.AssertExpectations(t)

	// Test with preferred node - should use the preferred node directly without calling algorithm
	resultNode, err = PlaceSandbox(ctx, algorithm, nodes, node2, sbxRequest, Requirements{}, false, nil)
	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node2, resultNode.Node)
	// Algorithm should not be called when preferred node is provided
	algorithm.AssertNotCalled(t, "chooseNode")
}

func TestPlaceSandbox_ContextTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
	defer cancel()

	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) {
			// Simulate slow node selection
			time.Sleep(10 * time.Millisecond)
		}).
		Return(nil, errors.New("timeout"))

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4),
	}, nil, sbxRequest, Requirements{}, false, nil)

	require.Error(t, err)
	assert.Nil(t, resultNode.Node)
	// The error could be either "timeout" from the algorithm or "request timed out" from ctx.Done()
	assert.True(t, err.Error() == "timeout" || strings.Contains(err.Error(), "request timed out"))
}

func TestPlaceSandbox_NoNodes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	algorithm := &mockAlgorithm{}
	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{}, nil, sbxRequest, Requirements{}, false, nil)

	require.Error(t, err)
	assert.Nil(t, resultNode.Node)
	assert.Contains(t, err.Error(), "no nodes available")
}

func TestPlaceSandbox_AllNodesExcluded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("no nodes available"))

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{
		nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4),
	}, nil, sbxRequest, Requirements{}, false, nil)

	require.Error(t, err)
	assert.Nil(t, resultNode.Node)
	assert.Contains(t, err.Error(), "no nodes available")
	algorithm.AssertExpectations(t)
}

func TestPlaceSandbox_ResourceExhausted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create test nodes - node1 will return ResourceExhausted, node2 will succeed
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 3, 4,
		nodemanager.WithSandboxCreateError(status.Error(codes.ResourceExhausted, "node exhausted")))
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 5, 4)
	nodes := []*nodemanager.Node{node1, node2}

	// Algorithm should be called twice - first returns node1 (exhausted), then node2 (succeeds)
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node1, nil).Once()
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node2, nil).Once()

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, Requirements{}, false, nil)

	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)
	assert.Equal(t, node2, resultNode.Node, "should succeed on node2 after node1 was exhausted")
	algorithm.AssertExpectations(t)

	// Verify node1 was NOT excluded (ResourceExhausted nodes should be retried)
	algorithm.AssertNumberOfCalls(t, "chooseNode", 2)
}

func TestPlaceSandbox_TriggersOptimisticUpdate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create a node and record the initial allocated CPU
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)
	initialCpuAllocated := node1.Metrics().CpuAllocated

	nodes := []*nodemanager.Node{node1}

	// Mock algorithm directly returns node1
	algorithm := &mockAlgorithm{}
	algorithm.On("chooseNode", mock.Anything, nodes, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(node1, nil)

	// Request 2 vCPUs
	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId: "test-optimistic-sandbox",
			Vcpu:      2,
			RamMb:     1024,
		},
	}

	resultNode, err := PlaceSandbox(ctx, algorithm, nodes, nil, sbxRequest, Requirements{}, false, nil)

	require.NoError(t, err)
	assert.NotNil(t, resultNode.Node)

	// Verify: After successful placement, the node's CpuAllocated should be increased by 2 from the base
	updatedCpuAllocated := resultNode.Node.Metrics().CpuAllocated
	assert.Equal(t, initialCpuAllocated+2, updatedCpuAllocated, "Node metrics should be optimistically updated after placement")
}
