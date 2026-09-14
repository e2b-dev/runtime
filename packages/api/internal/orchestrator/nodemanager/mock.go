package nodemanager

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// TestNode is an alias for Node used in testing
type TestNode = Node

// mockInfoClient implements infogrpc.InfoServiceClient
type mockInfoClient struct {
	infogrpc.InfoServiceClient
}

// ServiceInfo is a mock implementation that answers as a healthy orchestrator.
func (n *mockInfoClient) ServiceInfo(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*infogrpc.ServiceInfoResponse, error) {
	return &infogrpc.ServiceInfoResponse{
		NodeId:        "mock-node",
		ServiceId:     "mock-instance",
		ServiceStatus: infogrpc.ServiceInfoStatus_Healthy,
	}, nil
}

// mockSandboxClientFailingList fails the sandbox list call. This is how a live
// node fails a sync while still proving it is reachable: ServiceInfo answers
// and only the list call errors.
type mockSandboxClientFailingList struct {
	orchestrator.SandboxServiceClient
}

func (n *mockSandboxClientFailingList) List(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*orchestrator.SandboxListResponse, error) {
	return nil, errors.New("listing sandboxes failed")
}

// WithFailingSandboxList makes the node answer ServiceInfo but fail every
// sandbox list call.
func WithFailingSandboxList() TestOptions {
	return func(node *TestNode) {
		node.client.Sandbox = &mockSandboxClientFailingList{}
	}
}

// mockInfoClientSilent never answers, standing in for a node this replica
// genuinely cannot reach.
type mockInfoClientSilent struct {
	infogrpc.InfoServiceClient
}

func (n *mockInfoClientSilent) ServiceInfo(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*infogrpc.ServiceInfoResponse, error) {
	return nil, errors.New("unreachable")
}

// WithSilentInfoClient makes every ServiceInfo call fail.
func WithSilentInfoClient() TestOptions {
	return func(node *TestNode) {
		node.client.Info = &mockInfoClientSilent{}
	}
}

// mockSandboxClient implements orchestrator.SandboxServiceClient
type mockSandboxClient struct {
	orchestrator.SandboxServiceClient
}

// MockResolvedFirecrackerVersion is the resolved version the default mock
// Create echoes — deliberately distinct from any declared build version in
// the tests, so assertions can prove the record stores the ECHO rather than
// silently keeping the declared value.
const MockResolvedFirecrackerVersion = "v1.14-9.9.9"

// Create is a mock implementation that always returns success, echoing the
// filesystem-boot demand and the resolved Firecracker version like a current
// orchestrator build.
func (n *mockSandboxClient) Create(_ context.Context, req *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	return &orchestrator.SandboxCreateResponse{
		FilesystemBootApplied:      req.GetFilesystemBoot(),
		ResolvedFirecrackerVersion: MockResolvedFirecrackerVersion,
	}, nil
}

// mockLegacySandboxClient mimics an orchestrator that predates the
// filesystem_boot field: it succeeds but never echoes the applied boot path.
type mockLegacySandboxClient struct {
	orchestrator.SandboxServiceClient
}

func (n *mockLegacySandboxClient) Create(_ context.Context, _ *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	return &orchestrator.SandboxCreateResponse{}, nil
}

func (n *mockLegacySandboxClient) Delete(_ context.Context, _ *orchestrator.SandboxDeleteRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// mockTemplateClient implements templatemanager.TemplateServiceClient
type mockTemplateClient struct {
	templatemanager.TemplateServiceClient
}

// mockSandboxClientWithSleep implements orchestrator.SandboxServiceClient with a sleep on Create
type mockSandboxClientWithSleep struct {
	orchestrator.SandboxServiceClient

	baseSandboxCreateTime time.Duration
}

// Create is a mock implementation that always returns success, with a sleep
func (n *mockSandboxClientWithSleep) Create(_ context.Context, _ *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	if n.baseSandboxCreateTime != 0 {
		time.Sleep(time.Duration(rand.Int63n(2*n.baseSandboxCreateTime.Milliseconds())) * time.Millisecond)
	}

	return &orchestrator.SandboxCreateResponse{}, nil
}

// newMockGRPCClient creates a new mock gRPC client for testing
func newMockGRPCClient() *clusters.GRPCClient {
	// Create a dummy connection that will never be used
	conn, _ := grpc.NewClient("localhost:0", grpc.WithTransportCredentials(insecure.NewCredentials()))

	return &clusters.GRPCClient{
		Info:       &mockInfoClient{},
		Sandbox:    &mockSandboxClient{},
		Template:   &mockTemplateClient{},
		Connection: conn,
	}
}

type TestOptions func(node *TestNode)

func WithLegacySandboxClient() TestOptions {
	return func(node *TestNode) {
		node.client.Sandbox = &mockLegacySandboxClient{}
	}
}

func WithSandboxSleepingClient(baseSandboxCreateTime time.Duration) TestOptions {
	return func(node *TestNode) {
		node.client.Sandbox = &mockSandboxClientWithSleep{
			baseSandboxCreateTime: baseSandboxCreateTime,
		}
	}
}

func WithCPUInfo(cpuArch, cpuFamily, cpuModel string) TestOptions {
	return func(node *TestNode) {
		node.machineInfo.CPUArchitecture = cpuArch
		node.machineInfo.CPUFamily = cpuFamily
		node.machineInfo.CPUModel = cpuModel
	}
}

// WithOrchestratorVersion sets the orchestrator release the test node reports.
func WithOrchestratorVersion(version string) TestOptions {
	return func(node *TestNode) {
		node.meta.Version = version
	}
}

func WithLabels(labels []string) TestOptions {
	return func(node *TestNode) {
		labelsSet := make(map[string]struct{}, len(labels))
		for _, l := range labels {
			labelsSet[l] = struct{}{}
		}

		node.labels = labelsSet
	}
}

// mockSandboxClientWithError implements orchestrator.SandboxServiceClient that returns an error
type mockSandboxClientWithError struct {
	orchestrator.SandboxServiceClient

	err error
}

// Create is a mock implementation that returns a specific error
func (n *mockSandboxClientWithError) Create(_ context.Context, _ *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	return nil, n.err
}

func WithSandboxCreateError(err error) TestOptions {
	return func(node *TestNode) {
		node.client.Sandbox = &mockSandboxClientWithError{
			err: err,
		}
	}
}

// WithAllocatedMemoryBytes sets the initial allocated memory metric for the test node
func WithAllocatedMemoryBytes(bytes uint64) TestOptions {
	return func(node *TestNode) {
		node.metrics.MemoryAllocatedBytes = bytes
	}
}

// WithHugePages sets the hugepage pool the test node reports: page counts, as
// the kernel exposes them (total / used / reserved), plus the page size.
func WithHugePages(total, used, reserved, pageSizeBytes uint64) TestOptions {
	return func(node *TestNode) {
		node.metrics.HugePagesTotal = total
		node.metrics.HugePagesUsed = used
		node.metrics.HugePagesReserved = reserved
		node.metrics.HugePageSizeBytes = pageSizeBytes
	}
}

// MockSandboxClientCustom allows custom error logic per call
type MockSandboxClientCustom struct {
	orchestrator.SandboxServiceClient

	CreateFunc func() error
}

// Create calls the custom function to determine the error
func (n *MockSandboxClientCustom) Create(_ context.Context, _ *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	if n.CreateFunc != nil {
		if err := n.CreateFunc(); err != nil {
			return nil, err
		}
	}

	return &orchestrator.SandboxCreateResponse{}, nil
}

// SetSandboxClient allows setting a custom sandbox client on a test node
func (n *TestNode) SetSandboxClient(client orchestrator.SandboxServiceClient) {
	n.client.Sandbox = client
}

// NewTestNode creates a properly initialized Node for testing purposes
// It uses a mock gRPC client and has simplified Status() method behavior
func NewTestNode(id string, status api.NodeStatus, cpuAllocated int64, cpuCount uint32, options ...TestOptions) *TestNode {
	node := &Node{
		ID:            id,
		ClusterID:     uuid.New(),
		IPAddress:     "127.0.0.1",
		SandboxDomain: nil,
		PlacementMetrics: PlacementMetrics{
			sandboxesInProgress: smap.New[SandboxResources](),
			createSuccess:       atomic.Uint64{},
			createFails:         atomic.Uint64{},
		},

		client: newMockGRPCClient(),
		status: StatusInfo{Status: status, ChangedAt: time.Now()},
		metrics: Metrics{
			CpuAllocated: uint32(cpuAllocated),
			CpuCount:     cpuCount,
		},
	}

	for _, option := range options {
		option(node)
	}

	return node
}
