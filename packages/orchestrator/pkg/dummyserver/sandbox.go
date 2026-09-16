// Package dummyserver provides a non-functional implementation of the
// orchestrator gRPC surface. It exists so the api package can be wired up
// against a real gRPC orchestrator (including on macOS dev machines, where
// the real Linux-only orchestrator with firecracker, NBD, NFS, cgroup, etc.
// cannot run) and to give CI a lightweight orchestrator process for tests
// that only need the SandboxService control plane.
//
// Nothing here actually starts a sandbox; calls just record state in memory
// and return plausible responses.
package dummyserver

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// SandboxServer is an in-memory dummy implementation of orchestrator.SandboxServiceServer.
type SandboxServer struct {
	orchestrator.UnimplementedSandboxServiceServer

	clientID string

	mu        sync.Mutex
	sandboxes map[string]*orchestrator.RunningSandbox
}

// NewSandbox returns a SandboxServer with an empty sandbox map and a fixed
// per-process clientID (a UUID). The clientID is returned from Create so the
// api side can route follow-up calls back to this orchestrator.
func NewSandbox() *SandboxServer {
	return &SandboxServer{
		clientID:  uuid.NewString(),
		sandboxes: make(map[string]*orchestrator.RunningSandbox),
	}
}

// ClientID returns the synthetic client identifier for this dummy node.
func (s *SandboxServer) ClientID() string {
	return s.clientID
}

func (s *SandboxServer) Create(_ context.Context, req *orchestrator.SandboxCreateRequest) (*orchestrator.SandboxCreateResponse, error) {
	if req == nil || req.GetSandbox() == nil {
		return nil, status.Error(codes.InvalidArgument, "sandbox config is required")
	}

	sbxID := req.GetSandbox().GetSandboxId()
	if sbxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}

	startTime := req.GetStartTime()
	if startTime == nil {
		startTime = timestamppb.Now()
	}

	cfg, _ := proto.Clone(req.GetSandbox()).(*orchestrator.SandboxConfig)

	running := &orchestrator.RunningSandbox{
		Config:      cfg, //nolint:staticcheck // mirrors the real orchestrator until config is dropped
		ClientId:    s.clientID,
		StartTime:   startTime,
		EndTime:     req.GetEndTime(),
		SandboxId:   sbxID,
		TeamId:      req.GetSandbox().GetTeamId(),
		ExecutionId: req.GetSandbox().GetExecutionId(),
		Vcpu:        req.GetSandbox().GetVcpu(),
		RamMb:       req.GetSandbox().GetRamMb(),
	}

	s.mu.Lock()
	s.sandboxes[sbxID] = running
	s.mu.Unlock()

	return &orchestrator.SandboxCreateResponse{ClientId: s.clientID}, nil
}

func (s *SandboxServer) Update(_ context.Context, req *orchestrator.SandboxUpdateRequest) (*emptypb.Empty, error) {
	if req.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sbx, ok := s.sandboxes[req.GetSandboxId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %q not found", req.GetSandboxId())
	}

	if req.GetEndTime() != nil {
		sbx.EndTime = req.GetEndTime()
	}

	// Egress updates are accepted but ignored — there's no real network here.
	_ = req.GetEgress()

	return &emptypb.Empty{}, nil
}

func (s *SandboxServer) List(_ context.Context, _ *emptypb.Empty) (*orchestrator.SandboxListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*orchestrator.RunningSandbox, 0, len(s.sandboxes))
	for _, sbx := range s.sandboxes {
		// Clone to avoid handing out internal pointers.
		if cloned, ok := proto.Clone(sbx).(*orchestrator.RunningSandbox); ok {
			out = append(out, cloned)
		}
	}

	return &orchestrator.SandboxListResponse{Sandboxes: out}, nil
}

func (s *SandboxServer) Delete(_ context.Context, req *orchestrator.SandboxDeleteRequest) (*emptypb.Empty, error) {
	if req.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	if req.GetExecutionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "execution_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sbx, ok := s.sandboxes[req.GetSandboxId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %q not found", req.GetSandboxId())
	}
	if sbx.GetExecutionId() != req.GetExecutionId() {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %q execution changed", req.GetSandboxId())
	}
	delete(s.sandboxes, req.GetSandboxId())

	return &emptypb.Empty{}, nil
}

func (s *SandboxServer) Pause(_ context.Context, req *orchestrator.SandboxPauseRequest) (*orchestrator.SandboxPauseResponse, error) {
	if req.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	if req.GetExecutionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "execution_id is required")
	}

	// Pause is treated as a delete in the dummy: no real snapshotting happens.
	s.mu.Lock()
	defer s.mu.Unlock()
	sbx, ok := s.sandboxes[req.GetSandboxId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %q not found", req.GetSandboxId())
	}
	if sbx.GetExecutionId() != req.GetExecutionId() {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %q execution changed", req.GetSandboxId())
	}
	delete(s.sandboxes, req.GetSandboxId())

	return &orchestrator.SandboxPauseResponse{}, nil
}

func (s *SandboxServer) Checkpoint(_ context.Context, _ *orchestrator.SandboxCheckpointRequest) (*orchestrator.SandboxCheckpointResponse, error) {
	// No-op: there is no real disk/memory to checkpoint.
	return &orchestrator.SandboxCheckpointResponse{}, nil
}
