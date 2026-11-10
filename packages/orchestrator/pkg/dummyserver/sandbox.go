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
	"net/http"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// SandboxServer is an in-memory dummy implementation of orchestrator.SandboxServiceServer.
type SandboxServer struct {
	orchestrator.UnimplementedSandboxServiceServer

	clientID     string
	machine      *servicestatus.Machine
	maxSandboxes int64

	mu        sync.Mutex
	sandboxes map[string]*orchestrator.RunningSandbox
}

// NewSandbox returns a SandboxServer with an empty sandbox map and a fixed
// per-process clientID (a UUID). The clientID is returned from Create so the
// api side can route follow-up calls back to this orchestrator. Creates are
// refused while the machine is not Healthy or maxSandboxes are running; a
// limit of zero or less closes the node, as the real orchestrator's does.
func NewSandbox(machine *servicestatus.Machine, maxSandboxes int64) *SandboxServer {
	return &SandboxServer{
		clientID:     uuid.NewString(),
		machine:      machine,
		maxSandboxes: maxSandboxes,
		sandboxes:    make(map[string]*orchestrator.RunningSandbox),
	}
}

// ClientID returns the synthetic client identifier for this dummy node.
func (s *SandboxServer) ClientID() string {
	return s.clientID
}

// Count is the number of sandboxes recorded as running.
func (s *SandboxServer) Count() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return uint32(len(s.sandboxes))
}

func (s *SandboxServer) Create(_ context.Context, req *orchestrator.SandboxCreateRequest) (*orchestrator.SandboxCreateResponse, error) {
	if req == nil || req.GetSandbox() == nil {
		return nil, status.Error(codes.InvalidArgument, "sandbox config is required")
	}

	sbxID := req.GetSandbox().GetSandboxId()
	if sbxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}

	// The same refusal as the real orchestrator's, detail included: the api
	// retries elsewhere and leaves this node out of its next sample.
	if snap := s.machine.Snapshot(); snap.Status != orchestratorinfo.ServiceInfoStatus_Healthy {
		st := status.Newf(codes.ResourceExhausted, "node is not accepting new sandboxes (status %s), please retry", snap.Status)
		withDetails, err := st.WithDetails(&orchestrator.UserError{
			Code:       orchestrator.UserErrorCode_NODE_NOT_ACCEPTING_SANDBOXES,
			Message:    st.Message(),
			HttpStatus: http.StatusServiceUnavailable,
		})
		if err != nil {
			return nil, st.Err()
		}

		return nil, withDetails.Err()
	}

	startTime := req.GetStartTime()
	if startTime == nil {
		startTime = timestamppb.Now()
	}

	running := &orchestrator.RunningSandbox{
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
	defer s.mu.Unlock()

	if int64(len(s.sandboxes)) >= s.maxSandboxes {
		return nil, status.Error(codes.ResourceExhausted, "too many sandboxes on this node, please retry")
	}
	s.sandboxes[sbxID] = running

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

	s.mu.Lock()
	delete(s.sandboxes, req.GetSandboxId())
	s.mu.Unlock()

	return &emptypb.Empty{}, nil
}

func (s *SandboxServer) Pause(_ context.Context, req *orchestrator.SandboxPauseRequest) (*orchestrator.SandboxPauseResponse, error) {
	if req.GetSandboxId() == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}

	// Pause is treated as a delete in the dummy: no real snapshotting happens.
	s.mu.Lock()
	delete(s.sandboxes, req.GetSandboxId())
	s.mu.Unlock()

	return &orchestrator.SandboxPauseResponse{}, nil
}

func (s *SandboxServer) Checkpoint(_ context.Context, _ *orchestrator.SandboxCheckpointRequest) (*orchestrator.SandboxCheckpointResponse, error) {
	// No-op: there is no real disk/memory to checkpoint.
	return &orchestrator.SandboxCheckpointResponse{}, nil
}
