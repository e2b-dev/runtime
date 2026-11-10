package dummyserver

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// InfoOptions describe the dummy node and what it reports. Machine is required.
type InfoOptions struct {
	NodeID, ServiceID, Version, Commit string
	Labels                             []string
	Machine                            *servicestatus.Machine
	MaxSandboxes                       int64
	SandboxCount                       func() uint32
}

// InfoServer implements orchestratorinfo.InfoServiceServer over the shared
// status machine, so a test cluster running the dummy gets the status rules
// a real orchestrator has.
type InfoServer struct {
	orchestratorinfo.UnimplementedInfoServiceServer

	options     InfoOptions
	startedAt   time.Time
	machineInfo *orchestratorinfo.MachineInfo
}

func NewInfo(options InfoOptions) *InfoServer {
	return &InfoServer{
		options:   options,
		startedAt: time.Now(),
		machineInfo: &orchestratorinfo.MachineInfo{
			CpuArchitecture: runtime.GOARCH,
			CpuFamily:       "darwin-dummy",
			CpuModel:        "darwin-dummy",
			CpuModelName:    "darwin-dummy",
		},
	}
}

// NewMachine is the dummy's status machine. With a state dir, created when
// missing, a Standby survives a restart through the shared record file;
// without one nothing persists. A state dir that cannot be created returns no
// machine; an unreadable record is returned beside the machine for the caller
// to log, and the machine starts Healthy.
func NewMachine(ctx context.Context, stateDir string) (*servicestatus.Machine, error) {
	var store servicestatus.Store = servicestatus.NopStore{}
	if stateDir != "" {
		if err := os.MkdirAll(stateDir, 0o750); err != nil {
			return nil, err
		}
		store = servicestatus.NewFileStore(filepath.Join(stateDir, servicestatus.FileName))
	}
	machine := servicestatus.New(store, time.Now)

	return machine, machine.Restore(ctx)
}

func (s *InfoServer) ServiceInfo(_ context.Context, _ *emptypb.Empty) (*orchestratorinfo.ServiceInfoResponse, error) {
	resp := &orchestratorinfo.ServiceInfoResponse{
		NodeId:         s.options.NodeID,
		ServiceId:      s.options.ServiceID,
		ServiceVersion: s.options.Version,
		ServiceCommit:  s.options.Commit,
		ServiceRoles:   []orchestratorinfo.ServiceInfoRole{orchestratorinfo.ServiceInfoRole_Orchestrator},
		ServiceStartup: timestamppb.New(s.startedAt),
		MachineInfo:    s.machineInfo,
		Labels:         s.options.Labels,
		MaxSandboxes:   s.options.MaxSandboxes,
	}
	if s.options.SandboxCount != nil {
		resp.MetricSandboxesRunning = s.options.SandboxCount()
	}
	servicestatus.Fill(resp, s.options.Machine.Snapshot())

	return resp, nil
}

func (s *InfoServer) ServiceStatusOverride(ctx context.Context, req *orchestratorinfo.ServiceStatusChangeRequest) (*emptypb.Empty, error) {
	return servicestatus.HandleOverride(ctx, s.options.Machine.Override, req, nil)
}
