//go:build linux

package service

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type Server struct {
	orchestratorinfo.UnimplementedInfoServiceServer

	info        *ServiceInfo
	sandboxes   *sandbox.Map
	hostMetrics *metrics.HostMetrics
	refused     metric.Int64Counter
}

func NewInfoService(info *ServiceInfo, sandboxes *sandbox.Map, hostMetrics *metrics.HostMetrics, refused metric.Int64Counter) *Server {
	return &Server{
		info:        info,
		sandboxes:   sandboxes,
		hostMetrics: hostMetrics,
		refused:     refused,
	}
}

func (s *Server) ServiceInfo(ctx context.Context, _ *emptypb.Empty) (*orchestratorinfo.ServiceInfoResponse, error) {
	info := s.info
	// The status first: a create that passed the gate before a park had
	// registered its work before the gate, so work read after a Standby
	// snapshot includes it.
	snap := info.Snapshot()

	// Get host metrics for the orchestrator
	cpuMetrics, err := s.hostMetrics.GetCPUMetrics()
	if err != nil {
		logger.L().Warn(ctx, "Failed to get host metrics", zap.Error(err))
		cpuMetrics = &metrics.CPUMetrics{}
	}

	memoryMetrics, err := s.hostMetrics.GetMemoryMetrics(ctx)
	if err != nil {
		logger.L().Warn(ctx, "Failed to get host metrics", zap.Error(err))
		memoryMetrics = &metrics.MemoryMetrics{}
	}

	diskMetrics, err := s.hostMetrics.GetDiskMetrics()
	if err != nil {
		logger.L().Warn(ctx, "Failed to get host metrics", zap.Error(err))
		diskMetrics = []metrics.DiskInfo{}
	}

	// Calculate sandbox resource allocation
	sandboxVCpuAllocated := uint32(0)
	sandboxMemoryAllocated := uint64(0)
	sandboxDiskAllocated := uint64(0)

	for _, item := range s.sandboxes.Items() {
		sandboxVCpuAllocated += uint32(item.Config.Vcpu)
		sandboxMemoryAllocated += uint64(item.Config.RamMB) * 1024 * 1024
		sandboxDiskAllocated += uint64(item.Config.TotalDiskSizeMB) * 1024 * 1024
	}

	resp := &orchestratorinfo.ServiceInfoResponse{
		NodeId:          info.ClientId,
		ServiceId:       info.ServiceId,
		OutstandingWork: uint64(info.OutstandingWork()),
		MaxSandboxes:    info.MaxSandboxes.Load(),

		ServiceVersion: info.SourceVersion,
		ServiceCommit:  info.SourceCommit,

		ServiceStartup: timestamppb.New(info.Startup),
		ServiceRoles:   info.Roles,
		MachineInfo:    convertMachineInfo(info.MachineInfo),
		Labels:         info.Labels,

		// Allocated resources to sandboxes
		MetricCpuAllocated:         sandboxVCpuAllocated,
		MetricMemoryAllocatedBytes: sandboxMemoryAllocated,
		MetricDiskAllocatedBytes:   sandboxDiskAllocated,
		MetricSandboxesRunning:     uint32(s.sandboxes.Count()),

		// Host system usage metrics
		MetricCpuPercent:      uint32(cpuMetrics.UsedPercent),
		MetricMemoryUsedBytes: memoryMetrics.UsedBytes,

		// Host system total resources
		MetricCpuCount:         cpuMetrics.Count,
		MetricMemoryTotalBytes: memoryMetrics.TotalBytes,

		// Hugepage pool metrics (page counts)
		MetricHugepagesTotal:    memoryMetrics.HugePagesTotal,
		MetricHugepagesUsed:     memoryMetrics.HugePagesUsed,
		MetricHugepagesReserved: memoryMetrics.HugePagesReserved,
		MetricHugepageSizeBytes: memoryMetrics.HugePageSizeBytes,
		MetricHugepagesCapacity: memoryMetrics.HugePagesCapacity,
		MetricHugepagesBase:     hugePagesBase(memoryMetrics),

		// Detailed disk metrics
		MetricDisks: convertDiskMetrics(diskMetrics),

		// TODO: Remove when migrated
		MetricVcpuUsed:     int64(sandboxVCpuAllocated),
		MetricMemoryUsedMb: int64(sandboxMemoryAllocated / (1024 * 1024)),
		MetricDiskMb:       int64(sandboxDiskAllocated / (1024 * 1024)),
	}
	servicestatus.Fill(resp, snap)

	return resp, nil
}

// convertDiskMetrics converts internal DiskInfo to protobuf DiskMetrics
func convertDiskMetrics(disks []metrics.DiskInfo) []*orchestratorinfo.DiskMetrics {
	result := make([]*orchestratorinfo.DiskMetrics, len(disks))
	for i, disk := range disks {
		result[i] = &orchestratorinfo.DiskMetrics{
			MountPoint:     disk.MountPoint,
			Device:         disk.Device,
			FilesystemType: disk.FilesystemType,
			UsedBytes:      disk.UsedBytes,
			TotalBytes:     disk.TotalBytes,
		}
	}

	return result
}

// convertDiskMetrics converts internal DiskInfo to protobuf DiskMetrics
func convertMachineInfo(machineInfo machineinfo.MachineInfo) *orchestratorinfo.MachineInfo {
	return &orchestratorinfo.MachineInfo{
		CpuArchitecture: machineInfo.Arch,
		CpuFamily:       machineInfo.Family,
		CpuModel:        machineInfo.Model,
		CpuModelName:    machineInfo.ModelName,
		CpuFlags:        machineInfo.Flags,
	}
}

func (s *Server) ServiceStatusOverride(ctx context.Context, req *orchestratorinfo.ServiceStatusChangeRequest) (*emptypb.Empty, error) {
	logger.L().Info(ctx, "service status override request received",
		zap.String("status", req.GetServiceStatus().String()),
		zap.String("owner", req.GetOwner()),
		zap.Bool("force", req.GetForce()),
	)

	return servicestatus.HandleOverride(ctx, s.info.OverrideStatus, req, s.refused)
}

// hugePagesBase leaves the field unset when the base pool could not be read,
// so the API can tell it from a pool that is all overcommit.
func hugePagesBase(m *metrics.MemoryMetrics) *uint64 {
	return proto.ValueOrNil(m.HugePagesBaseRead, func() uint64 { return m.HugePagesBase })
}
