//go:build linux

package service

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// readinessPollInterval is how often a NotReady node asks whether it is ready.
const readinessPollInterval = time.Second

type ServiceInfo struct {
	ClientId  string
	ServiceId string

	SourceVersion string
	SourceCommit  string

	Startup      time.Time
	Roles        []orchestratorinfo.ServiceInfoRole
	Labels       []string
	MachineInfo  machineinfo.MachineInfo
	MaxSandboxes atomic.Int64

	// machine is nil in a zero ServiceInfo, which reports Healthy and panics
	// on a status change; tests that change status build a container.
	machine *servicestatus.Machine

	// held is true while the startup hold still waits for ready; holdMu
	// orders the hold's check-then-set against overrides.
	holdMu sync.Mutex
	held   bool

	outstandingWork atomic.Int64
}

var serviceRolesMapper = map[cfg.ServiceType]orchestratorinfo.ServiceInfoRole{
	cfg.Orchestrator:    orchestratorinfo.ServiceInfoRole_Orchestrator,
	cfg.TemplateManager: orchestratorinfo.ServiceInfoRole_TemplateBuilder,
}

func (s *ServiceInfo) Snapshot() servicestatus.Snapshot {
	if s.machine == nil {
		return servicestatus.Snapshot{}
	}

	return s.machine.Snapshot()
}

// Child work must be registered before its parent releases ownership.
func (s *ServiceInfo) TrackWork() func() {
	s.outstandingWork.Add(1)

	return s.finishWork
}

func (s *ServiceInfo) finishWork() {
	s.outstandingWork.Add(-1)
}

func (s *ServiceInfo) OutstandingWork() int64 {
	return s.outstandingWork.Load()
}

// SetStatus is the process's own transition; a status an operator or a sizer
// set stays.
func (s *ServiceInfo) SetStatus(ctx context.Context, status orchestratorinfo.ServiceInfoStatus) {
	s.machine.Set(ctx, status)
}

// OverrideStatus returns nil or a *servicestatus.RefusedError. A Healthy that
// lands while the startup hold still waits becomes NotReady until the node is
// ready.
func (s *ServiceInfo) OverrideStatus(ctx context.Context, req servicestatus.Request) error {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()

	err := s.machine.Override(ctx, req)
	if err == nil && s.held && req.Status == orchestratorinfo.ServiceInfoStatus_Healthy {
		s.SetStatus(ctx, orchestratorinfo.ServiceInfoStatus_NotReady)
	}

	return err
}

// HoldUntilReady reports NotReady until ready answers true, then Healthy, and
// never NotReady again. A status an operator or the shutdown sets in between
// ends the wait and stays, except a Standby: a restored or newly recorded
// park keeps the wait alive, so a release before the node is ready lands on
// NotReady and becomes Healthy once it is. It returns at once, and ready is
// polled in the background until the wait ends.
func (s *ServiceInfo) HoldUntilReady(ctx context.Context, ready func() bool) {
	s.holdMu.Lock()
	s.held = true
	s.holdMu.Unlock()
	s.SetStatus(ctx, orchestratorinfo.ServiceInfoStatus_NotReady)
	go func() {
		ticker := time.NewTicker(readinessPollInterval)
		defer ticker.Stop()
		for !s.becomeReady(ctx, ready()) {
			<-ticker.C
		}
	}()
}

// becomeReady makes a NotReady node Healthy when ready, and reports whether
// the wait is over: the node is ready, or a status other than NotReady or
// Standby replaced the hold. holdMu orders it against OverrideStatus, so a
// release cannot slip between the read and the set.
func (s *ServiceInfo) becomeReady(ctx context.Context, ready bool) bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()

	status := s.Snapshot().Status
	waiting := status == orchestratorinfo.ServiceInfoStatus_NotReady || status == orchestratorinfo.ServiceInfoStatus_Standby
	if waiting && !ready {
		return false
	}
	s.held = false
	if ready && status == orchestratorinfo.ServiceInfoStatus_NotReady {
		s.SetStatus(ctx, orchestratorinfo.ServiceInfoStatus_Healthy)
	}

	return true
}

// NewInfoContainer restores a Standby an earlier process recorded under the
// orchestrator's base directory; a record it cannot read is logged and the
// node starts Healthy.
func NewInfoContainer(ctx context.Context, clientId string, version string, commit string, instanceID string, machineInfo machineinfo.MachineInfo, config cfg.Config) *ServiceInfo {
	services := cfg.GetServices(config)
	serviceRoles := make([]orchestratorinfo.ServiceInfoRole, 0)

	for _, service := range services {
		if role, ok := serviceRolesMapper[service]; ok {
			serviceRoles = append(serviceRoles, role)
		}
	}

	machine := servicestatus.New(servicestatus.NewFileStore(filepath.Join(config.OrchestratorBaseDir, servicestatus.FileName)), time.Now)
	// The one instant of construction, before a restore replaces the snapshot.
	startup := machine.Snapshot().ChangedAt
	if err := machine.Restore(ctx); err != nil {
		logger.L().Error(ctx, "Failed to restore the service status, starting Healthy", zap.Error(err))
	}

	return &ServiceInfo{
		ClientId:  clientId,
		ServiceId: instanceID,

		machine: machine,

		Startup:     startup,
		Roles:       serviceRoles,
		Labels:      config.NodeLabels,
		MachineInfo: machineInfo,

		SourceVersion: version,
		SourceCommit:  commit,
	}
}
