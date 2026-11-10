// Package servicestatus is the service status machine an orchestrator reports
// through ServiceInfo: the process's own transitions, the overrides a sizer
// or an operator sends, and the Standby that outlives the process.
package servicestatus

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type Status = orchestratorinfo.ServiceInfoStatus

const (
	healthy      = orchestratorinfo.ServiceInfoStatus_Healthy
	draining     = orchestratorinfo.ServiceInfoStatus_Draining
	unhealthy    = orchestratorinfo.ServiceInfoStatus_Unhealthy
	standby      = orchestratorinfo.ServiceInfoStatus_Standby
	shuttingDown = orchestratorinfo.ServiceInfoStatus_ShuttingDown
	notReady     = orchestratorinfo.ServiceInfoStatus_NotReady
)

// CapabilityParkOwner says a Standby override may carry an owner that later
// overrides must match.
const CapabilityParkOwner = "park-owner"

var Capabilities = []string{CapabilityParkOwner}

// Snapshot is the reported status. Owner is set by a Standby and kept by the
// shutdown that follows it, so the next process can restore the park.
type Snapshot struct {
	Status    Status
	Owner     string
	ChangedAt time.Time
}

// Request is an override. Force bypasses the ownership rules only: it applies
// the carried status and owner over a recorded owner, and nothing else.
type Request struct {
	Status Status
	Owner  string
	Force  bool
}

type Reason string

const (
	ReasonShuttingDown      Reason = "shutting_down"       // ShuttingDown requested by an override
	ReasonNotReady          Reason = "not_ready"           // NotReady requested by an override
	ReasonTerminal          Reason = "terminal"            // current status is ShuttingDown
	ReasonInvalidStatus     Reason = "invalid_status"      // a status no override may ask for
	ReasonInvalidOwner      Reason = "invalid_owner"       // owner outside the label character set or too long
	ReasonDrainingToStandby Reason = "draining_to_standby" // Draining cannot park
	ReasonOwnerMismatch     Reason = "owner_mismatch"      // Standby under another owner
)

type RefusedError struct {
	Reason    Reason
	Requested Request
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("cannot override node status to %s: %s", e.Requested.Status, e.Reason)
}

// GRPCStatus is InvalidArgument for a malformed request and FailedPrecondition
// for every refusal the current state decides.
func (e *RefusedError) GRPCStatus() *status.Status {
	code := codes.FailedPrecondition
	if e.Reason == ReasonInvalidOwner || e.Reason == ReasonInvalidStatus {
		code = codes.InvalidArgument
	}

	return status.New(code, e.Error())
}

// MaxOwnerLength bounds an owner like a scheduling label.
const MaxOwnerLength = 63

var ownerPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// Rules is the whole transition table, pure. A refusal returns current
// unchanged and a *RefusedError.
func Rules(current Snapshot, req Request, now time.Time) (Snapshot, error) {
	var reason Reason
	switch {
	case req.Status == shuttingDown:
		reason = ReasonShuttingDown
	case req.Status == notReady:
		reason = ReasonNotReady
	case req.Status != healthy && req.Status != draining && req.Status != unhealthy && req.Status != standby:
		reason = ReasonInvalidStatus
	case current.Status == shuttingDown:
		reason = ReasonTerminal
	case req.Owner != "" && (len(req.Owner) > MaxOwnerLength || !ownerPattern.MatchString(req.Owner)):
		reason = ReasonInvalidOwner
	case current.Status == draining && req.Status == standby:
		reason = ReasonDrainingToStandby
	case !req.Force && req.Status == standby && current.Owner != "" && req.Owner != "" && req.Owner != current.Owner:
		// The one ownership rule: a park is not taken over. Anything else from
		// anyone applies and clears the owner, which only says whose park it is.
		reason = ReasonOwnerMismatch
	}
	if reason != "" {
		return current, &RefusedError{Reason: reason, Requested: req}
	}

	next := Snapshot{Status: req.Status, ChangedAt: now}
	if req.Status == standby {
		next.Owner = req.Owner
	}
	if next.Status == current.Status && next.Owner == current.Owner {
		next = current
	}

	return next, nil
}

type Machine struct {
	mu    sync.Mutex
	snap  Snapshot
	store Store
	now   func() time.Time
}

// New starts Healthy at now().
func New(store Store, now func() time.Time) *Machine {
	return &Machine{snap: Snapshot{Status: healthy, ChangedAt: now()}, store: store, now: now}
}

// Restore takes a Standby record left by an earlier process as the current
// snapshot. Any other record is ignored; an unreadable one is returned for
// the caller to log, and the machine stays as it is.
func (m *Machine) Restore(ctx context.Context) error {
	snap, found, err := m.store.Load()
	if err != nil {
		return err
	}
	if !found || snap.Status != standby {
		return nil
	}

	m.mu.Lock()
	m.snap = snap
	m.mu.Unlock()
	logger.L().Info(ctx, "Service status restored", zap.String("status", snap.Status.String()), zap.String("owner", snap.Owner))

	return nil
}

func (m *Machine) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.snap
}

// Set is the process's own transition: between its own statuses, Healthy and
// NotReady, and into ShuttingDown, which is terminal and carries the owner so
// the next process restores a Standby. A status an operator or a sizer set
// (Standby, Draining, Unhealthy) is Override's to move: Set leaves it, and
// never touches the store.
func (m *Machine) Set(ctx context.Context, status Status) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch {
	case m.snap.Status == shuttingDown, m.snap.Status == status: // terminal, or already there
	case status == shuttingDown:
		m.transition(ctx, Snapshot{Status: status, Owner: m.snap.Owner, ChangedAt: m.now()})
	case m.snap.Status == healthy, m.snap.Status == notReady:
		m.transition(ctx, Snapshot{Status: status, ChangedAt: m.now()})
	}
}

// Override applies req under Rules and brings the store in line: a Standby is
// saved, anything else clears it. A store failure is logged, not returned:
// the live status must not depend on the disk. It returns nil or a
// *RefusedError.
func (m *Machine) Override(ctx context.Context, req Request) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next, err := Rules(m.snap, req, m.now())
	if err != nil {
		return err
	}
	if next.Status != m.snap.Status || next.Owner != m.snap.Owner {
		m.transition(ctx, next)
	}
	if next.Status == standby {
		err = m.store.Save(m.snap)
	} else {
		err = m.store.Clear()
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to record the service status", zap.Error(err), zap.String("status", m.snap.Status.String()))
	}

	return nil
}

func (m *Machine) transition(ctx context.Context, next Snapshot) {
	m.snap = next
	logger.L().Info(ctx, "Service status changed", zap.String("status", next.Status.String()), zap.String("owner", next.Owner))
}
