package hoststats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SandboxHostStat represents a single host-level statistics sample
// for a Firecracker process running a sandbox
type SandboxHostStat struct {
	Timestamp          time.Time `ch:"timestamp"`
	SandboxID          string    `ch:"sandbox_id"`
	SandboxExecutionID string    `ch:"sandbox_execution_id"`
	SandboxTemplateID  string    `ch:"sandbox_template_id"`
	SandboxBuildID     string    `ch:"sandbox_build_id"`
	SandboxTeamID      uuid.UUID `ch:"sandbox_team_id"`

	SandboxVCPUCount int64 `ch:"sandbox_vcpu_count"` // number of virtual CPUs allocated to the sandbox
	SandboxMemoryMB  int64 `ch:"sandbox_memory_mb"`  // total memory allocated to the sandbox in megabytes

	// Cgroup v2 accounting — cumulative CPU counters
	CgroupCPUUsageUsec  uint64 `ch:"cgroup_cpu_usage_usec"`     // cumulative, microseconds
	CgroupCPUUserUsec   uint64 `ch:"cgroup_cpu_user_usec"`      // cumulative, microseconds
	CgroupCPUSystemUsec uint64 `ch:"cgroup_cpu_system_usec"`    // cumulative, microseconds
	CgroupMemoryUsage   uint64 `ch:"cgroup_memory_usage_bytes"` // current, bytes
	CgroupMemoryPeak    uint64 `ch:"cgroup_memory_peak_bytes"`  // interval peak, bytes (reset after each sample)

	// Pre-computed deltas between consecutive samples.
	DeltaCgroupCPUUsageUsec  uint64 `ch:"delta_cgroup_cpu_usage_usec"`
	DeltaCgroupCPUUserUsec   uint64 `ch:"delta_cgroup_cpu_user_usec"`
	DeltaCgroupCPUSystemUsec uint64 `ch:"delta_cgroup_cpu_system_usec"`
	IntervalUs               uint64 `ch:"interval_us"` // microseconds since previous sample

	// In-memory only; the ClickHouse insert does not persist these fields.
	// GuestMemoryMeasured is false without a page tracker and on the zero baseline row.
	GuestMemoryMeasured           bool   `ch:"-"`
	GuestMemoryHeldBytes          uint64 `ch:"-"`
	GuestMemoryHeldMiBMillis      uint64 `ch:"-"`
	DeltaGuestMemoryHeldMiBMillis uint64 `ch:"-"`

	SandboxType string `ch:"sandbox_type"` // "sandbox" or "build"
}

// Delivery is the interface for delivering host stats to storage backend
// This allows the orchestrator to depend on the interface rather than concrete implementation
type Delivery interface {
	Push(stat SandboxHostStat) error
	Close(ctx context.Context) error
}

// noopDelivery is a Delivery that discards all stats.
// Used in environments where host stats collection is not needed (CLI tools, tests).
type noopDelivery struct{}

var _ Delivery = (*noopDelivery)(nil)

// NewNoopDelivery returns a Delivery that silently discards all stats.
func NewNoopDelivery() Delivery {
	return &noopDelivery{}
}

func (d *noopDelivery) Push(_ SandboxHostStat) error  { return nil }
func (d *noopDelivery) Close(_ context.Context) error { return nil }

// multiDelivery fans out to every target. Push is serial (each target's Push
// is a non-blocking batcher send); Close is parallel so a stalled target
// can't block the others from draining.
type multiDelivery struct {
	targets []Delivery
}

var _ Delivery = (*multiDelivery)(nil)

// NewMultiDelivery returns noop for 0 targets and the target directly for 1,
// so callers can wrap unconditionally.
func NewMultiDelivery(targets ...Delivery) Delivery {
	switch len(targets) {
	case 0:
		return NewNoopDelivery()
	case 1:
		return targets[0]
	default:
		return &multiDelivery{targets: targets}
	}
}

func (m *multiDelivery) Push(stat SandboxHostStat) error {
	var err error
	for _, t := range m.targets {
		if e := t.Push(stat); e != nil {
			err = errors.Join(err, e)
		}
	}

	return err
}

func (m *multiDelivery) Close(ctx context.Context) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs error
	)
	for _, t := range m.targets {
		wg.Go(func() {
			if e := t.Close(ctx); e != nil {
				mu.Lock()
				errs = errors.Join(errs, e)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	return errs
}
