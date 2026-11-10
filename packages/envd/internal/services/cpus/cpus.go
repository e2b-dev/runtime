// Package cpus brings the guest's online CPU set to a requested count by writing
// /sys/devices/system/cpu/cpuN/online. The guest boots with maxcpus= below the vCPU
// count Firecracker created, so the extra CPUs are possible but offline until asked for.
package cpus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
)

const (
	defaultTargetFile = "/run/e2b/cpu-target"
	// maxTargetBytes bounds the target file read. A valid target is at most four digits;
	// the rest is headroom for whitespace.
	maxTargetBytes = 64

	defaultRetryBase = time.Second
	defaultRetryCap  = 30 * time.Second
	// defaultMaxStalled is how many runs in a row may fail without changing any CPU
	// before the worker gives up. A run that makes progress resets the count, so a slow
	// guest is retried for as long as CPUs keep moving.
	defaultMaxStalled = 10
)

// State is what the X-Envd-Cpus header and /metrics report.
type State struct {
	Online   int
	Possible int
	Target   int
	// Attempts counts reconcile runs since the current target was set.
	Attempts int
	// WritePending is how long the cpuN/online write now in the kernel has been running,
	// 0 when none is. A value that keeps growing means hotplug is wedged in the guest
	// kernel.
	WritePending time.Duration
}

type Manager interface {
	// SetTarget records n as the wanted online count, durably, and wakes the worker.
	// It only validates and persists; the sysfs writes happen afterwards.
	SetTarget(n int) error
	Status() State
	// Start restores a target persisted by a previous envd process and begins applying
	// targets until ctx ends.
	Start(ctx context.Context)
}

type Option func(*manager)

func WithTargetFile(path string) Option {
	return func(m *manager) { m.targetFile = path }
}

type manager struct {
	logger     zerolog.Logger
	sys        cpuSysfs
	children   *reaper.Registry
	targetFile string
	retryBase  time.Duration
	retryCap   time.Duration
	maxStalled int

	// mu guards target and attempts.
	mu       sync.Mutex
	target   int
	attempts int

	// wake carries a pending nudge to the worker.
	wake chan struct{}

	// writeStarted is when the cpuN/online write in progress began, and nil when none
	// is. It keeps the monotonic reading, so /init setting the guest clock back cannot
	// make WritePending negative. Status reads it without taking mu.
	writeStarted atomic.Pointer[time.Time]

	// maskWarned makes an unreadable CPU mask a single warning, not one per Status call:
	// /metrics asks every 15 seconds.
	maskWarned sync.Once
}

// New runs each sysfs write as a child process tracked by children, so a live upgrade
// can hand a write still in flight over to the next envd.
func New(logger zerolog.Logger, children *reaper.Registry, opts ...Option) Manager {
	m := &manager{
		logger:     logger,
		sys:        kernelCPUSysfs{dir: sysfsCPUDir, children: children},
		children:   children,
		targetFile: defaultTargetFile,
		retryBase:  defaultRetryBase,
		retryCap:   defaultRetryCap,
		maxStalled: defaultMaxStalled,
		wake:       make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

func (m *manager) SetTarget(n int) error {
	if err := m.validateTarget(n); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.saveTarget(n); err != nil {
		return err
	}
	m.target, m.attempts = n, 0
	m.wakeUp()

	return nil
}

func (m *manager) Status() State {
	possible, err := m.sys.Possible()
	var online []int
	if err == nil {
		online, err = m.sys.Online()
	}
	if err != nil {
		m.maskWarned.Do(func() {
			m.logger.Warn().Err(err).Msg("cpus: cannot read CPU masks, reporting zero counts")
		})
	}

	var pending time.Duration
	if started := m.writeStarted.Load(); started != nil {
		pending = time.Since(*started)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return State{
		Online:       len(online),
		Possible:     len(possible),
		Target:       m.target,
		Attempts:     m.attempts,
		WritePending: pending,
	}
}

func (m *manager) Start(ctx context.Context) {
	n, err := m.loadTarget()
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		m.logger.Warn().Err(err).Msg("cpus: ignoring persisted target")
	default:
		m.mu.Lock()
		m.target = n
		m.mu.Unlock()
		m.wakeUp()
	}

	go m.run(ctx)
}

// run waits for work and converges on it. Work arrives from SetTarget and Start, both of
// which leave a target of at least 1.
func (m *manager) run(ctx context.Context) {
	// A write the previous envd left running across a live upgrade could change the
	// online count after a reconcile settled, so it lands first, reported as pending.
	started := time.Now()
	m.writeStarted.Store(&started)
	m.children.WaitAdopted()
	m.writeStarted.Store(nil)

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			m.converge(ctx)
		}
	}
}

// converge applies the current target, retrying failed runs with exponential backoff
// until the online set matches, maxStalled runs in a row change nothing, or the context
// ends. A wake-up during the backoff means a new SetTarget: it retries at once with a
// fresh budget.
func (m *manager) converge(ctx context.Context) {
	stalled := 0
	for {
		changed, err := m.reconcile(ctx)
		if ctx.Err() != nil {
			return
		}

		m.mu.Lock()
		m.attempts++
		attempts, target := m.attempts, m.target
		m.mu.Unlock()

		if err == nil {
			return
		}
		if changed > 0 {
			stalled = 0
		} else {
			stalled++
		}
		if stalled >= m.maxStalled {
			m.logger.Error().Err(err).Int("attempts", attempts).Int("target", target).
				Msg("cpus: giving up on target until a new one arrives")

			return
		}

		delay := m.backoff(stalled)
		m.logger.Warn().Err(err).Int("attempt", attempts).Dur("retry_in", delay).Msg("cpus: reconcile failed")
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			stalled = 0
		case <-time.After(delay):
		}
	}
}

func (m *manager) backoff(attempt int) time.Duration {
	d := m.retryBase
	for i := 1; i < attempt && d < m.retryCap; i++ {
		d *= 2
	}

	return min(d, m.retryCap)
}

// reconcile brings the online set to the current target one CPU at a time: lowest
// offline CPUs first when growing, highest online CPUs first when shrinking. The target
// is re-read before every CPU, so the latest one always wins. CPU0 is never touched.
// Every write is checked against the online mask afterwards: the kernel reports success
// for a no-op write, and taking that at face value would redo the same CPU forever.
// It reports how many CPUs it changed, also when it fails part way.
func (m *manager) reconcile(ctx context.Context) (changed int, err error) {
	possible, err := m.sys.Possible()
	if err != nil {
		return 0, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return changed, err
		}
		m.mu.Lock()
		target := m.target
		m.mu.Unlock()

		online, err := m.sys.Online()
		if err != nil {
			return changed, err
		}
		// up is the direction of the next write: online a CPU when short, offline one when over.
		up := len(online) < target
		var cpu int
		switch {
		case len(online) == target:
			if changed > 0 {
				m.logger.Info().Int("online", len(online)).Int("possible", len(possible)).Int("changed", changed).
					Msg("cpus: online set matches target")
			}

			return changed, nil
		case up:
			var ok bool
			if cpu, ok = lowestOffline(possible, online); !ok {
				return changed, fmt.Errorf("cpu target %d exceeds %d possible CPUs", target, len(possible))
			}
		case len(online) > target:
			if cpu = online[len(online)-1]; cpu == 0 {
				return changed, fmt.Errorf("cpu target %d leaves only CPU0, which cannot go offline", target)
			}
		}

		if err := m.write(cpu, up); err != nil {
			return changed, err
		}
		if online, err = m.sys.Online(); err != nil {
			return changed, err
		}
		if slices.Contains(online, cpu) != up {
			return changed, fmt.Errorf("cpu%d: write reported success but the online mask did not change", cpu)
		}
		changed++
	}
}

// write flips one CPU. The sysfs write is synchronous: the kernel returns once the CPU
// is up or down. Nothing can interrupt it, so a bring-up the kernel never finishes blocks
// the worker. WritePending reports this;
func (m *manager) write(cpu int, online bool) error {
	started := time.Now()
	m.writeStarted.Store(&started)
	defer m.writeStarted.Store(nil)

	if err := m.sys.SetOnline(cpu, online); err != nil {
		return fmt.Errorf("set cpu%d online=%t: %w", cpu, online, err)
	}
	m.logger.Debug().Int("cpu", cpu).Bool("online", online).Msg("cpus: changed CPU state")

	return nil
}

// wakeUp nudges the worker; a pending nudge is enough, so it never blocks.
func (m *manager) wakeUp() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// validateTarget is the one check every target passes, whether it comes from /init or
// from the persisted file.
func (m *manager) validateTarget(n int) error {
	possible, err := m.sys.Possible()
	if err != nil {
		return err
	}
	if n < 1 || n > len(possible) {
		return fmt.Errorf("cpu target %d outside 1..%d", n, len(possible))
	}

	return nil
}

// saveTarget persists the target so a restarted envd resumes it. Callers hold mu.
func (m *manager) saveTarget(n int) error {
	if err := os.MkdirAll(filepath.Dir(m.targetFile), 0o755); err != nil {
		return err
	}
	tmp := m.targetFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(n)), 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, m.targetFile)
}

// loadTarget reads the persisted target. Anything other than a few digits naming a valid
// target is an error.
func (m *manager) loadTarget() (int, error) {
	f, err := os.Open(m.targetFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	b, err := io.ReadAll(io.LimitReader(f, maxTargetBytes+1))
	if err != nil {
		return 0, err
	}
	if len(b) > maxTargetBytes {
		return 0, fmt.Errorf("%s: longer than %d bytes", m.targetFile, maxTargetBytes)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", m.targetFile, err)
	}

	return n, m.validateTarget(n)
}

func lowestOffline(possible, online []int) (int, bool) {
	for _, cpu := range possible {
		if !slices.Contains(online, cpu) {
			return cpu, true
		}
	}

	return 0, false
}

type noop struct{}

// NewNoopManager is for guests that are not Firecracker sandboxes.
func NewNoopManager() Manager { return noop{} }

func (noop) SetTarget(int) error   { return nil }
func (noop) Status() State         { return State{} }
func (noop) Start(context.Context) {}
