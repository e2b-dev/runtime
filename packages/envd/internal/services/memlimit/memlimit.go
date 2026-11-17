// Package memlimit keeps the memory limit on the user and PTY cgroups in step with the
// guest's memory size. envd first computes that limit during the template build, so
// every snapshot carries a limit sized for the template's boot memory; when virtio-mem
// later plugs memory into the guest, MemTotal grows and the limit has to follow it, or
// the customer's processes cannot use the added memory.
//
// The limit is always recomputed from the current MemTotal, never adjusted by a delta,
// so a recompute is safe to run from any trigger, any number of times, and is correct
// after a missed event. Three triggers feed it: the kernel's memory block online and
// offline events, every /init (a sandbox paused between a plug and envd's reaction
// resumes with the memory already online, so no event will ever fire for it), and a
// slow safety check every 30 seconds for events the socket dropped.
//
// In virtio-mem's sub-block mode, plugging part of a memory block that is already online
// changes MemTotal without any event. The host covers that case: after it resizes a
// sandbox and Firecracker reports the plugged size reached the requested one, the
// orchestrator calls /init once more, which requests a recompute, and reads the result.
//
// The worker owns everything it reports: each recompute, whatever triggered it, refreshes
// a snapshot of the applied limit, the MemTotal it was computed from, the MemTotal last
// observed, the last error and the guest's memory layout. Status only copies that
// snapshot, so /init and /metrics read no files.
//
// envd writes the limit only when its computed value changes, and does not put back a
// limit changed from inside the guest; envd has always written it once at startup and
// left it alone afterwards. What it reports is the limit it last applied.
package memlimit

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/host"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

const (
	// defaultPeriod is the safety check for dropped events; see the package comment for
	// why it does not need to be fast.
	defaultPeriod = 30 * time.Second
	// defaultEventWindow is how long the first memory event of a burst waits for the
	// rest: a plug onlines one 128 MiB block at a time, one event each.
	defaultEventWindow = 25 * time.Millisecond
	// eventRetry is how long the event watcher waits before reopening its socket after
	// a failure; the periodic check covers the gap.
	eventRetry = 10 * time.Second
)

// Limit is the memory limit for a guest with memTotal bytes online, hotplugAdded of them
// in memory blocks virtio-mem added. It holds back an eighth of memTotal, at most 128 MiB,
// for the kernel and envd, plus the memory map of the added blocks: 64 bytes of struct page
// per 4 KiB page, so 1/64.
//
// Boot memory needs nothing extra: the kernel allocates its memory map during boot, before
// MemTotal is counted, so MemTotal already excludes it. A block virtio-mem adds is counted
// into MemTotal in full while its memory map is allocated afterwards from memory that was
// already counted, outside the cgroups, so an 8 GiB plug would otherwise use up the 128 MiB
// on its own. The map is allocated per added 128 MiB block, sub-blocks not yet plugged
// included, which is why hotplugAdded is in whole blocks (see HotplugAdded).
func Limit(memTotal, hotplugAdded uint64) uint64 {
	reserve := min(memTotal/8, uint64(128)<<20) + (min(hotplugAdded, memTotal)+63)/64

	return memTotal - min(reserve, memTotal)
}

// hotplugAdded is the memory virtio-mem added to the guest, from /proc/iomem. The kernel
// keeps that record itself, so it survives envd restarts, crashes and live upgrades, and its
// 128 MiB block granularity is the one the memory map is allocated at. If /proc/iomem cannot
// be read or parsed, it falls back to the larger of the memory grown since envd started
// and the movable zone, which holds the plugged memory under the auto-movable policy.
func hotplugAdded(read func() (uint64, error), memTotal, baseline uint64, layout host.MemoryLayout) (added uint64, fallback error) {
	added, err := read()
	if err == nil {
		return added, nil
	}

	return max(memTotal-min(memTotal, baseline), layout.Movable), err
}

// StartLimit is the limit for the cgroups envd creates at start-up, with memTotal bytes
// online: a restarted or live-upgraded envd in a grown guest holds back the memory map of
// what was plugged before it.
func StartLimit(memTotal uint64) uint64 {
	added, _ := hotplugAdded(host.HotplugAdded, memTotal, memTotal, host.ReadMemoryLayout())

	return Limit(memTotal, added)
}

// State is what the X-Envd-Memory header on /init and /metrics report.
type State struct {
	// MemTotal is the MemTotal the applied limit was computed from, not the guest's
	// current one, so the host can compare it with the memory it plugged.
	MemTotal uint64
	// Limit is the limit envd last applied to every managed cgroup, 0 when envd has
	// applied none.
	Limit uint64
	// Observed is the MemTotal the worker read last, 0 before its first read.
	Observed uint64
	// Pending is true when the applied limit is not computed from the guest's memory as
	// last observed (Observed differs from MemTotal), or the last recompute failed and
	// none has succeeded since. It is as current as the worker's last recompute, so it
	// can lag a change by up to one event or safety check; the host's definitive check is
	// comparing MemTotal with the memory it plugged. A recompute that is merely requested
	// while the applied limit already matches is not pending.
	Pending bool
	// Err is the last recompute's failure, empty when it succeeded.
	Err string
	// Layout is the guest's memory layout as of the last recompute.
	Layout host.MemoryLayout
}

type Manager interface {
	// Request asks for a recompute and returns at once.
	Request(reason string)
	Status() State
	// Start keeps the limit current until ctx ends.
	Start(ctx context.Context)
}

type manager struct {
	logger    zerolog.Logger
	limiter   cgroups.MemoryLimiter
	procTypes []cgroups.ProcessType

	readMemTotal func() (uint64, error)
	readLayout   func() host.MemoryLayout
	readAdded    func() (uint64, error)
	watchEvents  func(ctx context.Context, notify func()) error
	period       time.Duration
	eventWindow  time.Duration
	// baseline is the MemTotal when envd started, set by WithBaseline or by the first
	// MemTotal read. Only the fallback for an unreadable /proc/iomem uses it: memory grown
	// since then was plugged.
	baseline uint64
	// addedErr is the last /proc/iomem failure, logged once per distinct cause.
	addedErr string
	// synced, when set, is called after every recompute with the reasons it covered.
	synced func(reasons []string)

	wake chan struct{}

	mu          sync.Mutex
	reasons     map[string]struct{}
	eventTimer  *time.Timer
	state       State
	initialized bool
}

type Option func(*manager)

func WithMemTotalReader(read func() (uint64, error)) Option {
	return func(m *manager) { m.readMemTotal = read }
}

// WithHotplugAddedReader replaces the /proc/iomem read of the memory virtio-mem added.
func WithHotplugAddedReader(read func() (uint64, error)) Option {
	return func(m *manager) { m.readAdded = read }
}

func WithLayoutReader(read func() host.MemoryLayout) Option {
	return func(m *manager) { m.readLayout = read }
}

func WithEventWatcher(watch func(ctx context.Context, notify func()) error) Option {
	return func(m *manager) { m.watchEvents = watch }
}

func WithPeriod(period time.Duration) Option {
	return func(m *manager) { m.period = period }
}

func WithEventWindow(window time.Duration) Option {
	return func(m *manager) { m.eventWindow = window }
}

// WithBaseline sets the MemTotal envd started with, which the /proc/iomem fallback
// counts growth from.
func WithBaseline(memTotal uint64) Option {
	return func(m *manager) { m.baseline = memTotal }
}

// WithApplied records the limit already written for memTotal at start-up, normally the
// one written when the cgroups were created, so the manager neither reports 0 nor
// rewrites it.
func WithApplied(memTotal, limit uint64) Option {
	return func(m *manager) {
		m.state.MemTotal = memTotal
		m.state.Observed = memTotal
		m.state.Limit = limit
		m.initialized = true
	}
}

// New returns a manager for the user and PTY cgroups, the two whose limit is derived
// from the guest's memory size.
func New(logger zerolog.Logger, limiter cgroups.MemoryLimiter, opts ...Option) Manager {
	m := &manager{
		logger:       logger.With().Str("component", "memlimit").Logger(),
		limiter:      limiter,
		procTypes:    []cgroups.ProcessType{cgroups.ProcessTypeUser, cgroups.ProcessTypePTY},
		readMemTotal: host.MemTotal,
		readLayout:   host.ReadMemoryLayout,
		readAdded:    host.HotplugAdded,
		watchEvents:  watchMemoryEvents,
		period:       defaultPeriod,
		eventWindow:  defaultEventWindow,
		wake:         make(chan struct{}, 1),
		reasons:      map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

func (m *manager) Start(ctx context.Context) {
	go m.run(ctx)
	go m.watch(ctx)
	m.Request("start")
}

func (m *manager) Request(reason string) {
	m.record(reason)
	m.signal()
}

// requestAfterWindow records a memory event now, so its reason joins the next recompute,
// and wakes the worker once the event window closes. Status needs no record: it reports
// pending from MemTotal itself.
func (m *manager) requestAfterWindow(reason string) {
	m.record(reason)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eventTimer != nil {
		return
	}
	m.eventTimer = time.AfterFunc(m.eventWindow, func() {
		m.mu.Lock()
		m.eventTimer = nil
		m.mu.Unlock()
		m.signal()
	})
}

func (m *manager) record(reason string) {
	m.mu.Lock()
	m.reasons[reason] = struct{}{}
	m.mu.Unlock()
}

func (m *manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// Status copies the worker's last snapshot; it reads nothing.
func (m *manager) Status() State {
	m.mu.Lock()
	s := m.state
	m.mu.Unlock()

	s.Pending = s.Err != "" || s.Observed != s.MemTotal

	return s
}

func (m *manager) run(ctx context.Context) {
	ticker := time.NewTicker(m.period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-m.wake:
		}
		m.sync()
	}
}

func (m *manager) sync() {
	m.mu.Lock()
	reasons := make([]string, 0, len(m.reasons))
	for r := range m.reasons {
		reasons = append(reasons, r)
	}
	clear(m.reasons)
	prev := m.state
	initialized := m.initialized
	m.mu.Unlock()
	slices.Sort(reasons)
	if len(reasons) == 0 {
		reasons = append(reasons, "periodic")
	}
	next := prev
	next.Err = ""
	next.Layout = m.readLayout()

	memTotal, err := m.readMemTotal()
	if err != nil {
		next.Err = err.Error()
		if next.Err != prev.Err {
			m.logger.Warn().Err(err).Strs("reasons", reasons).Msg("could not read MemTotal")
		}
		m.finish(next, reasons, initialized)

		return
	}
	next.Observed = memTotal

	if memTotal == 0 {
		// A limit computed from it would be 0 and OOM-kill every process in the cgroups.
		err = errZeroMemTotal
		next.Err = err.Error()
		if next.Err != prev.Err {
			m.logger.Warn().Err(err).Strs("reasons", reasons).Msg("could not read MemTotal")
		}
		m.finish(next, reasons, initialized)

		return
	}

	if m.baseline == 0 {
		m.baseline = memTotal
	}
	added, fallback := hotplugAdded(m.readAdded, memTotal, m.baseline, next.Layout)
	if cause := errString(fallback); cause != m.addedErr {
		m.addedErr = cause
		if fallback != nil {
			m.logger.Warn().Err(fallback).Uint64("hotplug_added", added).Msg("could not read the hotplugged memory from /proc/iomem; estimating it from MemTotal growth and the movable zone")
		}
	}
	limit := Limit(memTotal, added)
	if initialized && limit == prev.Limit && prev.Err == "" {
		next.MemTotal = memTotal
		m.finish(next, reasons, true)

		return
	}

	var errs []error
	for _, procType := range m.procTypes {
		if err := m.limiter.SetMemoryLimit(procType, limit); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		// Limit stays at the last fully applied value so the next recompute retries.
		next.Err = err.Error()
		if next.Err != prev.Err {
			m.logger.Warn().Err(err).Strs("reasons", reasons).Uint64("mem_total", memTotal).Uint64("limit", limit).Msg("could not apply memory limit")
		}
		m.finish(next, reasons, initialized)

		return
	}

	next.MemTotal = memTotal
	next.Limit = limit
	m.logger.Info().Strs("reasons", reasons).Uint64("mem_total", memTotal).Uint64("previous_limit", prev.Limit).Uint64("limit", limit).Msg("memory limit applied")
	m.finish(next, reasons, true)
}

func (m *manager) finish(s State, reasons []string, initialized bool) {
	m.mu.Lock()
	m.state = s
	m.initialized = initialized
	m.mu.Unlock()

	if m.synced != nil {
		m.synced(reasons)
	}
}

func (m *manager) watch(ctx context.Context) {
	var lastErr string
	for {
		err := m.watchEvents(ctx, func() { m.requestAfterWindow("memory-event") })
		if ctx.Err() != nil || errors.Is(err, errUnsupported) {
			return
		}
		// Once per distinct failure: a socket that cannot be opened fails the same way
		// on every retry for the life of the sandbox.
		if err.Error() != lastErr {
			lastErr = err.Error()
			m.logger.Warn().Err(err).Msg("memory event watcher stopped; relying on periodic checks until it restarts")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(eventRetry):
		}
	}
}

var errUnsupported = errors.New("memory events unsupported on this platform")

var errZeroMemTotal = errors.New("MemTotal read as 0")

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

type noop struct{}

// NewNoopManager is the manager for an envd that does not manage cgroup limits.
func NewNoopManager() Manager { return noop{} }

func (noop) Request(string)        {}
func (noop) Status() State         { return State{} }
func (noop) Start(context.Context) {}
