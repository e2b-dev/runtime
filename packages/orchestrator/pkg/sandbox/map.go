//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// MapSubscriber receives lifecycle notifications from the sandbox Map.
//
// Callbacks are invoked synchronously from the goroutine that performed the
// state change. Implementations must be non-blocking; if async work is needed,
// the subscriber is responsible for dispatching it.
type MapSubscriber interface {
	// OnInsert is triggered when a sandbox transitions to the running state.
	OnInsert(ctx context.Context, sandbox *Sandbox)
	// OnStopping is triggered when a sandbox leaves the live registry (MarkStopping).
	OnStopping(ctx context.Context, sandbox *Sandbox)
	// OnNetworkRelease is triggered when a sandbox's network slot is released.
	OnNetworkRelease(ctx context.Context, sbx *Sandbox)
}

// Map tracks live sandboxes, outstanding lifecycle cleanup, and network assignments.
//
//   - live: keyed by sandboxID, holds the current routable lifecycle per
//     sandbox from MarkRunning until MarkStopping. It serves the API/proxy
//     lookup paths (Get, Items, Count).
//   - lifecycles: keyed by sandboxID/lifecycleID, holds every lifecycle whose
//     cleanup is still outstanding, from TrackLifecycle until MarkStopped in
//     Close. During checkpoint/resume an old lifecycle can still be cleaning
//     up while a new lifecycle with the same sandboxID is already live, so a
//     sandboxID can map to multiple lifecycle entries. Shutdown uses this set
//     (WaitLifecycles, LifecycleItems) to wait for cleanup to finish, not
//     just for sandboxes to stop being routable.
//   - network: an IP-to-sandbox index managed by AssignNetwork and
//     NetworkReleased, serving GetByHostPort lookups.
type Map struct {
	live       *smap.Map[*Sandbox]
	lifecycles *smap.Map[*Sandbox]
	network    *smap.Map[*Sandbox]

	registryMu   sync.Mutex
	reservations map[string]*Reservation

	lifecycleMu      sync.Mutex
	lifecycleChanged chan struct{}

	subs     []MapSubscriber
	subsLock sync.RWMutex
}

func NewSandboxesMap() *Map {
	return &Map{
		live:             smap.New[*Sandbox](),
		lifecycles:       smap.New[*Sandbox](),
		network:          smap.New[*Sandbox](),
		reservations:     map[string]*Reservation{},
		lifecycleChanged: make(chan struct{}),
	}
}

func sandboxLifecycleKey(sandboxID, lifecycleID string) string {
	return fmt.Sprintf("%s/%s", sandboxID, lifecycleID)
}

func (m *Map) Subscribe(subscriber MapSubscriber) {
	m.subsLock.Lock()
	defer m.subsLock.Unlock()

	m.subs = append(m.subs, subscriber)
}

func (m *Map) trigger(ctx context.Context, fn func(context.Context, MapSubscriber)) {
	m.subsLock.RLock()
	defer m.subsLock.RUnlock()

	for _, subscriber := range m.subs {
		fn(ctx, subscriber)
	}
}

func (m *Map) Items() map[string]*Sandbox {
	return m.live.Items()
}

func (m *Map) Count() int {
	return m.live.Count()
}

func (m *Map) Get(sandboxID string) (*Sandbox, bool) {
	return m.live.Get(sandboxID)
}

func (m *Map) LifecycleItems() []*Sandbox {
	items := m.lifecycles.Items()
	sandboxes := make([]*Sandbox, 0, len(items))
	for _, sbx := range items {
		sandboxes = append(sandboxes, sbx)
	}

	return sandboxes
}

func (m *Map) WaitLifecycles(ctx context.Context) error {
	for {
		m.lifecycleMu.Lock()
		if m.lifecycles.Count() == 0 {
			m.lifecycleMu.Unlock()

			return nil
		}

		changed := m.lifecycleChanged
		m.lifecycleMu.Unlock()

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for sandbox lifecycle cleanup: %w", ctx.Err())
		case <-changed:
		}
	}
}

// GetByHostPort looks up a sandbox by its host IP address parsed from hostPort.
func (m *Map) GetByHostPort(hostPort string) (*Sandbox, error) {
	reqIP, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, fmt.Errorf("error parsing remote address %s: %w", hostPort, err)
	}

	sbx, ok := m.network.Get(reqIP)
	if !ok {
		return nil, errors.New("sandbox not found")
	}

	return sbx, nil
}

// AssignNetwork registers a sandbox's IP so it is findable by GetByHostPort.
func (m *Map) AssignNetwork(ctx context.Context, sbx *Sandbox) {
	ip := sbx.Slot.HostIPString()
	m.network.Insert(ip, sbx)

	sbx.log().Info(ctx, "sandbox network map entry added",
		logger.WithLifecycleID(sbx.LifecycleID),
		logger.WithSandboxIP(ip),
	)
}

func (m *Map) TrackLifecycle(ctx context.Context, sbx *Sandbox) {
	m.lifecycleMu.Lock()
	if !m.lifecycles.InsertIfAbsent(sandboxLifecycleKey(sbx.Runtime.SandboxID, sbx.LifecycleID), sbx) {
		m.lifecycleMu.Unlock()

		return
	}
	m.notifyLifecycleChangeLocked()
	m.lifecycleMu.Unlock()

	sbx.log().Info(ctx, "sandbox lifecycle tracked",
		logger.WithLifecycleID(sbx.LifecycleID),
		logger.WithSandboxIP(sbx.Slot.HostIPString()),
	)
}

// ErrSandboxAlreadyRunning reports a live or reserved sandbox ID.
var ErrSandboxAlreadyRunning = errors.New("sandbox is already running on this node")

var ErrSandboxOperationInProgress = errors.New("sandbox operation already in progress")

// Reservation prevents concurrent starts; a failed start retains it through its own cleanup.
type Reservation struct {
	m         *Map
	sandboxID string
}

// Reserve takes the sandbox ID for a create that is about to start a VM.
// Refused with ErrSandboxAlreadyRunning while the ID is live or reserved.
func (m *Map) Reserve(sandboxID string) (*Reservation, error) {
	m.registryMu.Lock()
	defer m.registryMu.Unlock()

	if live, ok := m.live.Get(sandboxID); ok {
		return nil, fmt.Errorf("%w: lifecycle %s (execution %s) is live", ErrSandboxAlreadyRunning, live.LifecycleID, live.Runtime.ExecutionID)
	}
	if _, ok := m.reservations[sandboxID]; ok {
		return nil, fmt.Errorf("%w: another create is in flight", ErrSandboxAlreadyRunning)
	}
	r := &Reservation{m: m, sandboxID: sandboxID}
	m.reservations[sandboxID] = r

	return r, nil
}

// Release ends operation ownership; the live entry and physical lifecycles remain independent.
func (r *Reservation) Release() {
	r.m.registryMu.Lock()
	defer r.m.registryMu.Unlock()

	if r.m.reservations[r.sandboxID] == r {
		delete(r.m.reservations, r.sandboxID)
	}
}

func (r *Reservation) MarkRunning(ctx context.Context, sbx *Sandbox) error {
	return r.m.markRunning(ctx, sbx, r)
}

// MarkRunning registers an unreserved ID. Reserved IDs require Reservation.MarkRunning.
func (m *Map) MarkRunning(ctx context.Context, sbx *Sandbox) error {
	return m.markRunning(ctx, sbx, nil)
}

func (m *Map) markRunning(ctx context.Context, sbx *Sandbox, reservation *Reservation) error {
	m.registryMu.Lock()
	if reservation != nil && reservation.sandboxID != sbx.Runtime.SandboxID {
		m.registryMu.Unlock()

		return errors.New("sandbox reservation no longer held")
	}
	if m.reservations[sbx.Runtime.SandboxID] != reservation {
		m.registryMu.Unlock()

		return fmt.Errorf("%w: registration does not own the reservation", ErrSandboxAlreadyRunning)
	}
	if live, ok := m.live.Get(sbx.Runtime.SandboxID); ok {
		m.registryMu.Unlock()
		if live.LifecycleID == sbx.LifecycleID {
			return nil
		}

		return fmt.Errorf("%w: lifecycle %s (execution %s) is live", ErrSandboxAlreadyRunning, live.LifecycleID, live.Runtime.ExecutionID)
	}
	if sbx.cleanup != nil && sbx.cleanup.hasRun.Load() {
		m.registryMu.Unlock()

		return errors.New("sandbox cleanup has already started")
	}
	m.TrackLifecycle(ctx, sbx)
	m.live.Insert(sbx.Runtime.SandboxID, sbx)
	m.registryMu.Unlock()

	m.trigger(ctx, func(ctx context.Context, s MapSubscriber) {
		s.OnInsert(ctx, sbx)
	})

	sbx.log().Info(ctx, "adding sandbox to map",
		logger.WithLifecycleID(sbx.LifecycleID),
		logger.WithSandboxIP(sbx.Slot.HostIPString()),
		logger.WithEnvdVersion(sbx.Config.Envd.Version),
		logger.WithKernelVersion(sbx.Config.FirecrackerConfig.KernelVersion),
		logger.WithFirecrackerVersion(sbx.Config.FirecrackerConfig.FirecrackerVersion),
	)

	return nil
}

// MarkStopping removes the sandbox from live queries (Get, Items, Count) and notifies OnStopping subscribers.
// Returns true if the sandbox was successfully removed.
func (m *Map) MarkStopping(ctx context.Context, sandboxID, lifecycleID string) bool {
	_, err := m.markStopping(ctx, sandboxID, lifecycleID, false)

	return err == nil
}

// reclaimLiveEntryOnCleanup registers the teardown callback that drops this
// lifecycle's entry from the live map. It is the only teardown caller of
// MarkStopping; Close must not reclaim the entry itself.
//
// Its position in the cleanup chain is load-bearing: the chain runs backward, so
// registering after the network slot's release has been registered runs this
// callback before it, and the entry stays in the live map through the VM stop and
// the final host-stats sample. That is what decides when the sandbox leaves Get,
// Items and Count, and so when OnStopping reaches subscribers. It does not decide
// host-IP findability: GetByHostPort reads the network index, which the slot
// return clears asynchronously and well after this chain ends. Do not move this
// call.
func (m *Map) reclaimLiveEntryOnCleanup(ctx context.Context, cleanup *Cleanup, sandboxID, lifecycleID string) {
	cleanup.Add(ctx, func(ctx context.Context) error {
		// false is the normal outcome: an operation-initiated stop (delete, pause,
		// checkpoint) reclaims the entry before the chain runs, and a lifecycle that
		// never became live has no entry to reclaim.
		m.MarkStopping(ctx, sandboxID, lifecycleID)

		return nil
	})
}

// MarkStoppingReserved exchanges the matching live entry for a checkpoint hold before notifying subscribers.
func (m *Map) MarkStoppingReserved(ctx context.Context, sandboxID, lifecycleID string) (*Reservation, error) {
	return m.markStopping(ctx, sandboxID, lifecycleID, true)
}

func (m *Map) markStopping(ctx context.Context, sandboxID, lifecycleID string, reserve bool) (*Reservation, error) {
	var (
		stopped     *Sandbox
		reservation *Reservation
		stopErr     error
	)

	m.registryMu.Lock()
	m.live.RemoveCb(sandboxID, func(_ string, sbx *Sandbox, exists bool) bool {
		if !exists {
			return false
		}

		if sbx.LifecycleID != lifecycleID {
			return false
		}
		if reserve && m.reservations[sandboxID] != nil {
			stopErr = ErrSandboxOperationInProgress

			return false
		}

		sbx.log().Info(ctx, "marking sandbox as stopping",
			logger.WithLifecycleID(lifecycleID),
			logger.WithSandboxIP(sbx.Slot.HostIPString()),
		)

		stopped = sbx

		return true
	})
	if stopped != nil && reserve {
		reservation = &Reservation{m: m, sandboxID: sandboxID}
		m.reservations[sandboxID] = reservation
	}
	m.registryMu.Unlock()

	if stopped == nil {
		if stopErr != nil {
			return nil, stopErr
		}

		return nil, fmt.Errorf("sandbox '%s' lifecycle '%s' is no longer live", sandboxID, lifecycleID)
	}

	m.trigger(ctx, func(ctx context.Context, s MapSubscriber) {
		s.OnStopping(ctx, stopped)
	})

	return reservation, nil
}

func (m *Map) MarkStopped(ctx context.Context, sbx *Sandbox) {
	m.lifecycleMu.Lock()
	m.lifecycles.Remove(sandboxLifecycleKey(sbx.Runtime.SandboxID, sbx.LifecycleID))
	m.notifyLifecycleChangeLocked()
	m.lifecycleMu.Unlock()

	sbx.log().Info(ctx, "sandbox lifecycle stopped",
		logger.WithLifecycleID(sbx.LifecycleID),
		logger.WithSandboxIP(sbx.Slot.HostIPString()),
	)
}

func (m *Map) notifyLifecycleChangeLocked() {
	close(m.lifecycleChanged)
	m.lifecycleChanged = make(chan struct{})
}

// NetworkReleased unregisters a sandbox's IP and notifies OnNetworkRelease
// subscribers after a successful removal.
func (m *Map) NetworkReleased(ctx context.Context, ip string) {
	var sbx *Sandbox
	removed := m.network.RemoveCb(ip, func(_ string, v *Sandbox, exists bool) bool {
		if !exists {
			return false
		}

		sbx = v

		return exists
	})

	if !removed {
		return
	}

	sbx.log().Info(ctx, "sandbox network map entry removed",
		logger.WithLifecycleID(sbx.LifecycleID),
		logger.WithSandboxIP(ip),
	)

	m.trigger(ctx, func(ctx context.Context, s MapSubscriber) {
		s.OnNetworkRelease(ctx, sbx)
	})
}
