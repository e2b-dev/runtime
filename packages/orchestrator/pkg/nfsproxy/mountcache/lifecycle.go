//go:build linux

package mountcache

import (
	"context"
	"sync"
)

type lifecycleState struct {
	active  int
	closed  bool
	closedC chan struct{}
	changed *sync.Cond
}

// LifecycleGate owns MOUNT admission for one sandbox lifecycle from chroot
// allocation through handle-cache shard registration.
type LifecycleGate struct {
	mu     sync.Mutex
	owners map[Owner]*lifecycleState
}

// MountAdmission remains active until the outer handle-cache registration has
// either committed or failed.
type MountAdmission struct {
	gate  *LifecycleGate
	owner Owner
	state *lifecycleState
	done  sync.Once
}

// NewLifecycleGate creates an empty lifecycle admission gate.
func NewLifecycleGate() *LifecycleGate {
	return &LifecycleGate{mu: sync.Mutex{}, owners: make(map[Owner]*lifecycleState)}
}

// Begin admits one MOUNT while owner remains open. Closed idle states are
// deleted after release, so callers that resolved owner through a mutable
// network index must immediately revalidate that mapping after Begin; a
// delayed pre-release lookup can otherwise recreate only a provisional state.
func (g *LifecycleGate) Begin(owner Owner) (*MountAdmission, bool) {
	if !owner.valid() {
		return nil, false
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	state := g.owners[owner]
	if state == nil {
		state = &lifecycleState{active: 0, closed: false, closedC: make(chan struct{}), changed: nil}
		state.changed = sync.NewCond(&g.mu)
		g.owners[owner] = state
	}
	if state.closed {
		return nil, false
	}

	state.active++

	return &MountAdmission{gate: g, owner: owner, state: state, done: sync.Once{}}, true
}

// Register performs one resource registration only while the lifecycle remains
// open. Release closes the same state under this lock before joining MOUNTs.
func (a *MountAdmission) Register(register func()) bool {
	if a == nil || register == nil {
		return false
	}

	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()

	if a.gate.owners[a.owner] != a.state || a.state.closed {
		return false
	}

	register()

	return true
}

// Close rejects this owner after a post-admission network revalidation fails.
func (a *MountAdmission) Close() {
	if a == nil {
		return
	}

	a.gate.mu.Lock()
	if a.gate.owners[a.owner] == a.state {
		a.gate.closeLocked(a.state)
	}
	a.gate.mu.Unlock()
}

// Done releases this MOUNT's admission exactly once.
func (a *MountAdmission) Done() {
	if a == nil {
		return
	}

	a.done.Do(func() {
		a.gate.mu.Lock()
		if a.state.active > 0 {
			a.state.active--
		}
		a.state.changed.Broadcast()
		a.gate.deleteClosedIdleLocked(a.owner, a.state)
		a.gate.mu.Unlock()
	})
}

// Close rejects further registration without waiting for admitted MOUNTs.
// Their original admission tokens continue owning any late allocation.
func (g *LifecycleGate) Close(owner Owner) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if state := g.owners[owner]; state != nil {
		g.closeLocked(state)
	}
}

// CloseAndWaitContext closes owner admission, joining admitted MOUNTs until ctx
// ends. A failed join keeps the owner closed and its live admissions owned.
func (g *LifecycleGate) CloseAndWaitContext(ctx context.Context, owner Owner) error {
	if !owner.valid() {
		return nil
	}

	g.mu.Lock()
	state := g.owners[owner]
	if state == nil {
		g.mu.Unlock()

		return nil
	}

	g.closeLocked(state)
	stop := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		state.changed.Broadcast()
		g.mu.Unlock()
	})
	defer stop()
	for state.active > 0 {
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()

			return err
		}
		state.changed.Wait()
	}
	g.deleteClosedIdleLocked(owner, state)
	g.mu.Unlock()

	return nil
}

// CloseAndWait retains the unconditional join for callers that own late cleanup.
func (g *LifecycleGate) CloseAndWait(owner Owner) {
	_ = g.CloseAndWaitContext(context.Background(), owner)
}

// OwnerClosed returns the close notification for an existing owner state.
func (g *LifecycleGate) OwnerClosed(owner Owner) (<-chan struct{}, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	state := g.owners[owner]
	if state == nil {
		return nil, false
	}

	return state.closedC, true
}

func (g *LifecycleGate) closeLocked(state *lifecycleState) {
	if state.closed {
		return
	}

	state.closed = true
	close(state.closedC)
	state.changed.Broadcast()
}

func (g *LifecycleGate) deleteClosedIdleLocked(owner Owner, state *lifecycleState) {
	if state.closed && state.active == 0 && g.owners[owner] == state {
		delete(g.owners, owner)
	}
}
