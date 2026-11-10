package testharness

import (
	"context"
	"fmt"
	"sync"
)

// Point identifies which worker hook to park on. Values must match the
// parent package's faultPhase iota so the hook can cast across.
type Point uint8

const (
	// BeforeRLock parks before settleRequests.RLock.
	BeforeRLock Point = iota
	// BeforeFaultPage parks after settleRequests.RLock, before UFFDIO_COPY.
	BeforeFaultPage
	// BeforePrefaultRLock parks inside Prefault(), before settleRequests.RLock.
	// Value must stay in sync with faultPhaseBeforePrefaultRLock in the parent package.
	BeforePrefaultRLock
	// BeforeSettleLock parks the serve loop after it read a REMOVE batch,
	// before settleRequests.Lock; the address is the batch's first start.
	BeforeSettleLock
	// AfterDeferral parks a worker right after it re-queued its fault on
	// EAGAIN, before it returns.
	AfterDeferral
)

// Registry is the child-side barrier store consulted by the per-fault hook.
type Registry struct {
	mu     sync.Mutex
	next   uint64
	tokens map[uint64]*slot
	byKey  map[key]uint64
}

type key struct {
	addr  uintptr
	point Point
}

type slot struct {
	addr        uintptr
	point       Point
	arrived     chan struct{}
	release     chan struct{}
	arrivedOnce sync.Once
}

func NewRegistry() *Registry {
	return &Registry{
		tokens: make(map[uint64]*slot),
		byKey:  make(map[key]uint64),
	}
}

func (r *Registry) Install(addr uintptr, point Point) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.next++
	token := r.next
	s := &slot{
		addr:    addr,
		point:   point,
		arrived: make(chan struct{}),
		release: make(chan struct{}),
	}
	r.tokens[token] = s
	r.byKey[key{addr, point}] = token

	return token
}

func (r *Registry) lookupByAddr(addr uintptr, point Point) *slot {
	r.mu.Lock()
	defer r.mu.Unlock()

	token, ok := r.byKey[key{addr, point}]
	if !ok {
		return nil
	}

	return r.tokens[token]
}

func (r *Registry) WaitArrived(ctx context.Context, token uint64) error {
	r.mu.Lock()
	s, ok := r.tokens[token]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown barrier token %d", token)
	}

	select {
	case <-s.arrived:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees the barrier; unknown token is a no-op.
func (r *Registry) Release(token uint64) {
	r.mu.Lock()
	s, ok := r.tokens[token]
	delete(r.tokens, token)
	if ok {
		// A later Install at this key overwrites byKey; only delete if
		// it still maps to this token.
		k := key{s.addr, s.point}
		if r.byKey[k] == token {
			delete(r.byKey, k)
		}
	}
	r.mu.Unlock()

	if !ok {
		return
	}

	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

// ReleaseAll releases every still-installed barrier.
func (r *Registry) ReleaseAll() {
	r.mu.Lock()
	tokens := make([]uint64, 0, len(r.tokens))
	for t := range r.tokens {
		tokens = append(tokens, t)
	}
	r.mu.Unlock()

	for _, t := range tokens {
		r.Release(t)
	}
}

// Hook returns the per-fault hook to install on *Userfaultfd; faults
// without an installed slot are no-ops.
func (r *Registry) Hook() func(addr uintptr, point Point) {
	return func(addr uintptr, point Point) {
		s := r.lookupByAddr(addr, point)
		if s == nil {
			return
		}

		s.arrivedOnce.Do(func() {
			close(s.arrived)
		})

		<-s.release
	}
}
