//go:build linux

package block

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

type State uint8

const (
	// NotPresent: fall back to the previous layer.
	NotPresent State = iota
	// Dirty: this layer holds materialized data.
	Dirty
	// Zero: known-zero and installed (an in-place zero page); no need to
	// consult the previous layer.
	Zero
	// Removed: known-zero but NOT installed (the mapping was zapped, e.g. by
	// a UFFD REMOVE / madvise). Distinct from Zero so a write-protect fault
	// handler can tell "installed zero page the guest is writing" (must
	// become Dirty) from "page a concurrent REMOVE just zapped" (absent;
	// must not become Dirty, or a later reinstall fault would short-circuit
	// without installing). Exported alongside Zero as the empty set.
	Removed
	// Clean: installed write-protect-armed with content identical to the
	// source (a UFFDIO_COPY_MODE_WP install that has not been written).
	// Present like Dirty, but excluded from Export entirely: a snapshot
	// diff can inherit the page from the parent layer. A write-protect
	// fault promotes it to Dirty.
	Clean
	// Installing: absent, and a demand-fault worker owns its install. Set by
	// BeginInstall in the serve loop before the worker is dispatched, so
	// every further MISSING fault for the page is dropped instead of reading
	// the source again (the owner's UFFDIO_COPY wakes all of them). Leaves
	// via MarkInstalled / SetRange(Dirty) on install, AbortInstall when the
	// install did not happen, SetRange(Removed) when a REMOVE batch lands
	// first, and MarkWritten when a WP fault beats the owner's bookkeeping:
	// the owner's UFFDIO_COPY makes the page present and wakes the blocked
	// threads before the owner records the install, so a woken writer can
	// take a real WP fault on the now-present page while the tracker still
	// says Installing. A prefault may still install an Installing page (it
	// has the bytes in hand); only demand faults are deduped. Absent for
	// Export like NotPresent: nothing has been written to it.
	Installing

	// numStates is the sentinel for consumers sizing tables by State (e.g.
	// metric label arrays). Keep it immediately after the last state so
	// adding a state cannot leave a consumer's table short.
	numStates
)

// NumStates is the number of tracked states; see numStates.
const NumStates = int(numStates)

// String returns the metric/log label for the state.
func (s State) String() string {
	switch s {
	case NotPresent:
		return "not_present"
	case Dirty:
		return "dirty"
	case Zero:
		return "zero"
	case Removed:
		return "removed"
	case Clean:
		return "clean"
	case Installing:
		return "installing"
	default:
		return "unknown"
	}
}

type Tracker struct {
	mu                                      sync.RWMutex
	dirty, zero, removed, clean, installing *roaring.Bitmap
}

func NewTracker() *Tracker {
	return &Tracker{
		dirty:      roaring.New(),
		zero:       roaring.New(),
		removed:    roaring.New(),
		clean:      roaring.New(),
		installing: roaring.New(),
	}
}

// BeginInstall claims idx for a demand-fault install. A NotPresent page
// becomes Installing and claimed is true; any other state is returned
// unchanged with claimed false, so concurrent faults for one page collapse
// onto one installer.
func (t *Tracker) BeginInstall(idx uint32) (prev State, claimed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	prev = t.stateOf(idx)
	if prev != NotPresent {
		return prev, false
	}
	t.installing.Add(idx)

	return prev, true
}

// AbortInstall releases a BeginInstall claim whose install did not happen.
// An Installing page becomes NotPresent; a page in any other state is left
// alone. Releasing the claim raises nothing: the guest threads blocked on
// the page stay blocked, so the caller must still resolve the fault it
// holds. The serve loop re-queues it on EAGAIN and tears the handler down
// on a failed read or ESRCH; it never waits for the kernel to fault again.
func (t *Tracker) AbortInstall(idx uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.installing.Remove(idx)
}

// SetRange sets state for indices in [start, end). The index math.MaxUint32
// is unaddressable: end is the half-open upper bound and capped at MaxUint32.
//
// Caller contract, not checked here: NotPresent clears Installing claims
// like any other state, so callers must not apply it to a range that may be
// mid-install; a dropped claim lets a stale owner release a newer one, and
// the page then gets two installers. Only AbortInstall returns a claimed
// page to NotPresent. No production caller passes NotPresent today.
func (t *Tracker) SetRange(start, end uint32, state State) {
	if end <= start {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	s, e := uint64(start), uint64(end)
	target := t.bitmapFor(state)
	for _, bm := range []*roaring.Bitmap{t.dirty, t.zero, t.removed, t.clean, t.installing} {
		if bm == target {
			bm.AddRange(s, e)
		} else {
			bm.RemoveRange(s, e)
		}
	}
}

// MarkInstalled records a read-side install (state Zero or Clean) for
// [start, end), but only for indices currently NotPresent, Removed or
// Installing.
// Installed states are never touched: an installer that was suspended
// between its UFFDIO_COPY and this call must not downgrade a page a
// concurrent write-protect fault has already promoted to Dirty.
// Write-side installs use SetRange(…, Dirty) directly — Dirty is the top
// of the lattice and cannot downgrade anything.
func (t *Tracker) MarkInstalled(start, end uint32, state State) {
	if end <= start || (state != Zero && state != Clean) {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Single-page fast path: every fault-path caller passes one page, and
	// the mask algebra below costs several bitmap allocations per call
	// under the tracker mutex.
	if end-start == 1 {
		switch t.stateOf(start) {
		case Removed:
			t.removed.Remove(start)
			t.bitmapFor(state).Add(start)
		case Installing:
			t.installing.Remove(start)
			t.bitmapFor(state).Add(start)
		case NotPresent:
			t.bitmapFor(state).Add(start)
		case Dirty, Zero, Clean, numStates:
			// Installed states are never downgraded.
		}

		return
	}

	s, e := uint64(start), uint64(end)
	mask := roaring.New()
	mask.AddRange(s, e)
	mask.AndNot(t.dirty)
	mask.AndNot(t.zero)
	mask.AndNot(t.clean)
	// mask now holds the Removed, Installing and NotPresent indices of the range.
	t.removed.AndNot(mask)
	t.installing.AndNot(mask)
	t.bitmapFor(state).Or(mask)
}

// MarkWritten promotes the page at idx to Dirty on a write-protect fault
// resolution: the guest is completing a write to a present, WP-armed page.
//
// Removed pages are skipped — a WP fault can be stale (read from the event
// queue before a REMOVE batch zapped the page), and a Dirty entry for an
// absent page would let its reinstall MISSING fault short-circuit without
// installing or waking. A NotPresent or Installing page IS promoted: the
// kernel delivers WP faults only for present pages, so either state here
// means an installer's UFFDIO_COPY has landed and woken the writer while
// the installer has not yet reached MarkInstalled — which never downgrades,
// so the promotion sticks.
//
// Returns the state the page was in before the call.
func (t *Tracker) MarkWritten(idx uint32) (prev State) {
	t.mu.Lock()
	defer t.mu.Unlock()

	prev = t.stateOf(idx)
	switch prev {
	case Dirty, Removed:
		return prev
	case Zero:
		t.zero.Remove(idx)
	case Clean:
		t.clean.Remove(idx)
	case Installing:
		t.installing.Remove(idx)
	case NotPresent, numStates:
		// NotPresent: promotion sticks (see doc); numStates is unreachable.
	}
	t.dirty.Add(idx)

	return prev
}

// MarkWrittenPresent promotes idx to Dirty like MarkWritten, including from
// Removed: the caller has verified the page is PRESENT in the kernel, so a
// Removed reading is known-stale for it — a REMOVE batch that waited on the
// serve lock recorded Removed after a racing reinstall had already
// re-populated the page. Only the write-protect resolve's presence probe may
// call this; every other writer must go through MarkWritten, whose
// Removed-skip is what keeps genuinely absent pages out of the dirty set.
//
// Returns the state the page was in before the call.
func (t *Tracker) MarkWrittenPresent(idx uint32) (prev State) {
	t.mu.Lock()
	defer t.mu.Unlock()

	prev = t.stateOf(idx)
	if prev == Dirty {
		return prev
	}
	t.zero.Remove(idx)
	t.removed.Remove(idx)
	t.clean.Remove(idx)
	t.installing.Remove(idx)
	t.dirty.Add(idx)

	return prev
}

// MarkExported demotes the page at idx from Dirty to Clean after its content
// has been captured into an export: the page is still installed and
// write-protect-armed, and its content now matches the layer just exported,
// so only a subsequent write (which promotes via MarkWritten) should put it
// back in the next interval's dirty set. Any state other than Dirty is left
// untouched — in particular a concurrent REMOVE's Removed entry must not be
// resurrected to an installed state.
func (t *Tracker) MarkExported(idx uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.dirty.Contains(idx) {
		return
	}
	t.dirty.Remove(idx)
	t.clean.Add(idx)
}

// bitmapFor returns the bitmap backing the given state, or nil for NotPresent
// (and any unknown value, which then behaves like clearing the range).
func (t *Tracker) bitmapFor(state State) *roaring.Bitmap {
	switch state {
	case Dirty:
		return t.dirty
	case Zero:
		return t.zero
	case Removed:
		return t.removed
	case Clean:
		return t.clean
	case Installing:
		return t.installing
	case NotPresent:
		return nil
	default:
		return nil
	}
}

func (t *Tracker) Get(idx uint32) State {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.stateOf(idx)
}

// stateOf classifies idx. Caller holds t.mu (either mode). The single
// authoritative classifier: Get and MarkWritten both use it, so a new state
// cannot be classified differently by different entry points.
func (t *Tracker) stateOf(idx uint32) State {
	switch {
	case t.dirty.Contains(idx):
		return Dirty
	case t.zero.Contains(idx):
		return Zero
	case t.removed.Contains(idx):
		return Removed
	case t.clean.Contains(idx):
		return Clean
	case t.installing.Contains(idx):
		return Installing
	default:
		return NotPresent
	}
}

// HasRange reports whether every index in [start, end) is in the given state.
// Passing NotPresent always returns false.
// Empty ranges (end == start) are vacuously true; inverted ranges return false.
func (t *Tracker) HasRange(start, end uint32, state State) bool {
	if end <= start {
		return end == start
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	bm := t.bitmapFor(state)
	if bm == nil {
		return false
	}

	return bm.CardinalityInRange(uint64(start), uint64(end)) == uint64(end-start)
}

// Present reports whether every index in [start, end) is installed or
// removed, i.e. readable from the mapping. Installing pages have no bytes
// yet and do not count.
func (t *Tracker) Present(start, end uint32) bool {
	if end <= start {
		return end == start
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	s, e := uint64(start), uint64(end)
	// The state bitmaps are disjoint by invariant, so the cardinalities sum.
	return t.dirty.CardinalityInRange(s, e)+
		t.zero.CardinalityInRange(s, e)+
		t.removed.CardinalityInRange(s, e)+
		t.clean.CardinalityInRange(s, e) == e-s
}

// Export returns clones of the dirty and empty bitmaps. Zero and Removed
// pages both read back as zeros, so they merge into the empty set. Clean
// pages appear in neither: their content matches the source, so a snapshot
// diff inherits them from the parent layer. Installing pages appear in
// neither as well: nothing can have been written to a page that is not yet
// installed.
func (t *Tracker) Export() (dirty, empty *roaring.Bitmap) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.dirty.Clone(), roaring.Or(t.zero, t.removed)
}

// Counts returns how many pages are in each state. NotPresent stays 0: the
// tracker records the pages that left it, not the size of the memory.
func (t *Tracker) Counts() (counts [NumStates]uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for state := range numStates {
		if bm := t.bitmapFor(state); bm != nil {
			counts[state] = bm.GetCardinality()
		}
	}

	return counts
}

// ExportStates returns clones of the four installed/removed per-state
// bitmaps, without the empty-set merging Export applies. Installing pages
// are absent, like NotPresent.
func (t *Tracker) ExportStates() (dirty, zero, removed, clean *roaring.Bitmap) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.dirty.Clone(), t.zero.Clone(), t.removed.Clone(), t.clean.Clone()
}
