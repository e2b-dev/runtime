package cgroups

import (
	"errors"
	"io/fs"
	"syscall"
)

// ErrFrozenUnobservable is returned by Frozen when the manager cannot read freeze
// state at all, because it manages no cgroups. It is deliberately distinct from
// (false, nil), which means "this cgroup exists and is not frozen yet": there is
// nothing to wait for and nothing still running, so a caller must neither burn its
// budget polling for an answer that cannot change nor treat the result as a workload
// that would not stop.
var ErrFrozenUnobservable = errors.New("cgroup freeze state unobservable")

// PathManager is a Manager that can also address cgroups by absolute path. The
// hierarchy walk needs this: the cgroups it freezes belong to the customer and have no
// ProcessType, so they cannot be reached through the ProcessType-keyed methods.
//
// It is deliberately a separate interface rather than more methods on Manager. A guest
// whose manager does not implement it (the no-op manager, the non-Linux stub) simply
// cannot be walked, which is a state the caller must handle anyway -- and discovering
// that by type assertion is honest, where a Manager full of unimplemented path methods
// would not be.
type PathManager interface {
	Manager
	// Root is the cgroup2 mount point the walk starts from.
	Root() string
	// PathOf is where a ProcessType's cgroup lives. The name is configured rather than
	// derived -- ProcessTypePTY lives in "ptys" -- so a walk that wants to recognise the
	// static cgroups among the children it enumerates has to ask.
	PathOf(procType ProcessType) (string, bool)
	// ChildrenOf lists the immediate child cgroups of an absolute path. Only
	// directories: cgroupfs represents every cgroup as one, and the interface files
	// alongside them are not cgroups.
	ChildrenOf(path string) ([]string, error)
	FreezeAt(path string) error
	UnfreezeAt(path string) error
	// FrozenAt reports the SETTLED state, from the "frozen" field of cgroup.events --
	// the same thing Frozen reports for a ProcessType. This is what confirmation waits
	// on: it goes to 1 only once the tasks have actually stopped.
	FrozenAt(path string) (bool, error)
	// FreezeRequestedAt reports the cgroup's OWN requested state, from cgroup.freeze.
	// The thaw needs this one rather than the settled state, and the difference is
	// load-bearing: cgroup.freeze reads back what some freeze wrote *here*, which is
	// exactly the set to undo, whereas cgroup.events also reads frozen=1 for a cgroup
	// that is frozen only because an ancestor is. Thawing those individually would be
	// both wrong (the ancestor still holds them) and pointless (thawing the ancestor
	// releases them).
	FreezeRequestedAt(path string) (bool, error)
}

// MemoryLimiter is a Manager that can rewrite the memory limit of a cgroup it created.
// envd needs this because the guest's memory size changes after boot when virtio-mem
// plugs or unplugs memory, and the limit written at creation would otherwise stay
// sized for the boot memory. It is separate from Manager for the same reason as
// PathManager: a manager with no cgroups has no limit to rewrite, and callers find
// that out by type assertion.
type MemoryLimiter interface {
	// SetMemoryLimit sets memory.high and memory.max of procType's cgroup to limit
	// bytes. Lowering it below current usage makes the kernel reclaim, and may OOM-kill
	// inside the cgroup, before the write returns.
	SetMemoryLimit(procType ProcessType, limit uint64) error
}

type ProcessType string

const (
	ProcessTypePTY   ProcessType = "pty"
	ProcessTypeUser  ProcessType = "user"
	ProcessTypeSocat ProcessType = "socat"
	// ProcessTypeSystem stays in envd's root cgroup so it's unaffected by freeze.
	ProcessTypeSystem ProcessType = "system"
)

type Manager interface {
	GetFileDescriptor(procType ProcessType) (int, bool)
	Freeze(procType ProcessType) error
	Unfreeze(procType ProcessType) error
	// Frozen reports whether the cgroup has finished freezing, from the "frozen"
	// field of cgroup.events. Writing cgroup.freeze only *requests* a freeze: the
	// kernel stops each task at its next signal-delivery point, so a task inside an
	// uninterruptible wait stays runnable until that wait returns. Callers that need
	// the workload actually stopped must poll this rather than assume the write was
	// enough. Reading it reports STATE, not an acknowledgement of our write: a cgroup
	// the guest froze itself reads frozen too. Managers with no cgroups to read return
	// ErrFrozenUnobservable.
	Frozen(procType ProcessType) (bool, error)
	Close() error
}

// vanished reports whether err says the cgroup itself is no longer there. Both walks
// this package runs are observers of a tree the guest keeps mutating, so a cgroup that
// is enumerated and then removed before it can be read is the ordinary case, not a
// fault -- systemd retires a transient unit on every timer tick.
//
// Deliberately NOT "any error". The freeze sweep's Failed count describes the tree that
// is about to be snapshotted -- a cgroup that is still there and would not take the write,
// or would not report its state -- and a predicate wide enough to swallow a vanished
// cgroup swallows that too. The resume audit can skip on any error because it only
// observes; the sweep cannot, because Failed is a count someone acts on. Only the errnos a
// removal can produce belong here, and there are
// exactly two of them. Which one arrives depends only on where the removal lands
// relative to the open, so a guard naming either one alone is half a guard:
//
//	remove, then open           -> ENOENT
//	open, then remove, then use -> ENODEV
//
// The second is a cgroupfs property, not a POSIX one: an ordinary unlinked-but-open
// file keeps serving reads, which is why this cannot be reproduced on a tmpfs tree.
func vanished(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENODEV)
}
