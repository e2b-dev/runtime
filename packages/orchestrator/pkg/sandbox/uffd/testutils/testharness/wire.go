// Package testharness provides the wire types, typed RPC client, and
// barrier registry shared between the parent and child halves of the
// userfaultfd test harness.
package testharness

// Empty is the placeholder for net/rpc methods that take or return
// nothing; net/rpc still requires both args and reply pointers.
type Empty struct{}

type BootstrapArgs struct {
	MmapStart uint64
	Pagesize  int64
	TotalSize int64
	AlwaysWP  bool
	// Barriers gates the test-only worker hooks (off by default).
	Barriers bool
	// SourceDelayMs delays every source read in the child (0 = no delay),
	// standing in for a slow remote chunk fetch.
	SourceDelayMs int64
	// InstallDedupOff runs the child's serve loop with one worker per fault
	// message, the behaviour before UffdInstallDedupFlag. The environment
	// variable UFFD_INSTALL_DEDUP=0 does the same for every harness, so the
	// whole suite can be run in that mode.
	InstallDedupOff bool
	// ContentPath names a file holding the source pages. Inline hugepage
	// content as JSON costs seconds per Bootstrap under -race.
	ContentPath string
}

type BootstrapReply struct{}

// PageStateEntry is the wire form of a block.State for a single page offset.
type PageStateEntry struct {
	State  uint8
	Offset uint64
}

type PageStatesReply struct {
	Entries []PageStateEntry
}

type FaultBarrierArgs struct {
	Addr  uint64
	Point uint8
}

type FaultBarrierReply struct {
	Token uint64
}

type TokenArgs struct {
	Token uint64
}

// SourceStatsReply counts the child's source reads. Each in-flight read
// holds one page-sized scratch buffer.
type SourceStatsReply struct {
	Reads        int64
	PeakInFlight int64
}

// ServeStatsReply is the subset of the handler's ServeSnapshot the tests
// assert on.
type ServeStatsReply struct {
	Pages    int64
	Deduped  int64
	Deferred int64
}

// PrefaultArgs names the page to prefault from the child's own source bytes.
type PrefaultArgs struct {
	Offset int64
}

type PrefaultReply struct {
	Installed bool
}

// PageStateArgs names one page; PageStateReply carries its tracker state
// read under the tracker's own lock only, so it can be asked while workers
// hold settleRequests.
type PageStateArgs struct {
	Offset int64
}

type PageStateReply struct {
	State uint8
}

// EventPendingReply reports whether the child's uffd has an unread event.
type EventPendingReply struct {
	Pending bool
}

// FaultOffsetsReply carries the memfile offsets of dequeued fault events, in
// dequeue order.
type FaultOffsetsReply struct {
	Offsets []int64
}

// CoWBeginArgs installs a CoW export window over the given page indices.
type CoWBeginArgs struct {
	Pages []uint64
}

// CoWStateReply is a snapshot of the child's active (or finished) window.
type CoWStateReply struct {
	Copied      int64
	Canceled    bool
	CancelCause string
	// Sink is the identity-offset sink contents (pre-images captured so far;
	// unwritten ranges are zero). Sized like the guest memory — fine at the
	// 4k-page scale the CoW tests use.
	Sink []byte
}

// CoWSweepReply carries the sweep + uninstall outcome.
type CoWSweepReply struct {
	SweepError  string
	CancelCause string
}
