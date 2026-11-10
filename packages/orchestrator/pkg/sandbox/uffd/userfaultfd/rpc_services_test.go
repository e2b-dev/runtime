//go:build linux

package userfaultfd

// RPC service implementations for the cross-process UFFD test harness;
// in _test.go because they need *Userfaultfd internals.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/fdexit"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils/testharness"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

//nolint:containedctx // shutdown-aware ctx shared with RPC handlers; lifetime is the child process.
type harnessState struct {
	faultMu      sync.Mutex
	faultOffsets []int64

	uffdFd uintptr

	mu     sync.Mutex
	uffd   *Userfaultfd
	br     *testharness.Registry
	stop   func() // serve-stop fn; nil when paused
	ctx    context.Context
	cancel context.CancelFunc
	closed bool

	// data is the bootstrap source content: the CoW window reads pre-images
	// from it (pages installed from source and not yet written hold exactly
	// this content, which is the only view of "guest memory" the serving
	// child has — the parent's mmap is not mapped here).
	data *MemorySlicer
	// src is the delayed source wrapper; nil unless Bootstrap set a delay.
	src *slowSource
	// window/sink hold the active CoW export driven over RPC.
	window *CoWWindow
	sink   *cowHarnessSink
}

// cowHarnessSink is a lockable identity-offset WriterAt over a byte slice.
type cowHarnessSink struct {
	mu  sync.Mutex
	buf []byte
}

func (s *cowHarnessSink) WriteAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return copy(s.buf[off:], p), nil
}

func (s *cowHarnessSink) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]byte(nil), s.buf...)
}

// slowSource delays every read and records how many run at once.
type slowSource struct {
	inner    PageReader
	delay    time.Duration
	reads    atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
}

func (s *slowSource) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	s.reads.Add(1)
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for {
		peak := s.peak.Load()
		if n <= peak || s.peak.CompareAndSwap(peak, n) {
			break
		}
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(s.delay):
	}

	return s.inner.ReadAt(ctx, p, off)
}

func newHarnessState(uffdFd uintptr) *harnessState {
	ctx, cancel := context.WithCancel(context.Background())

	return &harnessState{
		uffdFd: uffdFd,
		ctx:    ctx,
		cancel: cancel,
	}
}

// startServeLocked is idempotent so a stray duplicate Resume cannot
// leak an untracked Serve goroutine. Caller must hold s.mu.
func (s *harnessState) startServeLocked() error {
	if s.stop != nil {
		return nil
	}

	exit, err := fdexit.New()
	if err != nil {
		return fmt.Errorf("fdexit.New: %w", err)
	}

	uffd := s.uffd
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := uffd.Serve(context.Background(), exit); err != nil {
			fmt.Fprintln(os.Stderr, "uffd.Serve:", err)
		}
	}()

	s.stop = func() {
		_ = exit.SignalExit()
		<-done
		exit.Close()
	}

	return nil
}

func (s *harnessState) stopServe() {
	// Drop s.mu before stop() — stop() blocks on the Serve drain, and any
	// concurrent RPC handler needing s.mu (e.g. WaitFaultHeld during a
	// parked barrier) would otherwise stall until the drain completes.
	s.mu.Lock()
	stop := s.stop
	s.stop = nil
	s.mu.Unlock()

	if stop != nil {
		stop()
	}
}

type Lifecycle struct {
	state *harnessState
}

func (l *Lifecycle) Bootstrap(args *testharness.BootstrapArgs, _ *testharness.BootstrapReply) error {
	content, err := os.ReadFile(args.ContentPath)
	if err != nil {
		return fmt.Errorf("read content: %w", err)
	}
	if int64(len(content)) != args.TotalSize {
		return fmt.Errorf("content size %d != expected %d", len(content), args.TotalSize)
	}

	data := NewMemorySlicer(content, args.Pagesize)

	mapping := memory.NewMapping([]memory.Region{
		{
			BaseHostVirtAddr: uintptr(args.MmapStart),
			Size:             uintptr(args.TotalSize),
			Offset:           0,
			PageSize:         uintptr(args.Pagesize),
		},
	})

	log, err := logger.NewDevelopmentLogger()
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}

	var src PageReader = data
	var slow *slowSource
	if args.SourceDelayMs > 0 {
		slow = &slowSource{inner: data, delay: time.Duration(args.SourceDelayMs) * time.Millisecond}
		src = slow
	}

	uffd, err := NewUserfaultfdFromFd(l.state.uffdFd, src, mapping, 0, log)
	if err != nil {
		return fmt.Errorf("NewUserfaultfdFromFd: %w", err)
	}

	if args.AlwaysWP {
		uffd.defaultCopyMode = UFFDIO_COPY_MODE_WP
	}
	if args.InstallDedupOff || os.Getenv("UFFD_INSTALL_DEDUP") == "0" {
		uffd.SetInstallDedup(false)
	}

	var br *testharness.Registry
	if args.Barriers {
		br = testharness.NewRegistry()
	}

	// Recorded unconditionally: a parent cannot tell from its own side whether
	// a page faulted, and the gated race tests branch on that.
	base := uintptr(args.MmapStart)
	var brHook func(uintptr, testharness.Point)
	if br != nil {
		brHook = br.Hook()
	}
	uffd.SetTestFaultHook(func(addr uintptr, p faultPhase) {
		if p == faultPhaseBeforeRLock {
			l.state.faultMu.Lock()
			l.state.faultOffsets = append(l.state.faultOffsets, int64(addr-base))
			l.state.faultMu.Unlock()
		}
		if brHook != nil {
			brHook(addr, testharness.Point(p))
		}
	})

	l.state.mu.Lock()
	defer l.state.mu.Unlock()
	l.state.uffd = uffd
	l.state.br = br
	l.state.data = data
	l.state.src = slow

	return l.state.startServeLocked()
}

// WaitReady is a no-op today (Bootstrap is synchronous); kept as a separate
// RPC so an async-Bootstrap variant can hold the parent here unchanged.
func (l *Lifecycle) WaitReady(_ *testharness.Empty, _ *testharness.Empty) error {
	return nil
}

func (l *Lifecycle) Shutdown(_ *testharness.Empty, _ *testharness.Empty) error {
	l.state.mu.Lock()
	defer l.state.mu.Unlock()
	if !l.state.closed {
		l.state.closed = true
		l.state.cancel()
	}

	return nil
}

type Paging struct {
	state *harnessState
}

// FaultOffsets reports the memfile offsets of every fault event the serve
// loop has dequeued (earliest hook phase), in dequeue order. Handler-side
// ground truth for the gated-race discriminators.
func (p *Paging) FaultOffsets(_ *testharness.Empty, reply *testharness.FaultOffsetsReply) error {
	p.state.faultMu.Lock()
	defer p.state.faultMu.Unlock()
	reply.Offsets = append([]int64(nil), p.state.faultOffsets...)

	return nil
}

func (p *Paging) SourceStats(_ *testharness.Empty, reply *testharness.SourceStatsReply) error {
	p.state.mu.Lock()
	src := p.state.src
	p.state.mu.Unlock()
	if src == nil {
		return errors.New("source stats need a SourceDelayMs bootstrap")
	}

	reply.Reads = src.reads.Load()
	reply.PeakInFlight = src.peak.Load()

	return nil
}

func (p *Paging) ServeStats(_ *testharness.Empty, reply *testharness.ServeStatsReply) error {
	p.state.mu.Lock()
	uffd := p.state.uffd
	p.state.mu.Unlock()
	if uffd == nil {
		return errors.New("Paging.ServeStats called before Lifecycle.Bootstrap")
	}

	st := uffd.ServeStats()
	reply.Pages = st.Pages
	reply.Deduped = st.Deduped
	reply.Deferred = st.Deferred

	return nil
}

func (p *Paging) Prefault(args *testharness.PrefaultArgs, reply *testharness.PrefaultReply) error {
	p.state.mu.Lock()
	uffd, data := p.state.uffd, p.state.data
	p.state.mu.Unlock()
	if uffd == nil {
		return errors.New("Paging.Prefault called before Lifecycle.Bootstrap")
	}

	page, err := data.Slice(context.Background(), args.Offset, uffd.PageSize())
	if err != nil {
		return err
	}
	installed, err := uffd.Prefault(context.Background(), args.Offset, page)
	if err != nil {
		return err
	}
	reply.Installed = installed

	return nil
}

func (p *Paging) PageState(args *testharness.PageStateArgs, reply *testharness.PageStateReply) error {
	p.state.mu.Lock()
	uffd := p.state.uffd
	p.state.mu.Unlock()
	if uffd == nil {
		return errors.New("Paging.PageState called before Lifecycle.Bootstrap")
	}
	reply.State = uint8(uffd.pageTracker.Get(uint32(header.BlockIdx(args.Offset, uffd.PageSize()))))

	return nil
}

func (p *Paging) EventPending(_ *testharness.Empty, reply *testharness.EventPendingReply) error {
	fds := []unix.PollFd{{Fd: int32(p.state.uffdFd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 0)
	if err != nil {
		return err
	}
	reply.Pending = n == 1 && fds[0].Revents&unix.POLLIN != 0

	return nil
}

func (p *Paging) States(_ *testharness.Empty, reply *testharness.PageStatesReply) error {
	p.state.mu.Lock()
	uffd := p.state.uffd
	p.state.mu.Unlock()
	if uffd == nil {
		return errors.New("Paging.States called before Lifecycle.Bootstrap")
	}

	entries, err := uffd.pageStateEntries()
	if err != nil {
		return err
	}
	reply.Entries = entries

	return nil
}

func (p *Paging) Pause(_ *testharness.Empty, _ *testharness.Empty) error {
	p.state.stopServe()

	return nil
}

func (p *Paging) Resume(_ *testharness.Empty, _ *testharness.Empty) error {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	return p.state.startServeLocked()
}

// pageStateEntries returns a wire-format snapshot of pageTracker.
// settleRequests.Lock drains fault workers (mirrors PrefetchData) so
// the snapshot is consistent w.r.t. concurrent installs.
func (u *Userfaultfd) pageStateEntries() ([]testharness.PageStateEntry, error) {
	u.settleRequests.Lock()
	defer u.settleRequests.Unlock()

	bmDirty, bmZero, bmRemoved, bmClean := u.pageTracker.ExportStates()
	entries := make([]testharness.PageStateEntry, 0,
		bmDirty.GetCardinality()+bmZero.GetCardinality()+bmRemoved.GetCardinality()+bmClean.GetCardinality())
	emit := func(bm *roaring.Bitmap, state block.State) {
		for _, idx := range bm.ToArray() {
			entries = append(entries, testharness.PageStateEntry{
				State:  uint8(state),
				Offset: uint64(idx) * uint64(u.pageSize),
			})
		}
	}
	emit(bmDirty, block.Dirty)
	emit(bmZero, block.Zero)
	emit(bmRemoved, block.Removed)
	emit(bmClean, block.Clean)

	return entries, nil
}

type Barriers struct {
	state *harnessState
}

func (b *Barriers) Install(args *testharness.FaultBarrierArgs, reply *testharness.FaultBarrierReply) error {
	br, err := b.registry()
	if err != nil {
		return err
	}
	reply.Token = br.Install(uintptr(args.Addr), testharness.Point(args.Point))

	return nil
}

func (b *Barriers) WaitHeld(args *testharness.TokenArgs, _ *testharness.Empty) error {
	br, err := b.registry()
	if err != nil {
		return err
	}

	return br.WaitArrived(b.state.ctx, args.Token)
}

func (b *Barriers) Release(args *testharness.TokenArgs, _ *testharness.Empty) error {
	br, err := b.registry()
	if err != nil {
		return err
	}
	br.Release(args.Token)

	return nil
}

func (b *Barriers) registry() (*testharness.Registry, error) {
	b.state.mu.Lock()
	br := b.state.br
	b.state.mu.Unlock()
	if br == nil {
		return nil, errors.New("Barriers RPC requires args.Barriers=true at Bootstrap")
	}

	return br, nil
}

// CoW drives a real CoW export window through the live serve loop: Begin
// arms + installs via BeginCoWExport, State snapshots progress/cancellation
// and the sink, Sweep drains and uninstalls via EndCoWExport — exercising the
// real readSerial/settleRequests interplay the isolated CoWWindow tests
// cannot.
type CoW struct {
	state *harnessState
}

func (c *CoW) Begin(args *testharness.CoWBeginArgs, _ *testharness.Empty) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	uffd := c.state.uffd
	if uffd == nil {
		return errors.New("CoW.Begin called before Lifecycle.Bootstrap")
	}
	if c.state.window != nil {
		return errors.New("CoW.Begin: a window is already installed")
	}

	pages := roaring.New()
	for _, p := range args.Pages {
		pages.Add(uint32(p))
	}

	size, err := c.state.data.Size()
	if err != nil {
		return err
	}
	sink := &cowHarnessSink{buf: make([]byte, size)}

	// Pre-images come from the bootstrap source: correct for pages installed
	// from source and not yet written, which is what the parent tests use.
	src := bytes.NewReader(c.state.data.Content())
	w, err := uffd.BeginCoWExport(pages, src, sink)
	if err != nil {
		return err
	}
	c.state.window = w
	c.state.sink = sink

	return nil
}

func (c *CoW) State(_ *testharness.Empty, reply *testharness.CoWStateReply) error {
	c.state.mu.Lock()
	w, sink := c.state.window, c.state.sink
	c.state.mu.Unlock()
	if w == nil {
		return errors.New("CoW.State: no window installed")
	}

	reply.Copied = w.Copied()
	if cause := w.CancelCause(); cause != nil {
		reply.Canceled = true
		reply.CancelCause = cause.Error()
	}
	reply.Sink = sink.snapshot()

	return nil
}

func (c *CoW) Sweep(_ *testharness.Empty, reply *testharness.CoWSweepReply) error {
	c.state.mu.Lock()
	w := c.state.window
	uffd := c.state.uffd
	c.state.mu.Unlock()
	if w == nil {
		return errors.New("CoW.Sweep: no window installed")
	}

	err := w.Sweep(context.Background())
	uffd.EndCoWExport(w)
	if err == nil {
		err = w.CancelCause()
	}
	if err != nil {
		reply.SweepError = err.Error()
	}
	if cause := w.CancelCause(); cause != nil {
		reply.CancelCause = cause.Error()
	}

	c.state.mu.Lock()
	c.state.window, c.state.sink = nil, nil
	c.state.mu.Unlock()

	return nil
}
