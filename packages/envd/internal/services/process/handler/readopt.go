package handler

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

// ReadoptArgs carries the per-process state from a live-upgrade handover blob
// plus the inherited fds (already present in this process's fd table — the
// outgoing envd dup'd them across execve with CLOEXEC cleared).
type ReadoptArgs struct {
	Pid    uint32
	Tag    *string
	Config *rpc.ProcessConfig
	CgType cgroups.ProcessType
	Stdout *os.File // nil if pty
	Stderr *os.File // nil if pty
	Stdin  *os.File // nil if disabled / pty
	Tty    *os.File // nil if non-pty
	// Timeout is the process's remaining timeout carried across the upgrade
	// (0 = none). Re-armed as a kill-timer so a timed-out process is still
	// killed after the swap.
	Timeout time.Duration
	// Thawed, if non-nil, is closed when the workload is next unfrozen. The
	// carried kill-timer waits on it before starting, so the remaining timeout
	// measures the process's running time and is not burned down while it is
	// still frozen (until the post-upgrade /init thaws it).
	Thawed <-chan struct{}
}

// Readopt reconstructs a Handler for a process that survived an envd
// self-upgrade. cmd is nil: the process keeps running with the
// same PID and the inherited pipe/PTY fds, so its I/O is uninterrupted. It is
// reaped via its pidfd (we are still the parent — execve preserves the PID),
// using a pid-specific wait4 so os/exec's reaping of post-upgrade processes is
// never disturbed.
func Readopt(args ReadoptArgs, logger *zerolog.Logger) *Handler {
	outMultiplex := NewMultiplexedChannel[rpc.ProcessEvent_Data](outputBufferSize)
	outCtx, outCancel := context.WithCancel(context.Background())
	_, cancel := context.WithCancel(context.Background())

	h := &Handler{
		Config:    args.Config,
		Tag:       args.Tag,
		logger:    logger,
		cancel:    cancel,
		outCtx:    outCtx,
		outCancel: outCancel,
		DataEvent: outMultiplex,
		EndEvent:  NewMultiplexedChannel[rpc.ProcessEvent_End](0),
		pid:       args.Pid,
		cgType:    args.CgType,
		readopted: true,
		tty:       args.Tty,
		stdoutF:   args.Stdout,
		stderrF:   args.Stderr,
		stdinF:    args.Stdin,
	}
	if args.Stdin != nil {
		h.stdin = args.Stdin
	}

	var outWg sync.WaitGroup
	if args.Tty != nil {
		outWg.Go(func() {
			h.pump(args.Tty, ptyChunkSize, &h.ptyBytes, func(b []byte) *rpc.ProcessEvent_DataEvent {
				return &rpc.ProcessEvent_DataEvent{Output: &rpc.ProcessEvent_DataEvent_Pty{Pty: b}}
			})
		})
	} else {
		if args.Stdout != nil {
			outWg.Go(func() {
				h.pump(args.Stdout, stdChunkSize, &h.stdoutBytes, func(b []byte) *rpc.ProcessEvent_DataEvent {
					return &rpc.ProcessEvent_DataEvent{Output: &rpc.ProcessEvent_DataEvent_Stdout{Stdout: b}}
				})
			})
		}
		if args.Stderr != nil {
			outWg.Go(func() {
				h.pump(args.Stderr, stdChunkSize, &h.stderrBytes, func(b []byte) *rpc.ProcessEvent_DataEvent {
					return &rpc.ProcessEvent_DataEvent{Output: &rpc.ProcessEvent_DataEvent_Stderr{Stderr: b}}
				})
			})
		}
	}

	go func() {
		outWg.Wait()
		close(outMultiplex.Source)
		outCancel()
	}()

	h.readoptTimeout = args.Timeout
	h.thawed = args.Thawed

	// NB: the reaper is NOT started here. It emits the terminal EndEvent, and a
	// process whose sleep timer expired during the freeze can exit the instant
	// it is unfrozen — before a caller (the service's trackTermination) has
	// subscribed to EndEvent. Starting the reaper here would race that
	// subscription and drop the exit code. The caller must subscribe first and
	// then call BeginReaping.
	return h
}

// BeginReaping starts the pidfd reaper (and re-arms the carried timeout). It
// must be called AFTER the caller has subscribed to the handler's EndEvent, so
// a fast-exiting re-adopted process cannot emit its terminal event before there
// is a subscriber to retain it.
func (p *Handler) BeginReaping() {
	go p.reapByPidfd()

	// Re-arm the carried process timeout: kill the process once the remaining
	// timeout elapses, unless it exits first (outCtx is cancelled on exit). This
	// restores the deadline that the pre-upgrade exec.CommandContext enforced.
	if p.readoptTimeout > 0 {
		go func() {
			// The new envd runs while the re-adopted workload is still frozen
			// (until the post-upgrade /init, or the fallback, thaws it). Don't
			// start the kill countdown until then, so the carried timeout measures
			// the process's running time and a short remaining timeout isn't burned
			// down — killing the process — before it can run again.
			if p.thawed != nil {
				select {
				case <-p.thawed:
				case <-p.outCtx.Done():
					return
				}
			}

			// Record the absolute deadline so Deadline() reports it: Upgrade
			// carries a process's remaining timeout forward across a (further,
			// chained) upgrade exclusively via Deadline(), so without this a
			// re-adopted timed process would run unbounded after a second handover.
			p.setDeadline(time.Now().Add(p.readoptTimeout))

			t := time.NewTimer(p.readoptTimeout)
			defer t.Stop()

			select {
			case <-t.C:
				_ = p.SendSignal(syscall.SIGKILL, false)
			case <-p.outCtx.Done():
			}
		}()
	}
}

// pump mirrors the New() reader loops: read a chunk, account it, and fan it out
// to subscribers (dropped if none — same semantics as the live path).
func (p *Handler) pump(r io.Reader, chunk int, counter *atomic.Int64, mk func([]byte) *rpc.ProcessEvent_DataEvent) {
	buf := make([]byte, chunk)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			counter.Add(int64(n))
			if p.DataEvent.HasSubscribers() {
				p.DataEvent.Source <- rpc.ProcessEvent_Data{Data: mk(slices.Clone(buf[:n]))}
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, syscall.EIO) {
			return
		}
		if readErr != nil {
			p.logger.Error().Err(readErr).Msg("readopt: error reading process output")

			return
		}
	}
}

// reapByPidfd blocks on the child's pidfd until it exits, then harvests the
// status with a pid-specific wait4 and emits the EndEvent (never wait4(-1),
// which would steal os/exec's post-upgrade children).
func (p *Handler) reapByPidfd() {
	// Prefer a pidfd to wait for exit, but if pidfd_open fails (the process
	// already exited, or fd exhaustion) fall back to a blocking pid-specific
	// wait4 below. Either way a terminal event is still emitted — never leave the
	// process orphaned in the live map with clients blocked on an EndEvent that
	// would otherwise never fire.
	if pidfd, err := pidfdOpen(int(p.pid)); err != nil {
		p.logger.Warn().Err(err).Uint32("pid", p.pid).Msg("readopt: pidfd_open failed; falling back to blocking wait4")
	} else {
		pfd := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
		for {
			if _, perr := unix.Poll(pfd, -1); perr != nil {
				if errors.Is(perr, unix.EINTR) {
					continue
				}
			}

			break
		}
		unix.Close(pidfd)
	}

	var ws syscall.WaitStatus
	_, werr := syscall.Wait4(int(p.pid), &ws, 0, nil)

	end := &rpc.ProcessEvent_EndEvent{}
	switch {
	case werr != nil:
		msg := werr.Error()
		end.Error = &msg
		end.Status = "wait4 error"
	case ws.Exited():
		end.Exited = true
		end.ExitCode = int32(ws.ExitStatus())
		end.Status = "exited"
	case ws.Signaled():
		end.ExitCode = int32(128 + int(ws.Signal()))
		end.Status = ws.Signal().String()
	}

	p.EndEvent.Source <- rpc.ProcessEvent_End{End: end}
	// Retain the terminal event synchronously — before closing the source — so a
	// Connect that forks after the close and falls back to the retention cache is
	// guaranteed to find this exit rather than race an asynchronous retain.
	if p.OnExit != nil {
		p.OnExit(end)
	}
	// Close the source after the terminal event has fanned out to whatever
	// subscribers existed at send time. A Connect that forks afterwards then
	// gets a closed channel (rather than a live subscriber that would block
	// forever, since no further end event is ever produced) and falls back to
	// the service's retained-terminal-event cache.
	close(p.EndEvent.Source)
	p.outCancel()
	p.cancel()

	p.logger.Info().
		Str("event_type", "process_end_readopted").
		Uint32("pid", p.pid).
		Interface("process_result", end).
		Msg("re-adopted process ended")
}
