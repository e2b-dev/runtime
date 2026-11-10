//go:build unix

package handler

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"

	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

// waitReadopted blocks on the child's pidfd until it exits, then harvests the
// status with a pid-specific wait4 (never wait4(-1), which would steal
// os/exec's post-upgrade children).
func (p *Handler) waitReadopted() *rpc.ProcessEvent_EndEvent {
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

	return end
}
