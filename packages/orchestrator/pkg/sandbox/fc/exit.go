//go:build linux

package fc

import (
	"syscall"

	"go.uber.org/zap"
)

// ExitInfo is how the Firecracker leader ended, as reported by wait(2).
// Exited and Signaled are mutually exclusive.
//
// wait(2) does not report who sent a signal: si_pid reaches the signalled
// process, not its parent, and SIGKILL cannot be caught. Naming a sender needs
// host-side tracing — an audit rule on kill(2), or an eBPF probe on
// signal_generate.
type ExitInfo struct {
	Exited     bool
	ExitCode   int
	Signaled   bool
	Signal     syscall.Signal
	CoreDumped bool

	// SentByStop is whether the signal that ended the process is one Stop had
	// sent by the time it was reaped. A different signal is not ours even if
	// Stop ran: a foreign SIGKILL during our SIGTERM, or our SIGTERM reaching
	// a process something else had already killed.
	SentByStop bool
}

// LogFields describes the exit for a log line.
func (e *ExitInfo) LogFields() []zap.Field {
	switch {
	case e == nil:
		return nil
	case e.Signaled:
		return []zap.Field{
			zap.String("fc_signal", e.Signal.String()),
			zap.Bool("fc_core_dumped", e.CoreDumped),
		}
	case e.Exited:
		return []zap.Field{zap.Int("fc_exit_code", e.ExitCode)}
	default:
		return nil
	}
}

// CrashCause labels how a Firecracker process that nobody asked to stop ended.
type CrashCause string

const (
	// CrashCauseCleanExit is Firecracker exiting 0. Usually the guest shut
	// down or rebooted, but a guest kernel panic (the guest reboots on panic)
	// and a triple fault end the same way and cannot be told apart here.
	CrashCauseCleanExit CrashCause = "clean_exit"

	// CrashCauseExitError is a non-zero exit that is not one of Firecracker's
	// signal-handler codes: Firecracker itself failed.
	CrashCauseExitError CrashCause = "exit_error"

	// CrashCauseExternalSignal is a kill signal nothing here asked for:
	// another process on the host, or the kernel's OOM killer. The alertable
	// cause — the sandbox dies without its snapshot.
	CrashCauseExternalSignal CrashCause = "external_signal"

	// CrashCauseFault is a fault the process raised by running: a memory
	// fault, a bad syscall, an abort, an illegal instruction. Firecracker
	// catches most of these and exits with a code of its own, so this covers
	// both forms. A memory fault lands here only on a cold boot; on a resume
	// the memory handler fails first, see CrashCauseMemoryHandlerFailed.
	CrashCauseFault CrashCause = "fault"

	// CrashCauseMemoryHandlerFailed is the memory handler failing to serve a
	// page (uffdio_copy ENOMEM, a failed read of the source) on a resumed
	// sandbox, after which teardown stopped Firecracker. ClassifyExit cannot
	// tell it from CrashCauseRequestedSignal; the caller that knows the
	// handler's outcome assigns it.
	CrashCauseMemoryHandlerFailed CrashCause = "memory_handler_failed"

	// CrashCauseRequestedSignal is a signal Stop sent with no stop reason
	// recorded and no memory handler failure: a teardown path that skipped
	// SetStopReason.
	CrashCauseRequestedSignal CrashCause = "requested_signal"

	// CrashCauseUnknown is an exit wait(2) never reported.
	CrashCauseUnknown CrashCause = "unknown"
)

// ClassifyExit labels an exit that ended an execution nobody asked to stop.
func ClassifyExit(info *ExitInfo) CrashCause {
	switch {
	case info == nil:
		return CrashCauseUnknown
	case info.Signaled && isFaultSignal(info.Signal):
		return CrashCauseFault
	case info.Signaled && info.SentByStop:
		return CrashCauseRequestedSignal
	case info.Signaled:
		return CrashCauseExternalSignal
	case info.Exited && info.ExitCode == 0:
		return CrashCauseCleanExit
	case info.Exited:
		if signal, ok := firecrackerHandledSignal(info.ExitCode); ok {
			if isFaultSignal(signal) {
				return CrashCauseFault
			}

			return CrashCauseExternalSignal
		}

		return CrashCauseExitError
	default:
		return CrashCauseUnknown
	}
}

// isFaultSignal reports whether the signal is one the kernel raises against a
// process for what it did, rather than one another process sent to end it.
// A fault outranks our own Stop: a SIGTERM in flight does not make the fault
// that actually killed the process our teardown.
func isFaultSignal(signal syscall.Signal) bool {
	switch signal {
	case syscall.SIGBUS, syscall.SIGSEGV, syscall.SIGILL, syscall.SIGFPE,
		syscall.SIGABRT, syscall.SIGSYS, syscall.SIGTRAP:
		return true
	default:
		return false
	}
}

// firecrackerHandledSignal maps the exit codes Firecracker's own signal
// handlers exit with (FcExitCode) back to the signal each one caught.
// SIGXFSZ and SIGXCPU are left out: they are resource limits, not a fault or
// a kill, and stay exit_error.
func firecrackerHandledSignal(code int) (syscall.Signal, bool) {
	switch code {
	case 148:
		return syscall.SIGSYS, true
	case 149:
		return syscall.SIGBUS, true
	case 150:
		return syscall.SIGSEGV, true
	case 156:
		return syscall.SIGHUP, true
	case 157:
		return syscall.SIGILL, true
	default:
		return 0, false
	}
}

func signalBit(sig syscall.Signal) uint64 {
	if sig <= 0 || sig >= 64 {
		return 0
	}

	return 1 << uint(sig)
}

// exitInfoFromStatus converts a reaped wait status into an ExitInfo. sent is
// the set of signals Stop had sent, one bit per signal.
func exitInfoFromStatus(status syscall.WaitStatus, sent uint64) *ExitInfo {
	if status.Signaled() {
		signal := status.Signal()

		return &ExitInfo{
			Signaled:   true,
			Signal:     signal,
			CoreDumped: status.CoreDump(),
			SentByStop: sent&signalBit(signal) != 0,
		}
	}

	if status.Exited() {
		return &ExitInfo{
			Exited:   true,
			ExitCode: status.ExitStatus(),
		}
	}

	// Stopped or continued, which Wait does not return on.
	return &ExitInfo{}
}
