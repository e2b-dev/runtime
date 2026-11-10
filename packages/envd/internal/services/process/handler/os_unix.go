//go:build unix

package handler

import (
	"syscall"

	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

// sysState is the per-OS state a handler keeps beside its exec.Cmd. A Linux
// child needs none: its cgroup and credentials are set on the command itself.
type sysState struct{}

// sysAfterStart runs once the child exists, before its pid is reported.
func (p *Handler) sysAfterStart() error { return nil }

// sysSignal delivers a signal to the live child.
func (p *Handler) sysSignal(signal syscall.Signal) error {
	return p.cmd.Process.Signal(signal)
}

// sysRelease frees the per-OS state after the child has been reaped.
func (p *Handler) sysRelease() {}

// discard frees the per-OS state of a command that never started.
func (*sysState) discard() {}

// sysEndEvent leaves the end event as the kernel reported it.
func (p *Handler) sysEndEvent(*rpc.ProcessEvent_EndEvent) {}
