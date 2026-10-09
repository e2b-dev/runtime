// Package reaper tracks the helper processes envd forks for itself, so a live upgrade
// can hand their pids to the new envd. They stay its children across the execve, since
// the pid does not change, and nothing else waits for them.
package reaper

import (
	"os/exec"
	"slices"
	"sync"
	"syscall"
)

type Registry struct {
	mu      sync.Mutex
	pids    map[int]struct{}
	adopted sync.WaitGroup
}

// Run is cmd.Run with the child's pid recorded for as long as it lives. The start
// happens under the lock, so an export held across it misses no child.
func (r *Registry) Run(cmd *exec.Cmd) error {
	r.mu.Lock()
	err := cmd.Start()
	if err == nil {
		r.add(cmd.Process.Pid)
	}
	r.mu.Unlock()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	r.remove(cmd.Process.Pid)

	return err
}

// Adopt takes over a child the previous envd handed over: it is reaped when it exits
// and listed until then, so a further upgrade hands it on again. Every Adopt must come
// before the first WaitAdopted.
func (r *Registry) Adopt(pid int) {
	if pid <= 0 {
		return
	}
	r.mu.Lock()
	r.add(pid)
	r.mu.Unlock()
	r.adopted.Go(func() {
		Reap(pid)
		r.remove(pid)
	})
}

// WaitAdopted returns once every child Adopt took over has been reaped.
func (r *Registry) WaitAdopted() { r.adopted.Wait() }

func (r *Registry) add(pid int) {
	if r.pids == nil {
		r.pids = make(map[int]struct{})
	}
	r.pids[pid] = struct{}{}
}

func (r *Registry) remove(pid int) {
	r.mu.Lock()
	delete(r.pids, pid)
	r.mu.Unlock()
}

// ExportHold returns the live pids and keeps new children from starting until release
// is called. A live upgrade holds it through the execve.
func (r *Registry) ExportHold() (pids []int32, release func()) {
	r.mu.Lock()
	for pid := range r.pids {
		pids = append(pids, int32(pid))
	}
	slices.Sort(pids)

	return pids, sync.OnceFunc(r.mu.Unlock)
}

// Reap waits for a child the previous envd handed over, so it does not linger as a
// zombie once it exits. It names one pid, so it never takes an exit status from os/exec.
func Reap(pid int) {
	if pid <= 0 {
		return
	}
	var ws syscall.WaitStatus
	for {
		if _, err := syscall.Wait4(pid, &ws, 0, nil); err != syscall.EINTR {
			return
		}
	}
}
