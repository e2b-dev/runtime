//go:build unix

package reaper

import "syscall"

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
