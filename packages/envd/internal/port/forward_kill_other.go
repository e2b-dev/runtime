//go:build unix && !linux

package port

import "syscall"

// killForwarder kills pid alone, and only while it still is a forwarding socat onto bind.
// killed reports a forwarder that was signalled.
func killForwarder(root, bind string, pid int) (killed bool, err error) {
	return killIf(pid, func() bool { return isForwarderProcess(root, bind, pid) })
}

// killIf kills pid alone, and only while is still holds for it. killed reports a process
// that was signalled.
func killIf(pid int, is func() bool) (killed bool, err error) {
	if !is() {
		return false, nil
	}

	return true, syscall.Kill(pid, syscall.SIGKILL)
}
