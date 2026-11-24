//go:build linux

package port

import (
	"errors"

	"golang.org/x/sys/unix"
)

// killForwarder kills pid alone, and only while it still is a forwarding socat onto bind.
// killed reports a forwarder that was signalled.
func killForwarder(root, bind string, pid int) (killed bool, err error) {
	return killIf(pid, func() bool { return isForwarderProcess(root, bind, pid) })
}

// killIf kills pid alone, and only while is still holds for it: a pidfd pins the process
// before is runs, so a pid reused since it was read is never signalled. killed reports a
// process that was signalled.
func killIf(pid int, is func() bool) (killed bool, err error) {
	fd, err := unix.PidfdOpen(pid, 0)
	switch {
	case errors.Is(err, unix.ESRCH):
		return false, nil
	case errors.Is(err, unix.ENOSYS):
		if !is() {
			return false, nil
		}

		return true, unix.Kill(pid, unix.SIGKILL)
	case err != nil:
		return false, err
	}
	defer unix.Close(fd)

	if !is() {
		return false, nil
	}
	if err := unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}
