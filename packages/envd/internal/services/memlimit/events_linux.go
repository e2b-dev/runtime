//go:build linux

package memlimit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// kernelUeventGroup is the netlink multicast group the kernel itself sends uevents on;
// udev rebroadcasts on group 2, which a guest without udev never has.
const kernelUeventGroup = 1

// watchMemoryEvents calls notify for every memory block the kernel brings online or
// takes offline, and after any overflow that may have dropped one, until ctx ends.
// Events only trigger a recompute from MemTotal, so one spoofed by a guest process
// changes nothing.
func watchMemoryEvents(ctx context.Context, notify func()) error {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, unix.NETLINK_KOBJECT_UEVENT)
	if err != nil {
		return fmt.Errorf("open uevent socket: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: kernelUeventGroup}); err != nil {
		_ = unix.Close(fd)

		return fmt.Errorf("bind uevent socket: %w", err)
	}

	// A non-blocking fd wrapped in os.File reads through the runtime poller, so Close
	// from the goroutine below unblocks the Read.
	sock := os.NewFile(uintptr(fd), "uevent")
	stop := context.AfterFunc(ctx, func() { _ = sock.Close() })
	defer func() {
		if stop() {
			_ = sock.Close()
		}
	}()

	buf := make([]byte, 16*1024)
	for {
		n, err := sock.Read(buf)
		if errors.Is(err, syscall.ENOBUFS) {
			notify()

			continue
		}
		if err != nil {
			return fmt.Errorf("read uevent: %w", err)
		}
		if isMemoryOnlineEvent(buf[:n]) {
			notify()
		}
	}
}
