//go:build linux

package nbd

import (
	"os"

	"github.com/Merovius/nbd/nbdnl"
	"golang.org/x/sys/unix"
)

// deviceOps are the kernel steps of an attach and its teardown that a caller
// may need to hold, fail or observe. linuxDeviceOps takes each step for real;
// tests wrap those functions and still reach the kernel.
type deviceOps struct {
	// netlinkConnect attaches the device through the netlink interface.
	netlinkConnect func(index uint32, socks []*os.File, size uint64, serverFlags nbdnl.ServerFlags, opts ...nbdnl.ConnectOption) (uint32, error)
	// openConnected opens the attached device for the mount's descriptor.
	openConnected func(path string) (*os.File, error)

	// openForSession, configure and blockSignals run on an ioctl session's
	// thread, in that order, before it enters NBD_DO_IT. configure claims the
	// device with the first NBD_SET_SOCK and reports whether that took.
	openForSession func(index uint32) (*os.File, error)
	configure      func(fd uintptr, cfg ioctlAttachConfig, sockFds []int) (claimed bool, err error)
	blockSignals   func() (restore func() error, err error)
	// deviceStarted reports the attached device as started.
	deviceStarted func(index uint32) bool
	// requestDisconnect issues NBD_DISCONNECT on a private duplicate of the
	// session's descriptor, and closeCtl closes the session's own duplicate.
	requestDisconnect func(fd int) error
	closeCtl          func(fd int) error
}

func linuxDeviceOps() *deviceOps {
	return &deviceOps{
		netlinkConnect: func(index uint32, socks []*os.File, size uint64, serverFlags nbdnl.ServerFlags, opts ...nbdnl.ConnectOption) (uint32, error) {
			return nbdnl.Connect(index, socks, size, 0, serverFlags, opts...)
		},
		openConnected: os.Open,
		openForSession: func(index uint32) (*os.File, error) {
			return os.OpenFile(GetDevicePath(index), os.O_RDWR, 0)
		},
		configure:     configureIoctlDevice,
		blockSignals:  blockParkedThreadSignals,
		deviceStarted: ioctlDeviceStarted,
		requestDisconnect: func(fd int) error {
			return nbdIoctl(uintptr(fd), nbdIoctlDisconnect, 0)
		},
		closeCtl: unix.Close,
	}
}

// blockParkedThreadSignals masks parkedThreadSigmask on the calling thread
// and returns the restore of its previous mask.
func blockParkedThreadSignals() (func() error, error) {
	var old unix.Sigset_t
	if err := unix.PthreadSigmask(unix.SIG_BLOCK, &parkedThreadSigmask, &old); err != nil {
		return nil, err
	}

	return func() error { return unix.PthreadSigmask(unix.SIG_SETMASK, &old, nil) }, nil
}
