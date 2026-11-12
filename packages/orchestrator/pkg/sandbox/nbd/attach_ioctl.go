//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The kernel's ioctl interface to the NBD driver (include/uapi/linux/nbd.h).
// Unlike the netlink interface it takes no process-wide lock, so attaches of
// different devices proceed in parallel; the price is one OS thread parked in
// NBD_DO_IT for the life of the attachment.
const (
	nbdIoctlSetSock       = 0xab00
	nbdIoctlSetBlksize    = 0xab01
	nbdIoctlDoIt          = 0xab03
	nbdIoctlClearSock     = 0xab04
	nbdIoctlSetSizeBlocks = 0xab07
	nbdIoctlDisconnect    = 0xab08
	nbdIoctlSetTimeout    = 0xab09
	nbdIoctlSetFlags      = 0xab0a

	// ioctlStartTimeout bounds the wait for the kernel to report the device
	// started after NBD_DO_IT was entered; the driver sets the capacity last.
	ioctlStartTimeout = 10 * time.Second

	// threadLimitProbe is a limit no process reaches, used to read the current one.
	threadLimitProbe = 1 << 24
)

var (
	errIoctlDeadconnTimeout = errors.New("the ioctl attach has no dead-connection timeout; only the netlink attach supports a nonzero one")
	errIoctlInvalidGeometry = errors.New("the device size is not a multiple of its block size")
	errIoctlInvalidConfig   = errors.New("the attach needs at least one socket and a nonzero size")
	errIoctlSignalMask      = errors.New("the NBD_DO_IT thread could not block signals")
	errIoctlSocketDup       = errors.New("the session could not duplicate its sockets")
	// errIoctlDeviceTaken is a device another owner holds: the attach retries
	// with another device, as a netlink attach does on EBUSY.
	errIoctlDeviceTaken  = errors.New("the device is held by another owner")
	errIoctlSessionEnded = errors.New("NBD_DO_IT returned before the device started")
	errIoctlStartTimeout = errors.New("the device did not start in time")

	nbdAttachCounter = utils.Must(meter.Int64Counter("orchestrator.nbd.device.attach",
		metric.WithDescription("NBD device attaches by kernel interface and outcome: success, taken (a lost device race, retried), cancelled (the caller went away) or failure."),
		metric.WithUnit("{attach}"),
	))
	nbdAttachDuration = utils.Must(meter.Int64Histogram("orchestrator.nbd.device.attach.duration",
		metric.WithDescription("Time from a device grant to a successful NBD attach, by kernel interface; the lock contention the ioctl interface removes shows here."),
		metric.WithUnit("ms"),
	))
	nbdIoctlSessionEndedCounter = utils.Must(meter.Int64Counter("orchestrator.nbd.device.ioctl_session.ended",
		metric.WithDescription("Returns of NBD_DO_IT by cause: a disconnect this process requested, or the kernel ending the session on its own after a socket loss, a request timeout or an interrupted wait."),
		metric.WithUnit("{session}"),
	))
)

// ioctlAttachConfig is what one ioctl attach needs; the values mirror the
// netlink attach's.
type ioctlAttachConfig struct {
	index     uint32
	socks     []*os.File
	size      uint64
	blockSize uint64
	timeout   time.Duration
	deadconn  time.Duration
	flags     uint64
	// maxDevices is the module's device count, which bounds the parked threads.
	maxDevices uint
	// afterConfigure runs on the session thread once the device is configured
	// and before the attach learns of it; closeCtl closes the session's
	// duplicate descriptor and disconnectIoctl issues the disconnect request.
	// Only tests set them.
	afterConfigure  func(index uint32)
	closeCtl        func(fd int) error
	disconnectIoctl func(fd int) error
	// started reports the device as started; nil reads sysfs. Only tests set
	// it and beforeConfigure, which runs on the session thread after the
	// device open and before the first NBD_SET_SOCK.
	started         func(index uint32) bool
	beforeConfigure func(index uint32)
}

// ioctlSessionHeldError is an attach failure that left its session holding
// the device: deviceReleased closes once the session has let go of it.
type ioctlSessionHeldError struct {
	err            error
	deviceReleased <-chan struct{}
}

func (e *ioctlSessionHeldError) Error() string { return e.err.Error() }

func (e *ioctlSessionHeldError) Unwrap() error { return e.err }

// ioctlSession is one device attached through NBD_DO_IT. The ioctl returns
// only when the kernel has ended the session, so a dedicated goroutine, locked
// to its OS thread, holds it for the life of the attachment.
type ioctlSession struct {
	index  uint32
	logger logger.Logger
	// sockFds are the session's own duplicates of the kernel's socket ends,
	// handed to the driver and shut down by disconnect (the ioctl disconnect
	// only asks the server to close, where the netlink one shuts the sockets
	// down in the kernel): the caller may close its files at any time once
	// it has left, so the session touches only descriptors it owns.
	sockFds []int

	// tid is the parked thread, for tests that signal it directly.
	tid int

	// unmasked leaves the parked thread's signals deliverable; only tests set
	// it, to show what the mask prevents.
	unmasked bool

	disconnectRequested atomic.Bool
	// endedByKernel records, the moment NBD_DO_IT returned, that nobody in
	// this process had asked for the session to end.
	endedByKernel   atomic.Bool
	closeCtl        func(fd int) error
	disconnectIoctl func(fd int) error
	// ctl is a duplicate of the parked descriptor for the disconnect request,
	// so no new open of the device, and the partition scan one triggers, is
	// needed to end a session. It is -1 once closed; ctlMu orders that
	// against its use.
	ctlMu sync.Mutex
	ctl   int
	// requests counts disconnect requests still holding their own duplicate
	// of ctl. Each is added under ctlMu while ctl is open, so every add
	// precedes done.
	requests sync.WaitGroup
	// exited closes the moment NBD_DO_IT returns, before any cleanup, which
	// can stall; done closes once the device descriptors are closed too.
	// exitErr is set before done closes.
	exited  chan struct{}
	done    chan struct{}
	exitErr error
}

// attachIoctl configures the device and enters NBD_DO_IT on a dedicated thread,
// then waits for the kernel to report the device started. Every ioctl, SET_SOCK
// included, runs on that one thread: the driver records the task that set the
// sockets up and refuses the rest from any other.
func attachIoctl(ctx context.Context, cfg ioctlAttachConfig, lg logger.Logger, unmasked bool) (*ioctlSession, error) {
	if cfg.deadconn != 0 {
		return nil, errIoctlDeadconnTimeout
	}
	if len(cfg.socks) == 0 || cfg.size == 0 {
		return nil, fmt.Errorf("%w: %d sockets, size %d", errIoctlInvalidConfig, len(cfg.socks), cfg.size)
	}
	if cfg.blockSize == 0 || cfg.size%cfg.blockSize != 0 {
		return nil, fmt.Errorf("%w: size %d, block size %d", errIoctlInvalidGeometry, cfg.size, cfg.blockSize)
	}
	ensureThreadLimit(ctx, lg, cfg.maxDevices)
	s := &ioctlSession{
		index:           cfg.index,
		logger:          lg,
		unmasked:        unmasked,
		ctl:             -1,
		exited:          make(chan struct{}),
		done:            make(chan struct{}),
		closeCtl:        cfg.closeCtl,
		disconnectIoctl: cfg.disconnectIoctl,
	}
	if s.closeCtl == nil {
		s.closeCtl = unix.Close
	}
	if s.disconnectIoctl == nil {
		s.disconnectIoctl = func(fd int) error { return nbdIoctl(uintptr(fd), nbdIoctlDisconnect, 0) }
	}
	for i, sock := range cfg.socks {
		fd, err := unix.FcntlInt(sock.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			s.closeSocks()

			return nil, fmt.Errorf("%w: socket %d for device %d: %w", errIoctlSocketDup, i, cfg.index, err)
		}
		s.sockFds = append(s.sockFds, fd)
	}
	configured := make(chan error, 1)
	go s.run(cfg, configured) //nolint:contextcheck // the session outlives the attach: it ends with the device, not with the caller

	// Configuration is a handful of ioctls, but the device open and each of
	// them can wait on the device's lock (a probe or a teardown of the same
	// device holds it), so a cancelled caller does not wait for it: the
	// session it abandons is ended by whoever sees it finish configuring.
	if err := s.awaitConfigured(ctx, configured); err != nil {
		return nil, err
	}

	started := cfg.started
	if started == nil {
		started = ioctlDeviceStarted
	}
	deadline := time.Now().Add(ioctlStartTimeout)
	for !started(cfg.index) {
		select {
		case <-s.exited:
			return nil, s.held(s.endedError())
		case <-ctx.Done():
			s.endEarly(ctx)

			return nil, s.held(ctx.Err())
		default:
		}
		if time.Now().After(deadline) {
			s.endEarly(ctx)

			return nil, s.held(fmt.Errorf("%w: device %d, %s", errIoctlStartTimeout, cfg.index, ioctlStartTimeout))
		}
		time.Sleep(100 * time.Microsecond)
	}
	// A device that reads as started may belong to a session that has already
	// ended, when another opener keeps its attributes; it is not handed back.
	select {
	case <-s.exited:
		return nil, s.held(s.endedError())
	default:
	}
	// A caller that went away while the device started gets no device.
	if err := ctx.Err(); err != nil {
		s.endEarly(ctx)

		return nil, s.held(err)
	}

	return s, nil
}

// held reports an attach failure that leaves the session to end on its own.
func (s *ioctlSession) held(err error) error {
	return &ioctlSessionHeldError{err: err, deviceReleased: s.deviceReleased()}
}

// deviceReleased closes once the session holds no descriptor of the device:
// its thread has closed its own and no disconnect request is still in flight
// on a duplicate. Until then the device can read as free in sysfs while the
// session still configures it or ends it.
func (s *ioctlSession) deviceReleased() <-chan struct{} {
	released := make(chan struct{})
	go func() {
		<-s.done
		s.requests.Wait()
		close(released)
	}()

	return released
}

// awaitConfigured waits for the session to finish configuring, or for the
// caller to go away. A configuration already finished is taken first, so an
// abandoned session is ended here when it can be; one still configuring when
// the caller leaves is ended by a goroutine once it finishes, on descriptors
// the session owns, and the error tells the caller when the device is let go.
func (s *ioctlSession) awaitConfigured(ctx context.Context, configured <-chan error) error {
	select {
	case err := <-configured:
		return s.configuredOrAbandoned(ctx, err)
	default:
	}
	select {
	case err := <-configured:
		return s.configuredOrAbandoned(ctx, err)
	case <-ctx.Done():
		go func() {
			if err := <-configured; err == nil {
				s.endEarly(context.WithoutCancel(ctx))
			}
		}()

		return s.held(ctx.Err())
	}
}

// configuredOrAbandoned ends a configured session whose caller has gone.
func (s *ioctlSession) configuredOrAbandoned(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		s.endEarly(ctx)

		return s.held(err)
	}

	return nil
}

// endEarly asks the kernel to end a session that will not be used and waits for
// its thread, so the caller's unwind finds the device free.
func (s *ioctlSession) endEarly(ctx context.Context) {
	if err := s.disconnect(context.WithoutCancel(ctx), disconnectTimeout); err != nil {
		s.logger.Warn(ctx, "failed to end an unused ioctl NBD session", zap.Uint32("device_index", s.index), zap.Error(err))
	}
}

// endedError is the error a session that ended before it was handed back
// reports. It carries the exit error only once the cleanup that publishes it
// has finished; a cleanup still held on the device is not waited for.
func (s *ioctlSession) endedError() error {
	select {
	case <-s.done:
		if s.exitErr != nil {
			return fmt.Errorf("%w: %w", errIoctlSessionEnded, s.exitErr)
		}
	default:
	}

	return errIoctlSessionEnded
}

// expectEnd records that this process is ending the session, before anything
// that makes NBD_DO_IT return: the mount's Close closes the server sockets
// first, which the kernel sees before any disconnect request.
func (s *ioctlSession) expectEnd() { s.disconnectRequested.Store(true) }

func (s *ioctlSession) run(cfg ioctlAttachConfig, configured chan<- error) {
	// A goroutine that exits while locked takes its thread with it; the
	// thread is returned to the runtime only when its signal mask is known good.
	runtime.LockOSThread()
	unlock := true
	defer func() {
		if unlock {
			runtime.UnlockOSThread()
		}
	}()

	f, err := os.OpenFile(GetDevicePath(cfg.index), os.O_RDWR, 0)
	if err != nil {
		s.failConfigure(configured, fmt.Errorf("open device %d: %w", cfg.index, err))

		return
	}
	fd := f.Fd()
	if cfg.beforeConfigure != nil {
		cfg.beforeConfigure(cfg.index)
	}
	if claimed, err := configureIoctlDevice(fd, cfg, s.sockFds); err != nil {
		// A clear is honoured on another owner's live device too, so it runs
		// only once this session's own claim succeeded.
		if claimed {
			_ = nbdIoctl(fd, nbdIoctlClearSock, 0)
		}
		_ = f.Close()
		s.failConfigure(configured, err)

		return
	}
	ctl, err := unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		_ = nbdIoctl(fd, nbdIoctlClearSock, 0)
		_ = f.Close()
		s.failConfigure(configured, fmt.Errorf("duplicate the device descriptor %d: %w", cfg.index, err))

		return
	}
	s.ctlMu.Lock()
	s.ctl = ctl
	s.ctlMu.Unlock()
	s.tid = unix.Gettid()

	// NBD_DO_IT waits interruptibly; a signal delivered to this thread makes the
	// driver shut the device's sockets down, and the restarted ioctl then fails
	// with EBUSY. The Go runtime sends this thread SIGURG for preemption and
	// the process receives SIGCHLD for every child, so the thread blocks every
	// signal it can while parked, and an attach whose thread cannot is refused.
	// Synchronous faults stay deliverable, they cannot occur inside the syscall
	// and must not be masked while Go runs, and so do the C library's
	// thread-management signals, 32 to 34, whose broadcasts wait for every
	// thread; Go's own all-threads syscall uses 33 too, so such a broadcast
	// would end the parked sessions rather than hang, and the orchestrator
	// makes none. The mask covers the ioctl and the few lines around it.
	var old unix.Sigset_t
	if !s.unmasked {
		if err := unix.PthreadSigmask(unix.SIG_BLOCK, &parkedThreadSigmask, &old); err != nil {
			_ = nbdIoctl(fd, nbdIoctlClearSock, 0)
			_ = f.Close()
			s.ctlMu.Lock()
			s.ctl = -1
			s.ctlMu.Unlock()
			_ = s.closeCtl(ctl)
			s.failConfigure(configured, fmt.Errorf("%w: device %d: %w", errIoctlSignalMask, cfg.index, err))

			return
		}
	}
	if cfg.afterConfigure != nil {
		cfg.afterConfigure(cfg.index)
	}
	configured <- nil

	exitErr := nbdIoctl(fd, nbdIoctlDoIt, 0)
	// Classified and announced now, before any cleanup a later Close could
	// overlap or a stalled device could hold.
	requested := s.disconnectRequested.Load()
	s.endedByKernel.Store(!requested)
	close(s.exited)
	if !s.unmasked {
		if err := unix.PthreadSigmask(unix.SIG_SETMASK, &old, nil); err != nil {
			// The thread ends with this goroutine instead of carrying the mask into the runtime.
			unlock = false
			s.logger.Error(context.Background(), "failed to restore the signal mask after NBD_DO_IT, retiring the thread", zap.Uint32("device_index", cfg.index), zap.Error(err))
		}
	}

	ctx := context.Background()
	// NBD_CLEAR_SOCK fails the requests still queued on the dead sockets at
	// once; left to the last close they would wait out the request timeout.
	if err := nbdIoctl(fd, nbdIoctlClearSock, 0); err != nil {
		s.logger.Warn(ctx, "NBD_CLEAR_SOCK failed after NBD_DO_IT returned", zap.Uint32("device_index", cfg.index), zap.Error(err))
	}
	if err := f.Close(); err != nil {
		s.logger.Warn(ctx, "failed to close the NBD_DO_IT descriptor", zap.Uint32("device_index", cfg.index), zap.Error(err))
	}
	// The duplicate shares the open file; the device is released only once
	// both are closed. The close runs outside the lock: a block device's last
	// close can wait on the device, and a disconnect request must not.
	s.ctlMu.Lock()
	ctl, s.ctl = s.ctl, -1
	s.ctlMu.Unlock()
	if ctl >= 0 {
		_ = s.closeCtl(ctl)
	}
	s.closeSocks()

	cause := "requested"
	if !requested {
		cause = "kernel"
		// The guest's device is gone: later requests fail with EIO. The mount's
		// Close still runs the teardown, so nothing else is needed here.
		s.logger.Error(ctx, "the kernel ended an NBD_DO_IT session nobody disconnected",
			zap.Uint32("device_index", cfg.index), zap.Error(exitErr))
	}
	nbdIoctlSessionEndedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("cause", cause)))
	s.exitErr = exitErr
	close(s.done)
}

// failConfigure ends a session that never reached NBD_DO_IT.
func (s *ioctlSession) failConfigure(configured chan<- error, err error) {
	s.closeSocks()
	configured <- err
	close(s.exited)
	close(s.done)
}

// closeSocks closes the session's duplicates of the socket ends. They are
// taken under the lock a disconnect shuts them down under, so a shutdown
// never reaches a closed, possibly reused, descriptor.
func (s *ioctlSession) closeSocks() {
	s.ctlMu.Lock()
	fds := s.sockFds
	s.sockFds = nil
	s.ctlMu.Unlock()
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

// configureIoctlDevice sets the sockets, then the geometry, timeout and
// flags. The first socket is the claim: the driver refuses it with EBUSY on a
// device another task set up or netlink bound, while it would apply the
// geometry to such a device. claimed reports whether that first socket took.
func configureIoctlDevice(fd uintptr, cfg ioctlAttachConfig, sockFds []int) (claimed bool, err error) {
	for i, sock := range sockFds {
		if err := nbdIoctl(fd, nbdIoctlSetSock, uintptr(sock)); err != nil {
			// Another task's device answers EBUSY; a netlink-bound one answers
			// EINVAL, which only a connected device turns into a lost race.
			if !claimed && (errors.Is(err, unix.EBUSY) || (errors.Is(err, unix.EINVAL) && ioctlDeviceConnected(cfg.index))) {
				return false, fmt.Errorf("%w: NBD_SET_SOCK on device %d: %w", errIoctlDeviceTaken, cfg.index, err)
			}

			return claimed, fmt.Errorf("NBD_SET_SOCK %d on device %d: %w", i, cfg.index, err)
		}
		claimed = true
	}
	steps := []struct {
		name string
		req  uintptr
		arg  uintptr
	}{
		{"NBD_SET_BLKSIZE", nbdIoctlSetBlksize, uintptr(cfg.blockSize)},
		{"NBD_SET_SIZE_BLOCKS", nbdIoctlSetSizeBlocks, uintptr(cfg.size / cfg.blockSize)},
		{"NBD_SET_TIMEOUT", nbdIoctlSetTimeout, uintptr(cfg.timeout / time.Second)},
		{"NBD_SET_FLAGS", nbdIoctlSetFlags, uintptr(cfg.flags)},
	}
	for _, step := range steps {
		if err := nbdIoctl(fd, step.req, step.arg); err != nil {
			return true, fmt.Errorf("%s on device %d: %w", step.name, cfg.index, err)
		}
	}

	return true, nil
}

// parkedThreadSigmask is every signal but the synchronous faults and the C
// library's internal ones (32 to 34: cancellation and the set-id broadcast).
// A mask does not cover stops and traps: a ptrace attach, SIGSTOP or a cgroup
// freeze still wakes the parked thread, and the driver ends the session.
var parkedThreadSigmask = func() unix.Sigset_t {
	var set unix.Sigset_t
	keep := map[int]bool{
		int(unix.SIGSEGV): true, int(unix.SIGBUS): true, int(unix.SIGFPE): true, int(unix.SIGILL): true,
		int(unix.SIGTRAP): true, int(unix.SIGABRT): true, int(unix.SIGSYS): true,
		32: true, 33: true, 34: true,
	}
	for sig := 1; sig < 65; sig++ {
		if keep[sig] {
			continue
		}
		set.Val[(sig-1)/64] |= 1 << (uint(sig-1) % 64)
	}

	return set
}()

var (
	threadLimitMu     sync.Mutex
	threadLimitRaised bool
)

// ensureThreadLimit raises the runtime's thread limit, once, by one thread
// per device the module can expose: the parked threads come on top of what
// the rest of the process already needs, which the previous limit budgeted
// (every sandbox keeps threads of its own blocked in syscalls), so the raise
// is additive rather than a fixed count the rest has to fit under.
func ensureThreadLimit(ctx context.Context, lg logger.Logger, maxDevices uint) {
	threadLimitMu.Lock()
	defer threadLimitMu.Unlock()
	if threadLimitRaised {
		return
	}
	// The limit is never lowered, even for an instant: setting it below the
	// threads alive ends the process. The previous value is learned by setting
	// a limit no process reaches, and the raised one is set in its place.
	previous := debug.SetMaxThreads(threadLimitProbe)
	final := previous + int(maxDevices)
	debug.SetMaxThreads(final)
	lg.Info(ctx, "raised the thread limit for ioctl NBD attaches",
		zap.Int("threads", final), zap.Int("previous", previous), zap.Uint("devices", maxDevices))
	threadLimitRaised = true
}

// disconnect asks the kernel to end the session and waits for the parked
// thread to return and the device to read as free, within timeout.
func (s *ioctlSession) disconnect(ctx context.Context, timeout time.Duration) error {
	s.disconnectRequested.Store(true)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-s.done:
		// The kernel ended the session already; only the device state is left to wait for.
	default:
		// The disconnect request marks the session as ending on the kernel's
		// side and asks the server to close. Whatever it returns, the shutdown
		// of the sockets below is what makes the receive workers return.
		s.requestDisconnect(ctx)
		s.ctlMu.Lock()
		for _, sock := range s.sockFds {
			if err := unix.Shutdown(sock, unix.SHUT_RDWR); err != nil && !errors.Is(err, unix.ENOTCONN) {
				s.logger.Warn(ctx, "failed to shut down an NBD socket", zap.Uint32("device_index", s.index), zap.Error(err))
			}
		}
		s.ctlMu.Unlock()
		select {
		case <-s.done:
		case <-ctx.Done():
			return fmt.Errorf("device %d: NBD_DO_IT did not return: %w", s.index, ctx.Err())
		}
	}

	for ioctlDeviceConnected(s.index) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("device %d still connected after NBD_DO_IT returned: %w", s.index, ctx.Err())
		default:
		}
		time.Sleep(100 * time.Microsecond)
	}

	return nil
}

// requestDisconnect issues NBD_DISCONNECT within ctx's bound on a private
// duplicate of the session descriptor, so a request the driver holds on the
// device's lock blocks neither the session's own close nor the caller.
func (s *ioctlSession) requestDisconnect(ctx context.Context) {
	s.ctlMu.Lock()
	ctl := -1
	if s.ctl >= 0 {
		var err error
		if ctl, err = unix.FcntlInt(uintptr(s.ctl), unix.F_DUPFD_CLOEXEC, 0); err != nil {
			s.logger.Warn(ctx, "failed to duplicate the NBD device descriptor for the disconnect request", zap.Uint32("device_index", s.index), zap.Error(err))
			ctl = -1
		} else {
			s.requests.Add(1)
		}
	}
	s.ctlMu.Unlock()
	if ctl < 0 {
		return
	}
	result := make(chan error, 1)
	go func() {
		defer s.requests.Done()
		err := s.disconnectIoctl(ctl)
		_ = unix.Close(ctl)
		result <- err
	}()
	select {
	case err := <-result:
		// ENOTCONN means the sockets were already gone; EINVAL that the driver
		// does not consider the device ioctl-controlled. The shutdown that
		// follows ends this process's session either way.
		if err != nil && !errors.Is(err, unix.ENOTCONN) {
			s.logger.Warn(ctx, "NBD_DISCONNECT failed, shutting the sockets down instead", zap.Uint32("device_index", s.index), zap.Error(err))
		}
	case <-ctx.Done():
		s.logger.Warn(ctx, "NBD_DISCONNECT did not return in time, shutting the sockets down instead", zap.Uint32("device_index", s.index))
	}
}

func nbdIoctl(fd uintptr, req, arg uintptr) error {
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, fd, req, arg); errno != 0 {
		return errno
	}

	return nil
}

// ioctlDeviceConnected is the driver's pid attribute, present from
// nbd_start_device until the configuration is released.
func ioctlDeviceConnected(index uint32) bool {
	b, err := os.ReadFile(fmt.Sprintf("%s/nbd%d/pid", sysBlockDir, index))

	return err == nil && strings.TrimSpace(string(b)) != ""
}

// ioctlDeviceStarted is the device with its capacity set, which the driver
// does after it creates the pid attribute.
func ioctlDeviceStarted(index uint32) bool {
	if !ioctlDeviceConnected(index) {
		return false
	}
	b, err := os.ReadFile(fmt.Sprintf("%s/nbd%d/size", sysBlockDir, index))

	return err == nil && strings.TrimSpace(string(b)) != "0"
}
