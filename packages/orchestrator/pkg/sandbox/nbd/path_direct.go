//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Merovius/nbd/nbdnl"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	// ioTimeout is the per-request timeout for the kernel NBD driver.
	// Must be greater than the backend fetch timeout (60s in streaming_chunk.go)
	// so the dispatch handler has time to respond before the kernel declares
	// the connection dead and returns EIO to the guest.
	ioTimeout = 90 * time.Second

	// The kernel waits here only for a replacement NBD socket. This mount never
	// reconnects: after its last connection is lost, fail queued I/O instead of
	// waiting for a recovery path that does not exist. Keep the separate 90s
	// request timeout so a healthy slow backend still has time to respond.
	deadconnTimeout = 0

	// disconnectTimeout should not be necessary if the disconnect is reliable
	disconnectTimeout = 30 * time.Second

	// deviceCloseWarnThreshold flags a stalled descriptor release: through a
	// live data path the flush and close finish well under a second.
	deviceCloseWarnThreshold = 5 * time.Second
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd")

var (
	nbdFlushCounter = utils.Must(meter.Int64Counter("orchestrator.nbd.device.flush",
		metric.WithDescription("Flushes of the kernel NBD device buffers, by outcome. A sync failure means the backend is missing writes the guest was told had landed, so a snapshot exported from it is incomplete."),
		metric.WithUnit("{flush}"),
	))
	// nbdUnwoundCounter is the only signal that a connect which cannot be handed
	// back was torn down rather than stranded. Without it neither the rate of the
	// failure nor the effect of handling it is visible: the pool counters answer
	// the class-level question (acquired - released has a floor that should return
	// to zero), not which path lost a device.
	nbdUnwoundCounter = utils.Must(meter.Int64Counter("orchestrator.nbd.device.unwound",
		metric.WithDescription("Connected NBD devices torn down by Open because it cannot return them, by the stage that failed. Each count is a kernel device and a pool slot that would otherwise be held for the life of the process."),
		metric.WithUnit("{device}"),
	))

	nbdSlowCloseCounter = utils.Must(meter.Int64Counter("orchestrator.nbd.device.close.slow",
		metric.WithDescription("NBD device descriptor releases slower than the watchdog threshold, tagged with the teardown step in progress when the watchdog fired. Close flushes writeback through the descriptor before releasing it, so a slow release is a teardown blocked behind commands the backend is not answering."),
		metric.WithUnit("{close}"),
	))

	nbdCloseDuration = utils.Must(meter.Int64Histogram("orchestrator.nbd.device.close.duration",
		metric.WithDescription("Time to flush writeback and close the NBD device descriptor during mount teardown. The stall watchdog warns past its threshold; this carries the full distribution."),
		metric.WithUnit("ms"),
	))

	nbdFlushSuccess           = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "success")))
	nbdFlushOpenFailure       = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "failure"), attribute.String("stage", "open")))
	nbdFlushSyncFailure       = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "failure"), attribute.String("stage", "sync")))
	nbdFlushInvalidateFailure = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "failure"), attribute.String("stage", "invalidate")))
)

type DirectPathMount struct {
	cancelfn     context.CancelFunc
	devicePool   *DevicePool
	featureFlags *featureflags.Client

	logger logger.Logger

	Backend         block.Device
	deviceIndex     uint32
	blockSize       uint64
	ioTimeout       time.Duration
	deadconnTimeout time.Duration

	// deviceFile is held open for the whole life of the mount so Flush can see
	// every writeback error, not just the ones its own sync produces. The kernel
	// samples the block device's writeback-error sequence when a descriptor is
	// opened and reports only errors recorded after that sample, so a descriptor
	// opened at flush time is blind to a backend failure that happened while the
	// guest was running - which is the failure worth reporting.
	deviceFile *os.File

	// ioctlSession is set when the device was attached through the ioctl
	// interface; Close then detaches through the same interface.
	ioctlSession *ioctlSession
	// ioctlUnmasked leaves the parked NBD_DO_IT thread's signals deliverable,
	// and ioctlAfterConfigure runs once the device is configured; only tests
	// set them.
	ioctlUnmasked        bool
	ioctlAfterConfigure  func(deviceIndex uint32)
	ioctlCloseCtl        func(fd int) error
	ioctlDisconnectIoctl func(fd int) error
	ioctlAbandoned       func(*ioctlSession)
	// openDeviceFn opens the connected device; only tests set it.
	openDeviceFn func(path string) (*os.File, error)
	// deferredReleaseTimeout bounds the release of a slot whose ioctl session
	// outlived its disconnect; zero is the pool's own close bound. Only tests
	// set it, and deferredReleaseDone, which receives that release's result.
	deferredReleaseTimeout time.Duration
	deferredReleaseDone    chan error

	// afterConnect runs as soon as nbdnl.Connect has succeeded, before the
	// wait-for-connected poll. Only tests set it, to cancel the context inside
	// the window where the kernel holds the device but Open has not returned it.
	afterConnect func(deviceIndex uint32)

	// Tests replace device operations without connecting a kernel NBD device.
	// Production leaves these nil and uses the same disconnect and pool calls.
	disconnectDeviceFn func(ctx context.Context, deviceIndex uint32) error
	releaseDeviceFn    func(ctx context.Context, deviceIndex uint32) error
	socketPairFn       func() ([2]int, error)

	dispatchers []*Dispatch
	socksClient []*os.File
	socksServer []io.Closer

	handlersWg sync.WaitGroup
}

// MountOption configures a DirectPathMount.
type MountOption func(*DirectPathMount)

// WithIOTimeout overrides the kernel NBD I/O timeout (default 90s).
func WithIOTimeout(d time.Duration) MountOption {
	return func(m *DirectPathMount) { m.ioTimeout = d }
}

// WithDeadconnTimeout overrides the kernel NBD reconnect wait (default 0).
func WithDeadconnTimeout(d time.Duration) MountOption {
	return func(m *DirectPathMount) { m.deadconnTimeout = d }
}

func NewDirectPathMount(b block.Device, devicePool *DevicePool, featureFlags *featureflags.Client, lg logger.Logger, opts ...MountOption) *DirectPathMount {
	m := &DirectPathMount{
		Backend:         b,
		blockSize:       4096,
		devicePool:      devicePool,
		featureFlags:    featureFlags,
		logger:          lg,
		socksClient:     make([]*os.File, 0),
		socksServer:     make([]io.Closer, 0),
		deviceIndex:     math.MaxUint32,
		ioTimeout:       ioTimeout,
		deadconnTimeout: deadconnTimeout,
	}

	for _, o := range opts {
		o(m)
	}

	return m
}

func (d *DirectPathMount) Open(ctx context.Context) (retDeviceIndex uint32, err error) {
	// The connect + wait-for-connected poll loop (and device-pool contention) are
	// otherwise invisible; both add up when this is opened once per measure/resize.
	ctx, span := tracer.Start(ctx, "direct-path-mount-open")

	// The dispatchers take a context only Close cancels: they serve the
	// writeback flush Close runs before that cancel, and a dispatcher bound
	// to the caller's context returns at its next request - that flush's
	// write - leaving the flush to the kernel's connection timeouts and EIO.
	handlerCtx, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	d.cancelfn = cancelHandlers

	defer func() {
		// Set the device index to the one returned, correctly capture error values
		d.deviceIndex = retDeviceIndex
		span.SetAttributes(attribute.Int64("nbd.device_index", int64(retDeviceIndex)))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		d.logger.Debug(ctx, "opening direct path mount", zap.Uint32("device_index", d.deviceIndex), zap.Error(err))
	}()

	telemetry.ReportEvent(ctx, "opening direct path mount")

	size, err := d.Backend.Size(ctx)
	if err != nil {
		return math.MaxUint32, err
	}

	telemetry.ReportEvent(ctx, "got backend size")

	var deviceIndex uint32

	// One attach uses one interface, whatever the flag says mid-retry.
	viaIoctl := d.featureFlags.BoolFlag(ctx, featureflags.NBDAttachViaIoctlFlag)

	for {
		deviceIndex, err = d.devicePool.GetDevice(ctx)
		if err != nil {
			return math.MaxUint32, err
		}

		telemetry.ReportEvent(ctx, "got device index")

		attachStart := time.Now()
		d.socksClient = make([]*os.File, 0)
		d.socksServer = make([]io.Closer, 0)
		d.dispatchers = make([]*Dispatch, 0)

		connections := d.featureFlags.IntFlag(ctx, featureflags.NBDConnectionsPerDevice)
		asyncWriteZeroes := d.featureFlags.BoolFlag(ctx, featureflags.NBDAsyncWriteZeroesFlag)

		for i := range connections {
			// Create the socket pairs
			sockPair, err := d.socketPair()
			if err != nil {
				closeErr := closeSocketPairs(d.socksClient, d.socksServer)
				releaseErr := d.devicePool.ReleaseDevice(ctx, deviceIndex)

				return math.MaxUint32, errors.Join(err, closeErr, releaseErr)
			}

			client := os.NewFile(uintptr(sockPair[0]), "client")
			server := os.NewFile(uintptr(sockPair[1]), "server")
			serverc, err := net.FileConn(server)
			if err != nil {
				// Close the current iteration's FDs (not yet added to d.socksClient/d.socksServer)
				client.Close()
				server.Close()
				closeErr := closeSocketPairs(d.socksClient, d.socksServer)
				releaseErr := d.devicePool.ReleaseDevice(ctx, deviceIndex)

				return math.MaxUint32, errors.Join(err, closeErr, releaseErr)
			}
			server.Close()

			// Capture deviceIndex here — it's reassigned on each retry
			// iteration of the outer for-loop (not a range loop, so Go 1.22+
			// loop variable fix doesn't apply).
			connLogger := d.logger.With(
				zap.Uint32("device_index", deviceIndex),
				zap.Int("socket_index", i),
			)

			dispatch := NewDispatch(serverc, d.Backend, asyncWriteZeroes, connLogger)
			// Start reading commands on the socket and dispatching them to our provider
			d.handlersWg.Go(func() {
				handleErr := dispatch.Handle(handlerCtx)
				// The error is expected to happen if the nbd (socket connection) is closed
				connLogger.Info(handlerCtx, "closing handler for NBD commands", zap.Error(handleErr))
			})

			d.socksServer = append(d.socksServer, serverc)
			d.socksClient = append(d.socksClient, client)
			d.dispatchers = append(d.dispatchers, dispatch)
		}

		var opts []nbdnl.ConnectOption
		opts = append(opts, nbdnl.WithBlockSize(d.blockSize))
		opts = append(opts, nbdnl.WithTimeout(d.ioTimeout))
		opts = append(opts, nbdnl.WithDeadconnTimeout(d.deadconnTimeout))

		const flagSendWriteZeroes nbdnl.ServerFlags = 1 << 6 // NBD_FLAG_SEND_WRITE_ZEROES, not in nbdnl
		serverFlags := nbdnl.FlagHasFlags | nbdnl.FlagCanMulticonn |
			nbdnl.FlagSendTrim | flagSendWriteZeroes

		var idx uint32
		var connectErr error
		attachInterface := "netlink"
		if viaIoctl {
			attachInterface = "ioctl"
			var session *ioctlSession
			session, connectErr = attachIoctl(ctx, ioctlAttachConfig{
				index:           deviceIndex,
				socks:           d.socksClient,
				size:            uint64(size),
				blockSize:       d.blockSize,
				timeout:         d.ioTimeout,
				deadconn:        d.deadconnTimeout,
				flags:           uint64(serverFlags),
				maxDevices:      d.devicePool.MaxDevices(),
				afterConfigure:  d.ioctlAfterConfigure,
				closeCtl:        d.ioctlCloseCtl,
				disconnectIoctl: d.ioctlDisconnectIoctl,
				abandoned:       d.ioctlAbandoned,
			}, d.logger, d.ioctlUnmasked)
			if connectErr == nil {
				idx = deviceIndex
				d.ioctlSession = session
			}
		} else {
			idx, connectErr = nbdnl.Connect(deviceIndex, d.socksClient, uint64(size), 0, serverFlags, opts...)
		}
		nbdAttachCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("interface", attachInterface),
			attribute.String("result", attachResult(connectErr)),
		))
		if connectErr == nil {
			nbdAttachDuration.Record(ctx, time.Since(attachStart).Milliseconds(),
				metric.WithAttributes(attribute.String("interface", attachInterface)))
			// The idx should be the same as deviceIndex, because we are connecting to it,
			// but we will use the one returned by nbdnl
			deviceIndex = idx

			if d.afterConnect != nil {
				d.afterConnect(deviceIndex)
			}

			break
		}

		d.logger.Error(ctx, "error opening NBD, retrying", zap.Error(connectErr), zap.Uint32("device_index", deviceIndex))

		// Sometimes (rare), there seems to be a BADF error here. Lets just retry for now...
		// Close things down and try again...
		err := closeSocketPairs(d.socksClient, d.socksServer)
		if err != nil {
			d.logger.Error(ctx, "error closing socket pairs on error opening NBD", zap.Error(err))
		}
		// A Close after a failed Open must not close them again.
		d.socksClient, d.socksServer, d.dispatchers = nil, nil, nil

		// A device another owner holds, which both interfaces report with
		// EBUSY and the ioctl one also as a typed lost race, was never this
		// mount's to free: a release would find it in use and keep the slot
		// out of the pool for the life of the process, so the slot is given
		// up instead and the feeder offers it again once the device is free.
		// Every other failure releases the device, on a context that cannot
		// have ended because a cancelled caller is one of the failures, and
		// waits for a session still ending in the kernel within the same
		// bound a disconnect gets.
		taken := errors.Is(connectErr, errIoctlDeviceTaken)
		if !viaIoctl && (errors.Is(connectErr, unix.EBUSY) || strings.Contains(connectErr.Error(), "device or resource busy")) {
			taken = true
		}
		// A cancelled ioctl attach, or one whose session ended before the
		// device started, hands the device to that session's own teardown
		// (its run goroutine ends the session and frees the device). Open
		// gives the slot back rather than spin-releasing a device the session
		// still holds, or one another owner took when a lost claim's caller
		// had already cancelled; the feeder re-offers it once sysfs reads it
		// free. These do not retry.
		sessionOwnsTeardown := viaIoctl && (errors.Is(connectErr, context.Canceled) ||
			errors.Is(connectErr, context.DeadlineExceeded) || errors.Is(connectErr, errIoctlSessionEnded))
		if taken || sessionOwnsTeardown {
			d.devicePool.Disown(ctx, deviceIndex)
		} else {
			err = d.devicePool.ReleaseDevice(context.WithoutCancel(ctx), deviceIndex, WithInfiniteRetry(), WithTimeout(disconnectTimeout), WithLogger(d.logger))
			if err != nil {
				d.logger.Error(ctx, "error opening NBD, error releasing device", zap.Error(err), zap.Uint32("device_index", deviceIndex))
			}
		}
		if sessionOwnsTeardown {
			return math.MaxUint32, connectErr
		}

		// A lost device race retries with another device, whatever errno the
		// kernel chose for it; the other failures below are final.
		if !taken &&
			(strings.Contains(connectErr.Error(), "invalid argument") ||
				errors.Is(connectErr, errIoctlDeadconnTimeout) || errors.Is(connectErr, errIoctlInvalidGeometry) ||
				errors.Is(connectErr, errIoctlInvalidConfig) || errors.Is(connectErr, errIoctlSignalMask) ||
				errors.Is(connectErr, errIoctlSocketDup)) {
			return math.MaxUint32, connectErr
		}

		select {
		case <-ctx.Done():
			return math.MaxUint32, errors.Join(connectErr, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}

	// Wait until it's connected... An ioctl attach returns with the device
	// started, so only the netlink attach waits here.
	for d.ioctlSession == nil {
		select {
		case <-ctx.Done():
			closeErr := d.closeConnected(ctx, deviceIndex, "wait_connected")

			return math.MaxUint32, errors.Join(ctx.Err(), closeErr)
		default:
		}

		telemetry.ReportEvent(ctx, "waiting for NBD connection")

		s, err := nbdnl.Status(deviceIndex)
		if err == nil && s.Connected {
			break
		}

		time.Sleep(100 * time.Microsecond)
	}
	// Cancellation that landed after the connect, in afterConnect or while an
	// ioctl session started, still means the caller owns no device.
	if err := ctx.Err(); err != nil {
		closeErr := d.closeConnected(ctx, deviceIndex, "wait_connected")

		return math.MaxUint32, errors.Join(err, closeErr)
	}

	telemetry.ReportEvent(ctx, "connected to NBD")

	// Open the device now, while it is still empty, so the descriptor's
	// writeback-error sample predates every write the guest will make. Flush
	// syncs this descriptor; see the deviceFile field for why a fresh one there
	// would not do.
	devicePath := GetDevicePath(deviceIndex)

	openDevice := os.Open
	if d.openDeviceFn != nil {
		openDevice = d.openDeviceFn
	}
	deviceFile, err := openDevice(devicePath)
	if err != nil {
		closeErr := d.closeConnected(ctx, deviceIndex, "device_open")

		return math.MaxUint32, errors.Join(
			fmt.Errorf("error opening NBD device %s: %w", devicePath, err),
			closeErr,
		)
	}

	d.deviceFile = deviceFile

	// The open can wait on the device, for a partition scan among other
	// things, so a caller that went away during it still gets no device, and
	// neither does one whose session the kernel ended meanwhile.
	if err := ctx.Err(); err != nil {
		closeErr := d.closeConnected(ctx, deviceIndex, "device_open")

		return math.MaxUint32, errors.Join(err, closeErr)
	}
	if d.ioctlSession != nil {
		select {
		case <-d.ioctlSession.exited:
			closeErr := d.closeConnected(ctx, deviceIndex, "device_open")

			return math.MaxUint32, errors.Join(d.ioctlSession.endedError(), closeErr)
		default:
		}
	}

	return deviceIndex, nil
}

// closeConnected tears down a device that is connected but that Open cannot
// return. It hands the work to Close rather than unwinding by hand: Close
// cancels the handlers and drains the dispatchers before disconnecting, and
// releases the slot with infinite retry. Close reads the index off the mount,
// and Open's deferred assignment has not run yet, so set it first. The context
// Close is given has to be detached: the wait_connected caller reaches here
// only after Open's context ended, and on that context Close's disconnect
// wait and slot release return at once, with the device still connected and
// the slot still taken.
func (d *DirectPathMount) closeConnected(ctx context.Context, deviceIndex uint32, stage string) error {
	nbdUnwoundCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("stage", stage)))

	// Warn, not Debug: this is rare, it means a device and a pool slot came within
	// one branch of being stranded, and the build failure the caller reports does
	// not name either of them.
	d.logger.Warn(ctx, "tearing down a connected NBD device Open cannot return",
		zap.Uint32("device_index", deviceIndex),
		zap.String("stage", stage),
	)

	d.deviceIndex = deviceIndex

	return d.Close(context.WithoutCancel(ctx))
}

// Flush writes all pending data through the NBD connection and then clears the
// kernel's block-device buffers. Call this before reading or exporting the
// backend directly so it cannot observe writes that are still cached by Linux.
//
// The fsync is the part that can report a failure: writeback errors are
// recorded on the block device's mapping and handed to whoever fsyncs it, so a
// write the kernel acknowledged to the guest but could not deliver to the
// backend surfaces here and nowhere else. It syncs the descriptor Open kept -
// one opened here would only see errors from its own writeback, missing the
// backend failure that happened while the guest was running. BLKFLSBUF writes
// the device back as well, but blkdev_flushbuf() discards sync_blockdev()'s
// return value and always reports success, so it is kept for the invalidation
// that has to follow - by the time it runs, its own writeback has nothing left
// to do. The device is connected without NBD_FLAG_SEND_FLUSH (see Open), which
// leaves the queue without a write cache, so the block layer completes the
// empty flush bio itself and the fsync never turns into an NBD_CMD_FLUSH the
// dispatcher would reject.
func (d *DirectPathMount) Flush(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "direct-path-mount-flush")
	defer span.End()

	file := d.deviceFile
	if file == nil {
		nbdFlushCounter.Add(ctx, 1, nbdFlushOpenFailure)

		return errors.New("no NBD device open to flush")
	}

	syncErr := file.Sync()
	if syncErr != nil {
		// The backend is now missing writes the guest was told had landed, so
		// anything exported from it is incomplete. Mark the span - the counter
		// says how often this happens, the span says to which sandbox. The
		// error itself is logged by the caller that handles it.
		span.SetAttributes(attribute.Int64("nbd.device_index", int64(d.deviceIndex)))
		span.RecordError(syncErr)
		span.SetStatus(codes.Error, "NBD device writeback failed, backend is missing acknowledged writes")
	}

	invalidateErr := invalidateDevice(file)

	// One flush, one count, so the rate reads as a share of flushes. A lost
	// writeback outranks a stale cache when both fail.
	switch {
	case syncErr != nil:
		nbdFlushCounter.Add(ctx, 1, nbdFlushSyncFailure)
	case invalidateErr != nil:
		nbdFlushCounter.Add(ctx, 1, nbdFlushInvalidateFailure)
	default:
		nbdFlushCounter.Add(ctx, 1, nbdFlushSuccess)
	}

	var errs []error
	if syncErr != nil {
		errs = append(errs, fmt.Errorf("sync NBD device: %w", syncErr))
	}

	if invalidateErr != nil {
		errs = append(errs, fmt.Errorf("flush NBD device buffers: %w", invalidateErr))
	}

	return errors.Join(errs...)
}

// invalidateDevice drops the kernel's block-device cache for the device
// behind file. Callers run it even when the preceding sync failed: the device
// index goes back to the pool either way, and leaving pages of a dead sandbox
// in the cache would outlive the error they are about to return.
func invalidateDevice(file *os.File) error {
	return unix.IoctlSetInt(int(file.Fd()), unix.BLKFLSBUF, 0)
}

func (d *DirectPathMount) Close(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "direct-path-mount-close")
	defer span.End()

	var errs []error

	idx := d.deviceIndex

	// Flush and release the descriptor while the dispatchers still
	// serve the device: the kernel's last-close page-cache teardown waits on
	// pages locked by any in-flight request - speculative readahead reads
	// included, not only dirty writeback - and once the data path below is
	// torn down those requests hang until the kernel gives the connection up
	// (deadconnTimeout; ioTimeout + deadconnTimeout for commands already in
	// flight). Closing here lets them complete through the live path instead.
	// The explicit sync covers the second leg: acknowledged writes land
	// before the descriptor goes away, without betting on this descriptor
	// being the device's last opener - it is not whenever a udev probe holds
	// the device open across Close.
	//
	// The intended contract: against a live but unanswering backend this
	// flush holds the device, its pool slot, and the handlers for up to the
	// kernel ceiling (ioTimeout + deadconnTimeout), ahead of the cancel below
	// -- deliberately. Tearing down first was faster only by abandoning
	// writes the kernel had already acknowledged to the guest.
	//
	// Each event marks a step that can block on the backend, so a stall in a
	// trace is attributable to one step instead of one interval covering
	// several. The watchdog fires while the stall is happening -- tagging the
	// step in progress -- then accounts the total on return, so a stuck
	// teardown is visible during the incident and not only after it.
	if d.deviceFile != nil {
		closeStart := time.Now()

		var stage atomic.Value
		stage.Store("sync")
		watchdog := time.AfterFunc(deviceCloseWarnThreshold, func() {
			nbdSlowCloseCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("stage", stage.Load().(string))))
			d.logger.Warn(ctx, "NBD device descriptor close stalled",
				zap.Duration("threshold", deviceCloseWarnThreshold),
				zap.Uint32("device_index", idx),
			)
		})

		telemetry.ReportEvent(ctx, "flushing NBD device writeback")
		if syncErr := d.deviceFile.Sync(); syncErr != nil {
			// Mark the span the way Flush does: the backend is missing writes
			// the guest was told had landed. The error also returns below.
			span.SetAttributes(attribute.Int64("nbd.device_index", int64(idx)))
			span.RecordError(syncErr)
			span.SetStatus(codes.Error, "NBD device writeback failed, backend is missing acknowledged writes")

			errs = append(errs, fmt.Errorf("sync NBD device: %w", syncErr))
		}

		stage.Store("invalidate")
		if invalidateErr := invalidateDevice(d.deviceFile); invalidateErr != nil {
			errs = append(errs, fmt.Errorf("flush NBD device buffers: %w", invalidateErr))
		}

		stage.Store("descriptor_close")
		telemetry.ReportEvent(ctx, "closing NBD device descriptor")
		if err := d.deviceFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing NBD device: %w", err))
		}

		if !watchdog.Stop() {
			d.logger.Warn(ctx, "NBD device descriptor close finished after stalling",
				zap.Duration("duration", time.Since(closeStart)),
				zap.Uint32("device_index", idx),
			)
		}

		nbdCloseDuration.Record(ctx, time.Since(closeStart).Milliseconds())

		d.deviceFile = nil
	}

	// Closing the server sockets below ends an ioctl session from the kernel's
	// side; the session has to know this process asked for it.
	if d.ioctlSession != nil {
		d.ioctlSession.expectEnd()
	}

	// Cancel the context, which will stop waiting on pending readAt/writeAt...
	telemetry.ReportEvent(ctx, "canceling context")
	if d.cancelfn != nil {
		d.cancelfn()
	}

	// Close all server socket pairs... Clearing the slices keeps a second Close
	// from reporting every socket as already closed, which the Open error path
	// would otherwise trigger for every caller that also closes on failure.
	telemetry.ReportEvent(ctx, "closing socket pairs server")
	for _, v := range d.socksServer {
		err := v.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("error closing server pair: %w", err))
		}
	}

	d.socksServer = nil

	// Now wait until the handlers return
	telemetry.ReportEvent(ctx, "await handlers return")
	d.handlersWg.Wait()

	// Now wait for any pending responses to be sent
	telemetry.ReportEvent(ctx, "waiting for pending responses")
	for _, d := range d.dispatchers {
		d.Drain()
	}

	// Disconnect NBD
	var disconnectErr error
	if idx != math.MaxUint32 {
		disconnectErr = d.disconnectDevice(ctx, idx)
		if disconnectErr != nil {
			errs = append(errs, fmt.Errorf("error disconnecting NBD: %w", disconnectErr))
		}
	}

	// Close all client socket pairs...
	telemetry.ReportEvent(ctx, "closing socket pairs client")
	for _, v := range d.socksClient {
		err := v.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("error closing socket pair client: %w", err))
		}
	}

	d.socksClient = nil

	// Release the device back to the pool, retry if it is in use
	if idx != math.MaxUint32 {
		telemetry.ReportEvent(ctx, "releasing device to the pool")
		var err error
		if d.ioctlSession != nil && disconnectErr != nil {
			err = d.releaseAfterSession(ctx, idx)
		} else {
			err = d.releaseDevice(ctx, idx)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("error releasing overlay device: %w", err))
		}
	}

	return errors.Join(errs...)
}

func (d *DirectPathMount) socketPair() ([2]int, error) {
	if d.socketPairFn != nil {
		return d.socketPairFn()
	}

	return syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
}

func (d *DirectPathMount) disconnectDevice(ctx context.Context, deviceIndex uint32) error {
	if d.disconnectDeviceFn != nil {
		return d.disconnectDeviceFn(ctx, deviceIndex)
	}
	if d.ioctlSession != nil {
		return d.ioctlSession.disconnect(ctx, disconnectTimeout)
	}

	return disconnectNBDWithTimeout(ctx, deviceIndex, disconnectTimeout)
}

func (d *DirectPathMount) releaseDevice(ctx context.Context, deviceIndex uint32) error {
	if d.releaseDeviceFn != nil {
		return d.releaseDeviceFn(ctx, deviceIndex)
	}

	return d.devicePool.ReleaseDevice(ctx, deviceIndex, WithInfiniteRetry(), WithLogger(d.logger))
}

// releaseAfterSession releases the slot of an ioctl session whose disconnect
// did not finish. The device reads as in use until every descriptor on it is
// closed, the session's own and the disconnect request's private duplicate,
// and either close can hang on the device; a release here would spin for as
// long as the hang lasts and hold the caller's teardown with it. One attempt
// reclaims a device that has freed since; otherwise the release moves to a
// goroutine under one deadline, like the pool's own close, and the caller
// learns that its slot is still claimed.
func (d *DirectPathMount) releaseAfterSession(ctx context.Context, deviceIndex uint32) error {
	ctx = context.WithoutCancel(ctx)
	if err := d.devicePool.ReleaseDevice(ctx, deviceIndex, WithLogger(d.logger)); err == nil {
		return nil
	}

	timeout := d.deferredReleaseTimeout
	if timeout == 0 {
		timeout = devicePoolCloseReleaseTimeout
	}
	go func() {
		err := d.devicePool.ReleaseDevice(ctx, deviceIndex, WithInfiniteRetry(), WithTimeout(timeout), WithLogger(d.logger))
		if err != nil {
			d.logger.Error(ctx, "NBD device slot left claimed: its device stayed in use after its ioctl session's disconnect",
				zap.Uint32("device_index", deviceIndex),
				zap.Duration("timeout", timeout),
				zap.Error(err),
			)
		}
		if d.deferredReleaseDone != nil {
			d.deferredReleaseDone <- err
		}
	}()

	return fmt.Errorf("device %d: slot release deferred, its device is still in use", deviceIndex)
}

func disconnectNBDWithTimeout(ctx context.Context, deviceIndex uint32, timeout time.Duration) error {
	// Now ask to disconnect
	telemetry.ReportEvent(ctx, "disconnecting NBD")
	err := nbdnl.Disconnect(deviceIndex)
	if err != nil {
		return err
	}

	// Wait until it's completely disconnected...
	telemetry.ReportEvent(ctx, "waiting for complete disconnection")
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-ctxTimeout.Done():
			return ctxTimeout.Err()
		default:
		}

		s, err := nbdnl.Status(deviceIndex)
		if err == nil && !s.Connected {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}

	return nil
}

func DisconnectDevice(ctx context.Context, deviceIndex DeviceSlot) error {
	return disconnectNBDWithTimeout(ctx, deviceIndex, disconnectTimeout)
}

func closeSocketPairs(socksClient []*os.File, socksServer []io.Closer) error {
	var errs []error
	for _, sock := range socksClient {
		if sock != nil {
			errs = append(errs, sock.Close())
		}
	}
	for _, sock := range socksServer {
		if sock != nil {
			errs = append(errs, sock.Close())
		}
	}

	return errors.Join(errs...)
}

// attachResult is the attach counter's outcome label. A lost device race is
// retried and a cancelled caller is its own outcome, so neither is lumped with
// a real failure that a rollout alert would watch.
func attachResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, errIoctlDeviceTaken) || errors.Is(err, unix.EBUSY) ||
		strings.Contains(err.Error(), "device or resource busy"):
		return "taken"
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	default:
		return "failure"
	}
}
