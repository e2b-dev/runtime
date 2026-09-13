//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	waitOnNBDError                = 50 * time.Millisecond
	devicePoolCloseReleaseTimeout = 10 * time.Minute
	sysBlockDir                   = "/sys/block"
	// releaseRetryDelay is how long ReleaseDevice waits before retrying a device
	// that is still in use.
	releaseRetryDelay = 500 * time.Millisecond
)

var (
	meter       = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd")
	slotCounter = utils.Must(meter.Int64UpDownCounter("orchestrator.nbd.slots_pool.ready",
		metric.WithDescription("Number of nbd slots ready to be used."),
		metric.WithUnit("{slot}"),
	))
	acquired = utils.Must(meter.Int64Counter("orchestrator.nbd.slots_pool.acquired",
		metric.WithDescription("Number of nbd slots acquired."),
		metric.WithUnit("{slot}"),
	))
	released = utils.Must(meter.Int64Counter("orchestrator.nbd.slots_pool.released",
		metric.WithDescription("Number of nbd slots released."),
		metric.WithUnit("{slot}"),
	))
)

// NoFreeSlotsError is returned when there are no free slots.
// You can retry the request after some time.
type NoFreeSlotsError struct{}

func (NoFreeSlotsError) Error() string {
	return "no free slots"
}

// DeviceInUseError is returned when the device that you wanted to release is still in use.
// You can retry the request after ensuring that the device is not in use anymore.
type DeviceInUseError struct{}

func (DeviceInUseError) Error() string {
	return "device in use"
}

var ErrClosed = errors.New("cannot read from a closed pool")

type (
	// DevicePath is the path to the nbd device.
	DevicePath = string
	// DeviceSlot is the slot number of the nbd device.
	DeviceSlot = uint32
)

// DevicePool requires the nbd module to be loaded before running.
//
// Use `sudo modprobe nbd nbds_max=4096` to set the max number of devices to 4096, which is a good default for now.
type DevicePool struct {
	done     chan struct{}
	doneOnce sync.Once

	maxDevices uint

	// We use the bitset to speedup the free device lookup.
	usedSlots *bitset.BitSet
	mu        sync.Mutex

	slots chan DeviceSlot
}

// PoolStatus summarises the current state of the device pool.
type PoolStatus struct {
	// Max is the total number of NBD device slots available on this host (nbds_max).
	Max uint
	// Used is the number of slots currently held by the pool.
	Used uint
	// PreWarmed is the number of slots pre-warmed and immediately available.
	PreWarmed int
}

// Status returns a snapshot of the pool's current usage without scanning sysfs.
func (d *DevicePool) Status() PoolStatus {
	d.mu.Lock()
	used := d.usedSlots.Count()
	d.mu.Unlock()

	return PoolStatus{
		Max:       d.maxDevices,
		Used:      used,
		PreWarmed: len(d.slots),
	}
}

func NewDevicePool(maxSlotsReady int) (*DevicePool, error) {
	if maxSlotsReady <= 0 {
		return nil, fmt.Errorf("maxSlotsReady must be > 0, got %d", maxSlotsReady)
	}

	maxDevices, err := getMaxDevices()
	if err != nil {
		return nil, fmt.Errorf("failed to get max devices: %w", err)
	}

	if maxDevices == 0 {
		return nil, errors.New("max devices is 0")
	}

	pool := &DevicePool{
		done:       make(chan struct{}),
		maxDevices: maxDevices,
		usedSlots:  bitset.New(maxDevices),
		slots:      make(chan DeviceSlot, int(math.Min(float64(maxSlotsReady), float64(maxDevices)))),
	}

	return pool, nil
}

var ErrNBDModuleNotLoaded = errors.New("NBD module not loaded")

func getMaxDevices() (uint, error) {
	data, err := os.ReadFile("/sys/module/nbd/parameters/nbds_max")

	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrNBDModuleNotLoaded
	}

	if err != nil {
		return 0, fmt.Errorf("failed to read nbds_max: %w", err)
	}

	maxDevices, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to parse nbds_max: %w", err)
	}

	return uint(maxDevices), nil
}

func ConnectedDevices() ([]DeviceSlot, error) {
	maxDevices, err := getMaxDevices()
	if err != nil {
		return nil, err
	}

	devices := make([]DeviceSlot, 0)
	for slot := DeviceSlot(0); slot < DeviceSlot(maxDevices); slot++ {
		connected, err := isDeviceConnectedIn(sysBlockDir, slot)
		if err != nil {
			return nil, err
		}
		if connected {
			devices = append(devices, slot)
		}
	}

	return devices, nil
}

func isDeviceConnectedIn(blockDir string, slot DeviceSlot) (bool, error) {
	pidFile := fmt.Sprintf("%s/nbd%d/pid", blockDir, slot)
	_, err := os.Stat(pidFile)
	if err == nil {
		return true, nil
	}
	if !os.IsNotExist(err) {
		return false, fmt.Errorf("failed to stat pid file: %w", err)
	}

	return false, nil
}

func (d *DevicePool) Populate(ctx context.Context) {
	defer close(d.slots)

	failedCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.done:
			return
		default:
		}

		device, err := d.getFreeDeviceSlot()
		if err != nil {
			if failedCount%100 == 0 {
				logger.L().Warn(ctx, "[nbd pool]: failed to create network",
					zap.Error(err),
					zap.Int("failed_count", failedCount),
				)
			}

			failedCount++
			time.Sleep(waitOnNBDError)

			continue
		}
		failedCount = 0

		slotCounter.Add(ctx, 1)

		// Use select to avoid panic if context is canceled before writing
		select {
		case <-ctx.Done():
			return
		case <-d.done:
			return
		case d.slots <- *device:
			// sent successfully
		}
	}
}

// The following files and resources are useful for checking if the device is free:
// /sys/devices/virtual/block/nbdX/pid
// /sys/block/nbdX/pid
// /sys/block/nbdX/size
// nbd-client -c
// https://unix.stackexchange.com/questions/33508/check-which-network-block-devices-are-in-use
// https://superuser.com/questions/919895/how-to-get-a-list-of-connected-nbd-devices-on-ubuntu
// https://github.com/NetworkBlockDevice/nbd/blob/17043b068f4323078637314258158aebbfff0a6c/nbd-client.c#L254
func (d *DevicePool) isDeviceFree(slot DeviceSlot) (bool, error) {
	connected, err := isDeviceConnectedIn(sysBlockDir, slot)
	if err != nil {
		return false, err
	}
	if connected {
		// File is present, therefore the device is in use.
		return false, nil
	}

	sizeFile := fmt.Sprintf("/sys/block/nbd%d/size", slot)

	data, err := os.ReadFile(sizeFile)
	if err != nil {
		return false, fmt.Errorf("failed to read size file: %w", err)
	}

	sizeStr := strings.TrimSpace(string(data))

	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("failed to parse size: %w", err)
	}

	return size == 0, nil
}

func (d *DevicePool) getMaybeEmptySlot(start DeviceSlot) (DeviceSlot, func(), bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	slot, ok := d.usedSlots.NextClear(uint(start))

	if !ok {
		return 0, func() {}, false
	}

	d.usedSlots.Set(slot)

	return uint32(slot), func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		d.usedSlots.Clear(slot)
	}, true
}

// Get a free device slot.
func (d *DevicePool) getFreeDeviceSlot() (*DeviceSlot, error) {
	start := uint32(0)

	for {
		slot, cleanup, ok := d.getMaybeEmptySlot(start)

		if !ok {
			cleanup()

			return nil, NoFreeSlotsError{}
		}

		free, err := d.isDeviceFree(slot)
		if err != nil {
			cleanup()

			return nil, fmt.Errorf("failed to check if device is free: %w", err)
		}

		if !free {
			// We clear the slot even though it is not free to prevent accidental accumulation of slots.
			cleanup()

			// We increment the start to avoid infinite loops.
			start++

			continue
		}

		return &slot, nil
	}
}

// GetDevice returns a slot if there is one available.
func (d *DevicePool) GetDevice(ctx context.Context) (DeviceSlot, error) {
	select {
	case <-d.done:
		return 0, ErrClosed
	case <-ctx.Done():
		return 0, ctx.Err()
	case slot := <-d.slots:
		acquired.Add(ctx, 1)
		slotCounter.Add(ctx, -1)

		return slot, nil
	}
}

func (d *DevicePool) release(ctx context.Context, idx DeviceSlot) error {
	free, err := d.isDeviceFree(idx)
	if err != nil {
		return fmt.Errorf("failed to check if device is free: %w", err)
	}

	if !free {
		return DeviceInUseError{}
	}

	d.mu.Lock()
	d.usedSlots.Clear(uint(idx))
	d.mu.Unlock()

	released.Add(ctx, 1)

	return nil
}

// ReleaseDevice will return an error if the device is not free and not release the slot — you can retry.
func (d *DevicePool) ReleaseDevice(ctx context.Context, idx DeviceSlot, opts ...ReleaseOption) error {
	opt := releaseOptions{}
	for _, o := range opts {
		o(&opt)
	}

	if opt.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opt.timeout)
		defer cancel()
	}

	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		attempt++

		err := d.release(ctx, idx)
		if err == nil {
			return nil
		}

		if !opt.infiniteRetry {
			return err
		}

		if attempt%100 == 0 {
			logger.L().Error(ctx, "error releasing device", zap.Int("attempt", attempt), zap.Error(err))
		}

		// Wait on the context too: Close bounds a stuck device with WithTimeout,
		// and shutdown cancels the context outright. An uninterruptible sleep
		// here would make every remaining attempt outlive both.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(releaseRetryDelay):
		}
	}
}

func GetDevicePath(slot DeviceSlot) DevicePath {
	return fmt.Sprintf("/dev/nbd%d", slot)
}

func (d *DevicePool) Close(ctx context.Context) error {
	logger.L().Info(ctx, "Closing device pool", zap.Uint("used_slots", d.usedSlots.Count()))

	d.doneOnce.Do(func() {
		close(d.done)
	})

	d.mu.Lock()

	var slotsToRelease []DeviceSlot
	for slotIdx, e := d.usedSlots.NextSet(0); e; slotIdx, e = d.usedSlots.NextSet(slotIdx + 1) {
		slotsToRelease = append(slotsToRelease, DeviceSlot(slotIdx))
	}

	d.mu.Unlock()

	var errs error
	for _, slot := range slotsToRelease {
		err := d.ReleaseDevice(ctx, slot,
			WithInfiniteRetry(),
			WithTimeout(devicePoolCloseReleaseTimeout),
		)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to release device %d: %w", slot, err))
		}
	}

	return errs
}

type releaseOptions struct {
	timeout       time.Duration
	infiniteRetry bool
}

type ReleaseOption func(*releaseOptions)

func WithTimeout(timeout time.Duration) ReleaseOption {
	return func(opts *releaseOptions) {
		opts.timeout = timeout
	}
}

func WithInfiniteRetry() ReleaseOption {
	return func(opts *releaseOptions) {
		opts.infiniteRetry = true
	}
}
