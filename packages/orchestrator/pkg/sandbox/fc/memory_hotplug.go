//go:build linux

package fc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
)

// Firecracker's default virtio-mem block and slot sizes, sent with every hotplug region. Upstream
// advises keeping both: a larger block makes unplugging harder, and a region must be a whole
// number of slots, which Firecracker protects and unprotects one at a time.
const (
	MemoryHotplugBlockMiB = 2
	MemoryHotplugSlotMiB  = 128
)

// MaxMemoryHotplugMiB caps the hotplug region of one VM, not its boot RAM: a VM without a region
// is never refused by it. It is a sanity bound, not a capacity rule: the team's RAM limit in the
// API is what a user meets, and a node's hugepage pool is what a sandbox fits in. It keeps an
// out-of-range request from sizing a sparse memfile and a Firecracker region far beyond any
// host, while staying well inside the 512 GiB window Firecracker reserves for the region.
const MaxMemoryHotplugMiB = 128 * 1024

// ErrMemoryHotplugUnsupported is a hotplug region on a boot that cannot carry one. The two errors
// below wrap it and name the cause; anything else is the region's size or the guest's boot.
var (
	ErrMemoryHotplugUnsupported = errors.New("this boot cannot carry a memory hotplug region")
	ErrMemoryHotplugFirecracker = fmt.Errorf("%w: the firecracker release does not report the full memory layout", ErrMemoryHotplugUnsupported)
	ErrMemoryHotplugKernel      = fmt.Errorf("%w: the guest kernel is older than virtio-mem hotplug needs", ErrMemoryHotplugUnsupported)
	// ErrMemoryRequestInvalid is a requested memory size the sandbox cannot take: below its boot
	// RAM, not a whole number of hotplug blocks, or beyond its region. It is the caller's error,
	// unlike the platform refusals above.
	ErrMemoryRequestInvalid error = &memoryRequestError{"the requested memory does not fit the sandbox"}
	// ErrMemoryHotplugShrink is a memory resume asked to run with less memory than its snapshot
	// had asked for. Hotplug only grows a sandbox for now: unplugging needs the guest to empty
	// the memory first and can stall or kill processes, so a smaller size takes a cold boot.
	ErrMemoryHotplugShrink = fmt.Errorf("%w: a memory resume cannot shrink the sandbox", ErrMemoryRequestInvalid)
)

// memoryRequestError is a caller's request the sandbox cannot take. It unwraps to
// ErrMemoryHotplugUnsupported so a start refuses it before taking resources, but its message
// names only the request, since nothing is wrong with the boot.
type memoryRequestError struct{ msg string }

func (e *memoryRequestError) Error() string { return e.msg }

func (e *memoryRequestError) Unwrap() error { return ErrMemoryHotplugUnsupported }

// The oldest guest kernels whose virtio-mem driver works with Firecracker's hotplug region:
// upstream Firecracker documents 5.16 on x86 (VIRTIO_MEM_F_UNPLUGGED_INACCESSIBLE) and 5.18 on
// arm64, both after the auto-movable online policy arrived in 5.15.
var minMemoryHotplugKernel = map[string][2]int{"amd64": {5, 16}, "arm64": {5, 18}}

var kernelVersionRe = regexp.MustCompile(`^vmlinux-(\d+)\.(\d+)`)

// kernelTooOldForMemoryHotplug reports whether a kernel slot name (vmlinux-6.1.158, optionally
// with a _hash suffix) is older than this architecture's minimum. A name it cannot read is not
// refused: only a version it can read and that is too old is.
func kernelTooOldForMemoryHotplug(arch, kernelVersion string) bool {
	m := kernelVersionRe.FindStringSubmatch(kernelVersion)
	floor, ok := minMemoryHotplugKernel[arch]
	if m == nil || !ok {
		return false
	}
	major, errMajor := strconv.Atoi(m[1])
	minor, errMinor := strconv.Atoi(m[2])
	if errMajor != nil || errMinor != nil {
		return false
	}

	return major < floor[0] || (major == floor[0] && minor < floor[1])
}

// ValidateMemoryHotplug checks a memory maximum against boot RAM: at least memoryMB, with the
// region above boot RAM a whole number of slots and at most MaxMemoryHotplugMiB, the same rule
// RefuseMemoryHotplug applies to a boot. memoryName and maxName are the caller's field or flag
// names, used in the error messages.
func ValidateMemoryHotplug(memoryMB, maxMemoryMB int64, memoryName, maxName string) error {
	if memoryMB < 1 {
		return fmt.Errorf("%s %d must be at least 1 when %s is set", memoryName, memoryMB, maxName)
	}
	if maxMemoryMB < memoryMB {
		return fmt.Errorf("%s %d must be at least %s %d", maxName, maxMemoryMB, memoryName, memoryMB)
	}
	region := maxMemoryMB - memoryMB
	if region%MemoryHotplugSlotMiB != 0 {
		return fmt.Errorf("%s %d minus %s %d is %d MiB, which must be a multiple of %d MiB",
			maxName, maxMemoryMB, memoryName, memoryMB, region, MemoryHotplugSlotMiB)
	}
	if region > MaxMemoryHotplugMiB {
		return fmt.Errorf("%s %d minus %s %d is a %d MiB hotplug region, above the %d MiB a region can have",
			maxName, maxMemoryMB, memoryName, memoryMB, region, MaxMemoryHotplugMiB)
	}

	return nil
}

// DefaultMemoryHotplugMaxRatio is the default cap on a hotplug region relative to boot RAM. The
// guest's auto-movable policy onlines plugged memory as movable while it stays within 301% of
// kernel memory, so a region up to three times boot RAM is onlined exactly as online_movable
// would, and all of it stays unpluggable.
const DefaultMemoryHotplugMaxRatio = 3

// ValidateMemoryHotplugRatio caps the hotplug region above memoryMB at maxRatio times memoryMB;
// maxRatio 0 is no cap. It is a build-time rule: a VM built under a looser cap still boots and
// resumes after the cap is lowered, so nothing on the start path checks it.
func ValidateMemoryHotplugRatio(memoryMB, maxMemoryMB, maxRatio int64, maxName string) error {
	if maxRatio <= 0 || maxMemoryMB-memoryMB <= maxRatio*memoryMB {
		return nil
	}
	largest := memoryMB + maxRatio*memoryMB/MemoryHotplugSlotMiB*MemoryHotplugSlotMiB

	return fmt.Errorf("%s %d is above the hotplug cap of %d times boot RAM (%d MiB); the largest allowed %s is %d",
		maxName, maxMemoryMB, maxRatio, memoryMB, maxName, largest)
}

// RefuseMemoryHotplug refuses a hotplug region of regionMB that this boot cannot carry: a size
// Firecracker would reject, a guest booted without a kernel command line, which cannot be given
// the online policy, a guest kernel too old for virtio-mem hotplug, or a Firecracker release
// whose memory endpoints pack plugged slots together (its exports copy the wrong pages once a
// gap appears). 0 is no region and always passes.
func (t Config) RefuseMemoryHotplug(regionMB int64) error {
	if regionMB == 0 {
		return nil
	}
	if regionMB < 0 || regionMB%MemoryHotplugSlotMiB != 0 || regionMB > MaxMemoryHotplugMiB {
		return fmt.Errorf("%w: a region of %d MiB is not a whole number of %d MiB slots up to %d MiB",
			ErrMemoryHotplugUnsupported, regionMB, MemoryHotplugSlotMiB, MaxMemoryHotplugMiB)
	}
	if t.GuestProfile().Boot().NoKernelArgs {
		return fmt.Errorf("%w: guest %s boots without a kernel command line", ErrMemoryHotplugUnsupported, t.GuestProfile().Name())
	}
	if kernelTooOldForMemoryHotplug(hostArch, t.KernelVersion) {
		return fmt.Errorf("%w: %s", ErrMemoryHotplugKernel, t.KernelVersion)
	}
	info, err := fcversion.New(t.FirecrackerVersion)
	if err != nil || !info.HasMemoryHotplug() {
		return fmt.Errorf("%w: %q", ErrMemoryHotplugFirecracker, t.FirecrackerVersion)
	}

	return nil
}

// RefuseMemoryHotplugRestore refuses a memory resume of a snapshot with a hotplug region of regionMB
// on a Firecracker whose memory endpoints pack plugged slots together, which would export the wrong
// pages. Only the release matters here: a restore keeps the snapshot's device and never boots the
// kernel again, so the kernel floor, the guest profile and the size rules RefuseMemoryHotplug applies
// to a boot do not, and tightening them later must not strand existing snapshots.
func (t Config) RefuseMemoryHotplugRestore(regionMB int64) error {
	if regionMB == 0 {
		return nil
	}
	info, err := fcversion.New(t.FirecrackerVersion)
	if err != nil || !info.HasMemoryHotplug() {
		return fmt.Errorf("%w: %q", ErrMemoryHotplugFirecracker, t.FirecrackerVersion)
	}

	return nil
}

// MemoryHotplugRefusalReason names why a region was refused, for counters and logs: firecracker
// (a release without the full memory layout), kernel (a guest kernel below the floor), shrink (a
// memory resume asked for less memory than its snapshot had), invalid_request (a requested memory
// size the sandbox cannot take), or invalid (the region's size or the guest's boot).
func MemoryHotplugRefusalReason(err error) string {
	switch {
	case errors.Is(err, ErrMemoryHotplugFirecracker):
		return "firecracker"
	case errors.Is(err, ErrMemoryHotplugKernel):
		return "kernel"
	case errors.Is(err, ErrMemoryHotplugShrink):
		return "shrink"
	case errors.Is(err, ErrMemoryRequestInvalid):
		return "invalid_request"
	default:
		return "invalid"
	}
}

// MemoryHotplugUsableMB is how much of the hotplug region the guest may plug while Firecracker
// asks for requestedMB: Firecracker refuses plugs past the request rounded up to a slot of slotMB
// (MemoryHotplugSlotMiB when slotMB is 0).
func MemoryHotplugUsableMB(requestedMB, slotMB int64) int64 {
	if slotMB <= 0 {
		slotMB = MemoryHotplugSlotMiB
	}
	if requestedMB <= 0 {
		return 0
	}

	return (requestedMB + slotMB - 1) / slotMB * slotMB
}

// MemoryHotplugStatus is the hotplug region's state as Firecracker reports it, in MiB: what the
// guest has plugged, the size it was asked to reach, and the region's size and slot size.
type MemoryHotplugStatus struct {
	PluggedMB   int64
	RequestedMB int64
	TotalMB     int64
	SlotMB      int64
}

// MemoryHotplugStatus reads the hotplug region's state from Firecracker now.
func (p *Process) MemoryHotplugStatus(ctx context.Context) (MemoryHotplugStatus, error) {
	status, err := p.client.getMemoryHotplug(ctx)
	if err != nil {
		return MemoryHotplugStatus{}, err
	}

	return MemoryHotplugStatus{
		PluggedMB:   status.PluggedSizeMib,
		RequestedMB: status.RequestedSizeMib,
		TotalMB:     status.TotalSizeMib,
		SlotMB:      status.SlotSizeMib,
	}, nil
}

// PluggedMemoryMB is the memory the guest has plugged into the hotplug region, as Firecracker
// reports it now.
func (p *Process) PluggedMemoryMB(ctx context.Context) (int64, error) {
	status, err := p.MemoryHotplugStatus(ctx)
	if err != nil {
		return 0, err
	}

	return status.PluggedMB, nil
}

// RequestMemoryHotplug asks the guest to plug requestedMB of the hotplug region. It returns once
// Firecracker took the target, before the guest plugged anything. Firecracker refuses it until the
// guest driver has activated the device, so a cold boot sends it after the guest is up.
func (p *Process) RequestMemoryHotplug(ctx context.Context, requestedMB int64) error {
	return p.client.requestMemoryHotplug(ctx, requestedMB)
}

// memoryHotplugTarget is the hotplug target a restore asks the guest to plug, in MiB, and whether
// the start named it (explicit) or kept the size its snapshot recorded.
type memoryHotplugTarget struct {
	requestedMB int64
	explicit    bool
}

// SetMemoryHotplugTarget sets how much of the hotplug region a following restore asks the guest
// to plug, in MiB. An explicit target is a size the start named, and a restored device that asks
// for more refuses it. A kept one is the size the snapshot's metadata recorded: the restore plugs
// up to it, and leaves a device that already asks for more as it is. Without a target the restore
// keeps the device's request and reads nothing.
func (p *Process) SetMemoryHotplugTarget(requestedMB int64, explicit bool) {
	p.memoryHotplugTarget = &memoryHotplugTarget{requestedMB: requestedMB, explicit: explicit}
}

// MemoryHotplugResized reports whether the restore sent a hotplug target that differs from the
// snapshot's, so the guest has memory to plug or unplug after it runs.
func (p *Process) MemoryHotplugResized() bool {
	return p.memoryHotplugResized
}

// applyMemoryHotplugTarget sends the restore's hotplug target when it is above the one the snapshot
// restored, and only then; an explicit target below the restored one is refused.
func (p *Process) applyMemoryHotplugTarget(ctx context.Context) error {
	target := p.memoryHotplugTarget
	if target == nil && p.memoryHotplugRestored == nil {
		return nil
	}
	status, err := p.MemoryHotplugStatus(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errMemoryHotplugStatusUnread, err)
	}
	if target != nil && target.requestedMB != status.RequestedMB {
		// The restored device is the truth: a target below its request would unplug memory under
		// running processes, whatever the snapshot's metadata recorded.
		if target.requestedMB < status.RequestedMB {
			if target.explicit {
				return fmt.Errorf("%w: it asks for %d MiB above boot RAM and the restored device had %d MiB",
					ErrMemoryHotplugShrink, target.requestedMB, status.RequestedMB)
			}
		} else {
			if err := p.RequestMemoryHotplug(ctx, target.requestedMB); err != nil {
				return err
			}
			p.memoryHotplugResized = true
			status.RequestedMB = target.requestedMB
		}
	}
	if p.memoryHotplugRestored != nil {
		return p.memoryHotplugRestored(status)
	}

	return nil
}

// errMemoryHotplugStatusUnread is the restore failing to read the restored hotplug device.
var errMemoryHotplugStatusUnread = errors.New("error reading the restored memory hotplug status")

// OnMemoryHotplugRestored registers fn to be told, before the vCPUs resume, the restored device's
// state, with RequestedMB the size the guest is asked to plug after any new target was sent.
func (p *Process) OnMemoryHotplugRestored(fn func(MemoryHotplugStatus) error) {
	p.memoryHotplugRestored = fn
}

// memoryHotplugAPIError is a hotplug call Firecracker answered with an error: its HTTP status and
// fault message.
type memoryHotplugAPIError interface {
	Code() int
	GetPayload() *models.Error
}

func memoryHotplugFault(err error) (int, string, bool) {
	if patch, ok := errors.AsType[*operations.PatchMemoryHotplugDefault](err); ok {
		return memoryHotplugFaultOf(patch)
	}
	if get, ok := errors.AsType[*operations.GetMemoryHotplugDefault](err); ok {
		return memoryHotplugFaultOf(get)
	}

	return 0, "", false
}

func memoryHotplugFaultOf(e memoryHotplugAPIError) (int, string, bool) {
	message := ""
	if payload := e.GetPayload(); payload != nil {
		message = payload.FaultMessage
	}

	return e.Code(), message, true
}

// MemoryHotplugFaultMessage is the fault message Firecracker answered a failed hotplug call with,
// "" for a failure it did not answer.
func MemoryHotplugFaultMessage(err error) string {
	_, message, _ := memoryHotplugFault(err)

	return message
}

// MemoryHotplugDeviceNotActive reports whether Firecracker refused a hotplug call because the guest
// driver has not activated the device yet, which passes once the guest is up.
func MemoryHotplugDeviceNotActive(err error) bool {
	_, message, ok := memoryHotplugFault(err)

	return ok && strings.Contains(strings.ToLower(message), "not active")
}

// MemoryHotplugErrorPermanent reports whether Firecracker answered a hotplug call with a client
// error retrying cannot change: any 4xx but an inactive device. A transport failure or a 5xx may
// pass and is not permanent.
func MemoryHotplugErrorPermanent(err error) bool {
	code, _, ok := memoryHotplugFault(err)

	return ok && code >= 400 && code < 500 && !MemoryHotplugDeviceNotActive(err)
}
