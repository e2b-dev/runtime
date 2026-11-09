//go:build linux

package fc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

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
)

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
// (a release without the full memory layout), kernel (a guest kernel below the floor), or invalid
// (the region's size or the guest's boot).
func MemoryHotplugRefusalReason(err error) string {
	switch {
	case errors.Is(err, ErrMemoryHotplugFirecracker):
		return "firecracker"
	case errors.Is(err, ErrMemoryHotplugKernel):
		return "kernel"
	default:
		return "invalid"
	}
}

// PluggedMemoryMB is the memory the guest has plugged into the hotplug region, as Firecracker
// reports it now.
func (p *Process) PluggedMemoryMB(ctx context.Context) (int64, error) {
	status, err := p.client.getMemoryHotplug(ctx)
	if err != nil {
		return 0, err
	}

	return status.PluggedSizeMib, nil
}
