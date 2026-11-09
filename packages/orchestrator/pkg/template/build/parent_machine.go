//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// errParentMachineSize refuses a build FROM a template whose guest profile
// resumes the parent, when the requested machine differs from the parent snapshot's.
var errParentMachineSize = errors.New("a template built from this parent resumes the parent's memory snapshot and must request its machine size")

// errParentNotResizable refuses a resizable build FROM a template whose guest
// profile resumes the parent: the parent's snapshot fixes the vCPU count, so no
// resume could give a sandbox more, though the build would be advertised so.
var errParentNotResizable = errors.New("a template built from this parent resumes the parent's memory snapshot, whose vCPU count is fixed, so it cannot be resizable")

// errParentNeedsMemory refuses a build FROM a template whose guest profile
// resumes the parent, when the parent has no memory image to resume.
var errParentNeedsMemory = errors.New("building from this template needs a parent with a memory snapshot; pause the base into a memory build first")

// errParentVcpusUnknown refuses a build FROM a template whose guest profile
// resumes the parent, when the parent records no vCPU count: the restored VM's
// size is then unknown, and the build would advertise a size it may not have.
var errParentVcpusUnknown = errors.New("building from this template needs a parent that records its vCPU count; pause the base again with a runtime that records it")

// errNoMemoryHotplug refuses a build with a memory maximum FROM a template whose guest profile
// cannot carry a hotplug region: one that resumes the parent, whose snapshot fixes the memory
// layout at the parent's first boot, or one that boots without a kernel command line, which
// cannot be told how to online plugged memory.
var errNoMemoryHotplug = errors.New("templates of this guest cannot grow their memory, so the build cannot have a maximum memory size above its memory size")

// parentMemorySize reports the memory size in MiB of a parent build's memory
// image, from its memfile in the template cache.
type parentMemorySize func(ctx context.Context, buildID string) (int64, error)

func cacheMemorySize(templateCache *sbxtemplate.Cache) parentMemorySize {
	return func(ctx context.Context, buildID string) (int64, error) {
		t, release, err := templateCache.GetTemplatePinned(ctx, buildID, false, true)
		defer release()
		if err != nil {
			return 0, fmt.Errorf("get parent template: %w", err)
		}

		memfile, err := t.Memfile(ctx)
		if err != nil {
			return 0, fmt.Errorf("open parent memfile: %w", err)
		}

		size, err := memfile.Size(ctx)
		if err != nil {
			return 0, fmt.Errorf("size parent memfile: %w", err)
		}

		return units.BytesToMB(size), nil
	}
}

// adoptParentMachine resolves the build's guest profile from the parent
// template and, when the profile resumes the parent for every layer, makes the
// build take what the parent's memory image fixes and the build's own
// configuration cannot know: the kernel slot, the Firecracker binary, the
// memory size and huge pages of the snapshot, its vCPU count and the device
// names it bakes. A build from an image, or from a parent of a profile that
// boots its layers, keeps its configuration. A non-default profile is served
// only while the guest-profiles flag is on, and the build is refused when the
// profile cannot run the Firecracker version it will launch: the parent's pin
// on the resume path, the request's otherwise.
func adoptParentMachine(
	ctx context.Context,
	bc buildcontext.BuildContext,
	index cache.Index,
	memorySize parentMemorySize,
	flags guest.Flags,
	log logger.Logger,
) (buildcontext.BuildContext, error) {
	if bc.Config.FromTemplate == nil {
		return bc, nil
	}

	parentBuildID := bc.Config.FromTemplate.GetBuildID()
	parent, err := index.Cached(ctx, parentBuildID)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// The base phase reports a missing parent with the user-facing message.
			return bc, nil
		}
		// The index reads the parent through metadata.FromBuildID, which
		// refuses a guest OS that no profile on this node claims.
		if errors.Is(err, guest.ErrUnclaimed) {
			return bc, phases.NewPhaseBuildError(basePhase, err)
		}

		return bc, fmt.Errorf("read the parent template: %w", err)
	}

	profile, err := parent.ServingGuestProfile(ctx, flags)
	if err != nil {
		return bc, phases.NewPhaseBuildError(basePhase, err)
	}
	bc.Config.Guest = profile

	tb := profile.Template()
	if bc.Config.MemoryHotplugMB() > 0 && (tb.AlwaysResume || profile.Boot().NoKernelArgs) {
		return bc, phases.NewPhaseBuildError(basePhase, fmt.Errorf(
			"%w: requested memory_mb=%d and max_memory_mb=%d", errNoMemoryHotplug, bc.Config.MemoryMB, bc.Config.MaxMemoryMB))
	}
	if !tb.AlwaysResume {
		return bc, refuseFirecracker(ctx, bc, profile)
	}

	if parent.IsFilesystemOnly() {
		return bc, phases.NewPhaseBuildError(basePhase, errParentNeedsMemory)
	}

	// The vCPU count is the parent snapshot's own; without it the request's size cannot be checked.
	if parent.VcpuCount <= 0 {
		return bc, phases.NewPhaseBuildError(basePhase, errParentVcpusUnknown)
	}

	memoryMB, err := memorySize(ctx, parentBuildID)
	if err != nil {
		return bc, fmt.Errorf("size the parent's memory image: %w", err)
	}
	// The memfile holds boot RAM and, after it, any hotplug region; the machine is the boot RAM.
	memoryMB -= parent.MemoryHotplugMB

	// The build row keeps the request's size and every sandbox is placed by it,
	// so it must be the machine the snapshot really is.
	if bc.Config.MemoryMB != memoryMB || bc.Config.VCpuCount != parent.VcpuCount {
		return bc, phases.NewPhaseBuildError(basePhase, fmt.Errorf(
			"%w: build with memory_mb=%d and cpu_count=%d (requested %d and %d)",
			errParentMachineSize, memoryMB, parent.VcpuCount, bc.Config.MemoryMB, bc.Config.VCpuCount))
	}

	if bc.Config.MaxVCpuCount > bc.Config.VCpuCount {
		return bc, phases.NewPhaseBuildError(basePhase, fmt.Errorf(
			"%w: build without max_cpu_count, or with max_cpu_count=%d (requested %d)",
			errParentNotResizable, parent.VcpuCount, bc.Config.MaxVCpuCount))
	}

	cfg := bc.Config
	changed := []zap.Field{
		zap.String("guest_os", profile.Label()),
		zap.String("kernel_version", parent.Template.KernelVersion),
		zap.String("firecracker_version", parent.Template.FirecrackerVersion),
		zap.Int64("memory_mb", memoryMB),
		zap.Int64("vcpu_count", parent.VcpuCount),
		zap.String("rootfs_drive_id", parent.RootfsDriveID),
		zap.String("net_iface_id", parent.NetIfaceID),
		zap.String("rootfs_file", parent.RootfsFile),
		zap.String("requested_kernel_version", cfg.KernelVersion),
		zap.String("requested_firecracker_version", cfg.FirecrackerVersion),
		zap.Int64("requested_memory_mb", cfg.MemoryMB),
		zap.Int64("requested_vcpu_count", cfg.VCpuCount),
	}

	cfg.KernelVersion = parent.Template.KernelVersion
	cfg.FirecrackerVersion = parent.Template.FirecrackerVersion
	// The snapshot's devices are addressed by the names it bakes.
	cfg.RootfsDriveID = parent.RootfsDriveID
	cfg.NetIfaceID = parent.NetIfaceID
	cfg.RootfsFile = parent.RootfsFile
	cfg.MemoryMB = memoryMB
	cfg.HugePages = tb.HugePages
	cfg.VCpuCount = parent.VcpuCount
	// A resumed snapshot has the vCPUs it was paused with; nothing to hotplug.
	cfg.MaxVCpuCount = 0
	// The parent's memory layout is fixed, and refused above if it was asked to grow.
	cfg.MaxMemoryMB = 0
	// Nothing boots a kernel here: the snapshot keeps its own command line, CPU template and balloon.
	cfg.CmdlineArgs = nil
	cfg.CPUTemplate = nil
	cfg.FreePageReporting = false
	cfg.FreePageHinting = false

	bc.Config = cfg
	if err := refuseFirecracker(ctx, bc, profile); err != nil {
		return bc, err
	}

	log.Info(ctx, "build from a template that resumes its parent takes the parent's machine",
		append(changed, logger.WithTemplateID(cfg.TemplateID), logger.WithBuildID(bc.Template.BuildID))...)

	return bc, nil
}

// refuseFirecracker refuses a build whose Firecracker version the guest
// profile cannot run, by its name, or whose binary does not report a feature
// the profile requires. The default profile accepts every version and asks
// the binary nothing.
func refuseFirecracker(ctx context.Context, bc buildcontext.BuildContext, profile guest.Profile) error {
	if err := profile.Firecracker().Accept(bc.Config.FirecrackerVersion); err != nil {
		return phases.NewPhaseBuildError(basePhase, err)
	}
	if err := bc.Config.FirecrackerConfig().RequireFeatures(ctx, bc.BuilderConfig); err != nil {
		missing, ok := errors.AsType[*fc.MissingFeaturesError](err)
		if !ok {
			// The binary could not be run on this builder: the builder's fault, which the user sees as an internal error.
			return fmt.Errorf("check the firecracker binary's features: %w", err)
		}
		logger.L().Warn(ctx, "refused a build whose firecracker binary lacks a feature its guest requires", zap.Error(err))

		// The user sees this error's text, so it is the refusal, which names no path of the builder.
		return phases.NewPhaseBuildError(basePhase, missing.Refusal())
	}

	return nil
}

var basePhase = phases.PhaseMeta{Phase: metrics.PhaseBase, StepType: "base"}
