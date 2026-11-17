package config

import (
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/oci/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

const (
	InstanceBuildPrefix = "b"

	// TemplateDefaultUser is the default user to use in the template to run all commands.
	TemplateDefaultUser = consts.TemplateDefaultUser
)

type TemplateConfig struct {
	// Builder version
	Version string

	// TeamID is the ID of the team to build the template for.
	TeamID string

	// TemplateID is the ID of the template to build.
	TemplateID string

	// CacheScope is the scope of layers and files caches.
	CacheScope string

	// Command to run when building the template.
	StartCmd string

	// The number of vCPUs online during the build and in the final template.
	VCpuCount int64

	// Capacity of the final template VM, of which VCpuCount are online. Provisioning
	// and step VMs boot with VCpuCount; 0 means the final VM also uses VCpuCount.
	MaxVCpuCount int64

	// The amount of RAM memory to allocate to the VM, in MiB.
	MemoryMB int64

	// Memory size, in MiB, the final template VM can grow to: MemoryMB of boot RAM plus a
	// hotplug region with nothing plugged. Provisioning and step VMs boot without the region;
	// 0 or MemoryMB means none.
	MaxMemoryMB int64

	// The amount of free rootfs working space provided before build steps, in MiB.
	DiskSizeMB int64

	// The amount of free rootfs target after build steps and before finalize, in MiB.
	// ext4 metadata and finalize writes may reduce the available space.
	FreeDiskSizeMB int64

	// HugePages sets whether the VM use huge pages.
	HugePages bool

	// FreePageReporting enables Firecracker's balloon free-page-reporting.
	FreePageReporting bool

	// FreePageHinting enables Firecracker's balloon free-page-hinting.
	FreePageHinting bool

	// Command to run to check if the template is ready.
	ReadyCmd string

	// FromImage is the base image to use for building the template.
	FromImage string

	// FromTemplate is the base template to use for building the template.
	FromTemplate *templatemanager.FromTemplateConfig

	// RegistryAuthProvider provides authentication for pulling the FromImage.
	RegistryAuthProvider auth.RegistryAuthProvider

	// Force rebuild of the template even if it is already cached.
	Force *bool

	// Steps to build the template.
	Steps []*templatemanager.TemplateStep

	// Firecracker version to use
	FirecrackerVersion string

	// Kernel version to use
	KernelVersion string

	// CmdlineArgs carries the extra guest kernel command line parameters this build boots
	// with, already parsed. Nil is the default command line. Read from the per-team feature
	// flag once, at the start of the build, so every boot in the build agrees and the
	// stored snapshot is self-describing.
	CmdlineArgs map[string]string

	// CPUTemplate is the CPU template every boot in this build applies, resolved once from
	// the per-team flag at build start. Nil is none.
	CPUTemplate *cputemplate.Template

	// Guest is the guest profile of the build, the parent template's when the build
	// starts FROM one; nil is the default profile.
	Guest guest.Profile

	// RootfsDriveID, NetIfaceID and RootfsFile are the device names the parent's memory
	// image bakes, which every layer of a build that resumes its parent addresses;
	// empty is the runtime's own name for each.
	RootfsDriveID string
	NetIfaceID    string
	RootfsFile    string
}

// GuestProfile is the guest profile of the build.
func (e TemplateConfig) GuestProfile() guest.Profile {
	if e.Guest == nil {
		return guest.Default()
	}

	return e.Guest
}

// GuestTemplate is the template build behaviour of the build's guest.
func (e TemplateConfig) GuestTemplate() guest.TemplateBuild {
	return e.GuestProfile().Template()
}

// FirecrackerConfig is how every VM of this build is driven: the kernel, the
// Firecracker binary, the guest profile and the device names its memory image bakes.
func (e TemplateConfig) FirecrackerConfig() fc.Config {
	return fc.Config{
		KernelVersion:      e.KernelVersion,
		FirecrackerVersion: e.FirecrackerVersion,
		Guest:              e.Guest,
		RootfsDriveID:      e.RootfsDriveID,
		NetIfaceID:         e.NetIfaceID,
		RootfsFile:         e.RootfsFile,
	}
}

// ObjectMetadata is the provenance stamped on a build's uploaded objects.
// buildOrigin distinguishes the final template build from intermediate
// build-cache layers.
func (e TemplateConfig) ObjectMetadata(buildOrigin storage.ObjectOrigin) storage.ObjectMetadata {
	return storage.ObjectMetadata{
		storage.ObjectMetadataTeamID:      e.TeamID,
		storage.ObjectMetadataTemplateID:  e.TemplateID,
		storage.ObjectMetadataBuildOrigin: string(buildOrigin),
	}
}

func MemfilePageSize(hugePages bool) int64 {
	if hugePages {
		return header.HugepageSize
	}

	return header.PageSize
}

func (e TemplateConfig) RootfsBlockSize() int64 {
	return header.RootfsBlockSize
}

// VcpuSizing labels the final VM size a build requests: absent (maxVCpus 0),
// equal, or resizable (more vCPUs than are online).
func VcpuSizing(vcpus, maxVCpus int64) string {
	switch {
	case maxVCpus == 0:
		return "absent"
	case maxVCpus > vcpus:
		return "resizable"
	default:
		return "equal"
	}
}

// MemoryHotplugMB is the hotplug region of the final template VM, in MiB; 0 is none.
func (e TemplateConfig) MemoryHotplugMB() int64 {
	return MemoryHotplugRegionMB(e.MemoryMB, e.MaxMemoryMB)
}

// MemoryHotplugRegionMB is the hotplug region above memoryMB of boot RAM for a maximum of
// maxMemoryMB; 0 when the maximum is absent or not above boot RAM.
func MemoryHotplugRegionMB(memoryMB, maxMemoryMB int64) int64 {
	if maxMemoryMB <= memoryMB {
		return 0
	}

	return maxMemoryMB - memoryMB
}

// MemorySizing labels the memory a build requests: absent (maxMemoryMB 0), equal, or resizable
// (a hotplug region above boot RAM).
func MemorySizing(memoryMB, maxMemoryMB int64) string {
	switch {
	case maxMemoryMB == 0:
		return "absent"
	case maxMemoryMB > memoryMB:
		return "resizable"
	default:
		return "equal"
	}
}
