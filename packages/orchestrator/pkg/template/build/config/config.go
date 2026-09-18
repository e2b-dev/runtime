package config

import (
	"time"

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

	// The number of vCPUs to allocate to the VM.
	VCpuCount int64

	// The amount of RAM memory to allocate to the VM, in MiB.
	MemoryMB int64

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

	// Maximum time to wait for StartCmd when no ReadyCmd is provided.
	// Zero means use the built-in default (20s).
	StartCmdTimeout time.Duration

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
