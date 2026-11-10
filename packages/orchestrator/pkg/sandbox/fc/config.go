//go:build linux

package fc

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/artifact"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	envsDisk     = "/mnt/disks/fc-envs/v1"
	buildDirName = "builds"

	rootfsDriveID = "rootfs"

	entropyBytesSize    int64 = 1024 // 1 KB
	entropyRefillTime   int64 = 100
	entropyOneTimeBurst int64 = 0
)

type Config struct {
	KernelVersion      string
	FirecrackerVersion string

	// The device names a snapshot bakes. Every snapshot this code base writes
	// uses the defaults (drive "rootfs", interface "eth0", rootfs file
	// "rootfs.ext4"), and an empty field means that default. A snapshot made
	// outside it (an imported image) names them differently: the restore then addresses the drive and the interface
	// by these names, presents the rootfs at the baked file name, and sends the
	// slot's tap through network_overrides.
	RootfsDriveID string
	NetIfaceID    string
	RootfsFile    string

	// Guest is the profile of the template's guest OS; nil is the default profile.
	Guest guest.Profile
}

// GuestProfile returns the profile that decides how the guest boots and restores.
func (t Config) GuestProfile() guest.Profile {
	if t.Guest == nil {
		return guest.Default()
	}

	return t.Guest
}

// ForColdBoot returns a copy with only RootfsDriveID and NetIfaceID cleared, so
// RootfsDriveIDOrDefault and NetIfaceIDOrDefault fall back to the runtime's drive
// ID and the caller's default. A restore must not use it: it addresses the
// devices by the IDs the snapshot holds.
func (t Config) ForColdBoot() Config {
	t.RootfsDriveID = ""
	t.NetIfaceID = ""

	return t
}

// RootfsDriveIDOrDefault returns the block device ID to address on the API.
func (t Config) RootfsDriveIDOrDefault() string {
	if t.RootfsDriveID == "" {
		return rootfsDriveID
	}

	return t.RootfsDriveID
}

// NetIfaceIDOrDefault returns the network interface ID to address on the API;
// def is the slot's own name, which every snapshot this code base writes bakes.
func (t Config) NetIfaceIDOrDefault(def string) string {
	if t.NetIfaceID == "" {
		return def
	}

	return t.NetIfaceID
}

// RootfsFileName returns the file name the guest's rootfs is presented at
// inside the sandbox directory.
func (t Config) RootfsFileName() string {
	if t.RootfsFile == "" {
		return artifact.RootfsFileName
	}

	return t.RootfsFile
}

func (t Config) SandboxKernelDir() string {
	return t.KernelVersion
}

func (t Config) HostKernelPath(config cfg.BuilderConfig) string {
	// Prefer arch-prefixed path ({version}/{arch}/vmlinux.bin) for multi-arch support.
	// Fall back to legacy flat path ({version}/vmlinux.bin) for existing production nodes.
	archPath := filepath.Join(config.HostKernelsDir, t.KernelVersion, utils.TargetArch(), artifact.KernelFileName)
	if _, err := os.Stat(archPath); err == nil {
		return archPath
	} else if !errors.Is(err, os.ErrNotExist) {
		// Non-existence errors (e.g. permission denied) should not silently fall back
		// to the legacy path, as that could use the wrong binary.
		return archPath
	}

	return filepath.Join(config.HostKernelsDir, t.KernelVersion, artifact.KernelFileName)
}

func (t Config) FirecrackerPath(config cfg.BuilderConfig) string {
	// Prefer arch-prefixed path ({version}/{arch}/firecracker) for multi-arch support.
	// Fall back to legacy flat path ({version}/firecracker) for existing production nodes
	// that haven't migrated to the arch-prefixed layout yet.
	archPath, legacyPath := t.firecrackerPaths(config)
	if _, err := os.Stat(archPath); err == nil {
		return archPath
	} else if !errors.Is(err, os.ErrNotExist) {
		// Non-existence errors (e.g. permission denied) should not silently fall back
		// to the legacy path, as that could use the wrong binary.
		return archPath
	}

	return legacyPath
}

func (t Config) firecrackerPaths(config cfg.BuilderConfig) (archPath, legacyPath string) {
	return filepath.Join(config.FirecrackerVersionsDir, t.FirecrackerVersion, utils.TargetArch(), artifact.FirecrackerBinaryName),
		filepath.Join(config.FirecrackerVersionsDir, t.FirecrackerVersion, artifact.FirecrackerBinaryName)
}

type RootfsPaths struct {
	TemplateVersion uint64
	TemplateID      string
	BuildID         string
}

var ConstantRootfsPaths = RootfsPaths{
	// The version is always 2 for the constant rootfs paths format change.
	TemplateVersion: 2,
}

// Deprecated: Use static rootfs path instead.
func (t RootfsPaths) DeprecatedSandboxRootfsDir() string {
	return filepath.Join(envsDisk, t.TemplateID, buildDirName, t.BuildID)
}
