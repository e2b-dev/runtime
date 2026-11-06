// Package guest declares how the orchestrator adapts to the operating system of a template's guest.
//
// A profile describes one guest operating system. The guest OS attribute in the build metadata
// names the profile. The empty attribute names the default profile. The orchestrator refuses a
// build whose attribute no registered profile claims. metadata.Template.GuestProfile is the one
// place that turns the attribute into a profile.
package guest

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// Profile is the set of decisions that depend on the guest operating system.
// The zero value of each returned struct is the default behaviour.
// The default profile returns the zero value from every method except Name and Label.
type Profile interface {
	// Name returns the guest OS attribute value that the profile claims.
	Name() string
	// Label returns the value of the guest_os attribute on metrics, spans and refusal details.
	Label() string

	// Boot returns the Firecracker configuration of a cold boot.
	Boot() BootConfig
	// Restore returns the Firecracker configuration of a snapshot restore.
	Restore() RestoreConfig
	// Firecracker returns how the orchestrator selects the Firecracker binary and the kernel slot payload of a sandbox.
	Firecracker() FirecrackerSelection
	// EnvdInit returns the time budgets and the payload of the envd start.
	EnvdInit() EnvdInit

	// Capabilities returns the lifecycle operations that the guest does not support.
	Capabilities() Capabilities

	// Reset returns the classification of a guest reset and the boot identity probe.
	Reset() Reset

	// Template returns the template build behaviour of the guest.
	Template() TemplateBuild
	// FirstBoot returns the restart tolerance of the first boot of a base image.
	FirstBoot() FirstBoot
}

// BootConfig is the guest part of the Firecracker configuration of a cold boot.
type BootConfig struct {
	// NoKernelArgs sends an empty command line with the boot source.
	NoKernelArgs bool
	// PCI enables the PCI transport for the devices.
	PCI bool
	// MachineConfigExtra holds fields that the orchestrator merges into the machine configuration body.
	// A key that the body already has, or a value that is not valid JSON, refuses the boot before the first Firecracker request.
	// An empty map leaves the body unchanged.
	MachineConfigExtra map[string]json.RawMessage
	// NoRootDevice keeps the drive from being announced as the root device.
	NoRootDevice bool
	// BlockSectorSize is the logical block size that the drive advertises.
	// Zero keeps the size of the backing device.
	BlockSectorSize int64
	// NoEntropy omits the entropy device.
	NoEntropy bool
	// NoBalloon omits the balloon device, and with it free page reporting and free page hinting.
	NoBalloon bool
	// NoCPUTemplate applies no CPU template, neither the build's template nor an override.
	NoCPUTemplate bool
	// RefuseVcpusAboveTarget refuses a cold boot whose VM has more vCPUs than its target.
	RefuseVcpusAboveTarget bool
	// NoFilesystemRecovery skips the host-side journal recovery of the rootfs before a cold boot and before an in-place reboot.
	NoFilesystemRecovery bool
	// NoEnvdOfflineUpgrade skips the host-side envd replacement on the rootfs before a cold boot.
	NoEnvdOfflineUpgrade bool
}

// RestoreConfig is the guest part of the Firecracker configuration of a snapshot restore.
type RestoreConfig struct {
	// ClockRealtime advances the guest clock by the wall-clock time that the snapshot was paused.
	ClockRealtime bool
	// NoPowerOffPatch sends no power-off device declaration before the snapshot load.
	NoPowerOffPatch bool
	// MmdsStoreOptional tolerates a snapshot whose microVM has no MMDS store.
	MmdsStoreOptional bool
	// NoMemfd keeps the guest memory off the memfd backend.
	NoMemfd bool
	// NoSyncWriteProtect keeps the guest memory off synchronous write protection.
	NoSyncWriteProtect bool
}

// FirecrackerSelection is how the orchestrator selects the Firecracker binary and the kernel slot payload of a sandbox.
type FirecrackerSelection struct {
	// PinToBuild keeps the Firecracker binary and the kernel slot payload that the template metadata records.
	// PinToBuild also skips the fleet's version map.
	PinToBuild bool
}

// EnvdInit is the set of time budgets and the payload of the envd start.
type EnvdInit struct {
	// RequestTimeoutFloor is the minimum budget of one envd init request.
	// Zero keeps the budget of the flag.
	RequestTimeoutFloor time.Duration
	// ColdBootEnvdTimeout is the envd wait of a cold boot that has no caller budget.
	// Zero keeps the default wait.
	ColdBootEnvdTimeout time.Duration
	// NoCABundle sends no CA bundle with the envd init request.
	NoCABundle bool
}

// Capabilities is the set of lifecycle operations that the guest does not support.
type Capabilities struct {
	// NoFilesystemOnlyPause refuses a pause or a checkpoint that persists only the filesystem.
	NoFilesystemOnlyPause bool
	// NoReclaim skips the memory reclaim before a pause.
	NoReclaim bool
	// NoFreeze skips the cgroup freeze of the guest processes through envd.
	NoFreeze bool
	// NoQuiesce skips the filesystem quiesce through envd.
	NoQuiesce bool
	// NoEnvdLiveUpgrade skips the replacement of a running envd.
	NoEnvdLiveUpgrade bool
}

// DefaultBootIdentityCommand is the guest command that prints the identity of the current boot of the default guest.
const DefaultBootIdentityCommand = "cat /proc/sys/kernel/random/boot_id"

// Reset is the classification of a guest reset and the boot identity probe.
type Reset struct {
	// CleanExitBeforeEnvdIsReset classifies a clean Firecracker exit before envd answers as a guest reset.
	CleanExitBeforeEnvdIsReset bool
	// BootIdentityCommand is the guest command that prints the identity of the current boot.
	// Empty keeps DefaultBootIdentityCommand.
	BootIdentityCommand string
	// SameBoot reports whether two outputs of the boot identity command name the same boot.
	// Nil compares the trimmed outputs.
	SameBoot func(before, after string) bool
	// TypicalBoot is the expected time from a reset to a ready envd.
	// Zero keeps the default time.
	TypicalBoot time.Duration
	// Crash names the Firecracker metrics through which the guest reports a crash.
	Crash CrashReport
}

// IdentityCommand returns the boot identity command of the guest.
func (r Reset) IdentityCommand() string {
	if r.BootIdentityCommand == "" {
		return DefaultBootIdentityCommand
	}

	return r.BootIdentityCommand
}

// IsSameBoot reports whether two outputs of the boot identity command name the same boot.
func (r Reset) IsSameBoot(before, after string) bool {
	if r.SameBoot == nil {
		return strings.TrimSpace(before) == strings.TrimSpace(after)
	}

	return r.SameBoot(before, after)
}

// CrashReport names a counter group in the Firecracker metrics.
// The zero value means that the guest reports no crash.
type CrashReport struct {
	// Group is the name of the metrics group.
	Group string
	// CountField is the field of the group that counts the crashes since the last flush.
	CountField string
	// CodeField is the field of the group that holds the code of the last crash.
	CodeField string
}

// Phase names a template build phase that a profile can skip.
type Phase string

const (
	// PhaseUser is the creation of the default user of the template.
	PhaseUser Phase = "user"
	// PhaseEnsureFreeDisk is the growth of the rootfs after the steps and before the finalize phase.
	PhaseEnsureFreeDisk Phase = "ensure-free-disk"
	// PhaseOptimize is the optimize phase after the finalize phase.
	PhaseOptimize Phase = "optimize"
	// PhaseConfigure is the configuration script of the finalize phase.
	PhaseConfigure Phase = "configure"
	// PhaseCertBundle is the trust store bundle of the finalize phase.
	PhaseCertBundle Phase = "cert-bundle"
)

// TemplateBuild is the template build behaviour of the guest.
type TemplateBuild struct {
	// Renderer renders the build steps.
	// Nil keeps the default commands.
	Renderer StepRenderer
	// SkipPhases lists the build phases that the guest skips.
	SkipPhases []Phase
	// AlwaysResume makes every build of the guest a resume of its parent.
	// The build adopts the parent's machine: the kernel slot payload, the Firecracker binary, the memory size, the vCPU count and the device names.
	// The build refuses a parent that has only a filesystem snapshot, and a requested machine size that differs from the parent's.
	// Every step and the finalize phase resume the previous layer, and never create a sandbox.
	// The parent's start command keeps running, so the build refuses a new start command.
	AlwaysResume bool
	// KeepGuestEnvd keeps the envd of the image, and never replaces it with the host's envd.
	KeepGuestEnvd bool
	// SyncCommand is the guest command that flushes the disk writes after a step.
	// Empty keeps the default command.
	SyncCommand string
	// DefaultUser is the user of the build commands when the parent sets none.
	// Empty keeps the default user.
	DefaultUser string
	// DefaultWorkDir is the working directory of the build commands when the parent sets none.
	// Empty keeps the default directory.
	DefaultWorkDir string
	// HugePages backs the memory image of every template of the guest with huge pages.
	HugePages bool
	// LayerPause holds the pause options of a build layer.
	LayerPause LayerPause
}

// Skips reports whether the guest skips the build phase p.
func (t TemplateBuild) Skips(p Phase) bool {
	return slices.Contains(t.SkipPhases, p)
}

// Step is one template build step as the renderer receives it.
type Step struct {
	// Type is the upper-case step type: RUN, COPY, ADD, WORKDIR, ENV, ARG or USER.
	Type string
	// Args are the step arguments.
	Args []string
	// FilesHash identifies the archive of a COPY or ADD step.
	FilesHash string
}

// StepState is the build state that a step reads and changes.
type StepState struct {
	// User is the user of the commands.
	User string
	// WorkDir is the working directory of the commands.
	WorkDir string
	// Env holds the environment variables of the commands.
	Env map[string]string
}

// RenderedStep is the guest command of one step and the build state after it.
type RenderedStep struct {
	// Command is the guest command.
	// Command can be a compound script that stops at its first failure.
	// Empty runs nothing in the guest.
	Command string
	// WorkDirFromOutput makes the trimmed output of Command the new working directory.
	// Command then runs without a working directory, and an empty output fails the step.
	WorkDirFromOutput bool
	// Warnings are the lines that the build log shows as warnings for the step.
	Warnings []string
	// ArchivePath is the guest path that receives the archive of a COPY or ADD step.
	ArchivePath string
	// State is the build state after the step.
	State StepState
}

// StepRenderer renders template build steps into guest commands.
type StepRenderer interface {
	// Render returns the guest command of one step and the build state after it.
	Render(step Step, state StepState) (RenderedStep, error)
}

// LayerPause is the set of pause options of a template build layer.
type LayerPause struct {
	// Dedup writes the memory diff of the layer deduplicated against the parent.
	Dedup bool
	// DedupFlag gates Dedup when it is set.
	DedupFlag *featureflags.BoolFlag
}

// FirstBoot is the restart tolerance of the first boot of a base image.
// The zero value refuses a first boot.
type FirstBoot struct {
	// MaxResets is the number of guest resets that a first boot tolerates before envd answers.
	MaxResets int
	// EnvdWait is the envd wait of each boot attempt.
	EnvdWait time.Duration
}
