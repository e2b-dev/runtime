//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// resumingProfile is a guest profile whose builds resume their parent for every layer.
type resumingProfile struct{ guest.Profile }

func (resumingProfile) Name() string  { return "resuming" }
func (resumingProfile) Label() string { return "resuming" }
func (resumingProfile) Template() guest.TemplateBuild {
	return guest.TemplateBuild{AlwaysResume: true, HugePages: true}
}

// bootingProfile is a guest profile whose builds boot their layers like the default profile.
type bootingProfile struct{ guest.Profile }

func (bootingProfile) Name() string  { return "booting" }
func (bootingProfile) Label() string { return "booting" }

// pickyProfile boots its layers on a Firecracker of its own line.
type pickyProfile struct{ guest.Profile }

func (pickyProfile) Name() string  { return "picky" }
func (pickyProfile) Label() string { return "picky" }
func (pickyProfile) Firecracker() guest.FirecrackerSelection {
	return guest.FirecrackerSelection{
		AcceptVersion: func(version string) error {
			if version != "v9.9.9-picky" {
				return errWrongLine
			}

			return nil
		},
	}
}

var errWrongLine = errors.New("not the picky line")

// pickyResumingProfile resumes its parent and refuses Firecracker versions as pickyProfile does.
type pickyResumingProfile struct{ pickyProfile }

func (pickyResumingProfile) Name() string  { return "picky-resuming" }
func (pickyResumingProfile) Label() string { return "picky-resuming" }
func (pickyResumingProfile) Template() guest.TemplateBuild {
	return guest.TemplateBuild{AlwaysResume: true, HugePages: true}
}

func init() {
	r, err := guest.NewRegistry(resumingProfile{guest.Default()}, bootingProfile{guest.Default()}, pickyProfile{guest.Default()}, pickyResumingProfile{pickyProfile{guest.Default()}})
	if err != nil {
		panic(err)
	}
	if err = guest.Install(r); err != nil {
		panic(err)
	}
}

// flagsOn answers every flag read with its value.
type flagsOn bool

func (f flagsOn) BoolFlag(context.Context, featureflags.BoolFlag, ...ldcontext.Context) bool {
	return bool(f)
}

// failingFlags fails the test on any flag read.
type failingFlags struct{ t *testing.T }

func (f failingFlags) BoolFlag(_ context.Context, flag featureflags.BoolFlag, _ ...ldcontext.Context) bool {
	f.t.Fatalf("the flag %s was read", flag.Key())

	return false
}

type parentIndex struct {
	parents map[string]metadata.Template
}

func (p parentIndex) LayerMetaFromHash(context.Context, string) (cache.LayerMetadata, error) {
	return cache.LayerMetadata{}, storage.ErrObjectNotExist
}

func (p parentIndex) SaveLayerMeta(context.Context, string, cache.LayerMetadata) error { return nil }

func (p parentIndex) Cached(_ context.Context, buildID string) (metadata.Template, error) {
	m, ok := p.parents[buildID]
	if !ok {
		return metadata.Template{}, storage.ErrObjectNotExist
	}

	return m, nil
}

func (p parentIndex) Version() string { return "test" }

func requestedContext(parent string) buildcontext.BuildContext {
	return buildcontext.BuildContext{
		Template: storage.Paths{BuildID: "child"},
		Config: config.TemplateConfig{
			TemplateID:         "tpl",
			FromTemplate:       &templatemanager.FromTemplateConfig{Alias: "parent", BuildID: parent},
			KernelVersion:      "vmlinux-6.1.158",
			FirecrackerVersion: "v1.14.1_431f1fc",
			MemoryMB:           512,
			VCpuCount:          1,
			MaxVCpuCount:       4,
			CmdlineArgs:        map[string]string{"quiet": ""},
			FreePageReporting:  true,
		},
	}
}

func TestAdoptParentMachineTakesTheParentsMachine(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"resumed-parent": {
			RootfsDriveID: "root", NetIfaceID: "net0", RootfsFile: "disk.img",
			Version:   metadata.CurrentVersion,
			GuestOS:   "resuming",
			VcpuCount: 2,
			Template:  metadata.TemplateMetadata{BuildID: "resumed-parent", KernelVersion: "kernel-slot-1", FirecrackerVersion: "v9.9.9-parent"},
		},
	}}
	sized := func(_ context.Context, buildID string) (int64, error) {
		assert.Equal(t, "resumed-parent", buildID)

		return 4096, nil
	}

	in := requestedContext("resumed-parent")
	in.Config.MemoryMB, in.Config.VCpuCount, in.Config.MaxVCpuCount = 4096, 2, 0
	bc, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)

	cfg := bc.Config
	assert.Equal(t, "resuming", cfg.GuestProfile().Name())
	assert.Equal(t, "kernel-slot-1", cfg.KernelVersion)
	assert.Equal(t, "v9.9.9-parent", cfg.FirecrackerVersion)
	assert.Equal(t, int64(4096), cfg.MemoryMB)
	assert.True(t, cfg.HugePages, "huge pages come from the profile")
	assert.Equal(t, int64(2), cfg.VCpuCount)
	assert.Zero(t, cfg.MaxVCpuCount)
	assert.Nil(t, cfg.CmdlineArgs)
	assert.Nil(t, cfg.CPUTemplate)
	assert.False(t, cfg.FreePageReporting)
	assert.Equal(t, "root", cfg.RootfsDriveID)
	assert.Equal(t, "net0", cfg.NetIfaceID)
	assert.Equal(t, "disk.img", cfg.RootfsFile)

	fcc := cfg.FirecrackerConfig()
	assert.Equal(t, "resuming", fcc.GuestProfile().Name())
	assert.Equal(t, "kernel-slot-1", fcc.KernelVersion)
	assert.Equal(t, "root", fcc.RootfsDriveID)
	assert.Equal(t, "vmlinux-6.1.158", in.Config.KernelVersion, "the caller's configuration is not mutated")
}

func TestAdoptParentMachineLeavesABootingParentAlone(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"default-parent": {Version: metadata.CurrentVersion, Template: metadata.TemplateMetadata{BuildID: "default-parent", KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.14.1_431f1fc"}},
		"booting-parent": {Version: metadata.CurrentVersion, GuestOS: "booting", Template: metadata.TemplateMetadata{BuildID: "booting-parent", KernelVersion: "k", FirecrackerVersion: "f"}},
	}}
	sized := func(context.Context, string) (int64, error) {
		t.Fatal("a booting parent's memory is never sized")

		return 0, nil
	}

	in := requestedContext("default-parent")
	bc, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, guest.DefaultName, bc.Config.GuestProfile().Name())
	bc.Config.Guest = nil
	assert.Equal(t, in, bc, "a default parent leaves the configuration as it was")

	bc, err = adoptParentMachine(t.Context(), requestedContext("booting-parent"), index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, "booting", bc.Config.GuestProfile().Name(), "the profile is taken")
	assert.Equal(t, "vmlinux-6.1.158", bc.Config.KernelVersion, "and the machine is the request's")

	in.Config.FromTemplate = nil
	in.Config.FromImage = "ubuntu:22.04"
	bc, err = adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, in, bc)

	bc, err = adoptParentMachine(t.Context(), requestedContext("missing"), index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err, "a missing parent is the base phase's error to report")
	assert.Equal(t, requestedContext("missing"), bc)
}

type failingIndex struct {
	parentIndex

	err error
}

func (f failingIndex) Cached(context.Context, string) (metadata.Template, error) {
	return metadata.Template{}, f.err
}

func TestAdoptParentMachineReportsAnIndexError(t *testing.T) {
	t.Parallel()

	boom := errors.New("object store: 503")
	_, err := adoptParentMachine(t.Context(), requestedContext("p"), failingIndex{err: boom}, nil, flagsOn(true), logger.NewNopLogger())
	require.ErrorIs(t, err, boom, "a transient read failure never leaves a resuming parent on the booting pipeline")
}

func TestAdoptParentMachineRefusesAnUnclaimedGuest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		index cache.Index
	}{
		{
			// The hash index reads the parent through metadata.FromBuildID, which refuses the guest OS itself.
			name:  "refused by the index",
			index: failingIndex{err: fmt.Errorf("error reading template metadata: template metadata: %w %q", guest.ErrUnclaimed, "unclaimed")},
		},
		{
			name:  "read without the guest check",
			index: parentIndex{parents: map[string]metadata.Template{"p": {Version: metadata.CurrentVersion, GuestOS: "unclaimed"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := adoptParentMachine(t.Context(), requestedContext("p"), tt.index, nil, flagsOn(true), logger.NewNopLogger())
			require.ErrorIs(t, err, guest.ErrUnclaimed)
			require.NotNil(t, phases.UnwrapPhaseBuildError(err), "a parent of a guest this node cannot build is the build's refusal, not an internal error")
		})
	}
}

func TestAdoptParentMachineRefusesADiskOnlyParent(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"disk": {Version: metadata.CurrentVersion, GuestOS: "resuming", FilesystemOnly: true},
	}}
	sized := func(context.Context, string) (int64, error) { return 0, errors.New("unreachable") }

	_, err := adoptParentMachine(t.Context(), requestedContext("disk"), index, sized, flagsOn(true), logger.NewNopLogger())
	require.ErrorIs(t, err, errParentNeedsMemory)
}

// The build row keeps the requested size and serves every sandbox by it, so a
// request that differs from the parent snapshot's machine is refused.
func TestAdoptParentMachineRefusesAnotherMachineSize(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"p": {Version: metadata.CurrentVersion, GuestOS: "resuming", VcpuCount: 2},
	}}
	sized := func(context.Context, string) (int64, error) { return 4096, nil }

	for _, size := range [][2]int64{{512, 1}, {4096, 4}, {8192, 2}} {
		in := requestedContext("p")
		in.Config.MemoryMB, in.Config.VCpuCount = size[0], size[1]
		_, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
		require.ErrorIs(t, err, errParentMachineSize, "requested %v", size)
	}
}

// A resumed parent's snapshot has a fixed vCPU count, so a resizable request is
// refused rather than advertised with a capacity no resume can give; a maximum
// equal to the count is not resizable and is taken.
func TestAdoptParentMachineRefusesAResizableRequest(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"p": {Version: metadata.CurrentVersion, GuestOS: "resuming", VcpuCount: 2},
	}}
	sized := func(context.Context, string) (int64, error) { return 4096, nil }

	in := requestedContext("p")
	in.Config.MemoryMB, in.Config.VCpuCount, in.Config.MaxVCpuCount = 4096, 2, 4
	_, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.ErrorIs(t, err, errParentNotResizable)
	require.NotNil(t, phases.UnwrapPhaseBuildError(err), "the request is the user's to change")

	in.Config.MaxVCpuCount = 2
	_, err = adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)
}

// A resumed parent that records no vCPU count is refused: the size the child
// would advertise could differ from the VM its snapshot restores.
func TestAdoptParentMachineRefusesAnUnknownVcpuCount(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"p": {Version: metadata.CurrentVersion, GuestOS: "resuming"},
	}}
	sized := func(context.Context, string) (int64, error) { return 4096, nil }

	in := requestedContext("p")
	in.Config.MemoryMB, in.Config.VCpuCount = 4096, 8
	_, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.ErrorIs(t, err, errParentVcpusUnknown)
}

// A build whose profile resumes every layer never gets the disk growth: its
// layer is filesystem-only, and finalize resumes the layer before it from memory.
func TestRunsEnsureFreeDisk(t *testing.T) {
	t.Parallel()

	assert.True(t, runsEnsureFreeDisk(true, guest.TemplateBuild{}))
	assert.False(t, runsEnsureFreeDisk(false, guest.TemplateBuild{}), "the flag gates it")
	assert.False(t, runsEnsureFreeDisk(true, guest.TemplateBuild{SkipPhases: []guest.Phase{guest.PhaseEnsureFreeDisk}}))
	assert.False(t, runsEnsureFreeDisk(true, guest.TemplateBuild{AlwaysResume: true}), "whatever the skip list says")
}

// A profile that keeps the image's envd reports the version that envd recorded
// in the final snapshot, not the host binary the build never installed.
func TestTemplateEnvdVersion(t *testing.T) {
	t.Parallel()

	kept := metadata.Template{RunningEnvdVersion: "0.5.12"}
	keep := guest.TemplateBuild{KeepGuestEnvd: true}

	assert.Equal(t, "0.10.0", templateEnvdVersion("0.10.0", guest.TemplateBuild{}, kept), "an installed envd is the host's")
	assert.Equal(t, "0.5.12", templateEnvdVersion("0.10.0", keep, kept))
	assert.Equal(t, "0.10.0", templateEnvdVersion("0.10.0", keep, metadata.Template{}), "a snapshot that records none runs the host's version")
}

// A parent of a non-default guest is refused while the flag is off, before
// the parent's memory is sized; the flag is read on every build.
func TestAdoptParentMachineRefusesANonDefaultGuestWithTheFlagOff(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"resumed": {Version: metadata.CurrentVersion, GuestOS: "resuming", VcpuCount: 1},
		"booted":  {Version: metadata.CurrentVersion, GuestOS: "booting"},
	}}
	sized := func(context.Context, string) (int64, error) {
		t.Fatal("a refused parent's memory is never sized")

		return 0, nil
	}

	for _, parent := range []string{"resumed", "booted"} {
		_, err := adoptParentMachine(t.Context(), requestedContext(parent), index, sized, flagsOn(false), logger.NewNopLogger())
		var notEnabled *guest.NotEnabledError
		require.ErrorAs(t, err, &notEnabled, parent)
		assert.Contains(t, err.Error(), "is not enabled on this node", parent)
	}
}

// A build FROM a default parent, or from an image, never reads the flag.
func TestAdoptParentMachineNeverReadsTheFlagForTheDefaultGuest(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"default-parent": {Version: metadata.CurrentVersion, Template: metadata.TemplateMetadata{BuildID: "default-parent"}},
	}}
	sized := func(context.Context, string) (int64, error) { return 512, nil }

	bc, err := adoptParentMachine(t.Context(), requestedContext("default-parent"), index, sized, failingFlags{t}, logger.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, guest.DefaultName, bc.Config.GuestProfile().Name())

	in := requestedContext("default-parent")
	in.Config.FromTemplate = nil
	in.Config.FromImage = "ubuntu:22.04"
	_, err = adoptParentMachine(t.Context(), in, index, sized, failingFlags{t}, logger.NewNopLogger())
	require.NoError(t, err)
}

// A profile's Firecracker version rule runs on the version the build will
// use: the request's for a booting profile.
func TestAdoptParentMachineRefusesTheWrongFirecracker(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"p": {Version: metadata.CurrentVersion, GuestOS: "picky"},
	}}
	sized := func(context.Context, string) (int64, error) { return 512, nil }

	in := requestedContext("p")
	_, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.ErrorIs(t, err, errWrongLine, "the request's version is not the profile's line")

	in.Config.FirecrackerVersion = "v9.9.9-picky"
	bc, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, "picky", bc.Config.GuestProfile().Name())
}

// On the resume path the version checked is the parent's pin, which the build
// takes, not the one the request names.
func TestAdoptParentMachineChecksTheParentsFirecrackerOnTheResumePath(t *testing.T) {
	t.Parallel()

	index := parentIndex{parents: map[string]metadata.Template{
		"other-line": {Version: metadata.CurrentVersion, GuestOS: "picky-resuming", VcpuCount: 2, Template: metadata.TemplateMetadata{BuildID: "other-line", FirecrackerVersion: "v1.17-0.2.0"}},
		"picky-line": {Version: metadata.CurrentVersion, GuestOS: "picky-resuming", VcpuCount: 2, Template: metadata.TemplateMetadata{BuildID: "picky-line", FirecrackerVersion: "v9.9.9-picky"}},
	}}
	sized := func(context.Context, string) (int64, error) { return 4096, nil }

	in := requestedContext("other-line")
	in.Config.MemoryMB, in.Config.VCpuCount, in.Config.MaxVCpuCount = 4096, 2, 0
	in.Config.FirecrackerVersion = "v9.9.9-picky"
	_, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.ErrorIs(t, err, errWrongLine, "the parent's pin is checked, whatever the request names")
	require.NotNil(t, phases.UnwrapPhaseBuildError(err))

	in = requestedContext("picky-line")
	in.Config.MemoryMB, in.Config.VCpuCount, in.Config.MaxVCpuCount = 4096, 2, 0
	bc, err := adoptParentMachine(t.Context(), in, index, sized, flagsOn(true), logger.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, "v9.9.9-picky", bc.Config.FirecrackerVersion, "the parent's pin is the build's")
}
