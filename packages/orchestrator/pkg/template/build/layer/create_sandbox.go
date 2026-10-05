//go:build linux

package layer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/constants"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// CreateSandbox creates sandboxes for new templates
type CreateSandbox struct {
	config         *sandbox.Config
	timeout        time.Duration
	sandboxFactory *sandbox.Factory

	rootfsCachePath string
	ioEngine        *string
	preBootFn       sandbox.PreBootFn
}

const (
	minEnvdVersionForKVMClock = "0.2.11"                 // Minimum version of envd that supports KVM clock
	DefaultIoEngine           = models.DriveIoEngineSync // Use the Sync io engine by default to avoid issues with Async.
)

var _ SandboxCreator = (*CreateSandbox)(nil)

type createSandboxOptions struct {
	rootfsCachePath string
	ioEngine        *string
	preBootFn       sandbox.PreBootFn
}

type CreateSandboxOption func(*createSandboxOptions)

func WithIoEngine(ioEngine string) CreateSandboxOption {
	return func(opts *createSandboxOptions) {
		opts.ioEngine = &ioEngine
	}
}

func WithRootfsCachePath(rootfsCachePath string) CreateSandboxOption {
	return func(opts *createSandboxOptions) {
		opts.rootfsCachePath = rootfsCachePath
	}
}

// WithPreBootFn sets a callback that runs after the rootfs is ready but before
// Firecracker boots. The callback receives the rootfs device path and can
// modify filesystem on the host side.
func WithPreBootFn(fn sandbox.PreBootFn) CreateSandboxOption {
	return func(opts *createSandboxOptions) {
		opts.preBootFn = fn
	}
}

// ReservedBlocksOptions returns CreateSandboxOption(s) that set reserved blocks
// on the rootfs before the guest boots, if the BuildReservedDiskSpaceMB feature
// flag is greater than zero. Returns nil otherwise.
func ReservedBlocksOptions(ctx context.Context, featureFlags *featureflags.Client, blockSize int64) []CreateSandboxOption {
	reservedDiskSpaceMB := int64(featureFlags.IntFlag(ctx, featureflags.BuildReservedDiskSpaceMB))
	if reservedDiskSpaceMB <= 0 {
		return nil
	}

	return []CreateSandboxOption{
		WithPreBootFn(func(ctx context.Context, rootfsPath string) error {
			return filesystem.SetReservedBlocksOnHost(ctx, rootfsPath, reservedDiskSpaceMB, blockSize)
		}),
	}
}

func NewCreateSandbox(config *sandbox.Config, sandboxFactory *sandbox.Factory, timeout time.Duration, options ...CreateSandboxOption) *CreateSandbox {
	opts := &createSandboxOptions{
		rootfsCachePath: "",
		ioEngine:        new(DefaultIoEngine),
	}
	for _, option := range options {
		option(opts)
	}

	return &CreateSandbox{
		config:          config,
		timeout:         timeout,
		rootfsCachePath: opts.rootfsCachePath,
		sandboxFactory:  sandboxFactory,
		ioEngine:        opts.ioEngine,
		preBootFn:       opts.preBootFn,
	}
}

func (cs *CreateSandbox) Sandbox(
	ctx context.Context,
	layerExecutor *LayerExecutor,
	sourceTemplate sbxtemplate.Template,
) (s *sandbox.Sandbox, err error) {
	// Create new memfile with the size of the sandbox RAM, this updates the underlying memfile.
	// This is ok as the sandbox is started from the beginning.
	memfile, err := block.NewEmpty(
		units.MBToBytes(cs.config.RamMB),
		config.MemfilePageSize(cs.config.HugePages),
		uuid.MustParse(sourceTemplate.Files().BuildID),
	)
	if err != nil {
		return nil, fmt.Errorf("create memfile: %w", err)
	}

	template := sbxtemplate.NewMaskTemplate(sourceTemplate, sbxtemplate.WithMemfile(memfile))

	kvmClock, err := utils.IsGTEVersion(cs.config.Envd.Version, minEnvdVersionForKVMClock)
	if err != nil {
		return nil, fmt.Errorf("error comparing envd version: %w", err)
	}

	// In case of a new sandbox, base template ID is now used as the potentially exported template base ID.
	sbx, err := cs.sandboxFactory.CreateSandbox(
		ctx,
		cs.config,
		sandboxtypes.RuntimeMetadata{
			TemplateID:  layerExecutor.Config.TemplateID,
			SandboxID:   config.InstanceBuildPrefix + id.Generate(),
			ExecutionID: uuid.NewString(),
			TeamID:      layerExecutor.Config.TeamID,
			BuildID:     layerExecutor.Template.BuildID,
			SandboxType: sandboxtypes.SandboxTypeBuild,
		},
		template,
		cs.timeout,
		cs.rootfsCachePath,
		fc.ProcessOptions{
			InitScriptPath:      constants.SystemdInitPath,
			KernelLogs:          env.IsDevelopment(),
			SystemdToKernelLogs: false,
			KvmClock:            kvmClock,
			IoEngine:            cs.ioEngine,
			// Every layer boot in a build gets the same arguments: base, each RUN
			// step, and the finalize boot whose memory becomes the template all go
			// through here. That is deliberate — a step that observes the kernel must
			// see the one the template ships with, which is also why the arguments
			// are part of the layer cache key. Only the BusyBox provisioning boot is
			// left on the default, because its VM is discarded.
			CmdlineArgs: layerExecutor.Config.CmdlineArgs,
			// Same reasoning as CmdlineArgs above, but for the CPU template.
			CPUTemplate: layerExecutor.Config.CPUTemplate,
		},
		nil,
		cs.preBootFn,
	)
	if err != nil {
		return nil, fmt.Errorf("create sandbox: %w", err)
	}
	defer func() {
		if err != nil {
			// Close the sandbox in case of error to avoid leaking resources
			err = errors.Join(err, sbx.Close(ctx))
		}
	}()

	err = sbx.WaitForEnvd(
		ctx,
		sandbox.StartTypeCreate,
		waitEnvdTimeout,
	)
	if err != nil {
		return nil, fmt.Errorf("wait for envd: %w", err)
	}

	return sbx, nil
}
