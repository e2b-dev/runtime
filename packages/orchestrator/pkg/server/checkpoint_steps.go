package server

import (
	"context"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// resumeFreshSteps are the steps of a resume-fresh checkpoint that act on a
// real Firecracker VM. Tests pass their own to checkpointResumeFreshWith to
// drive the checkpoint without one.
type resumeFreshSteps interface {
	snapshot(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest) (*snapshotResult, error)
	template(ctx context.Context, sbx *sandbox.Sandbox, buildID string) (sbxtemplate.Template, func(), error)
	resume(ctx context.Context, template sbxtemplate.Template, sbx *sandbox.Sandbox) (*sandbox.Sandbox, error)
	stopAndClose(ctx context.Context, sbx *sandbox.Sandbox) error
	stopAsync(ctx context.Context, sbx *sandbox.Sandbox)
}

type serverResumeFreshSteps struct {
	s *Server
}

func (r serverResumeFreshSteps) snapshot(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxCheckpointRequest) (*snapshotResult, error) {
	// Always a full memory snapshot: the fresh sandbox is resumed from the new
	// build, which a memoryless build could only cold-boot (Checkpoint routes
	// filesystem-only requests to checkpointInPlace instead). The resume is
	// immediate, so the diff must be materialized synchronously — never defer
	// the rootfs export here, and never maintain the paused sandbox.
	return r.s.snapshotAndCacheSandbox(ctx, sbx, in.GetBuildId(), in.GetMetadata(), storage.ObjectOriginSnapshotTemplate, false, false, false, sandboxStops, sandbox.WithMemoryRequired())
}

func (r serverResumeFreshSteps) template(ctx context.Context, sbx *sandbox.Sandbox, buildID string) (sbxtemplate.Template, func(), error) {
	// Pinned for the resumed sandbox's lifetime; see the Create path for why.
	return r.s.templateCache.GetTemplatePinned(ctx, buildID, true, false,
		sbxtemplate.GetTemplateOpts{MaxSandboxLengthHours: sbx.Config.MaxSandboxLengthHours})
}

func (r serverResumeFreshSteps) resume(ctx context.Context, template sbxtemplate.Template, sbx *sandbox.Sandbox) (*sandbox.Sandbox, error) {
	// Resume the sandbox keeping the same ExecutionID (stable identity for
	// the API, routing catalog, and analytics) but with a fresh LifecycleID
	// so the old sandbox's cleanup goroutine won't
	// accidentally evict the resumed sandbox from the map.
	releaseHugePages := r.s.holdHugePages(ctx, sbx.Config)
	defer releaseHugePages()

	return r.s.sandboxFactory.ResumeSandbox(
		ctx,
		template,
		sbx.Config,
		sandboxtypes.RuntimeMetadata{
			TemplateID:  sbx.Runtime.TemplateID,
			SandboxID:   sbx.Runtime.SandboxID,
			ExecutionID: sbx.Runtime.ExecutionID,
			TeamID:      sbx.Runtime.TeamID,
			BuildID:     sbx.Runtime.BuildID,
			SandboxType: sbx.Runtime.SandboxType,
		},
		sbx.GetStartedAt(),
		sbx.GetEndAt(),
		sbx.APIStoredConfig,
		// Defer routing until after the upgrade's post-/init (markSandboxLive).
		sandbox.WithDeferredLiveRegistration(),
		sandbox.WithExecutionStartedAt(sbx.GetExecutionStartedAt()),
	)
}

func (r serverResumeFreshSteps) stopAndClose(ctx context.Context, sbx *sandbox.Sandbox) error {
	return stopAndCloseSandbox(ctx, sbx)
}

func (r serverResumeFreshSteps) stopAsync(ctx context.Context, sbx *sandbox.Sandbox) {
	r.s.stopSandboxAsync(ctx, sbx)
}
