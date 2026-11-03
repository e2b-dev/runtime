//go:build linux

package server

import (
	"context"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// pauseSteps are the steps of a pause that act on a real Firecracker VM or
// on the storage behind it. Tests pass their own to pauseWith to drive the
// pause to its response without one.
type pauseSteps interface {
	snapshot(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxPauseRequest, filesystemOnly, deferRootfsExport, envdUnresponsive bool) (*snapshotResult, error)
	upload(ctx context.Context, sbx *sandbox.Sandbox, res *snapshotResult)
	stopAsync(ctx context.Context, sbx *sandbox.Sandbox)
}

type serverPauseSteps struct {
	s *Server
}

func (p serverPauseSteps) snapshot(ctx context.Context, sbx *sandbox.Sandbox, in *orchestrator.SandboxPauseRequest, filesystemOnly, deferRootfsExport, envdUnresponsive bool) (*snapshotResult, error) {
	return p.s.snapshotAndCacheSandbox(ctx, sbx, in.GetBuildId(), map[string]string{storage.ObjectMetadataTemplateID: in.GetTemplateId()}, storage.ObjectOriginPause, filesystemOnly, deferRootfsExport, envdUnresponsive, sandboxStops)
}

func (p serverPauseSteps) upload(ctx context.Context, sbx *sandbox.Sandbox, res *snapshotResult) {
	p.s.uploadSnapshotAsync(ctx, sbx, res)
}

func (p serverPauseSteps) stopAsync(ctx context.Context, sbx *sandbox.Sandbox) {
	p.s.stopSandboxAsync(ctx, sbx)
}
