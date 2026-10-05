package nodemanager

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"

	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	grpcshared "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

type NodeMetadata struct {
	// Service instance ID is unique identifier for every orchestrator process, after restart it will change.
	// In the future, we want to migrate to using this ID instead of node ID for tracking orchestrators-
	ServiceInstanceID string

	Commit  string
	Version string
}

func (n *Node) setMetadata(md NodeMetadata) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.meta = md
}

func (n *Node) Metadata() NodeMetadata {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	return n.meta
}

func (n *Node) GetSandboxCreateCtx(ctx context.Context, req *orchestrator.SandboxCreateRequest) (*clusters.GRPCClient, context.Context) {
	// Pass snapshot (is_resume) via metadata so the server-side stats handler
	// can include it in otelgrpc metric attributes during TagRPC.
	ctx = metadata.AppendToOutgoingContext(ctx, grpcshared.IsResumeMetadataKey, strconv.FormatBool(req.GetSandbox().GetSnapshot()))

	return n.client, ctx
}
