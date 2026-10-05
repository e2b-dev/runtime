package template_manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/queries"
	clustersshared "github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/template-manager")

var _ templateManagerClient = (*TemplateManager)(nil)

type processingBuilds struct {
	templateID string
}

type TemplateManager struct {
	clusters      *clusters.Pool
	lock          sync.Mutex
	processing    map[uuid.UUID]processingBuilds
	buildCache    *templatecache.TemplatesBuildCache
	templateCache *templatecache.TemplateCache
	sqlcDB        *sqlcdb.Client

	featureFlags *featureflags.Client
}

type DeleteBuild struct {
	BuildID    uuid.UUID
	TemplateID string

	ClusterID uuid.UUID
	NodeID    string
}

const (
	syncInterval = time.Minute * 1
)

func New(
	sqlcDB *sqlcdb.Client,
	clusters *clusters.Pool,
	buildCache *templatecache.TemplatesBuildCache,
	templateCache *templatecache.TemplateCache,
	featureFlags *featureflags.Client,
) (*TemplateManager, error) {
	tm := &TemplateManager{
		sqlcDB:        sqlcDB,
		buildCache:    buildCache,
		templateCache: templateCache,
		clusters:      clusters,
		featureFlags:  featureFlags,

		lock:       sync.Mutex{},
		processing: make(map[uuid.UUID]processingBuilds),
	}

	return tm, nil
}

func (tm *TemplateManager) BuildsStatusPeriodicalSync(ctx context.Context) {
	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dbCtx, dbxCtxCancel := context.WithTimeout(ctx, 5*time.Second)
			buildsRunning, err := tm.sqlcDB.GetInProgressTemplateBuilds(dbCtx)
			if err != nil {
				logger.L().Error(ctx, "Error getting running builds for periodical sync", zap.Error(err))
				dbxCtxCancel()

				continue
			}

			logger.L().Info(ctx, "Running periodical sync of builds statuses", zap.Int("count", len(buildsRunning)))
			for _, b := range buildsRunning {
				go func(b queries.GetInProgressTemplateBuildsRow) {
					err := tm.BuildStatusSync(ctx, b.EnvBuild.ID, b.ActiveEnv.ID, clustersshared.WithClusterFallback(b.ActiveEnv.ClusterID), b.EnvBuild.ClusterNodeID)
					if err != nil {
						logger.L().Error(ctx, "Error syncing build status", zap.Error(err), zap.String("buildID", b.EnvBuild.ID.String()))
					}
				}(b)
			}

			dbxCtxCancel()
		}
	}
}

func (tm *TemplateManager) GetAvailableBuildClient(ctx context.Context, clusterID uuid.UUID) (*clusters.Instance, error) {
	cluster, ok := tm.clusters.GetClusterById(clusterID)
	if !ok {
		return nil, fmt.Errorf("cluster with ID '%s' not found", clusterID)
	}

	// Set feature flags context for cluster
	ctx = featureflags.AddToContext(ctx, featureflags.ClusterContext(clusterID))

	nodeInfoJSON := tm.featureFlags.JSONFlag(ctx, featureflags.BuildNodeInfo)
	nodeInfo := machineinfo.FromLDValue(ctx, nodeInfoJSON)
	builder, err := cluster.GetAvailableTemplateBuilder(ctx, nodeInfo)
	if err != nil {
		if errors.Is(err, clusters.ErrAvailableTemplateBuilderNotFound) {
			// Fallback to any template builder
			logger.L().Warn(ctx, "No available template builder found with the specified machine info, falling back to any available template builder", zap.String("clusterID", clusterID.String()))

			builder, err = cluster.GetAvailableTemplateBuilder(ctx, machineinfo.MachineInfo{})
			if err != nil {
				return nil, fmt.Errorf("failed to get any available template builder for cluster '%s': %w", clusterID, err)
			}

			return builder, nil
		}

		return nil, fmt.Errorf("failed to get available template builder for cluster '%s': %w", clusterID, err)
	}

	return builder, nil
}

func (tm *TemplateManager) GetClusterResources(clusterID uuid.UUID) (clusters.ClusterResource, error) {
	cluster, ok := tm.clusters.GetClusterById(clusterID)
	if !ok {
		return nil, errors.New("cluster not found")
	}

	return cluster.GetResources(), nil
}

func (tm *TemplateManager) GetClusterBuildClient(clusterID uuid.UUID, nodeID string) (*clusters.GRPCClient, error) {
	cluster, ok := tm.clusters.GetClusterById(clusterID)
	if !ok {
		return nil, errors.New("cluster not found")
	}

	instance, err := cluster.GetTemplateBuilderByNodeID(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get builder by id '%s': %w", nodeID, err)
	}

	return instance.GetClient(), nil
}

func (tm *TemplateManager) DeleteBuild(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) error {
	ctx, span := tracer.Start(ctx, "delete-template",
		trace.WithAttributes(
			telemetry.WithBuildID(buildID.String()),
		),
	)
	defer span.End()

	client, err := tm.GetClusterBuildClient(clusterID, nodeID)
	if err != nil {
		// nodeID can be an orchestrator ID, if the build corresponds to a snapshot.
		// We may want to improve this later by adding the Delete method to Orchestrator as well.
		// This way we can remove the build (snapshot) from cache as well
		node, err := tm.GetAvailableBuildClient(ctx, clusterID)
		if err != nil {
			return fmt.Errorf("failed to get any available node in the cluster: %w", err)
		}
		nodeID = node.NodeID

		logger.L().Info(ctx, "Fallback to available node", zap.String("nodeID", nodeID), zap.String("clusterID", clusterID.String()))
		client, err = tm.GetClusterBuildClient(clusterID, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get builder client: %w", err)
		}
	}

	_, err = client.Template.TemplateBuildDelete(
		ctx, &templatemanagergrpc.TemplateBuildDeleteRequest{
			BuildID:    buildID.String(),
			TemplateID: templateID,
		},
	)

	err = utils.UnwrapGRPCError(err)
	if err != nil {
		return fmt.Errorf("failed to delete env build '%s': %w", buildID, err)
	}

	return nil
}

func (tm *TemplateManager) DeleteBuilds(ctx context.Context, builds []DeleteBuild) error {
	for _, build := range builds {
		err := tm.DeleteBuild(ctx, build.BuildID, build.TemplateID, build.ClusterID, build.NodeID)
		if err != nil {
			return fmt.Errorf("failed to delete env build '%s': %w", build.BuildID, err)
		}
	}

	return nil
}

func (tm *TemplateManager) GetStatus(ctx context.Context, buildID uuid.UUID, templateID string, clusterID uuid.UUID, nodeID string) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	client, err := tm.GetClusterBuildClient(clusterID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get builder client: %w", err)
	}

	// error unwrapping is done in the caller
	return client.Template.TemplateBuildStatus(
		ctx, &templatemanagergrpc.TemplateStatusRequest{
			BuildID: buildID.String(), TemplateID: templateID,
		},
	)
}
