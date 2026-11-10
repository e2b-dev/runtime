package template_manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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

	builderPinFallback metric.Int64Counter
	// builderVersionRejected counts builder lookups that failed only because no
	// builder in the cluster is at the version the build needs.
	builderVersionRejected metric.Int64Counter
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
	meter := otel.Meter("github.com/e2b-dev/infra/packages/api/internal/template-manager")
	builderPinFallback, err := telemetry.GetCounter(meter, telemetry.ApiTemplateBuildBuilderPinFallback)
	if err != nil {
		return nil, fmt.Errorf("failed to create builder pin fallback counter: %w", err)
	}

	builderVersionRejected, err := telemetry.GetCounter(meter, telemetry.ApiTemplateBuildBuilderVersionRejected)
	if err != nil {
		return nil, fmt.Errorf("failed to create builder version rejected counter: %w", err)
	}

	tm := &TemplateManager{
		sqlcDB:        sqlcDB,
		buildCache:    buildCache,
		templateCache: templateCache,
		clusters:      clusters,
		featureFlags:  featureFlags,

		builderPinFallback:     builderPinFallback,
		builderVersionRejected: builderVersionRejected,

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

// GetAvailableBuildClient picks a builder in the cluster. A non-nil minVersion
// restricts the choice to builders at or above it; see
// clusters.ErrTemplateBuilderVersionTooOld.
func (tm *TemplateManager) GetAvailableBuildClient(ctx context.Context, clusterID uuid.UUID, minVersion *semver.Version) (*clusters.Instance, error) {
	cluster, ok := tm.clusters.GetClusterById(clusterID)
	if !ok {
		return nil, fmt.Errorf("cluster with ID '%s' not found", clusterID)
	}

	// Set feature flags context for cluster
	ctx = featureflags.AddToContext(ctx, featureflags.ClusterContext(clusterID))

	nodeInfoJSON := tm.featureFlags.JSONFlag(ctx, featureflags.BuildNodeInfo)
	nodeInfo := machineinfo.FromLDValue(ctx, nodeInfoJSON)
	builder, err := cluster.GetAvailableTemplateBuilder(ctx, nodeInfo, minVersion)
	switch {
	case errors.Is(err, clusters.ErrTemplateBuilderVersionTooOld) && nodeInfo.CPUArchitecture != "":
		logger.L().Warn(ctx, "No template builder with the pinned machine info is at the required version, falling back to any builder at that version",
			zap.Error(err), zap.String("requiredVersion", minVersion.String()), zap.String("clusterID", clusterID.String()))
		if tm.builderPinFallback != nil {
			tm.builderPinFallback.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster_id", clusterID.String())))
		}

		builder, err = cluster.GetAvailableTemplateBuilder(ctx, machineinfo.MachineInfo{}, minVersion)
	case errors.Is(err, clusters.ErrAvailableTemplateBuilderNotFound):
		// Fallback to any template builder
		logger.L().Warn(ctx, "No available template builder found with the specified machine info, falling back to any available template builder", zap.Error(err), zap.String("clusterID", clusterID.String()))

		builder, err = cluster.GetAvailableTemplateBuilder(ctx, machineinfo.MachineInfo{}, minVersion)
	}

	if errors.Is(err, clusters.ErrTemplateBuilderVersionTooOld) && tm.builderVersionRejected != nil {
		tm.builderVersionRejected.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster_id", clusterID.String())))
	}

	if err != nil {
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

// buildDispatchClient resolves the builder on nodeID that a new build is sent
// to. The caller checked the version of the builder it picked, but only the
// machine is carried here and the process on it can change in between (a
// restart or rollback). A build that needs minVersion must not reach an older
// process, which would ignore maxVCpuCount and build a fixed-size template.
func (tm *TemplateManager) buildDispatchClient(ctx context.Context, clusterID uuid.UUID, nodeID string, minVersion *semver.Version) (*clusters.GRPCClient, error) {
	cluster, ok := tm.clusters.GetClusterById(clusterID)
	if !ok {
		return nil, errors.New("cluster not found")
	}

	instance, err := cluster.GetTemplateBuilderByNodeID(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get builder by id '%s': %w", nodeID, err)
	}

	if minVersion != nil && !clusters.InstanceAtLeast(instance.GetInfo().ServiceVersion, minVersion) {
		if tm.builderVersionRejected != nil {
			tm.builderVersionRejected.Add(ctx, 1, metric.WithAttributes(
				attribute.String("cluster_id", clusterID.String()),
				attribute.String("stage", "dispatch"),
			))
		}

		return nil, fmt.Errorf("builder on '%s' runs %q, below the required %s: %w",
			nodeID, instance.GetInfo().ServiceVersion, minVersion, clusters.ErrTemplateBuilderVersionTooOld)
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
		node, err := tm.GetAvailableBuildClient(ctx, clusterID, nil)
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
