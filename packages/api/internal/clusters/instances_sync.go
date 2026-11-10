package clusters

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// Instance sync store handles synchronization of instances in each cluster and checking its state
type instancesSyncStore struct {
	clusterID uuid.UUID

	discovery        servicediscovery.Discoverer
	instances        *smap.Map[*Instance]
	instanceCreation func(ctx context.Context, item servicediscovery.Instance) (*Instance, error)

	// onInsert runs after an instance has been added to the pool, when set.
	onInsert func()
}

func (d instancesSyncStore) SourceList(ctx context.Context) ([]servicediscovery.Instance, error) {
	items, err := d.discovery.ListInstances(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster instances from service discovery: %w", err)
	}

	return items, nil
}

func (d instancesSyncStore) SourceExists(_ context.Context, s []servicediscovery.Instance, p *Instance) bool {
	for _, item := range s {
		if item.WorkloadID == p.workloadID {
			return true
		}
	}

	return false
}

func (d instancesSyncStore) PoolList(_ context.Context) []*Instance {
	mapped := make([]*Instance, 0)
	for _, item := range d.instances.Items() {
		mapped = append(mapped, item)
	}

	return mapped
}

// Keyed on the same identity SourceExists compares, so a process that restarts
// on a machine already in the pool is replaced rather than skipped: keying the
// map on the machine and the source check on the process is what made a
// restarted builder disappear for a cycle.
func (d instancesSyncStore) PoolExists(_ context.Context, s servicediscovery.Instance) bool {
	_, found := d.instances.Get(s.WorkloadID)

	return found
}

func (d instancesSyncStore) PoolInsert(ctx context.Context, item servicediscovery.Instance) {
	logger.L().Info(ctx, "Adding instance into cluster pool",
		logger.WithClusterID(d.clusterID),
		logger.WithNodeID(item.NodeID),
	)

	// Instant is synced immediately after creation to ensure it's working before adding to the pool.
	instance, err := d.instanceCreation(ctx, item)
	if err != nil {
		logger.L().Error(ctx, "Failed to create cluster instance during pool insert",
			zap.Error(err),
			logger.WithClusterID(d.clusterID),
			logger.WithNodeID(item.NodeID),
		)

		return
	}

	d.instances.Insert(item.WorkloadID, instance)

	if d.onInsert != nil {
		d.onInsert()
	}
}

func (d instancesSyncStore) PoolUpdate(ctx context.Context, instance *Instance) {
	_ = d.tryToSyncInstance(ctx, instance)
}

func (d instancesSyncStore) PoolRemove(ctx context.Context, instance *Instance) {
	info := instance.GetInfo()
	logger.L().Info(ctx, "Removing instance from cluster pool",
		logger.WithClusterID(d.clusterID),
		logger.WithNodeID(instance.NodeID),
		logger.WithServiceInstanceID(info.ServiceInstanceID),
	)

	// Try to gracefully close the instance
	d.tryToCloseInstance(ctx, instance)

	d.instances.Remove(instance.workloadID)
}

func (d instancesSyncStore) tryToCloseInstance(ctx context.Context, instance *Instance) {
	closeErr := instance.Close()
	if closeErr != nil {
		info := instance.GetInfo()
		logger.L().Error(ctx, "Failed to close cluster instance after sync failure",
			zap.Error(closeErr),
			logger.WithClusterID(d.clusterID),
			logger.WithNodeID(instance.NodeID),
			logger.WithServiceInstanceID(info.ServiceInstanceID),
		)
	}
}

func (d instancesSyncStore) tryToSyncInstance(ctx context.Context, instance *Instance) bool {
	err := instance.Sync(ctx)
	if err != nil {
		info := instance.GetInfo()
		logger.L().Error(ctx, "Failed to sync cluster instance",
			zap.Error(err),
			logger.WithClusterID(d.clusterID),
			logger.WithNodeID(instance.NodeID),
			logger.WithServiceInstanceID(info.ServiceInstanceID),
		)

		return false
	}

	return true
}
