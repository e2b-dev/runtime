// Package clusterassign orders assigning a team to a remote cluster against
// recording that cluster's deletion and deleting it. The cluster records live
// in the cluster service, so a team write and the check of the record it
// names cannot share a transaction. An advisory lock in the database that
// holds the teams orders them instead: an assignment checks the record while
// holding it, and a deletion takes it once recorded.
package clusterassign

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

var (
	ErrUnavailable = errors.New("cluster management is not available")
	// ErrNotFound reports a cluster the cluster service has no record of.
	ErrNotFound = errors.New("cluster not found")
	// ErrDeleting reports a cluster whose deletion is recorded.
	ErrDeleting          = errors.New("cluster is being deleted")
	ErrPlacementConflict = errors.New("project cannot be created on this cluster")
)

// Lock holds the cluster's assignment lock until tx ends. Take it before
// locking any team row, so every holder waits in the same order.
func Lock(ctx context.Context, tx pgx.Tx, clusterID uuid.UUID) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "cluster assignment "+clusterID.String()); err != nil {
		return fmt.Errorf("take the cluster assignment lock: %w", err)
	}

	return nil
}

// Check runs under Lock on the first insert of a remote project.
func Check(ctx context.Context, service clustersgrpc.ClusterServiceClient, clusterID, projectID uuid.UUID) error {
	// The local cluster has no record, and the cluster service refuses its nil ID.
	if clusterID == consts.LocalClusterID {
		return ErrNotFound
	}
	if service == nil {
		return ErrUnavailable
	}

	_, err := service.CheckProjectCreation(ctx, clustersgrpc.CheckProjectCreationRequest_builder{
		ProjectId: projectID.String(),
		ClusterId: clusterID.String(),
	}.Build())
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.NotFound:
		return ErrNotFound
	case codes.Unavailable:
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case codes.FailedPrecondition:
		for _, detail := range status.Convert(err).Details() {
			if reason, ok := detail.(*clustersgrpc.ClusterErrorDetail); ok {
				switch reason.GetReason() {
				case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED:
					return ErrDeleting
				case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_PROJECT_PLACEMENT_CONFLICT:
					return ErrPlacementConflict
				}
			}
		}
	}

	return fmt.Errorf("check project creation: %w", err)
}

// Settle waits for assignments that checked admission before the drain.
// An already claimed project can still finish creating its team afterwards.
func Settle(ctx context.Context, db *sqlcdb.Client, clusterID uuid.UUID) error {
	_, tx, err := db.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin settling cluster assignments: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	return Lock(ctx, tx, clusterID)
}
