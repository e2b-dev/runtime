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

	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

var (
	// ErrNotFound reports a cluster the cluster service has no record of.
	ErrNotFound = errors.New("cluster not found")
	// ErrDeleting reports a cluster whose deletion is recorded.
	ErrDeleting = errors.New("cluster is being deleted")
)

// Lock holds the cluster's assignment lock until tx ends. Take it before
// locking any team row, so every holder waits in the same order.
func Lock(ctx context.Context, tx pgx.Tx, clusterID uuid.UUID) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "cluster assignment "+clusterID.String()); err != nil {
		return fmt.Errorf("take the cluster assignment lock: %w", err)
	}

	return nil
}

// Check returns ErrNotFound or ErrDeleting for a cluster no team may be newly
// assigned to. Call it holding Lock.
func Check(ctx context.Context, service clustersgrpc.ClusterServiceClient, clusterID uuid.UUID) error {
	// The local cluster has no record, and the cluster service refuses its nil
	// ID outright. Without a cluster service there are no remote clusters.
	if clusterID == consts.LocalClusterID || service == nil {
		return ErrNotFound
	}

	resp, err := service.GetCluster(ctx, clustersgrpc.GetClusterRequest_builder{ClusterId: clusterID.String()}.Build())
	switch {
	case status.Code(err) == codes.NotFound:
		return ErrNotFound
	case err != nil:
		return fmt.Errorf("look up the cluster: %w", err)
	case resp.GetCluster().HasDeletionRequestedAt():
		return ErrDeleting
	}

	return nil
}

// Settle returns once every assignment holding the cluster's lock has
// committed. Called after a deletion is recorded, it leaves every later
// assignment to find the deletion, so the caller's next reading of the
// cluster's teams is final.
func Settle(ctx context.Context, db *sqlcdb.Client, clusterID uuid.UUID) error {
	_, tx, err := db.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin settling cluster assignments: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	return Lock(ctx, tx, clusterID)
}
