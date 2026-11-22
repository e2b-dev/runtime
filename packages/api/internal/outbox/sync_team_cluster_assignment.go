package outbox

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/outbox"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

// teamClusterAssignmentWorker copies a team's cluster to the cluster service,
// which calls the team a project. It reads the team when it runs rather than
// carrying the change, so jobs that run late or twice still leave the cluster
// service holding the team's current cluster.
type teamClusterAssignmentWorker struct {
	river.WorkerDefaults[outbox.SyncTeamClusterAssignment]

	db *sqlcdb.Client
	// clusters is nil without a cluster service.
	clusters clustersgrpc.ClusterServiceClient
	steps    sharedriver.JobSteps
}

func (w *teamClusterAssignmentWorker) Work(ctx context.Context, job *river.Job[outbox.SyncTeamClusterAssignment]) error {
	teamID := job.Args.TeamID

	// Without a cluster service there are no remote clusters to be assigned to.
	if w.clusters == nil {
		w.steps.Skipped(ctx, "sync", "no cluster service")

		return nil
	}

	txDB, tx, err := w.db.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin team cluster sync: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := w.sync(ctx, txDB, teamID); err != nil {
		return err
	}

	// The lock lasts as long as the session. One lost while the cluster
	// service was answering let a later job read before this one had written,
	// so the attempt fails and the retry reads both sides again.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("end team cluster sync: %w", err)
	}

	return nil
}

// sync holds the team's sync lock in txDB's transaction, so a later job
// cannot write an older cluster after this one.
func (w *teamClusterAssignmentWorker) sync(ctx context.Context, txDB *sqlcdb.Client, teamID uuid.UUID) error {
	var current, recorded *uuid.UUID
	if err := w.steps.Step(ctx, "read_team_cluster", func(ctx context.Context) error {
		if err := txDB.LockTeamClusterSync(ctx, teamID); err != nil {
			return fmt.Errorf("lock team cluster sync: %w", err)
		}
		cluster, err := txDB.GetLiveTeamCluster(ctx, teamID)
		if err != nil && !dberrors.IsNotFoundError(err) {
			return fmt.Errorf("read team cluster: %w", err)
		}
		current = cluster

		return nil
	}); err != nil {
		return err
	}

	if err := w.steps.Step(ctx, "read_recorded_cluster", func(ctx context.Context) error {
		resp, err := w.clusters.LookupProjectAssignments(ctx, clustersgrpc.LookupProjectAssignmentsRequest_builder{
			ProjectIds: []string{teamID.String()},
		}.Build())
		if err != nil {
			return fmt.Errorf("look up recorded cluster: %w", err)
		}
		for _, assignment := range resp.GetAssignments() {
			clusterID, err := uuid.Parse(assignment.GetClusterId())
			if err != nil {
				return fmt.Errorf("parse recorded cluster: %w", err)
			}
			recorded = &clusterID
		}

		return nil
	}); err != nil {
		return err
	}

	switch {
	case current == nil && recorded == nil, current != nil && recorded != nil && *current == *recorded:
		w.steps.Skipped(ctx, "sync", "already recorded")

		return nil
	case current != nil:
		err := w.steps.StepWith(ctx, "assign", sharedriver.StepOptions{SkipReason: clusterTakesNoTeam}, func(ctx context.Context) error {
			_, err := w.clusters.AssignProject(ctx, clustersgrpc.AssignProjectRequest_builder{
				ProjectId: teamID.String(),
				ClusterId: current.String(),
			}.Build())
			if err != nil {
				return fmt.Errorf("record team cluster: %w", err)
			}

			return nil
		})
		// Retrying cannot change the answer. The write that takes the team
		// off the cluster enqueues its own sync.
		if clusterTakesNoTeam(err) != "" {
			return nil
		}

		return err
	default:
		return w.steps.Step(ctx, "detach", func(ctx context.Context) error {
			_, err := w.clusters.DetachProject(ctx, clustersgrpc.DetachProjectRequest_builder{
				ProjectId: teamID.String(),
				ClusterId: recorded.String(),
			}.Build())
			if err != nil {
				return fmt.Errorf("clear recorded team cluster: %w", err)
			}

			return nil
		})
	}
}

// clusterTakesNoTeam names why the cluster service will keep refusing to
// record a team on the cluster, or returns "" for any other outcome.
func clusterTakesNoTeam(err error) string {
	st := status.Convert(err)
	if st.Code() == codes.NotFound {
		return "the cluster service does not know the cluster"
	}
	for _, detail := range st.Details() {
		reason, ok := detail.(*clustersgrpc.ClusterErrorDetail)
		if ok {
			switch reason.GetReason() {
			case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED:
				return "the cluster is being deleted"
			case clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_PROJECT_PLACEMENT_CONFLICT:
				return "the project has no claim on this cluster"
			}
		}
	}

	return ""
}
