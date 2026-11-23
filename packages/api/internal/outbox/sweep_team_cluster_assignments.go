package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

const (
	teamClusterSweepTimeout = 10 * time.Minute
	// The most projects the cluster service looks up in one call.
	teamClusterSweepPage = 1000
)

type jobsInserter interface {
	InsertMany(ctx context.Context, params []river.InsertManyParams) ([]*rivertype.JobInsertResult, error)
}

// teamClusterSweepWorker finds the live teams whose cluster the cluster
// service does not record and enqueues each one's sync. It covers the teams
// assigned before their writes enqueued one, and a record lost since. A record
// the cluster service holds for a team no longer on a cluster is not found
// here: the write that took the team off enqueued its sync.
type teamClusterSweepWorker struct {
	river.WorkerDefaults[outbox.SweepTeamClusterAssignments]

	db *sqlcdb.Client
	// clusters is nil without a cluster service.
	clusters clustersgrpc.ClusterServiceClient
	jobs     jobsInserter
	pageSize int32
	steps    sharedriver.JobSteps
}

func (w *teamClusterSweepWorker) Timeout(*river.Job[outbox.SweepTeamClusterAssignments]) time.Duration {
	return teamClusterSweepTimeout
}

func (w *teamClusterSweepWorker) Work(ctx context.Context, _ *river.Job[outbox.SweepTeamClusterAssignments]) error {
	if w.clusters == nil {
		w.steps.Skipped(ctx, "sweep", "no cluster service")

		return nil
	}

	return w.steps.Step(ctx, "sweep", func(ctx context.Context) error {
		after := uuid.Nil
		for {
			teams, err := w.db.ListLiveTeamClusters(ctx, queries.ListLiveTeamClustersParams{After: after, PageSize: w.pageSize})
			if err != nil {
				return fmt.Errorf("list team clusters: %w", err)
			}
			if len(teams) == 0 {
				return nil
			}
			if err := w.enqueueUnrecorded(ctx, teams); err != nil {
				return err
			}
			after = teams[len(teams)-1].ID
		}
	})
}

func (w *teamClusterSweepWorker) enqueueUnrecorded(ctx context.Context, teams []queries.ListLiveTeamClustersRow) error {
	projectIDs := make([]string, 0, len(teams))
	for _, team := range teams {
		projectIDs = append(projectIDs, team.ID.String())
	}
	resp, err := w.clusters.LookupProjectAssignments(ctx, clustersgrpc.LookupProjectAssignmentsRequest_builder{ProjectIds: projectIDs}.Build())
	if err != nil {
		return fmt.Errorf("look up recorded clusters: %w", err)
	}
	recorded := make(map[string]string, len(resp.GetAssignments()))
	for _, assignment := range resp.GetAssignments() {
		recorded[assignment.GetProjectId()] = assignment.GetClusterId()
	}

	var syncs []river.InsertManyParams
	for _, team := range teams {
		if recorded[team.ID.String()] == team.ClusterID.String() {
			continue
		}
		syncs = append(syncs, river.InsertManyParams{
			Args: outbox.SyncTeamClusterAssignment{TeamID: team.ID},
			InsertOpts: &river.InsertOpts{
				// Below the syncs a write enqueues and the teardowns, which
				// a plane's first sweep would otherwise hold up.
				Priority: sharedriver.PriorityLow,
				// A team whose sync keeps failing would otherwise gather
				// another retrying one with every sweep.
				UniqueOpts: outbox.UniqueWhileOpen(),
			},
		})
	}
	if len(syncs) == 0 {
		return nil
	}
	if _, err := w.jobs.InsertMany(ctx, syncs); err != nil {
		return fmt.Errorf("enqueue team cluster syncs: %w", err)
	}

	return nil
}
