package readmodel

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

var ErrSnapshotNotFound = errors.New("no snapshot found")

type SnapshotBuild struct {
	BuildID       uuid.UUID
	ClusterNodeID string
}
type SnapshotBuilds struct {
	TemplateID string
	Builds     []SnapshotBuild
}

func GetSnapshotBuilds(ctx context.Context, db *sqlcdb.Client, teamID uuid.UUID, sandboxID string) (SnapshotBuilds, error) {
	snapshotWithBuilds, err := db.GetSnapshotBuilds(ctx, queries.GetSnapshotBuildsParams{
		SandboxID: sandboxID,
		TeamID:    teamID,
	})
	if err != nil {
		return SnapshotBuilds{}, fmt.Errorf("error getting snapshot with builds: %w", err)
	}

	if len(snapshotWithBuilds) == 0 {
		return SnapshotBuilds{}, ErrSnapshotNotFound
	}

	snapshot := SnapshotBuilds{TemplateID: snapshotWithBuilds[0].TemplateID, Builds: []SnapshotBuild{}}
	for _, build := range snapshotWithBuilds {
		// Due to left join we have to check if the build is present
		if build.BuildID == nil || build.BuildClusterNodeID == nil {
			continue
		}

		snapshot.Builds = append(snapshot.Builds, SnapshotBuild{BuildID: *build.BuildID, ClusterNodeID: *build.BuildClusterNodeID})
	}

	return snapshot, nil
}
