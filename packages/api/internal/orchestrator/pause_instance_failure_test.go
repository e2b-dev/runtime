package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// pauseFailingSandboxClient rejects every Pause with a fixed error.
type pauseFailingSandboxClient struct {
	orchestrator.SandboxServiceClient

	err error
}

func (c *pauseFailingSandboxClient) Pause(_ context.Context, _ *orchestrator.SandboxPauseRequest, _ ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	return nil, c.err
}

// newPauseFixture returns an orchestrator on a test database, a node whose Pause
// always fails with pauseErr, and the sandbox to pause.
func newPauseFixture(t *testing.T, pauseErr error) (*Orchestrator, *testutils.Database, *nodemanager.Node, sandbox.Sandbox) {
	t.Helper()

	db := testutils.SetupDatabase(t)

	sem, err := utils.NewAdjustableSemaphore(1)
	require.NoError(t, err)

	o := &Orchestrator{sqlcDB: db.SqlcClient, snapshotUpsertSem: sem}

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sourceBuildID := testutils.CreateTestBuild(t, t.Context(), db, baseTemplateID, string(types.BuildStatusUploaded))

	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	node.SetSandboxClient(&pauseFailingSandboxClient{err: pauseErr})

	sbx := sandbox.Sandbox{
		SandboxID:      "sbx-" + uuid.NewString()[:8],
		TeamID:         teamID,
		BaseTemplateID: baseTemplateID,
		BuildID:        sourceBuildID,
		ClusterID:      consts.LocalClusterID,
		StartTime:      time.Now(),
		VCpu:           2,
		RamMB:          2048,
	}

	return o, db, node, sbx
}

// snapshotBuildStatus reads back the build UpsertSnapshot created for the
// sandbox, not the source build the fixture seeded.
func snapshotBuildStatus(t *testing.T, db *testutils.Database, sandboxID string) (string, bool) {
	t.Helper()

	var (
		buildStatus string
		finishedAt  *time.Time
	)

	err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT b.status, b.finished_at
		 FROM public.snapshots s
		 JOIN public.env_build_assignments eba ON eba.env_id = s.env_id
		 JOIN public.env_builds b ON b.id = eba.build_id
		 WHERE s.sandbox_id = $1
		 ORDER BY b.created_at DESC
		 LIMIT 1`,
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return errors.New("no snapshot build found")
			}

			return rows.Scan(&buildStatus, &finishedAt)
		},
		sandboxID,
	)
	require.NoError(t, err)

	return buildStatus, finishedAt != nil
}

// UpsertSnapshot has committed the snapshot row and its build before the node
// RPC runs, so a rejected pause must still close the build out.
func TestPauseSandbox_FailsBuildWhenPauseRPCFails(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, errors.New("node exploded"))

	err := o.pauseSandbox(t.Context(), node, sbx, false)
	require.Error(t, err)

	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, string(types.BuildStatusFailed), buildStatus)
	assert.True(t, hasFinishedAt, "a terminal build must record finished_at")
}

// A full pause queue returns a distinct error, and closes the build out too.
func TestPauseSandbox_FailsBuildWhenPauseQueueExhausted(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, status.Error(codes.ResourceExhausted, "queue full"))

	err := o.pauseSandbox(t.Context(), node, sbx, false)
	require.ErrorIs(t, err, PauseQueueExhaustedError{})

	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, string(types.BuildStatusFailed), buildStatus)
	assert.True(t, hasFinishedAt, "a terminal build must record finished_at")
}
