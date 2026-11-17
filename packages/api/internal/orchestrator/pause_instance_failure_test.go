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

// pauseCapturingSandboxClient accepts every Pause, keeps the last request and
// answers with envdUnresponsive as the node's word on the guest agent.
type pauseCapturingSandboxClient struct {
	orchestrator.SandboxServiceClient

	last             *orchestrator.SandboxPauseRequest
	envdUnresponsive bool
}

func (c *pauseCapturingSandboxClient) Pause(_ context.Context, in *orchestrator.SandboxPauseRequest, _ ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	c.last = in

	return &orchestrator.SandboxPauseResponse{EnvdUnresponsive: c.envdUnresponsive}, nil
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
	sourceBuildID := testutils.CreateTestBuild(t, t.Context(), db, baseTemplateID, types.BuildStatusGroupReady)

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
func snapshotBuildStatus(t *testing.T, db *testutils.Database, sandboxID string) (types.BuildStatusGroup, bool) {
	t.Helper()

	var (
		buildStatus types.BuildStatusGroup
		finishedAt  *time.Time
	)

	err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT b.status_group, b.finished_at
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

	err := o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.Error(t, err)

	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupFailed, buildStatus)
	assert.True(t, hasFinishedAt, "a terminal build must record finished_at")
}

// A pause the node refuses for a checkpoint in flight keeps its own type
// under the retryable refusal, and closes the build out like any refusal.
func TestPauseSandbox_FailsBuildWhenCheckpointInProgress(t *testing.T) {
	t.Parallel()

	refusal, err := status.New(codes.ResourceExhausted, "sandbox is being checkpointed, please retry").WithDetails(
		&orchestrator.UserError{Code: orchestrator.UserErrorCode_CHECKPOINT_IN_PROGRESS, HttpStatus: 503},
	)
	require.NoError(t, err)
	o, db, node, sbx := newPauseFixture(t, refusal.Err())

	err = o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.ErrorIs(t, err, PauseRefusedCheckpointInProgressError{})
	require.ErrorIs(t, err, PauseQueueExhaustedError{}, "a checkpoint in flight is one of the retryable refusals")

	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupFailed, buildStatus)
	assert.True(t, hasFinishedAt, "a terminal build must record finished_at")
}

// A pause the node refuses because the guest agent did not answer its probe
// keeps its own type under the retryable refusal, and closes the build out
// like any refusal.
func TestPauseSandbox_FailsBuildWhenEnvdUnresponsive(t *testing.T) {
	t.Parallel()

	refusal, err := status.New(codes.ResourceExhausted, "sandbox 'x' guest agent is not responding, please retry").WithDetails(
		&orchestrator.UserError{Code: orchestrator.UserErrorCode_ENVD_UNRESPONSIVE, HttpStatus: 503},
	)
	require.NoError(t, err)
	o, db, node, sbx := newPauseFixture(t, refusal.Err())

	err = o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.ErrorIs(t, err, PauseRefusedEnvdUnresponsiveError{})
	require.ErrorIs(t, err, PauseQueueExhaustedError{}, "an unresponsive guest agent is one of the retryable refusals")
	require.NotErrorIs(t, err, PauseRefusedCheckpointInProgressError{})

	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupFailed, buildStatus)
	assert.True(t, hasFinishedAt, "a terminal build must record finished_at")
}

// A full pause queue returns a distinct error, and closes the build out too.
func TestPauseSandbox_FailsBuildWhenPauseQueueExhausted(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, status.Error(codes.ResourceExhausted, "queue full"))

	err := o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.ErrorIs(t, err, PauseQueueExhaustedError{})

	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupFailed, buildStatus)
	assert.True(t, hasFinishedAt, "a terminal build must record finished_at")
}

// Whether the node may take a memory snapshot over a hung guest travels on
// the pause request as the snapshot kind does: a plain memory pause allows
// it, since this API persists the node's answer; a pause that requires a
// healthy guest, and a filesystem-only pause, send nothing and leave the
// node strict.
func TestPauseSandbox_AllowEnvdUnresponsiveReachesTheNode(t *testing.T) {
	t.Parallel()

	o, _, node, sbx := newPauseFixture(t, nil)
	client := &pauseCapturingSandboxClient{}
	node.SetSandboxClient(client)
	o.snapshotCache = noopSnapshotCache{}

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	require.NotNil(t, client.last)
	assert.True(t, client.last.GetAllowEnvdUnresponsive(), "a plain memory pause lets the node proceed")
	assert.False(t, client.last.GetFilesystemOnly())
	assert.Equal(t, sbx.SandboxID, client.last.GetSandboxId())

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause, RequireEnvdHealthy: true}))
	assert.False(t, client.last.GetAllowEnvdUnresponsive(), "a pause that requires a healthy guest leaves the node strict")

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause, FilesystemOnly: true}))
	assert.True(t, client.last.GetFilesystemOnly())
	assert.False(t, client.last.GetAllowEnvdUnresponsive(), "a filesystem-only pause never allows it")
}

// snapshotBuildEnvdUnresponsive reads the mark off the sandbox's latest build.
func snapshotBuildEnvdUnresponsive(t *testing.T, db *testutils.Database, sandboxID string) *bool {
	t.Helper()

	var mark *bool
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT b.envd_unresponsive
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

			return rows.Scan(&mark)
		},
		sandboxID,
	)
	require.NoError(t, err)

	return mark
}

// The node's word that it took the memory snapshot over an unresponsive guest
// agent lands on the build row with the build's success, and a build whose
// agent answered carries no mark.
func TestPauseSandbox_EnvdUnresponsiveReachesTheBuildRow(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	client := &pauseCapturingSandboxClient{envdUnresponsive: true}
	node.SetSandboxClient(client)
	o.snapshotCache = noopSnapshotCache{}

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupReady, buildStatus)
	assert.True(t, hasFinishedAt)
	mark := snapshotBuildEnvdUnresponsive(t, db, sbx.SandboxID)
	require.NotNil(t, mark, "the mark must be written with the build's success")
	assert.True(t, *mark)

	client.envdUnresponsive = false
	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	assert.Nil(t, snapshotBuildEnvdUnresponsive(t, db, sbx.SandboxID), "the next pause's build, whose agent answered, carries no mark")
}
