package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// checkpointStubClient answers every Checkpoint with a fixed error and keeps
// the requests it saw.
type checkpointStubClient struct {
	orchestrator.SandboxServiceClient

	err error

	mu       sync.Mutex
	requests []*orchestrator.SandboxCheckpointRequest
}

func (c *checkpointStubClient) Checkpoint(_ context.Context, in *orchestrator.SandboxCheckpointRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCheckpointResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, in)
	c.mu.Unlock()

	if c.err != nil {
		return nil, c.err
	}

	return &orchestrator.SandboxCheckpointResponse{}, nil
}

func (c *checkpointStubClient) lastRequest(t *testing.T) *orchestrator.SandboxCheckpointRequest {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.requests)

	return c.requests[len(c.requests)-1]
}

type kindFixture struct {
	o      *Orchestrator
	db     *testutils.Database
	node   *nodemanager.Node
	client *checkpointStubClient
	sbx    sandbox.Sandbox
}

// newKindFixture is a Running sandbox in the store, on a node whose Checkpoint
// answers with checkpointErr, over a test database.
func newKindFixture(t *testing.T, checkpointErr error) kindFixture {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)

	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider(), nil)
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	sem, err := utils.NewAdjustableSemaphore(1)
	require.NoError(t, err)

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sourceBuildID := testutils.CreateTestBuild(t, t.Context(), db, baseTemplateID, string(types.BuildStatusUploaded))

	client := &checkpointStubClient{err: checkpointErr}
	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	node.ClusterID = consts.LocalClusterID
	node.SetSandboxClient(client)

	o := &Orchestrator{
		sqlcDB:            db.SqlcClient,
		snapshotUpsertSem: sem,
		sandboxStore: sandbox.NewStore(
			storage,
			redisreservations.NewReservationStorage(redisClient, storage.Notifier()),
			sandbox.Callbacks{
				AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
			},
		),
		snapshotCache: noopSnapshotCache{},
		nodes:         smap.New[*nodemanager.Node](),
	}
	o.nodes.Insert(o.scopedNodeID(consts.LocalClusterID, node.ID), node)

	sbx := sandbox.Sandbox{
		SandboxID:         "sbx-" + uuid.NewString()[:8],
		TemplateID:        baseTemplateID,
		ExecutionID:       uuid.NewString(),
		TeamID:            teamID,
		BuildID:           sourceBuildID,
		BaseTemplateID:    baseTemplateID,
		MaxInstanceLength: time.Hour,
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		VCpu:              2,
		RamMB:             512,
		NodeID:            node.ID,
		ClusterID:         consts.LocalClusterID,
		State:             sandbox.StateRunning,
	}
	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))

	return kindFixture{o: o, db: db, node: node, client: client, sbx: sbx}
}

func (f kindFixture) kind(t *testing.T) bool {
	t.Helper()
	kind, err := f.o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), f.sbx.SandboxID)
	require.NoError(t, err)

	return kind
}

func (f kindFixture) state(t *testing.T) sandbox.State {
	t.Helper()
	current, err := f.o.GetSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)

	return current.State
}

func refusedCheckpointErr() error {
	return status.Error(codes.FailedPrecondition, "a checkpoint is already in progress")
}

// A fork checkpoint the node refuses restores the source to Running on its
// previous build, so a filesystem-only row must stay filesystem-only: this is
// the relabelling a memory:false snapshot followed by a fork used to cause.
func TestCheckpointSandbox_RefusedKeepsFilesystemOnlyKind(t *testing.T) {
	t.Parallel()

	f := newKindFixture(t, refusedCheckpointErr())
	seedFilesystemOnlyBuild(t, f.o, f.node, f.sbx)

	require.Error(t, f.o.CheckpointSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID))

	assert.True(t, f.kind(t), "a refused fork checkpoint must not relabel the ready build")
	assert.Equal(t, sandbox.StateRunning, f.state(t))
}

// A fork checkpoint that succeeds is a memory snapshot, and the row says so
// once its build is ready.
func TestCheckpointSandbox_SuccessRecordsMemoryKind(t *testing.T) {
	t.Parallel()

	f := newKindFixture(t, nil)
	seedFilesystemOnlyBuild(t, f.o, f.node, f.sbx)

	require.NoError(t, f.o.CheckpointSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID))

	assert.False(t, f.kind(t))
	assert.False(t, f.client.lastRequest(t).GetFilesystemOnly(), "a fork never asks for a filesystem-only checkpoint")
	buildStatus, _ := snapshotBuildStatus(t, f.db, f.sbx.SandboxID)
	assert.Equal(t, string(types.BuildStatusSuccess), buildStatus)
	assert.Equal(t, sandbox.StateRunning, f.state(t))
}

// A snapshot template records the requested kind, in both directions, and the
// request sent to the node carries the same kind.
func TestCreateSnapshotTemplate_RecordsRequestedKind(t *testing.T) {
	t.Parallel()

	f := newKindFixture(t, nil)

	_, err := f.o.CreateSnapshotTemplate(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, SnapshotTemplateOpts{Tag: id.DefaultTag, FilesystemOnly: true})
	require.NoError(t, err)
	assert.True(t, f.kind(t), "memory:false records a filesystem-only build")
	assert.True(t, f.client.lastRequest(t).GetFilesystemOnly())
	buildStatus, _ := snapshotBuildStatus(t, f.db, f.sbx.SandboxID)
	assert.Equal(t, string(types.BuildStatusUploaded), buildStatus)

	_, err = f.o.CreateSnapshotTemplate(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, SnapshotTemplateOpts{Tag: id.DefaultTag})
	require.NoError(t, err)
	assert.False(t, f.kind(t), "a memory snapshot records a memory build")
	assert.False(t, f.client.lastRequest(t).GetFilesystemOnly())
	assert.Equal(t, sandbox.StateRunning, f.state(t))
}

// A refused memory snapshot template leaves a filesystem-only row alone.
func TestCreateSnapshotTemplate_RefusedKeepsCurrentKind(t *testing.T) {
	t.Parallel()

	f := newKindFixture(t, refusedCheckpointErr())
	seedFilesystemOnlyBuild(t, f.o, f.node, f.sbx)

	_, err := f.o.CreateSnapshotTemplate(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, SnapshotTemplateOpts{Tag: id.DefaultTag})
	require.Error(t, err)

	assert.True(t, f.kind(t))
	assert.Equal(t, sandbox.StateRunning, f.state(t))
}
