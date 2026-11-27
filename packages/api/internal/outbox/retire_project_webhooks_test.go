package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	webhooks "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management/clienttests"
)

func TestLeaderWithoutWebhookBackendRescuesInterruptedRetirement(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := testutils.SetupDatabase(t)
	deps := Dependencies{Pool: db.SqlcClient.Pool(), Config: Config{MaxWorkers: 1, BacklogInterval: time.Second}}
	leader, err := New(deps)
	require.NoError(t, err)
	deps.Webhooks = clienttests.NewMockWebhookManagementServiceClient(t)
	writer, err := New(deps)
	require.NoError(t, err)
	inserted, err := writer.Client().Insert(ctx, outbox.RetireProjectWebhooks{ProjectID: uuid.New()}, nil)
	require.NoError(t, err)
	_, err = deps.Pool.Exec(ctx, `UPDATE river.river_job SET state = 'running', attempt = 1, attempted_at = now() - interval '1 hour' WHERE id = $1`, inserted.Job.ID)
	require.NoError(t, err)

	require.NoError(t, leader.Start(ctx))
	t.Cleanup(func() { require.NoError(t, leader.Stop(context.WithoutCancel(ctx))) })
	require.Eventually(t, func() bool {
		job, err := leader.Client().JobGet(ctx, inserted.Job.ID)

		return err == nil && job.State != rivertype.JobStateRunning
	}, 15*time.Second, 50*time.Millisecond)
	job, err := leader.Client().JobGet(ctx, inserted.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateRetryable, job.State)
}

func TestWebhookOutageDoesNotBlockSandboxTeardown(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	project := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.AuthDb.TestsRawSQL(t.Context(), `UPDATE teams SET is_blocked = true, deleted_at = now() WHERE id = $1`, project))
	testCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	started, fail := make(chan struct{}), make(chan struct{})
	backend := clienttests.NewMockWebhookManagementServiceClient(t)
	req := mock.MatchedBy(func(req *webhooks.RetireProjectWebhooksRequest) bool { return req.GetProjectId() == project.String() })
	backend.EXPECT().RetireProjectWebhooks(mock.Anything, req).RunAndReturn(func(ctx context.Context, _ *webhooks.RetireProjectWebhooksRequest, _ ...grpc.CallOption) (*webhooks.RetireProjectWebhooksResponse, error) {
		close(started)
		select {
		case <-fail:
			return nil, status.Error(codes.Unavailable, "offline")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-testCtx.Done():
			return nil, testCtx.Err()
		}
	}).Once()
	backend.EXPECT().RetireProjectWebhooks(mock.Anything, req).Return(webhooks.RetireProjectWebhooksResponse_builder{}.Build(), nil).Once()

	r, err := New(Dependencies{
		Pool: db.SqlcClient.Pool(), Teams: db.AuthDb, Webhooks: backend, Sandboxes: &teardownRecorder{},
		Config: Config{MaxWorkers: 1, BacklogInterval: time.Second},
	})
	require.NoError(t, err)
	events, unsubscribe := r.Client().Subscribe(river.EventKindJobCompleted, river.EventKindJobFailed)
	defer unsubscribe()
	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, r.Stop(context.WithoutCancel(t.Context()))) })
	retirement, err := r.Client().Insert(testCtx, outbox.RetireProjectWebhooks{ProjectID: project}, nil)
	require.NoError(t, err)
	select {
	case <-started:
	case <-testCtx.Done():
		require.Fail(t, "webhook retirement did not start")
	}
	teardown, err := r.Client().Insert(testCtx, outbox.TeardownTeamResources{TeamID: project}, nil)
	require.NoError(t, err)
	nextJob := func() *rivertype.JobRow {
		t.Helper()
		select {
		case event := <-events:
			return event.Job
		case <-testCtx.Done():
			require.Fail(t, "timed out waiting for teardown")

			return nil
		}
	}
	completed := nextJob()
	require.Equal(t, teardown.Job.ID, completed.ID)
	require.Equal(t, rivertype.JobStateCompleted, completed.State)
	close(fail)
	retrying := nextJob()
	require.Equal(t, retirement.Job.ID, retrying.ID)
	require.Equal(t, rivertype.JobStateRetryable, retrying.State)
	_, err = r.Client().JobRetry(testCtx, retrying.ID)
	require.NoError(t, err)
	completed = nextJob()
	require.Equal(t, retirement.Job.ID, completed.ID)
	require.Equal(t, rivertype.JobStateCompleted, completed.State)
}
