package template_manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// These tests cover the terminal writes a poll can end with. The poll context
// carries the buildTimeout deadline, so every one of those writes can be made
// on a context that has just expired — and pgx fast-fails an expired context
// while the retry wrapper does not retry context errors. A write made on it
// never lands, leaving the build in progress forever.

var _ templateManagerClient = (*recordingClient)(nil)

// recordedWrite captures one terminal write and, crucially, whether its context
// was already expired when it was made. A write on an expired context is a write
// that would not have landed in production.
type recordedWrite struct {
	group   types.BuildStatusGroup
	message string
	ctxErr  error
}

// recordingClient is a templateManagerClient that refuses work on an expired
// context, the way pgxpool does, and records every call so a test can assert on
// both the content of a write and the liveness of the context it used.
type recordingClient struct {
	mu sync.Mutex

	// status is served by GetStatus once it is reached.
	status *templatemanagergrpc.TemplateBuildStatusResponse
	// getStatusErr, when set, is returned by GetStatus on a live context.
	getStatusErr error
	// onGetStatus runs inside GetStatus, after the context has been read. It
	// lets a test land the poll deadline during the status call.
	onGetStatus func()

	// terminalStatusLost makes SetTerminalStatus report that another poller had
	// already ended the build, the way the fenced query does.
	terminalStatusLost bool

	setStatusWrites   []recordedWrite
	setFinishedWrites []recordedWrite
	deleteBuildCalls  []recordedWrite
	getStatusCalls    int
}

func (c *recordingClient) SetTerminalStatus(ctx context.Context, _ uuid.UUID, group types.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	w := recordedWrite{group: group, ctxErr: ctx.Err()}
	if reason != nil {
		w.message = reason.GetMessage()
	}
	c.setStatusWrites = append(c.setStatusWrites, w)

	if ctx.Err() != nil {
		return false, fmt.Errorf("simulated db failure on expired context: %w", ctx.Err())
	}

	return !c.terminalStatusLost, nil
}

func (c *recordingClient) DeleteBuild(ctx context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.deleteBuildCalls = append(c.deleteBuildCalls, recordedWrite{ctxErr: ctx.Err()})

	return nil
}

func (c *recordingClient) SetFinished(ctx context.Context, _ uuid.UUID, _ int64, _, _, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setFinishedWrites = append(c.setFinishedWrites, recordedWrite{ctxErr: ctx.Err()})

	if ctx.Err() != nil {
		return fmt.Errorf("simulated db failure on expired context: %w", ctx.Err())
	}

	return nil
}

func (c *recordingClient) GetStatus(ctx context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.getStatusCalls++

	ctxErrOnEntry := ctx.Err()
	if c.onGetStatus != nil {
		c.onGetStatus()
	}

	if ctxErrOnEntry != nil {
		return nil, fmt.Errorf("simulated rpc failure on expired context: %w", ctxErrOnEntry)
	}
	if c.getStatusErr != nil {
		return nil, c.getStatusErr
	}

	return c.status, nil
}

func (c *recordingClient) writes() ([]recordedWrite, []recordedWrite) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]recordedWrite(nil), c.setStatusWrites...), append([]recordedWrite(nil), c.setFinishedWrites...)
}

func (c *recordingClient) deletes() []recordedWrite {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]recordedWrite(nil), c.deleteBuildCalls...)
}

func newChecker(client templateManagerClient) *PollBuildStatus {
	return &PollBuildStatus{
		client:     client,
		logger:     logger.NewNopLogger(),
		templateID: "tmpl-1",
		buildID:    uuid.New(),
		clusterID:  uuid.New(),
		nodeID:     "node-1",
	}
}

// expiredContext returns a context that is already past its deadline, so poll's
// ctx.Done() case is the only ready one on the first iteration (the ticker needs
// a full second). No select coin flip is involved.
func expiredContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)

	return ctx
}

// livePollContext returns a context that outlives one tick, so the ticker case
// runs while the context is still live.
func livePollContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	return ctx, cancel
}

func TestPoll_TimeoutMarksBuildFailedOnALiveContext(t *testing.T) {
	t.Parallel()

	client := &recordingClient{
		status: &templatemanagergrpc.TemplateBuildStatusResponse{
			Status: templatemanagergrpc.TemplateBuildState_Building,
		},
	}

	newChecker(client).poll(expiredContext(t))

	statusWrites, finishedWrites := client.writes()

	require.Len(t, statusWrites, 1, "a timed-out build must be recorded as failed exactly once")
	require.Empty(t, finishedWrites)
	require.Equal(t, types.BuildStatusGroupFailed, statusWrites[0].group)
	require.Contains(t, statusWrites[0].message, "Maximum build time")
	require.NoError(t, statusWrites[0].ctxErr,
		"the terminal write must not reuse the expired poll context, or it never lands")

	deletes := client.deletes()
	require.Len(t, deletes, 1, "the builder has no deadline of its own, so closing the row must also stop the build")
	require.NoError(t, deletes[0].ctxErr)
}

func TestPoll_TimeoutDoesNotCancelABuildAnotherPollerEnded(t *testing.T) {
	t.Parallel()

	// Every instance polls every build, so this poller's deadline can fire after
	// a peer recorded the real outcome. The fenced write reports the loss, and a
	// build that is already over must keep its artifacts.
	client := &recordingClient{
		status: &templatemanagergrpc.TemplateBuildStatusResponse{
			Status: templatemanagergrpc.TemplateBuildState_Building,
		},
		terminalStatusLost: true,
	}

	newChecker(client).poll(expiredContext(t))

	require.Empty(t, client.deletes(), "a build another poller ended must not be cancelled on the node")
}

func TestPoll_TimeoutMidCheckIsRecordedAsATimeout(t *testing.T) {
	t.Parallel()

	// The build deadline lands while a status check is in flight. The check fails
	// because of the closing, which says nothing about the build, so it must not
	// be recorded as a build error. The hook waits for the real deadline rather
	// than racing it, so the ordering is deterministic.
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(1050*time.Millisecond))
	defer cancel()

	client := &recordingClient{
		getStatusErr: errors.New("rpc error: code = DeadlineExceeded desc = context deadline exceeded"),
		onGetStatus:  func() { <-ctx.Done() },
	}

	newChecker(client).poll(ctx)

	statusWrites, finishedWrites := client.writes()

	require.Empty(t, finishedWrites)
	require.Len(t, statusWrites, 1)
	require.Equal(t, types.BuildStatusGroupFailed, statusWrites[0].group)
	require.Contains(t, statusWrites[0].message, "Maximum build time")
	require.NotContains(t, strings.ToLower(statusWrites[0].message), "unrecoverable",
		"the poll window closing is not the build failing")
	require.NoError(t, statusWrites[0].ctxErr)
}

func TestPoll_CancelledPollerLeavesTheBuildInProgress(t *testing.T) {
	t.Parallel()

	// The poll context is cancelled rather than expired: the API instance is
	// shutting down, and the build still has time left. Failing it here would
	// destroy healthy builds on every restart, and every instance polls every
	// build, so one terminating instance would take them all.
	client := &recordingClient{
		status: &templatemanagergrpc.TemplateBuildStatusResponse{
			Status: templatemanagergrpc.TemplateBuildState_Building,
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	newChecker(client).poll(ctx)

	statusWrites, finishedWrites := client.writes()

	require.Empty(t, statusWrites, "a build outliving its poller must not be recorded as failed")
	require.Empty(t, finishedWrites)
	require.Empty(t, client.deletes(), "a build that still has time must keep running on the node")
}

func TestPoll_CompletedBuildIsRecordedWhenTheWindowClosesMidCheck(t *testing.T) {
	t.Parallel()

	// The deadline lands during the very call that observes the build finishing.
	// The outcome is the build's, so it is recorded rather than lost to a
	// timeout the build never hit.
	ctx, cancel := livePollContext(t)
	client := &recordingClient{
		status: &templatemanagergrpc.TemplateBuildStatusResponse{
			Status: templatemanagergrpc.TemplateBuildState_Completed,
			Metadata: &templatemanagergrpc.TemplateBuildMetadata{
				RootfsSizeKey:  100,
				EnvdVersionKey: "1.0.0",
			},
		},
		onGetStatus: cancel,
	}

	newChecker(client).poll(ctx)

	statusWrites, finishedWrites := client.writes()

	require.Empty(t, statusWrites, "a completed build must never be recorded as failed")
	require.Len(t, finishedWrites, 1, "the real outcome must be recorded")
	require.NoError(t, finishedWrites[0].ctxErr)
	require.Empty(t, client.deletes())
}

func TestPoll_BuildersFailureReasonIsRecordedWhenTheWindowClosesMidCheck(t *testing.T) {
	t.Parallel()

	// Same instant, but the builder's terminal state is a failure. Its reason is
	// the real outcome and must survive the window closing.
	ctx, cancel := livePollContext(t)
	client := &recordingClient{
		status: &templatemanagergrpc.TemplateBuildStatusResponse{
			Status: templatemanagergrpc.TemplateBuildState_Failed,
			Reason: &templatemanagergrpc.TemplateBuildStatusReason{
				Message: "layer 3 command exited 1",
			},
		},
		onGetStatus: cancel,
	}

	newChecker(client).poll(ctx)

	statusWrites, _ := client.writes()

	require.Len(t, statusWrites, 1)
	require.Equal(t, "layer 3 command exited 1", statusWrites[0].message)
	require.NoError(t, statusWrites[0].ctxErr)
}

func TestPoll_TerminalErrorOnALiveContextStillFailsTheBuild(t *testing.T) {
	t.Parallel()

	// A genuine non-context error from the builder (node gone, build dropped from
	// its cache) is terminal and must still fail the build, on a live context.
	client := &recordingClient{
		getStatusErr: errors.New("build not found on node"),
	}

	ctx, _ := livePollContext(t)

	newChecker(client).poll(ctx)

	statusWrites, finishedWrites := client.writes()

	require.Empty(t, finishedWrites)
	require.Len(t, statusWrites, 1)
	require.Equal(t, types.BuildStatusGroupFailed, statusWrites[0].group)
	require.Contains(t, statusWrites[0].message, "unrecoverable error")
	require.NoError(t, statusWrites[0].ctxErr)
	require.Empty(t, client.deletes(), "only the build deadline cancels the build on the node")
}
