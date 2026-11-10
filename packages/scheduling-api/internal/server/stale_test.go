package server_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func TestListStaleTransitions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	pauseProject, killProject := uuid.NewString(), uuid.NewString()
	paused := staleTransition(t, client, pool, pauseProject, "i00000000000000000001", scheduling.TransitionAction_TRANSITION_ACTION_PAUSE, 2*time.Hour)
	killed := staleTransition(t, client, pool, killProject, "i00000000000000000002", scheduling.TransitionAction_TRANSITION_ACTION_KILL, time.Hour)
	staleTransition(t, client, pool, killProject, "i00000000000000000003", scheduling.TransitionAction_TRANSITION_ACTION_KILL, time.Minute)

	var listed []*scheduling.StaleTransition
	token := ""
	for page := range 3 {
		resp, err := client.ListStaleTransitions(ctx, scheduling.ListStaleTransitionsRequest_builder{PageSize: 1, PageToken: token}.Build())
		require.NoError(t, err)
		listed = append(listed, resp.GetTransitions()...)
		token = resp.GetNextPageToken()
		if page < 2 {
			require.Len(t, resp.GetTransitions(), 1)
			require.NotEmpty(t, token)
		} else {
			require.Empty(t, resp.GetTransitions())
			require.Empty(t, token)
		}
	}
	require.Len(t, listed, 2)
	for i, want := range []staleSandbox{paused, killed} {
		got := listed[i]
		require.Equal(t, want.project, got.GetProjectId())
		require.Equal(t, want.sandboxID, got.GetSandboxId(), "the normal sandbox ID, not the actor UUID")
		require.Equal(t, want.tid, got.GetTransitionId())
		require.Equal(t, want.action, got.GetAction())
		require.Equal(t, want.executionID, got.GetExecutionId())
		require.Equal(t, "node-1", got.GetNodeId())
		require.NotEmpty(t, got.GetClusterId())
		require.True(t, got.HasTransitionAt())
	}
}

func TestListStaleTransitionsInvalidRequests(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, _ := newClient(t)

	encode := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }
	for name, token := range map[string]string{
		"not base64":      "!!!",
		"wrong version":   encode("2:1:" + sandboxID),
		"bad time":        encode("1:x:" + sandboxID),
		"time past range": encode("1:9223372036854775807:" + sandboxID),
		"negative time":   encode("1:-1:" + sandboxID),
		"bad sandbox ID":  encode("1:1:" + uuid.NewString()),
		"extra part":      encode("1:1:" + sandboxID + ":x"),
	} {
		_, err := client.ListStaleTransitions(ctx, scheduling.ListStaleTransitionsRequest_builder{PageToken: token}.Build())
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
		require.Equal(t, "page_token: must be a next_page_token of ListStaleTransitions", status.Convert(err).Message(), name)
	}
	for _, size := range []int32{-1, 1001} {
		_, err := client.ListStaleTransitions(ctx, scheduling.ListStaleTransitionsRequest_builder{PageSize: size}.Build())
		require.Equal(t, codes.InvalidArgument, status.Code(err), size)
	}
	resp, err := client.ListStaleTransitions(ctx, scheduling.ListStaleTransitionsRequest_builder{
		PageToken: encode("1:1:" + sandboxID),
	}.Build())
	require.NoError(t, err, "a well-formed token for a sandbox that does not exist")
	require.Empty(t, resp.GetTransitions())
}

func TestListStaleTransitionsInternalError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	pool.Close()

	_, err := client.ListStaleTransitions(ctx, scheduling.ListStaleTransitionsRequest_builder{}.Build())
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, "internal error", status.Convert(err).Message())
}

type staleSandbox struct {
	project, sandboxID, tid, executionID string
	action                               scheduling.TransitionAction
}

func staleTransition(
	t *testing.T, client scheduling.SchedulingServiceClient, pool *pgxpool.Pool,
	project, sandbox string, action scheduling.TransitionAction, ago time.Duration,
) staleSandbox {
	t.Helper()
	ctx := t.Context()

	rid := uuid.NewString()
	reserveReq := reserveRequest(project, sandbox, rid, 10)
	reserved, err := client.Reserve(ctx, reserveReq)
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	putReq := putRequest(project, rid)
	putReq.SetSandboxId(sandbox)
	stored, err := client.Put(ctx, putReq)
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())

	tid := uuid.NewString()
	beginReq := beginRequest(project, tid, action)
	beginReq.SetSandboxId(sandbox)
	begun, err := client.BeginTransition(ctx, beginReq)
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, begun.GetOutcome())

	actorID, err := id.SandboxIDToUUID(sandbox)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET transition_at = now() - make_interval(secs => $2) WHERE actor_id = $1`,
		actorID, ago.Seconds())
	require.NoError(t, err)

	return staleSandbox{
		project:     project,
		sandboxID:   sandbox,
		tid:         tid,
		executionID: putReq.GetExecutionId(),
		action:      action,
	}
}
