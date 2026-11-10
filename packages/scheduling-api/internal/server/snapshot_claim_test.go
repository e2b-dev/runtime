package server_test

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func TestSnapshotClaimsAreProjectScoped(t *testing.T) {
	t.Parallel()
	for _, purged := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "purged"}[purged], func(t *testing.T) {
			t.Parallel()
			client, pool := newClient(t)
			ctx := t.Context()
			owner, other, claim := uuid.NewString(), uuid.NewString(), uuid.NewString()
			if purged {
				start(t, client, owner, uuid.NewString(), 10)
				transition := uuid.NewString()
				_, err := client.BeginTransition(ctx, beginRequest(owner, transition, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE))
				require.NoError(t, err)
				_, err = client.FinishTransition(ctx, finishTransitionRequest(owner, transition))
				require.NoError(t, err)
				requireSnapshotClaimNotFound(t, client, other, claim)
				_, err = pool.Exec(ctx, `UPDATE scheduling_actor_run SET gone_at = now() - interval '1 hour'`)
				require.NoError(t, err)
				deleted, err := store.New(pool, pool).PurgeTombstones(ctx, 10)
				require.NoError(t, err)
				require.EqualValues(t, 1, deleted)
			}
			requireSnapshotClaim(t, client, other, claim, true)
			requireSnapshotClaim(t, client, owner, claim, true)
			releaseSnapshotClaim(t, client, owner, claim)
			rid := uuid.NewString()
			reserved, err := client.Reserve(ctx, reserveRequest(owner, sandboxID, rid, 10))
			require.NoError(t, err)
			require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
			requireSnapshotClaimNotFound(t, client, other, claim)
			requireSnapshotClaim(t, client, owner, claim, false)
			_, err = client.Put(ctx, putRequest(owner, rid))
			require.NoError(t, err)
			requireSnapshotClaimNotFound(t, client, other, claim)
			requireSnapshotClaim(t, client, owner, claim, false)
		})
	}
}

func TestSnapshotClaimsOverlapAndReleaseIndependently(t *testing.T) {
	t.Parallel()
	client, _ := newClient(t)
	ctx := t.Context()
	project := uuid.NewString()
	claims := []string{uuid.NewString(), uuid.NewString()}
	responses := make([]*scheduling.ClaimSnapshotDeletionResponse, len(claims))
	errs := make([]error, len(claims))
	var wg sync.WaitGroup
	ready := make(chan struct{})
	for i, claim := range claims {
		wg.Go(func() {
			<-ready
			responses[i], errs[i] = client.ClaimSnapshotDeletion(ctx, scheduling.ClaimSnapshotDeletionRequest_builder{ProjectId: project, SandboxId: sandboxID, ClaimId: claim}.Build())
		})
	}
	close(ready)
	wg.Wait()
	for i := range claims {
		require.NoError(t, errs[i])
		require.True(t, responses[i].GetClaimed())
	}
	releaseSnapshotClaim(t, client, uuid.NewString(), claims[1])
	releaseSnapshotClaim(t, client, project, uuid.NewString())
	releaseSnapshotClaim(t, client, project, claims[0])
	_, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 10))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	releaseSnapshotClaim(t, client, project, claims[1])
	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
}

func TestSnapshotClaimRenewalAndExpiry(t *testing.T) {
	t.Parallel()
	client, pool := newClient(t)
	ctx := t.Context()
	project, first, second := uuid.NewString(), uuid.NewString(), uuid.NewString()
	requireSnapshotClaim(t, client, project, first, true)
	requireSnapshotClaim(t, client, project, second, true)
	_, err := pool.Exec(ctx, `UPDATE scheduling_kill_claim SET until = now() - interval '1 second'`)
	require.NoError(t, err)
	requireSnapshotClaim(t, client, project, second, true)
	var until time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT until FROM scheduling_kill_claim WHERE claim_id = $1`, first).Scan(&until))
	require.True(t, until.Before(time.Now()), "renewal does not extend another claim")
	_, err = client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 10))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = pool.Exec(ctx, `UPDATE scheduling_kill_claim SET until = now() - interval '1 second'`)
	require.NoError(t, err)
	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	requireSnapshotClaim(t, client, project, second, false)
}

func TestSnapshotClaimAndReserveAreAtomic(t *testing.T) {
	t.Parallel()
	client, _ := newClient(t)
	ctx := t.Context()
	project := uuid.NewString()
	for range 20 {
		claim, rid := uuid.NewString(), uuid.NewString()
		var claimed *scheduling.ClaimSnapshotDeletionResponse
		var reserved *scheduling.ReserveResponse
		var claimErr, reserveErr error
		var wg sync.WaitGroup
		ready := make(chan struct{})
		wg.Go(func() {
			<-ready
			claimed, claimErr = client.ClaimSnapshotDeletion(ctx, scheduling.ClaimSnapshotDeletionRequest_builder{ProjectId: project, SandboxId: sandboxID, ClaimId: claim}.Build())
		})
		wg.Go(func() {
			<-ready
			reserved, reserveErr = client.Reserve(ctx, reserveRequest(project, sandboxID, rid, 10))
		})
		close(ready)
		wg.Wait()
		require.NoError(t, claimErr)
		if claimed.GetClaimed() {
			require.Equal(t, codes.FailedPrecondition, status.Code(reserveErr))
			releaseSnapshotClaim(t, client, project, claim)
		} else {
			require.NoError(t, reserveErr)
			require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
			_, err := client.FinishStart(ctx, finishStartRequest(project, rid))
			require.NoError(t, err)
		}
	}
}

func requireSnapshotClaim(t *testing.T, client scheduling.SchedulingServiceClient, project, claim string, want bool) {
	t.Helper()
	got, err := client.ClaimSnapshotDeletion(t.Context(), scheduling.ClaimSnapshotDeletionRequest_builder{ProjectId: project, SandboxId: sandboxID, ClaimId: claim}.Build())
	require.NoError(t, err)
	require.Equal(t, want, got.GetClaimed())
}

func releaseSnapshotClaim(t *testing.T, client scheduling.SchedulingServiceClient, project, claim string) {
	t.Helper()
	_, err := client.ReleaseSnapshotDeletionClaim(t.Context(), scheduling.ReleaseSnapshotDeletionClaimRequest_builder{ProjectId: project, SandboxId: sandboxID, ClaimId: claim}.Build())
	require.NoError(t, err)
}

func requireSnapshotClaimNotFound(t *testing.T, client scheduling.SchedulingServiceClient, project, claim string) {
	t.Helper()
	_, err := client.ClaimSnapshotDeletion(t.Context(), scheduling.ClaimSnapshotDeletionRequest_builder{ProjectId: project, SandboxId: sandboxID, ClaimId: claim}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))
}
