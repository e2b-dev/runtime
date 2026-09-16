package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestCathedralSandboxOperationConcurrentReservationBindsOneSandbox(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "cathedral-operation-race")

	const contenders = 16
	const key = "cathedral-create-race"
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	var wg sync.WaitGroup
	winners := make(chan string, contenders)
	errorsCh := make(chan error, contenders)
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, err := db.SqlcClient.ReserveCathedralSandboxOperation(t.Context(), queries.ReserveCathedralSandboxOperationParams{
				TeamID:         teamID,
				IdempotencyKey: key,
				RequestSha256:  digest,
				SandboxID:      fmt.Sprintf("i-contender-%02d", i),
			})
			if err == nil {
				winners <- op.SandboxID
				return
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				errorsCh <- err
			}
		}()
	}
	wg.Wait()
	close(winners)
	close(errorsCh)

	for err := range errorsCh {
		require.NoError(t, err)
	}
	var winnerIDs []string
	for sandboxID := range winners {
		winnerIDs = append(winnerIDs, sandboxID)
	}
	require.Len(t, winnerIDs, 1)

	op, err := db.SqlcClient.GetCathedralSandboxOperation(t.Context(), queries.GetCathedralSandboxOperationParams{
		TeamID:         teamID,
		IdempotencyKey: key,
	})
	require.NoError(t, err)
	assert.Equal(t, winnerIDs[0], op.SandboxID)
	assert.Equal(t, digest, op.RequestSha256)
	assert.Equal(t, "reserved", op.State)
}

func TestCathedralLifecycleOperationCannotCompleteWithoutBoundTerminalEvidence(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "cathedral-lifecycle-evidence")

	const (
		key         = "cathedral-delete-1"
		digest      = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		sandboxID   = "i-delete-bound"
		executionID = "exec-delete-bound"
	)
	remaining := int64(60_000)
	op, err := db.SqlcClient.ReserveCathedralSandboxLifecycleOperation(t.Context(), queries.ReserveCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: key, RequestSha256: digest,
		OperationKind: "delete", SandboxID: sandboxID, ExecutionID: executionID,
		RemainingLifetimeMs: &remaining,
	})
	require.NoError(t, err)
	assert.Equal(t, "reserved", op.State)
	assert.Equal(t, "pending", op.CleanupState)

	// Reserved is not dispatched and therefore cannot be promoted by a stale
	// observer that merely noticed the registry row disappear.
	rows, err := db.SqlcClient.CompleteCathedralSandboxLifecycleOperation(t.Context(), queries.CompleteCathedralSandboxLifecycleOperationParams{
		ExecutionRemovedAt: time.Now(), CleanupState: "completed", ResultJson: `{}`,
		TeamID: teamID, OperationKey: key, RequestSha256: digest,
		OperationKind: "delete", SandboxID: sandboxID, ExecutionID: executionID,
	})
	require.NoError(t, err)
	assert.Zero(t, rows)

	dispatch, err := db.SqlcClient.MarkCathedralSandboxLifecycleDispatching(t.Context(), queries.MarkCathedralSandboxLifecycleDispatchingParams{
		LeaseDuration: pgtype.Interval{Microseconds: time.Minute.Microseconds(), Valid: true},
		TeamID:        teamID, OperationKey: key, RequestSha256: digest,
		OperationKind: "delete", SandboxID: sandboxID, ExecutionID: executionID,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), dispatch.DispatchAttempt)

	// A stale execution identity cannot complete the operation.
	rows, err = db.SqlcClient.CompleteCathedralSandboxLifecycleOperation(t.Context(), queries.CompleteCathedralSandboxLifecycleOperationParams{
		ExecutionRemovedAt: time.Now(), CleanupState: "completed", ResultJson: `{}`,
		DispatchAttempt: dispatch.DispatchAttempt,
		TeamID:          teamID, OperationKey: key, RequestSha256: digest,
		OperationKind: "delete", SandboxID: sandboxID, ExecutionID: "exec-new",
	})
	require.NoError(t, err)
	assert.Zero(t, rows)

	rows, err = db.SqlcClient.CompleteCathedralSandboxLifecycleOperation(t.Context(), queries.CompleteCathedralSandboxLifecycleOperationParams{
		ExecutionRemovedAt: time.Now(), CleanupState: "failed", ResultJson: `{"evidence_source":"execution_bound_node_rpc"}`,
		DispatchAttempt: dispatch.DispatchAttempt,
		TeamID:          teamID, OperationKey: key, RequestSha256: digest,
		OperationKind: "delete", SandboxID: sandboxID, ExecutionID: executionID,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	ready, err := db.SqlcClient.GetCathedralSandboxLifecycleOperation(t.Context(), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: key})
	require.NoError(t, err)
	assert.Equal(t, "completed", ready.State)
	assert.Equal(t, "failed", ready.CleanupState)
	require.NotNil(t, ready.ExecutionRemovedAt)

	rows, err = db.SqlcClient.UpdateCathedralSandboxLifecycleCleanup(t.Context(), queries.UpdateCathedralSandboxLifecycleCleanupParams{
		CleanupState: "completed", TeamID: teamID, OperationKey: key, RequestSha256: digest,
		SandboxID: sandboxID, ExecutionID: executionID,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)
	ready, err = db.SqlcClient.GetCathedralSandboxLifecycleOperation(t.Context(), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: key})
	require.NoError(t, err)
	assert.Equal(t, "completed", ready.State, "cleanup recovery must not redispatch compute")
	assert.Equal(t, "completed", ready.CleanupState)
}

func TestCathedralLifecycleOperationRepeatedKeyNeverRedispatchesOrRebinds(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "cathedral-lifecycle-key")
	const digest = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

	_, err = db.SqlcClient.ReserveCathedralSandboxLifecycleOperation(t.Context(), queries.ReserveCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: "cathedral-pause-1", RequestSha256: digest,
		OperationKind: "pause", SandboxID: "sbx-one", ExecutionID: "exec-one",
	})
	require.NoError(t, err)
	_, err = db.SqlcClient.ReserveCathedralSandboxLifecycleOperation(t.Context(), queries.ReserveCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: "cathedral-pause-1", RequestSha256: digest,
		OperationKind: "pause", SandboxID: "sbx-two", ExecutionID: "exec-two",
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)

	op, err := db.SqlcClient.GetCathedralSandboxLifecycleOperation(t.Context(), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: "cathedral-pause-1"})
	require.NoError(t, err)
	assert.Equal(t, "sbx-one", op.SandboxID)
	assert.Equal(t, "exec-one", op.ExecutionID)

	dispatch, err := db.SqlcClient.MarkCathedralSandboxLifecycleDispatching(t.Context(), queries.MarkCathedralSandboxLifecycleDispatchingParams{
		LeaseDuration: pgtype.Interval{Microseconds: time.Minute.Microseconds(), Valid: true},
		TeamID:        teamID, OperationKey: op.OperationKey, RequestSha256: digest,
		OperationKind: "pause", SandboxID: op.SandboxID, ExecutionID: op.ExecutionID,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), dispatch.DispatchAttempt)
	_, err = db.SqlcClient.MarkCathedralSandboxLifecycleDispatching(t.Context(), queries.MarkCathedralSandboxLifecycleDispatchingParams{
		LeaseDuration: pgtype.Interval{Microseconds: time.Minute.Microseconds(), Valid: true},
		TeamID:        teamID, OperationKey: op.OperationKey, RequestSha256: digest,
		OperationKind: "pause", SandboxID: op.SandboxID, ExecutionID: op.ExecutionID,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows, "a repeated key cannot win dispatch twice")
}

func TestCathedralLifecycleExpiredDispatchCanBeRequeuedWithGenerationFence(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "cathedral-lifecycle-lease")
	const digest = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	op, err := db.SqlcClient.ReserveCathedralSandboxLifecycleOperation(t.Context(), queries.ReserveCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: "cathedral-pause-lease", RequestSha256: digest,
		OperationKind: "pause", SandboxID: "sbx-lease", ExecutionID: "exec-lease", FilesystemOnly: true,
	})
	require.NoError(t, err)
	assert.True(t, op.FilesystemOnly)

	dispatch, err := db.SqlcClient.MarkCathedralSandboxLifecycleDispatching(t.Context(), queries.MarkCathedralSandboxLifecycleDispatchingParams{
		LeaseDuration: pgtype.Interval{Microseconds: (-time.Second).Microseconds(), Valid: true},
		TeamID:        teamID, OperationKey: op.OperationKey, RequestSha256: digest,
		OperationKind: op.OperationKind, SandboxID: op.SandboxID, ExecutionID: op.ExecutionID,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), dispatch.DispatchAttempt)

	rows, err := db.SqlcClient.RequeueExpiredCathedralSandboxLifecycleDispatch(t.Context(), queries.RequeueExpiredCathedralSandboxLifecycleDispatchParams{
		ErrorMessage: "proved no-op", TeamID: teamID, OperationKey: op.OperationKey,
		RequestSha256: digest, ExecutionID: op.ExecutionID, DispatchAttempt: dispatch.DispatchAttempt + 1,
	})
	require.NoError(t, err)
	assert.Zero(t, rows, "a stale recovery generation must not move the active lease")

	rows, err = db.SqlcClient.RequeueExpiredCathedralSandboxLifecycleDispatch(t.Context(), queries.RequeueExpiredCathedralSandboxLifecycleDispatchParams{
		ErrorMessage: "proved no-op", TeamID: teamID, OperationKey: op.OperationKey,
		RequestSha256: digest, ExecutionID: op.ExecutionID, DispatchAttempt: dispatch.DispatchAttempt,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	requeued, err := db.SqlcClient.GetCathedralSandboxLifecycleOperation(t.Context(), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
	require.NoError(t, err)
	assert.Equal(t, "reserved", requeued.State)
	assert.Nil(t, requeued.DispatchLeaseExpiresAt)
}

func TestCathedralSandboxOperationSurvivesAmbiguousCreateAndStoresImmutableResponse(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	teamID := seedTeam(t, sqlDB, "cathedral-operation-recovery")

	const key = "cathedral-create-recovery"
	const digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const sandboxID = "i-provider-accepted"
	op, err := db.SqlcClient.ReserveCathedralSandboxOperation(t.Context(), queries.ReserveCathedralSandboxOperationParams{
		TeamID:         teamID,
		IdempotencyKey: key,
		RequestSha256:  digest,
		SandboxID:      sandboxID,
	})
	require.NoError(t, err)
	assert.Equal(t, sandboxID, op.SandboxID)

	rows, err := db.SqlcClient.MarkCathedralSandboxOperationCreating(t.Context(), queries.MarkCathedralSandboxOperationCreatingParams{
		TeamID:         teamID,
		IdempotencyKey: key,
		RequestSha256:  digest,
		SandboxID:      sandboxID,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	// This lookup represents process recovery after the provider accepted the
	// sandbox but the HTTP response was lost. The original binding must survive.
	recovered, err := db.SqlcClient.GetCathedralSandboxOperation(t.Context(), queries.GetCathedralSandboxOperationParams{
		TeamID:         teamID,
		IdempotencyKey: key,
	})
	require.NoError(t, err)
	assert.Equal(t, "creating", recovered.State)
	assert.Equal(t, sandboxID, recovered.SandboxID)

	const response = `{"sandboxID":"i-provider-accepted","templateID":"base","clientID":"","envdVersion":"0.5.0"}`
	rows, err = db.SqlcClient.CompleteCathedralSandboxOperation(t.Context(), queries.CompleteCathedralSandboxOperationParams{
		ResponseJson:   response,
		TeamID:         teamID,
		IdempotencyKey: key,
		RequestSha256:  digest,
		SandboxID:      sandboxID,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	rows, err = db.SqlcClient.CompleteCathedralSandboxOperation(t.Context(), queries.CompleteCathedralSandboxOperationParams{
		ResponseJson:   `{"sandboxID":"different"}`,
		TeamID:         teamID,
		IdempotencyKey: key,
		RequestSha256:  digest,
		SandboxID:      sandboxID,
	})
	require.NoError(t, err)
	assert.Zero(t, rows, "a terminal replay response must be immutable")

	ready, err := db.SqlcClient.GetCathedralSandboxOperation(t.Context(), queries.GetCathedralSandboxOperationParams{
		TeamID:         teamID,
		IdempotencyKey: key,
	})
	require.NoError(t, err)
	require.NotNil(t, ready.ResponseJson)
	assert.Equal(t, response, *ready.ResponseJson)
}
