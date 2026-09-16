package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
)

const (
	cathedralLifecycleDispatchTimeout = 2 * time.Minute
	cathedralLifecycleDispatchLease   = 3 * time.Minute
	cathedralLifecycleWriteTimeout    = 5 * time.Second
	cathedralLifecycleWriteAttempts   = 3
)

func cathedralLifecycleDispatchContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), cathedralLifecycleDispatchTimeout)
}

type cathedralLifecycleOrchestrator interface {
	GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error)
	RemoveSandboxWithEvidence(context.Context, uuid.UUID, string, sandbox.RemoveOpts) (orchestrator.SandboxRemovalEvidence, error)
}

func (a *APIStore) cathedralLifecycleBackend() cathedralLifecycleOrchestrator {
	if a.lifecycleBackendOverride != nil {
		return a.lifecycleBackendOverride
	}

	return a.orchestrator
}

func hashCathedralLifecycleRequest(sandboxID string, body api.CathedralLifecycleOperationRequest) (string, error) {
	filesystemOnly := body.FilesystemOnly != nil && *body.FilesystemOnly
	canonical, err := json.Marshal(struct {
		SandboxID      string                                          `json:"sandbox_id"`
		Operation      api.CathedralLifecycleOperationRequestOperation `json:"operation"`
		ExecutionID    string                                          `json:"execution_id"`
		FilesystemOnly bool                                            `json:"filesystem_only"`
	}{
		SandboxID: sandboxID, Operation: body.Operation,
		ExecutionID: body.ExecutionId, FilesystemOnly: filesystemOnly,
	})
	if err != nil {
		return "", fmt.Errorf("marshal lifecycle request: %w", err)
	}

	digest := sha256.Sum256(canonical)

	return hex.EncodeToString(digest[:]), nil
}

func lifecycleOperationToAPI(op queries.CathedralSandboxLifecycleOperation) api.CathedralLifecycleOperation {
	result := api.CathedralLifecycleOperation{
		OperationKey:        op.OperationKey,
		Operation:           api.CathedralLifecycleOperationOperation(op.OperationKind),
		SandboxId:           op.SandboxID,
		ExecutionId:         op.ExecutionID,
		State:               api.CathedralLifecycleOperationState(op.State),
		CleanupState:        api.CathedralLifecycleOperationCleanupState(op.CleanupState),
		ExecutionRemovedAt:  op.ExecutionRemovedAt,
		SnapshotBuildId:     op.SnapshotBuildID,
		SnapshotCompletedAt: op.SnapshotCompletedAt,
		RemainingLifetimeMs: op.RemainingLifetimeMs,
		ErrorMessage:        op.ErrorMessage,
	}
	if op.ErrorCode != nil {
		code := int(*op.ErrorCode)
		result.ErrorCode = &code
	}

	return result
}

func lifecycleHTTPStatus(op queries.CathedralSandboxLifecycleOperation, replay bool) int {
	if replay {
		return http.StatusOK
	}
	if op.State == "completed" {
		return http.StatusCreated
	}

	return http.StatusAccepted
}

func lifecycleRequestFromOperation(op queries.CathedralSandboxLifecycleOperation) api.CathedralLifecycleOperationRequest {
	filesystemOnly := op.FilesystemOnly
	return api.CathedralLifecycleOperationRequest{
		Operation:      api.CathedralLifecycleOperationRequestOperation(op.OperationKind),
		ExecutionId:    op.ExecutionID,
		FilesystemOnly: &filesystemOnly,
	}
}

func lifecycleLeaseInterval() pgtype.Interval {
	return pgtype.Interval{Microseconds: cathedralLifecycleDispatchLease.Microseconds(), Valid: true}
}

func frozenLifetimeMilliseconds(remaining time.Duration) int64 {
	if remaining <= 0 {
		return 0
	}

	return int64((remaining+time.Second-1)/time.Second) * 1000
}

func (a *APIStore) persistCathedralLifecycleState(ctx context.Context, write func(context.Context) (int64, error)) error {
	var lastErr error
	for attempt := 0; attempt < cathedralLifecycleWriteAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cathedralLifecycleWriteTimeout)
		rows, err := write(writeCtx)
		cancel()
		if err == nil && rows == 1 {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("lifecycle state transition affected %d rows", rows)
		}
		lastErr = err
	}

	return fmt.Errorf("persist Cathedral lifecycle terminal state: %w", lastErr)
}

func (a *APIStore) GetV1CathedralLifecycleOperationsIdempotencyKey(c *gin.Context, operationKey api.CathedralOperationKey) {
	if !cathedralIdempotencyKeyPattern.MatchString(operationKey) {
		a.sendAPIStoreError(c, http.StatusBadRequest, "invalid Cathedral operation key")
		return
	}

	teamID := auth.MustGetTeamID(c)
	op, err := a.sqlcDB.GetCathedralSandboxLifecycleOperation(c.Request.Context(), queries.GetCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: operationKey,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cathedral lifecycle operation not found")
		return
	}
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to read Cathedral lifecycle operation")
		return
	}
	op, err = a.recoverCathedralLifecycleOperation(c.Request.Context(), teamID, op)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "lifecycle outcome is pending durable recovery")
		return
	}

	c.JSON(http.StatusOK, lifecycleOperationToAPI(op))
}

func (a *APIStore) GetV1CathedralSandboxesSandboxIDIdentity(c *gin.Context, sandboxID api.SandboxID) {
	teamID := auth.MustGetTeamID(c)
	shortID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")
		return
	}

	current, err := a.cathedralLifecycleBackend().GetSandbox(c.Request.Context(), teamID, shortID)
	if err != nil || current.TeamID != teamID {
		a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(shortID))
		return
	}
	if current.ExecutionID == "" {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "sandbox has no execution identity")
		return
	}

	c.JSON(http.StatusOK, api.CathedralSandboxIdentity{
		SandboxId:   shortID,
		ExecutionId: current.ExecutionID,
		State:       api.CathedralSandboxIdentityState(current.State),
	})
}

func (a *APIStore) PostV1CathedralSandboxesSandboxIDLifecycleOperations(
	c *gin.Context,
	sandboxID api.SandboxID,
	params api.PostV1CathedralSandboxesSandboxIDLifecycleOperationsParams,
) {
	teamID := auth.MustGetTeamID(c)
	shortID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")
		return
	}
	if !cathedralIdempotencyKeyPattern.MatchString(params.IdempotencyKey) {
		a.sendAPIStoreError(c, http.StatusBadRequest, "invalid Cathedral operation key")
		return
	}

	body, err := ginutils.ParseBody[api.CathedralLifecycleOperationRequest](c.Request.Context(), c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))
		return
	}
	if body.ExecutionId == "" || !body.Operation.Valid() {
		a.sendAPIStoreError(c, http.StatusBadRequest, "operation and execution_id are required")
		return
	}
	if body.Operation == api.CathedralLifecycleOperationRequestOperationDelete && body.FilesystemOnly != nil && *body.FilesystemOnly {
		a.sendAPIStoreError(c, http.StatusBadRequest, "filesystem_only is only valid for pause")
		return
	}

	digest, err := hashCathedralLifecycleRequest(shortID, body)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to normalize lifecycle request")
		return
	}
	kind := string(body.Operation)

	// Recover before consulting the live registry. A completed delete has no
	// live record by definition, and that absence must not erase its receipt.
	existing, getErr := a.sqlcDB.GetCathedralSandboxLifecycleOperation(c.Request.Context(), queries.GetCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: params.IdempotencyKey,
	})
	if getErr == nil {
		if existing.RequestSha256 != digest || existing.OperationKind != kind || existing.SandboxID != shortID || existing.ExecutionID != body.ExecutionId {
			a.sendAPIStoreError(c, http.StatusConflict, "Idempotency-Key was already used for a different lifecycle request")
			return
		}
		existing, err = a.recoverCathedralLifecycleOperation(c.Request.Context(), teamID, existing)
		if err != nil {
			a.sendAPIStoreError(c, http.StatusInternalServerError, "lifecycle outcome is pending durable recovery")
			return
		}
		c.JSON(http.StatusOK, lifecycleOperationToAPI(existing))
		return
	}
	if !errors.Is(getErr, pgx.ErrNoRows) {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to inspect Cathedral lifecycle operation")
		return
	}

	// Ownership and execution identity are checked before the durable claim;
	// the execution pin is checked again atomically when removal starts.
	current, err := a.cathedralLifecycleBackend().GetSandbox(c.Request.Context(), teamID, shortID)
	if err != nil || current.TeamID != teamID {
		a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(shortID))
		return
	}
	if current.ExecutionID != body.ExecutionId {
		a.sendAPIStoreError(c, http.StatusConflict, "sandbox execution identity changed")
		return
	}

	op, err := a.sqlcDB.ReserveCathedralSandboxLifecycleOperation(c.Request.Context(), queries.ReserveCathedralSandboxLifecycleOperationParams{
		TeamID: teamID, OperationKey: params.IdempotencyKey, RequestSha256: digest,
		OperationKind: kind, SandboxID: shortID, ExecutionID: body.ExecutionId,
		FilesystemOnly: body.FilesystemOnly != nil && *body.FilesystemOnly,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		op, err = a.sqlcDB.GetCathedralSandboxLifecycleOperation(c.Request.Context(), queries.GetCathedralSandboxLifecycleOperationParams{
			TeamID: teamID, OperationKey: params.IdempotencyKey,
		})
	}
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to reserve Cathedral lifecycle operation")
		return
	}
	if op.RequestSha256 != digest || op.OperationKind != kind || op.SandboxID != shortID || op.ExecutionID != body.ExecutionId {
		a.sendAPIStoreError(c, http.StatusConflict, "Idempotency-Key was already used for a different lifecycle request")
		return
	}
	op, err = a.recoverCathedralLifecycleOperation(c.Request.Context(), teamID, op)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "lifecycle outcome is pending durable recovery")
		return
	}
	c.JSON(lifecycleHTTPStatus(op, false), lifecycleOperationToAPI(op))
}

func (a *APIStore) recoverCathedralLifecycleOperation(ctx context.Context, teamID uuid.UUID, op queries.CathedralSandboxLifecycleOperation) (queries.CathedralSandboxLifecycleOperation, error) {
	if op.State == "completed" && op.OperationKind == "delete" && (op.CleanupState == "pending" || op.CleanupState == "failed") {
		cleanupState := "completed"
		cleanup := a.deleteSnapshot
		if a.lifecycleSnapshotCleanupOverride != nil {
			cleanup = a.lifecycleSnapshotCleanupOverride
		}
		if cleanupErr := cleanup(ctx, op.SandboxID, teamID); cleanupErr != nil && !errors.Is(cleanupErr, db.ErrSnapshotNotFound) {
			cleanupState = "failed"
		}
		if err := a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
			return a.sqlcDB.UpdateCathedralSandboxLifecycleCleanup(writeCtx, queries.UpdateCathedralSandboxLifecycleCleanupParams{
				CleanupState: cleanupState, TeamID: teamID, OperationKey: op.OperationKey,
				RequestSha256: op.RequestSha256, SandboxID: op.SandboxID, ExecutionID: op.ExecutionID,
			})
		}); err != nil {
			return op, err
		}
		return a.sqlcDB.GetCathedralSandboxLifecycleOperation(context.WithoutCancel(ctx), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
	}
	if op.State == "completed" || op.State == "failed" || op.State == "unknown" {
		return op, nil
	}

	if op.State == "dispatching" {
		if op.DispatchLeaseExpiresAt != nil && time.Now().Before(*op.DispatchLeaseExpiresAt) {
			return op, nil
		}

		current, getErr := a.cathedralLifecycleBackend().GetSandbox(ctx, teamID, op.SandboxID)
		if getErr != nil || current.TeamID != teamID || current.ExecutionID != op.ExecutionID {
			message := "dispatch lease expired and the pinned execution can no longer be proven safe to retry"
			err := a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
				return a.sqlcDB.MarkCathedralSandboxLifecycleUnknown(writeCtx, queries.MarkCathedralSandboxLifecycleUnknownParams{
					ErrorMessage: message, TeamID: teamID, OperationKey: op.OperationKey,
					RequestSha256: op.RequestSha256, OperationKind: op.OperationKind,
					SandboxID: op.SandboxID, ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
				})
			})
			if err != nil {
				return op, err
			}
			return a.sqlcDB.GetCathedralSandboxLifecycleOperation(context.WithoutCancel(ctx), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
		}
		if current.State != sandbox.StateRunning {
			message := fmt.Sprintf("dispatch lease expired while pinned execution remained in %s", current.State)
			err := a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
				return a.sqlcDB.MarkCathedralSandboxLifecycleUnknown(writeCtx, queries.MarkCathedralSandboxLifecycleUnknownParams{
					ErrorMessage: message, TeamID: teamID, OperationKey: op.OperationKey,
					RequestSha256: op.RequestSha256, OperationKind: op.OperationKind,
					SandboxID: op.SandboxID, ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
				})
			})
			if err != nil {
				return op, err
			}
			return a.sqlcDB.GetCathedralSandboxLifecycleOperation(context.WithoutCancel(ctx), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
		}

		rows, err := a.sqlcDB.RequeueExpiredCathedralSandboxLifecycleDispatch(ctx, queries.RequeueExpiredCathedralSandboxLifecycleDispatchParams{
			ErrorMessage: "expired dispatch proved to be a no-op; retrying pinned execution",
			TeamID:       teamID, OperationKey: op.OperationKey, RequestSha256: op.RequestSha256,
			ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
		})
		if err != nil {
			return op, err
		}
		if rows == 0 {
			return a.sqlcDB.GetCathedralSandboxLifecycleOperation(ctx, queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
		}
		op.State = "reserved"
	}

	dispatching, err := a.sqlcDB.MarkCathedralSandboxLifecycleDispatching(ctx, queries.MarkCathedralSandboxLifecycleDispatchingParams{
		LeaseDuration: lifecycleLeaseInterval(), TeamID: teamID, OperationKey: op.OperationKey,
		RequestSha256: op.RequestSha256, OperationKind: op.OperationKind,
		SandboxID: op.SandboxID, ExecutionID: op.ExecutionID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return a.sqlcDB.GetCathedralSandboxLifecycleOperation(ctx, queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
	}
	if err != nil {
		return op, err
	}

	dispatchCtx, cancel := cathedralLifecycleDispatchContext(ctx)
	defer cancel()
	if err := a.dispatchCathedralLifecycle(dispatchCtx, teamID, dispatching, lifecycleRequestFromOperation(dispatching)); err != nil {
		return dispatching, err
	}

	return a.sqlcDB.GetCathedralSandboxLifecycleOperation(context.WithoutCancel(ctx), queries.GetCathedralSandboxLifecycleOperationParams{TeamID: teamID, OperationKey: op.OperationKey})
}

func (a *APIStore) dispatchCathedralLifecycle(ctx context.Context, teamID uuid.UUID, op queries.CathedralSandboxLifecycleOperation, body api.CathedralLifecycleOperationRequest) error {
	action := sandbox.StateActionKill
	if body.Operation == api.CathedralLifecycleOperationRequestOperationPause {
		action = sandbox.StateActionPause
	}
	evidence, err := a.cathedralLifecycleBackend().RemoveSandboxWithEvidence(ctx, teamID, op.SandboxID, sandbox.RemoveOpts{
		Action: action, Reason: sandbox.KillReasonRequest,
		FilesystemOnly:    body.FilesystemOnly != nil && *body.FilesystemOnly,
		ExpectExecutionID: op.ExecutionID,
	})
	if err != nil || !evidence.Confirmed {
		message := "provider lifecycle outcome is not terminally confirmed"
		if err != nil {
			message = err.Error()
		}
		if errors.Is(err, sandbox.ErrExecutionMismatch) {
			return a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
				return a.sqlcDB.FailCathedralSandboxLifecycleOperation(writeCtx, queries.FailCathedralSandboxLifecycleOperationParams{
					ErrorCode: http.StatusConflict, ErrorMessage: message, TeamID: teamID,
					OperationKey: op.OperationKey, RequestSha256: op.RequestSha256,
					OperationKind: op.OperationKind, SandboxID: op.SandboxID,
					ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
				})
			})
		}
		if errors.Is(err, sandbox.ErrTransitionRestored) || errors.Is(err, orchestrator.PauseQueueExhaustedError{}) {
			rows, requeueErr := a.sqlcDB.RequeueCathedralSandboxLifecycleDispatch(context.WithoutCancel(ctx), queries.RequeueCathedralSandboxLifecycleDispatchParams{
				ErrorMessage: message, TeamID: teamID, OperationKey: op.OperationKey,
				RequestSha256: op.RequestSha256, ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
			})
			if requeueErr != nil || rows != 1 {
				return fmt.Errorf("persist retryable Cathedral lifecycle state: rows=%d: %w", rows, requeueErr)
			}
			return nil
		}
		if evidence.AlreadyInProgress {
			return nil
		}
		return a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
			return a.sqlcDB.MarkCathedralSandboxLifecycleUnknown(writeCtx, queries.MarkCathedralSandboxLifecycleUnknownParams{
				ErrorMessage: message, TeamID: teamID, OperationKey: op.OperationKey,
				RequestSha256: op.RequestSha256, OperationKind: op.OperationKind,
				SandboxID: op.SandboxID, ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
			})
		})
	}

	now := time.Now().UTC()
	cleanupState := "not_required"
	if op.OperationKind == "delete" {
		cleanupState = "completed"
		cleanup := a.deleteSnapshot
		if a.lifecycleSnapshotCleanupOverride != nil {
			cleanup = a.lifecycleSnapshotCleanupOverride
		}
		if cleanupErr := cleanup(ctx, op.SandboxID, teamID); cleanupErr != nil && !errors.Is(cleanupErr, db.ErrSnapshotNotFound) {
			cleanupState = "failed"
		}
	}

	var snapshotBuildID *string
	var snapshotCompletedAt *time.Time
	if op.OperationKind == "pause" {
		if evidence.SnapshotBuildID == "" {
			return a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
				return a.sqlcDB.MarkCathedralSandboxLifecycleUnknown(writeCtx, queries.MarkCathedralSandboxLifecycleUnknownParams{
					ErrorMessage: "pause node completion lacked durable snapshot identity", TeamID: teamID,
					OperationKey: op.OperationKey, RequestSha256: op.RequestSha256,
					OperationKind: op.OperationKind, SandboxID: op.SandboxID,
					ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
				})
			})
		}
		snapshotBuildID = &evidence.SnapshotBuildID
		snapshotCompletedAt = &now
	}

	resultJSON, _ := json.Marshal(map[string]any{
		"evidence_source": "execution_bound_node_rpc",
		"cleanup_state":   cleanupState,
	})
	var remainingLifetimeMs *int64
	if op.OperationKind == "pause" {
		remaining := frozenLifetimeMilliseconds(evidence.RemainingLifetime)
		remainingLifetimeMs = &remaining
	}
	return a.persistCathedralLifecycleState(ctx, func(writeCtx context.Context) (int64, error) {
		return a.sqlcDB.CompleteCathedralSandboxLifecycleOperation(writeCtx, queries.CompleteCathedralSandboxLifecycleOperationParams{
			ExecutionRemovedAt: now, SnapshotBuildID: snapshotBuildID,
			SnapshotCompletedAt: snapshotCompletedAt, CleanupState: cleanupState,
			RemainingLifetimeMs: remainingLifetimeMs, ResultJson: string(resultJSON),
			TeamID: teamID, OperationKey: op.OperationKey, RequestSha256: op.RequestSha256,
			OperationKind: op.OperationKind, SandboxID: op.SandboxID,
			ExecutionID: op.ExecutionID, DispatchAttempt: op.DispatchAttempt,
		})
	})
}
