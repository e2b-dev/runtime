package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

const cathedralIdempotencyAckHeader = "X-E2B-Idempotency-Key"

var cathedralIdempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

type cathedralCreateClaim struct {
	key           string
	requestSHA256 string
	sandboxID     string
}

func hashCathedralCreateRequest(body api.PostSandboxesJSONRequestBody) (string, error) {
	canonical, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal normalized create request: %w", err)
	}

	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// inspectCathedralCreate performs the read half of the durable-create protocol.
// It runs before template lookup so a completed replay still works after the
// referenced template changes or disappears. New keys are only inserted after
// the rest of request validation succeeds.
func (a *APIStore) inspectCathedralCreate(
	c *gin.Context,
	teamID uuid.UUID,
	key *string,
	body api.PostSandboxesJSONRequestBody,
) (*cathedralCreateClaim, bool) {
	if key == nil {
		return nil, true
	}
	if !cathedralIdempotencyKeyPattern.MatchString(*key) {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Idempotency-Key must contain 8 to 128 letters, numbers, periods, underscores, colons, or hyphens.")
		return nil, false
	}

	requestSHA256, err := hashCathedralCreateRequest(body)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to normalize create request")
		return nil, false
	}
	claim := &cathedralCreateClaim{key: *key, requestSHA256: requestSHA256}

	operation, err := a.sqlcDB.GetCathedralSandboxOperation(c.Request.Context(), queries.GetCathedralSandboxOperationParams{
		TeamID:         teamID,
		IdempotencyKey: claim.key,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return claim, true
	}
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to inspect durable create operation")
		return nil, false
	}

	return a.resumeCathedralCreate(c, claim, operation)
}

func (a *APIStore) resumeCathedralCreate(
	c *gin.Context,
	claim *cathedralCreateClaim,
	operation queries.CathedralSandboxOperation,
) (*cathedralCreateClaim, bool) {
	if operation.RequestSha256 != claim.requestSHA256 {
		a.sendAPIStoreError(c, http.StatusConflict, "Idempotency-Key was already used for a different sandbox create request.")
		return nil, false
	}
	claim.sandboxID = operation.SandboxID

	switch operation.State {
	case "ready":
		if operation.ResponseJson == nil {
			a.sendAPIStoreError(c, http.StatusInternalServerError, "durable create operation has no stored response")
			return nil, false
		}
		c.Header(cathedralIdempotencyAckHeader, claim.key)
		c.Data(http.StatusCreated, "application/json", []byte(*operation.ResponseJson))
		return nil, false
	case "failed":
		code := http.StatusInternalServerError
		if operation.ErrorCode != nil && *operation.ErrorCode >= 400 && *operation.ErrorCode <= 599 {
			code = int(*operation.ErrorCode)
		}
		message := "durable create operation failed"
		if operation.ErrorMessage != nil && *operation.ErrorMessage != "" {
			message = *operation.ErrorMessage
		}
		a.sendAPIStoreError(c, code, message)
		return nil, false
	case "reserved", "creating":
		return claim, true
	default:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "durable create operation has an invalid state")
		return nil, false
	}
}

// claimCathedralCreate atomically binds a new key to one sandbox ID. When a
// concurrent request wins the insert, the loser adopts the winner's ID.
func (a *APIStore) claimCathedralCreate(
	c *gin.Context,
	teamID uuid.UUID,
	claim *cathedralCreateClaim,
) (string, bool) {
	if claim == nil {
		return InstanceIDPrefix + id.Generate(), true
	}
	if claim.sandboxID != "" {
		return claim.sandboxID, true
	}

	proposedID := InstanceIDPrefix + id.Generate()
	operation, err := a.sqlcDB.ReserveCathedralSandboxOperation(c.Request.Context(), queries.ReserveCathedralSandboxOperationParams{
		TeamID:         teamID,
		IdempotencyKey: claim.key,
		RequestSha256:  claim.requestSHA256,
		SandboxID:      proposedID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		operation, err = a.sqlcDB.GetCathedralSandboxOperation(c.Request.Context(), queries.GetCathedralSandboxOperationParams{
			TeamID:         teamID,
			IdempotencyKey: claim.key,
		})
	}
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to reserve durable create operation")
		return "", false
	}

	resumed, proceed := a.resumeCathedralCreate(c, claim, operation)
	if !proceed {
		return "", false
	}
	return resumed.sandboxID, true
}

func (a *APIStore) markCathedralCreateStarted(ctx context.Context, teamID uuid.UUID, claim *cathedralCreateClaim, sandboxID string) error {
	if claim == nil {
		return nil
	}
	rows, err := a.sqlcDB.MarkCathedralSandboxOperationCreating(ctx, queries.MarkCathedralSandboxOperationCreatingParams{
		TeamID:         teamID,
		IdempotencyKey: claim.key,
		RequestSha256:  claim.requestSHA256,
		SandboxID:      sandboxID,
	})
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("durable create operation transition affected %d rows", rows)
	}
	return nil
}

func (a *APIStore) completeCathedralCreate(ctx context.Context, teamID uuid.UUID, claim *cathedralCreateClaim, sandboxID string, response []byte) error {
	if claim == nil {
		return nil
	}
	rows, err := a.sqlcDB.CompleteCathedralSandboxOperation(ctx, queries.CompleteCathedralSandboxOperationParams{
		ResponseJson:   string(response),
		TeamID:         teamID,
		IdempotencyKey: claim.key,
		RequestSha256:  claim.requestSHA256,
		SandboxID:      sandboxID,
	})
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("durable create operation completion affected %d rows", rows)
	}
	return nil
}

func (a *APIStore) GetV1CathedralCapabilities(c *gin.Context) {
	c.JSON(http.StatusOK, api.CathedralCapabilities{
		Schema:                     api.N1,
		DurableCreateIdempotency:   true,
		OperationLookup:            true,
		SafeFork:                   false,
		DurableLifecycleOperations: true,
		SafeDelete:                 true,
		SafePause:                  true,
		PreservesRemainingLifetime: true,
		ExecutionIdentity:          true,
	})
}

func (a *APIStore) GetV1CathedralOperationsIdempotencyKey(c *gin.Context, idempotencyKey api.CathedralOperationKey) {
	if !cathedralIdempotencyKeyPattern.MatchString(idempotencyKey) {
		a.sendAPIStoreError(c, http.StatusBadRequest, "invalid Cathedral operation key")
		return
	}

	teamInfo := auth.MustGetTeamInfo(c)
	operation, err := a.sqlcDB.GetCathedralSandboxOperation(c.Request.Context(), queries.GetCathedralSandboxOperationParams{
		TeamID:         teamInfo.Team.ID,
		IdempotencyKey: idempotencyKey,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cathedral operation not found")
		return
	}
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "failed to read Cathedral operation")
		return
	}

	result := api.CathedralSandboxOperation{
		IdempotencyKey: operation.IdempotencyKey,
		SandboxId:      operation.SandboxID,
		State:          api.CathedralSandboxOperationState(operation.State),
		ErrorCode:      nil,
		ErrorMessage:   operation.ErrorMessage,
	}
	if operation.ErrorCode != nil {
		code := int(*operation.ErrorCode)
		result.ErrorCode = &code
	}
	if operation.State == "ready" && operation.ResponseJson != nil {
		var sandbox api.Sandbox
		if err := json.Unmarshal([]byte(*operation.ResponseJson), &sandbox); err != nil {
			a.sendAPIStoreError(c, http.StatusInternalServerError, "durable operation response is invalid")
			return
		}
		result.Sandbox = &sandbox
	}

	c.JSON(http.StatusOK, result)
}
