package sandbox_catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	catalogRedisTimeout = time.Second * 1

	// Catalog delete script outcomes, distinguished for diagnostics.
	catalogDeleteAbsent        = 0 // no entry
	catalogDeleteDeleted       = 1 // entry deleted (expected identity matched)
	catalogDeleteMismatch      = 2 // entry kept — a different identity owns it now
	catalogDeleteUnreadable    = 3 // entry kept — undecodable value or unverifiable identity
	catalogDeleteOwnerRequired = 4 // legacy deletion cannot verify a lifecycle-aware entry
)

// deleteIfSameExecution is the legacy execution-only delete. Records carrying
// any lifecycle_id field require the full owner comparison instead.
var deleteIfSameExecution = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then
  return 0
end
local ok, info = pcall(cjson.decode, v)
if not (ok and type(info) == 'table' and type(info.execution_id) == 'string' and info.execution_id ~= '' and ARGV[1] ~= '') then
  return 3
end
if info.lifecycle_id ~= nil then
  return 4
end
if info.execution_id == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 2
`)

// deleteIfSameOwner compares all ownership dimensions and deletes in one Redis
// operation. Refused deletes leave the record and its expiration untouched.
var deleteIfSameOwner = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then
  return 0
end
local ok, info = pcall(cjson.decode, v)
if not (ok and type(info) == 'table') then
  return 3
end
for _, field in ipairs({'execution_id', 'lifecycle_id', 'orchestrator_id'}) do
  if type(info[field]) ~= 'string' or info[field] == '' then
    return 3
  end
end
if info.execution_id ~= ARGV[1] or info.lifecycle_id ~= ARGV[2] or info.orchestrator_id ~= ARGV[3] then
  return 2
end
redis.call('DEL', KEYS[1])
return 1
`)

// routingKeyPrefix is the orchestrator-owned routing record.
const routingKeyPrefix = "sandbox:routing:"

type RedisSandboxCatalog struct {
	redisClient redis.UniversalClient
}

var _ SandboxesCatalog = (*RedisSandboxCatalog)(nil)

// NewRedisSandboxCatalog reads and writes the sandbox:routing:{sandboxID} record.
func NewRedisSandboxCatalog(redisClient redis.UniversalClient) *RedisSandboxCatalog {
	return &RedisSandboxCatalog{redisClient: redisClient}
}

func (c *RedisSandboxCatalog) GetSandbox(ctx context.Context, sandboxID string) (*SandboxInfo, error) {
	spanCtx, span := tracer.Start(ctx, "sandbox-catalog-get")
	defer span.End()

	ctx, ctxCancel := context.WithTimeout(spanCtx, catalogRedisTimeout)
	defer ctxCancel()

	data, err := c.redisClient.Get(ctx, c.getCatalogKey(sandboxID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrSandboxNotFound
		}

		return nil, fmt.Errorf("failed to get sandbox info from redis: %w", err)
	}

	var info *SandboxInfo
	err = json.Unmarshal(data, &info)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal sandbox info: %w", err)
	}

	return info, nil
}

func (c *RedisSandboxCatalog) StoreSandbox(ctx context.Context, sandboxID string, sandboxInfo *SandboxInfo, expiration time.Duration) error {
	spanCtx, span := tracer.Start(ctx, "sandbox-catalog-store")
	defer span.End()

	logger.L().Debug(ctx, "storing sandbox in redis catalog", logger.WithSandboxID(sandboxID))

	ctx, ctxCancel := context.WithTimeout(spanCtx, catalogRedisTimeout)
	defer ctxCancel()

	bytes, err := json.Marshal(*sandboxInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox info: %w", err)
	}

	status := c.redisClient.Set(ctx, c.getCatalogKey(sandboxID), string(bytes), expiration)
	if status.Err() != nil {
		logger.L().Error(ctx, "Error while storing sandbox in redis", logger.WithSandboxID(sandboxID), zap.Error(status.Err()))

		return fmt.Errorf("failed to store sandbox info in redis: %w", status.Err())
	}

	return nil
}

// DeleteSandbox deletes a legacy entry with a matching nonempty execution ID.
// Lifecycle-aware entries must use DeleteSandboxIfOwner. A mismatch, an
// unverifiable value or an absent key are not errors.
func (c *RedisSandboxCatalog) DeleteSandbox(ctx context.Context, sandboxID string, executionID string) error {
	spanCtx, span := tracer.Start(ctx, "sandbox-catalog-delete")
	defer span.End()

	ctx, ctxCancel := context.WithTimeout(spanCtx, catalogRedisTimeout)
	defer ctxCancel()

	outcome, err := deleteIfSameExecution.Run(ctx, c.redisClient, []string{c.getCatalogKey(sandboxID)}, executionID).Int()
	if err != nil {
		span.RecordError(err)

		return fmt.Errorf("failed to delete sandbox info from redis: %w", err)
	}

	switch outcome {
	case catalogDeleteDeleted:
		span.SetAttributes(attribute.String("delete.outcome", "deleted"))
		logger.L().Debug(ctx, "deleted sandbox from redis catalog", logger.WithSandboxID(sandboxID))
	case catalogDeleteMismatch:
		// A newer execution owns the entry — the stale-teardown race the atomic delete guards against.
		span.SetAttributes(attribute.String("delete.outcome", "execution-mismatch"))
		logger.L().Debug(ctx, "kept sandbox catalog entry owned by a different execution", logger.WithSandboxID(sandboxID))
	case catalogDeleteUnreadable:
		span.SetAttributes(attribute.String("delete.outcome", "unreadable"))
		logger.L().Warn(ctx, "sandbox catalog entry is unreadable; leaving it to expire via TTL", logger.WithSandboxID(sandboxID))
	case catalogDeleteAbsent:
		span.SetAttributes(attribute.String("delete.outcome", "absent"))
	case catalogDeleteOwnerRequired:
		span.SetAttributes(attribute.String("delete.outcome", "owner-required"))
		logger.L().Debug(ctx, "kept lifecycle-aware sandbox catalog entry requiring full owner comparison", logger.WithSandboxID(sandboxID))
	default:
		span.SetAttributes(attribute.String("delete.outcome", "unexpected"))
		logger.L().Warn(ctx, "unexpected sandbox catalog delete outcome", logger.WithSandboxID(sandboxID), zap.Int("outcome", outcome))
	}

	return nil
}

// DeleteSandboxIfOwner removes only the exact execution/lifecycle/orchestrator
// owner. Legacy records lacking any owner field are unverifiable, not wildcards.
func (c *RedisSandboxCatalog) DeleteSandboxIfOwner(ctx context.Context, sandboxID string, owner SandboxOwner) (DeleteOutcome, error) {
	if owner.ExecutionID == "" || owner.LifecycleID == "" || owner.OrchestratorID == "" {
		return "", ErrInvalidSandboxOwner
	}

	spanCtx, span := tracer.Start(ctx, "sandbox-catalog-delete-if-owner")
	defer span.End()

	ctx, ctxCancel := context.WithTimeout(spanCtx, catalogRedisTimeout)
	defer ctxCancel()

	result, err := deleteIfSameOwner.Run(ctx, c.redisClient, []string{c.getCatalogKey(sandboxID)},
		owner.ExecutionID, owner.LifecycleID, owner.OrchestratorID).Int()
	if err != nil {
		span.RecordError(err)

		return "", fmt.Errorf("failed to delete sandbox info from redis: %w", err)
	}

	var outcome DeleteOutcome
	switch result {
	case catalogDeleteAbsent:
		outcome = DeleteOutcomeAbsent
	case catalogDeleteDeleted:
		outcome = DeleteOutcomeDeleted
	case catalogDeleteMismatch:
		outcome = DeleteOutcomeSuperseded
	case catalogDeleteUnreadable:
		outcome = DeleteOutcomeUnverifiable
	default:
		err := fmt.Errorf("unexpected sandbox catalog owner delete outcome: %d", result)
		span.RecordError(err)

		return "", err
	}

	span.SetAttributes(attribute.String("delete.outcome", string(outcome)))

	return outcome, nil
}

func (c *RedisSandboxCatalog) getCatalogKey(sandboxID string) string {
	return routingKeyPrefix + sandboxID
}

func (c *RedisSandboxCatalog) Close(_ context.Context) error {
	return nil
}
