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

	// deleteIfSameOwner outcomes, distinguished for diagnostics.
	catalogDeleteAbsent     = 0 // no entry
	catalogDeleteDeleted    = 1 // entry deleted (execution and lifecycle matched)
	catalogDeleteMismatch   = 2 // entry kept — another execution or lifecycle owns it now
	catalogDeleteUnreadable = 3 // entry kept — value undecodable or missing execution_id
)

// deleteIfSameOwner deletes the catalog entry only if its stored execution_id
// still matches ARGV[1] and, when the entry names a lifecycle, its
// lifecycle_id is ARGV[2], as one atomic server-side script: a stale teardown
// can't remove an entry a concurrent StoreSandbox wrote for a newer
// execution, or for the same execution moved to another lifecycle. An entry
// without a lifecycle, which an older writer stored, is owned by its
// execution alone. Returns the catalogDelete* outcome.
var deleteIfSameOwner = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then
  return 0
end
local ok, info = pcall(cjson.decode, v)
if not (ok and type(info) == 'table' and type(info.execution_id) == 'string') then
  return 3
end
if info.execution_id ~= ARGV[1] then
  return 2
end
if type(info.lifecycle_id) == 'string' and info.lifecycle_id ~= '' and info.lifecycle_id ~= ARGV[2] then
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

// DeleteSandbox deletes the entry if its execution ID matches and, when the
// entry names a lifecycle, it is lifecycleID, and returns the Redis error, if
// any. A mismatch, an unreadable value or an absent key are not errors.
func (c *RedisSandboxCatalog) DeleteSandbox(ctx context.Context, sandboxID, executionID, lifecycleID string) error {
	spanCtx, span := tracer.Start(ctx, "sandbox-catalog-delete")
	defer span.End()

	ctx, ctxCancel := context.WithTimeout(spanCtx, catalogRedisTimeout)
	defer ctxCancel()

	outcome, err := deleteIfSameOwner.Run(ctx, c.redisClient, []string{c.getCatalogKey(sandboxID)}, executionID, lifecycleID).Int()
	if err != nil {
		span.RecordError(err)

		return fmt.Errorf("failed to delete sandbox info from redis: %w", err)
	}

	switch outcome {
	case catalogDeleteDeleted:
		span.SetAttributes(attribute.String("delete.outcome", "deleted"))
		logger.L().Debug(ctx, "deleted sandbox from redis catalog", logger.WithSandboxID(sandboxID))
	case catalogDeleteMismatch:
		// A newer execution or lifecycle owns the entry — the stale-teardown race the atomic delete guards against.
		span.SetAttributes(attribute.String("delete.outcome", "execution-mismatch"))
		logger.L().Debug(ctx, "kept sandbox catalog entry owned by a different execution or lifecycle", logger.WithSandboxID(sandboxID))
	case catalogDeleteUnreadable:
		span.SetAttributes(attribute.String("delete.outcome", "unreadable"))
		logger.L().Warn(ctx, "sandbox catalog entry is unreadable; leaving it to expire via TTL", logger.WithSandboxID(sandboxID))
	case catalogDeleteAbsent:
		span.SetAttributes(attribute.String("delete.outcome", "absent"))
	default:
		span.SetAttributes(attribute.String("delete.outcome", "unexpected"))
		logger.L().Warn(ctx, "unexpected sandbox catalog delete outcome", logger.WithSandboxID(sandboxID), zap.Int("outcome", outcome))
	}

	return nil
}

func (c *RedisSandboxCatalog) getCatalogKey(sandboxID string) string {
	return routingKeyPrefix + sandboxID
}

func (c *RedisSandboxCatalog) Close(_ context.Context) error {
	return nil
}
