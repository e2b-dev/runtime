package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
)

type staticAuthStore struct {
	hashes  []string
	members []uuid.UUID
}

func (s staticAuthStore) GetTeamByHashedAPIKey(context.Context, string) (*types.Team, error) {
	return nil, nil
}

func (s staticAuthStore) GetTeamByID(context.Context, uuid.UUID) (*types.Team, error) {
	return nil, nil
}

func (s staticAuthStore) GetTeamByIDAndUserID(context.Context, uuid.UUID, string) (*types.Team, error) {
	return nil, nil
}

func (s staticAuthStore) GetTeamAPIKeyHashes(context.Context, uuid.UUID) ([]string, error) {
	return s.hashes, nil
}

func (s staticAuthStore) GetTeamMemberIDs(context.Context, uuid.UUID) ([]uuid.UUID, error) {
	return s.members, nil
}

// A block or limits delivery commits before it evicts, and reports success to
// a caller that will not retry. The eviction failing therefore has to be an
// error, or the auth path keeps serving the entry the write just outdated.
func TestInvalidateTeamCacheReportsAFailedEviction(t *testing.T) {
	t.Parallel()

	unreachable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = unreachable.Close() })

	service := &AuthService{
		store: staticAuthStore{hashes: []string{"hash-1"}, members: []uuid.UUID{uuid.New()}},
		teamCache: &authCache{cache: cache.NewRedisCache(cache.RedisConfig[*types.Team]{
			RedisClient:  unreachable,
			TTL:          authInfoExpiration,
			RedisTimeout: 200 * time.Millisecond,
			LockTTL:      cache.RedisLockOff,
			RedisPrefix:  authCacheRedisPrefix,
		})},
	}

	err := service.InvalidateTeamCache(t.Context(), uuid.New())
	require.ErrorContains(t, err, "failed to evict team cache entries")
	require.ErrorContains(t, err, "hash-1", "the sweep continues past the first failure")
}
