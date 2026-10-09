package idempotency

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestRedisStore(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	responseDeadline := time.Now().Add(time.Minute)

	t.Run("atomic across instances and fixed retention", func(t *testing.T) {
		t.Parallel()
		key := recordKey(uuid.NewString(), "private-key")
		require.NotContains(t, key, "private-key")
		var owners atomic.Int64
		var winner string
		var wg sync.WaitGroup
		reserveErrors := make([]error, 32)
		for i := range reserveErrors {
			wg.Go(func() {
				s := &redisStore{client: client}
				owner := uuid.NewString()
				result, err := s.reserve(t.Context(), key, "hash", owner, 60, responseDeadline)
				reserveErrors[i] = err
				if err != nil {
					return
				}
				if result.state == stateOwner {
					owners.Add(1)
					winner = owner
				} else {
					assert.Equal(t, statePending, result.state)
				}
			})
		}
		wg.Wait()
		for _, err := range reserveErrors {
			require.NoError(t, err)
		}
		require.EqualValues(t, 1, owners.Load())
		s := &redisStore{client: client}
		ttl, err := client.PTTL(t.Context(), key).Result()
		require.NoError(t, err)
		require.Greater(t, ttl, 50*time.Second)
		require.LessOrEqual(t, ttl, time.Minute)
		expiresAt, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		pending, err := s.reserve(t.Context(), key, "hash", "retry", 3600, responseDeadline)
		require.NoError(t, err)
		require.Equal(t, statePending, pending.state)
		mismatch, err := s.reserve(t.Context(), key, "different", uuid.NewString(), 3600, responseDeadline)
		require.NoError(t, err)
		require.Equal(t, stateMismatch, mismatch.state)
		want := cachedResponse{Status: 503, Headers: http.Header{"Content-Type": {"application/json"}}, Body: []byte{0, 255, '\n'}}
		require.ErrorIs(t, s.complete(t.Context(), key, "other", want), errOwnershipLost)
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, s.complete(t.Context(), key, winner, want))
		encoded, err := client.HGet(t.Context(), key, "response").Result()
		require.NoError(t, err)
		require.JSONEq(t, `{"status":503,"body":"AP8K","headers":{"Content-Type":["application/json"]}}`, encoded)
		completedExpiry, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, expiresAt, completedExpiry)
		result, err := s.reserve(t.Context(), key, "hash", "retry", 3600, responseDeadline)
		require.NoError(t, err)
		require.Equal(t, stateComplete, result.state)
		require.Equal(t, want, result.response)
		replayedExpiry, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, expiresAt, replayedExpiry)
		require.ErrorIs(t, s.complete(t.Context(), key, winner, cachedResponse{Status: 200}), errOwnershipLost)
	})

	for _, state := range []recordState{statePending, stateComplete} {
		t.Run(string(state)+" expiry allows a new owner", func(t *testing.T) {
			t.Parallel()
			s := &redisStore{client: client}
			key := uuid.NewString()
			_, err := s.reserve(t.Context(), key, "hash", "old", 1, responseDeadline)
			require.NoError(t, err)
			if state == stateComplete {
				require.NoError(t, s.complete(t.Context(), key, "old", cachedResponse{Status: 201}))
			}
			require.Eventually(t, func() bool { return client.Exists(t.Context(), key).Val() == 0 }, 2*time.Second, 10*time.Millisecond)
			require.ErrorIs(t, s.complete(t.Context(), key, "old", cachedResponse{Status: 201}), errOwnershipLost)
			require.Zero(t, client.Exists(t.Context(), key).Val())
			result, err := s.reserve(t.Context(), key, "new-hash", "new", 60, responseDeadline)
			require.NoError(t, err)
			require.Equal(t, stateOwner, result.state)
			require.ErrorIs(t, s.complete(t.Context(), key, "old", cachedResponse{Status: 201}), errOwnershipLost)
			result, err = s.reserve(t.Context(), key, "new-hash", "retry", 3600, responseDeadline)
			require.NoError(t, err)
			require.Equal(t, statePending, result.state)
			want := cachedResponse{Status: 202}
			require.NoError(t, s.complete(t.Context(), key, "new", want))
			result, err = s.reserve(t.Context(), key, "new-hash", "retry", 3600, responseDeadline)
			require.NoError(t, err)
			require.Equal(t, stateComplete, result.state)
			require.Equal(t, want, result.response)
		})
	}

	t.Run("invalid and corrupt records fail closed", func(t *testing.T) {
		t.Parallel()
		s := &redisStore{client: client}
		key := uuid.NewString()
		_, err := s.reserve(t.Context(), key, "hash", "owner", 0, responseDeadline)
		require.Error(t, err)
		require.Zero(t, client.Exists(t.Context(), key).Val())
		require.NoError(t, client.Set(t.Context(), key, "wrong type", time.Minute).Err())
		_, err = s.reserve(t.Context(), key, "hash", "owner", 60, responseDeadline)
		require.Error(t, err)
		require.Error(t, s.complete(t.Context(), key, "owner", cachedResponse{Status: 201}))
		for _, values := range [][]string{
			{},
			{"invalid", "", "", "", ""},
			{"false", "hash", "invalid", "", ""},
			{"false", "hash", "", "1000", "invalid"},
		} {
			_, err := decodeRecord(values, "hash")
			require.Error(t, err)
		}
	})
}

func TestRedisDeadlineDuringSocketRead(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	require.NoError(t, reserveScript.Load(t.Context(), client).Err())
	require.NoError(t, client.Do(t.Context(), "CLIENT", "PAUSE", 500).Err())
	s := &redisStore{client: client}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.reserve(ctx, uuid.NewString(), "hash", "owner", 60, time.Now().Add(time.Minute))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 300*time.Millisecond)
}

type faultStore struct {
	store

	reserveErr, completeErr error
	completeAfterWrite      bool
	beforeComplete          func(context.Context)
}

func (s faultStore) reserve(ctx context.Context, key, digest, owner string, retentionSeconds int, responseDeadline time.Time) (record, error) {
	if s.reserveErr != nil {
		return record{}, s.reserveErr
	}

	return s.store.reserve(ctx, key, digest, owner, retentionSeconds, responseDeadline)
}

func (s faultStore) complete(ctx context.Context, key, owner string, result cachedResponse) error {
	if s.beforeComplete != nil {
		s.beforeComplete(ctx)
	}
	if s.completeErr != nil && !s.completeAfterWrite {
		return s.completeErr
	}

	return errors.Join(s.store.complete(ctx, key, owner, result), s.completeErr)
}
