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
	// Parallel subtests start after this function returns, so each call takes a fresh deadline.
	responseDeadline := func() time.Time { return time.Now().Add(time.Minute) }

	t.Run("atomic across instances and fixed retention", func(t *testing.T) {
		t.Parallel()
		key := recordKey(uuid.NewString(), "private-key")
		require.NotContains(t, key, "private-key")
		var owners atomic.Int64
		var winner string
		var wg sync.WaitGroup
		reserveErrors := make([]error, 32)
		started := redisMillis(t, client)
		for i := range reserveErrors {
			wg.Go(func() {
				s := &redisStore{client: client}
				owner := uuid.NewString()
				result, err := s.reserve(t.Context(), key, "hash", owner, 3600, responseDeadline())
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
		elapsed := time.Duration(redisMillis(t, client)-started) * time.Millisecond
		require.NotEqual(t, time.Duration(-1), ttl, "key has no expiry")
		require.GreaterOrEqual(t, ttl, time.Hour-elapsed)
		require.LessOrEqual(t, ttl, time.Hour)
		expiresAt, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		pending, err := s.reserve(t.Context(), key, "hash", "retry", 3600, responseDeadline())
		require.NoError(t, err)
		require.Equal(t, statePending, pending.state)
		mismatch, err := s.reserve(t.Context(), key, "different", uuid.NewString(), 3600, responseDeadline())
		require.NoError(t, err)
		require.Equal(t, stateMismatch, mismatch.state)
		want := cachedResponse{Status: 503, Headers: http.Header{"Content-Type": {"application/json"}}, Body: []byte{0, 255, '\n'}}
		require.ErrorIs(t, s.complete(t.Context(), key, "other", want, false), errOwnershipLost)
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, s.complete(t.Context(), key, winner, want, false))
		encoded, err := client.HGet(t.Context(), key, "response").Result()
		require.NoError(t, err)
		require.JSONEq(t, `{"status":503,"body":"AP8K","headers":{"Content-Type":["application/json"]}}`, encoded)
		completedExpiry, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, expiresAt, completedExpiry)
		result, err := s.reserve(t.Context(), key, "hash", "retry", 3600, responseDeadline())
		require.NoError(t, err)
		require.Equal(t, stateComplete, result.state)
		require.Equal(t, want, result.response)
		replayedExpiry, err := client.PExpireTime(t.Context(), key).Result()
		require.NoError(t, err)
		require.Equal(t, expiresAt, replayedExpiry)
		require.ErrorIs(t, s.complete(t.Context(), key, winner, cachedResponse{Status: 200}, false), errOwnershipLost)
	})

	for _, state := range []recordState{statePending, stateComplete} {
		t.Run(string(state)+" removal allows a new owner", func(t *testing.T) {
			t.Parallel()
			s := &redisStore{client: client}
			key := uuid.NewString()
			_, err := s.reserve(t.Context(), key, "hash", "old", 60, responseDeadline())
			require.NoError(t, err)
			if state == stateComplete {
				require.NoError(t, s.complete(t.Context(), key, "old", cachedResponse{Status: 201}, false))
			}
			// Redis drops an expired key on its next access, so a delete is what a lapsed retention looks like to the store.
			require.NoError(t, client.Del(t.Context(), key).Err())
			require.ErrorIs(t, s.complete(t.Context(), key, "old", cachedResponse{Status: 201}, false), errOwnershipLost)
			require.Zero(t, client.Exists(t.Context(), key).Val())
			result, err := s.reserve(t.Context(), key, "new-hash", "new", 60, responseDeadline())
			require.NoError(t, err)
			require.Equal(t, stateOwner, result.state)
			require.ErrorIs(t, s.complete(t.Context(), key, "old", cachedResponse{Status: 201}, false), errOwnershipLost)
			result, err = s.reserve(t.Context(), key, "new-hash", "retry", 3600, responseDeadline())
			require.NoError(t, err)
			require.Equal(t, statePending, result.state)
			want := cachedResponse{Status: 202}
			require.NoError(t, s.complete(t.Context(), key, "new", want, false))
			result, err = s.reserve(t.Context(), key, "new-hash", "retry", 3600, responseDeadline())
			require.NoError(t, err)
			require.Equal(t, stateComplete, result.state)
			require.Equal(t, want, result.response)
		})
	}

	t.Run("invalid and corrupt records fail closed", func(t *testing.T) {
		t.Parallel()
		s := &redisStore{client: client}
		key := uuid.NewString()
		_, err := s.reserve(t.Context(), key, "hash", "owner", 0, responseDeadline())
		require.Error(t, err)
		require.Zero(t, client.Exists(t.Context(), key).Val())
		require.NoError(t, client.Set(t.Context(), key, "wrong type", time.Minute).Err())
		_, err = s.reserve(t.Context(), key, "hash", "owner", 60, responseDeadline())
		require.Error(t, err)
		require.Error(t, s.complete(t.Context(), key, "owner", cachedResponse{Status: 201}, false))
		for _, values := range [][]string{
			{},
			{"invalid", "", "", "", "", ""},
			{"false", "hash", "invalid", "", "", ""},
			{"false", "hash", "", "1000", "invalid", ""},
		} {
			_, err := decodeRecord(values, "hash")
			require.Error(t, err)
		}
	})
}

func TestRedisDeadlineDuringSocketRead(t *testing.T) {
	t.Parallel()
	// CLIENT PAUSE stalls every client on the server, so this test needs its own.
	client := redisutils.SetupContainerInstance(t)
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

func TestLegacyRecordsRemainReplayOnly(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	for _, tc := range []struct {
		name     string
		encoded  string
		response cachedResponse
	}{
		{
			name:     "success",
			encoded:  `{"status":201,"body":"Y3JlYXRlZA==","headers":{"Content-Type":["text/plain"]}}`,
			response: cachedResponse{Status: http.StatusCreated, Body: []byte("created"), Headers: http.Header{"Content-Type": {"text/plain"}}},
		},
		{
			name:     "permanent failure",
			encoded:  `{"status":400,"body":"aW52YWxpZA==","headers":{"Should-Retry":["false"]}}`,
			response: cachedResponse{Status: http.StatusBadRequest, Body: []byte("invalid"), Headers: http.Header{"Should-Retry": {"false"}}},
		},
		{
			name:     "retryable failure",
			encoded:  `{"status":503,"body":"YnVzeQ==","headers":{"Should-Retry":["true"]}}`,
			response: cachedResponse{Status: http.StatusServiceUnavailable, Body: []byte("busy"), Headers: http.Header{"Should-Retry": {"true"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			key := recordKey(uuid.NewString(), "key")
			// Legacy hashes have no state field; their retry hints never authorize re-execution.
			fields := map[string]string{
				"fingerprint":          "hash",
				"owner":                "legacy-owner",
				"expected_response_at": "1",
				"response":             tc.encoded,
			}
			require.NoError(t, client.HSet(t.Context(), key, fields).Err())
			require.NoError(t, client.Expire(t.Context(), key, time.Hour).Err())
			expiresAt, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			result, err := s.reserve(t.Context(), key, "hash", "new-owner", 3600, time.Now().Add(time.Minute))
			require.NoError(t, err)
			assert.Equal(t, stateComplete, result.state)
			assert.Equal(t, tc.response, result.response)
			mismatch, err := s.reserve(t.Context(), key, "changed", "new-owner", 3600, time.Now().Add(time.Minute))
			require.NoError(t, err)
			assert.Equal(t, stateMismatch, mismatch.state)
			require.ErrorIs(t, s.complete(t.Context(), key, "legacy-owner", cachedResponse{Status: http.StatusCreated}, true), errOwnershipLost)
			require.NoError(t, s.release(t.Context(), key, "legacy-owner"))
			after, err := client.HGetAll(t.Context(), key).Result()
			require.NoError(t, err)
			assert.Equal(t, fields, after)
			afterExpiry, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			assert.Equal(t, expiresAt, afterExpiry)
		})
	}
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

func (s faultStore) complete(ctx context.Context, key, owner string, result cachedResponse, retryable bool) error {
	if s.beforeComplete != nil {
		s.beforeComplete(ctx)
	}
	if s.completeErr != nil && !s.completeAfterWrite {
		return s.completeErr
	}

	return errors.Join(s.store.complete(ctx, key, owner, result, retryable), s.completeErr)
}
