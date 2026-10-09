package idempotency

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestCompletionFailureReturnsOriginalResponse(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	for _, failure := range []struct {
		name       string
		err        error
		afterWrite bool
	}{
		{"write failure", errors.New("injected write failure"), false},
		{"timeout", context.DeadlineExceeded, false},
		{"lost acknowledgement", errors.New("injected acknowledgement failure"), true},
	} {
		for _, status := range []int{http.StatusCreated, http.StatusBadRequest, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("%s/%d", failure.name, status), func(t *testing.T) {
				t.Parallel()
				flags, _ := testFlags(t, false, 86400)
				team := uuid.New()
				key := recordKey(team.String(), "key")
				body := fmt.Sprintf("{\"code\":%d}\n", status)
				if status == http.StatusCreated {
					body = "{\"sandboxID\":\"original\",\"envdAccessToken\":\"token\"}\n"
				}
				var reserved map[string]string
				var expiresAt time.Duration
				fault := faultStore{
					store:              s,
					completeErr:        failure.err,
					completeAfterWrite: failure.afterWrite,
					beforeComplete: func(ctx context.Context) {
						var err error
						reserved, err = client.HGetAll(ctx, key).Result()
						require.NoError(t, err)
						expiresAt, err = client.PExpireTime(ctx, key).Result()
						require.NoError(t, err)
					},
				}
				calls := 0
				r := testRouter(t, flags, fault, team, time.Minute, func(c *gin.Context) {
					calls++
					c.Header("Location", "/sandboxes/original")
					c.Header("X-Request-ID", "original-request")
					c.Data(status, "application/json", []byte(body))
				})
				original := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
				require.Equal(t, status, original.Code, original.Body.String())
				require.Equal(t, body, original.Body.String())
				require.Equal(t, "application/json", original.Header().Get("Content-Type"))
				require.Equal(t, "/sandboxes/original", original.Header().Get("Location"))
				require.Equal(t, "original-request", original.Header().Get("X-Request-ID"))
				if !failure.afterWrite {
					after, err := client.HGetAll(t.Context(), key).Result()
					require.NoError(t, err)
					require.Equal(t, reserved, after)
				}
				r = testRouter(t, flags, s, team, time.Minute, func(_ *gin.Context) { calls++ })
				retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
				if failure.afterWrite {
					require.Equal(t, status, retry.Code, retry.Body.String())
					require.Equal(t, body, retry.Body.String())
				} else {
					require.Equal(t, http.StatusConflict, retry.Code, retry.Body.String())
					require.Contains(t, retry.Body.String(), `"error_code":"idempotency_in_progress"`)
					require.NoError(t, client.HSet(t.Context(), key, "expected_response_at", 1).Err())
					retry = request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
					require.Equal(t, http.StatusUnprocessableEntity, retry.Code, retry.Body.String())
					require.Contains(t, retry.Body.String(), `"error_code":"idempotency_outcome_unknown"`)
					require.Empty(t, client.HGet(t.Context(), key, "response").Val())
					require.Equal(t, reserved["owner"], client.HGet(t.Context(), key, "owner").Val())
				}
				require.Equal(t, 1, calls)
				retryExpiry, err := client.PExpireTime(t.Context(), key).Result()
				require.NoError(t, err)
				require.Equal(t, expiresAt, retryExpiry)
			})
		}
	}
}

func TestCompletionOwnershipLossReturnsOriginalResponse(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	flags, _ := testFlags(t, false, 86400)
	team := uuid.New()
	key := recordKey(team.String(), "key")
	digest, err := fingerprint(jsonRequest(t), validatedRequest(t, flags, validBody))
	require.NoError(t, err)
	newResponse := cachedResponse{Status: http.StatusCreated, Body: []byte(`{"sandboxID":"new-owner"}`)}
	var replacement map[string]string
	var expiresAt time.Duration
	fault := faultStore{store: s, beforeComplete: func(ctx context.Context) {
		require.NoError(t, client.Del(ctx, key).Err())
		result, err := s.reserve(ctx, key, digest, "new-owner", 86400, time.Now().Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, stateOwner, result.state)
		require.NoError(t, s.complete(ctx, key, "new-owner", newResponse))
		replacement, err = client.HGetAll(ctx, key).Result()
		require.NoError(t, err)
		expiresAt, err = client.PExpireTime(ctx, key).Result()
		require.NoError(t, err)
	}}
	calls := 0
	r := testRouter(t, flags, fault, team, time.Minute, func(c *gin.Context) {
		calls++
		c.Data(http.StatusCreated, "application/json", []byte(`{"sandboxID":"original"}`))
	})
	original := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusCreated, original.Code, original.Body.String())
	require.JSONEq(t, `{"sandboxID":"original"}`, original.Body.String())
	r = testRouter(t, flags, s, team, time.Minute, func(_ *gin.Context) { calls++ })
	retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, newResponse.Status, retry.Code, retry.Body.String())
	require.Equal(t, newResponse.Body, retry.Body.Bytes())
	require.Equal(t, 1, calls)
	after, err := client.HGetAll(t.Context(), key).Result()
	require.NoError(t, err)
	require.Equal(t, replacement, after)
	retryExpiry, err := client.PExpireTime(t.Context(), key).Result()
	require.NoError(t, err)
	require.Equal(t, expiresAt, retryExpiry)
}
