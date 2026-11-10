package idempotency

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func operationRouter(t *testing.T, s store, teamID uuid.UUID, operation func(*gin.Context) *apierrors.APIError) *gin.Engine {
	t.Helper()
	flags, source := testFlags(t, true, 86400)
	source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{
		"POST /widgets/:id": true,
		"PUT /widgets/:id":  true,
	})))
	r := gin.New()
	r.Use(gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) { c.AbortWithStatus(http.StatusInternalServerError) }))
	r.Use(func(c *gin.Context) { auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{ID: teamID}}) })
	r.Use(sharedmiddleware.RequestTimeout(time.Minute), middleware(s, flags))
	handler := func(c *gin.Context) {
		var body struct {
			Value string `json:"value"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.AbortWithStatus(http.StatusBadRequest)

			return
		}
		Execute(c, body, func() *apierrors.APIError { return operation(c) })
	}
	r.POST("/widgets/:id", handler)
	r.PUT("/widgets/:id", handler)

	return r
}

func TestRetryableOperation(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	for _, code := range []int{429, 500, 503, 504} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			t.Parallel()
			teamID := uuid.New()
			key := recordKey(teamID.String(), "key")
			calls := 0
			r := operationRouter(t, &redisStore{client: client}, teamID, func(c *gin.Context) *apierrors.APIError {
				calls++
				if calls <= 2 {
					return &apierrors.APIError{Code: code, ClientMsg: "try again", Retryable: apierrors.CanRetry}
				}
				c.JSON(http.StatusCreated, gin.H{"id": "created"})

				return nil
			})
			first := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, code, first.Code)
			assert.Equal(t, "true", first.Header().Get(apierrors.ShouldRetryHeader))
			expiresAt, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			for _, changed := range []struct{ method, path, body string }{
				{"POST", "/widgets/one", `{"value":"b"}`},
				{"POST", "/widgets/two", `{"value":"a"}`},
				{"PUT", "/widgets/one", `{"value":"a"}`},
			} {
				response := methodRequest(t.Context(), r, changed.method, changed.path, changed.body, []string{"key"})
				assert.Equal(t, http.StatusConflict, response.Code)
				assert.Contains(t, response.Body.String(), "idempotency_request_mismatch")
			}
			second := request(t.Context(), r, "/widgets/one", `{ "value": "a" }`, []string{"key"})
			assert.Equal(t, code, second.Code)
			third := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, http.StatusCreated, third.Code)
			fourth := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, third.Code, fourth.Code)
			assert.Equal(t, third.Body.String(), fourth.Body.String())
			assert.Empty(t, fourth.Header().Get(apierrors.ShouldRetryHeader))
			assert.Equal(t, 3, calls)
			afterExpiry, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			assert.Equal(t, expiresAt, afterExpiry)
		})
	}
}

func TestReturnedErrorControlsReexecution(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	for _, tc := range []struct {
		name      string
		retryable apierrors.Retryability
		written   bool
	}{
		{name: "unclassified"},
		{name: "explicit refusal", retryable: apierrors.NoRetry},
		{name: "response already written", retryable: apierrors.CanRetry, written: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			r := operationRouter(t, &redisStore{client: client}, uuid.New(), func(c *gin.Context) *apierrors.APIError {
				calls++
				c.Header(apierrors.ShouldRetryHeader, "true")
				if tc.written {
					c.JSON(http.StatusCreated, gin.H{"id": "already-created"})
				}

				return &apierrors.APIError{Code: http.StatusServiceUnavailable, ClientMsg: "unavailable", Retryable: tc.retryable}
			})
			first := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			second := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, first.Code, second.Code)
			assert.Equal(t, first.Body.String(), second.Body.String())
			assert.Equal(t, 1, calls)
		})
	}
}

func TestConcurrentRetryableOperation(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	teamID := uuid.New()
	started := make(chan struct{})
	release := make(chan struct{})
	releaseOwner := sync.OnceFunc(func() { close(release) })
	var calls atomic.Int64
	operation := func(c *gin.Context) *apierrors.APIError {
		if calls.Add(1) == 1 {
			return &apierrors.APIError{Code: http.StatusServiceUnavailable, ClientMsg: "no capacity", Retryable: apierrors.CanRetry}
		}
		close(started)
		<-release
		c.JSON(http.StatusCreated, gin.H{"id": "created"})

		return nil
	}
	routers := []*gin.Engine{
		operationRouter(t, &redisStore{client: client}, teamID, operation),
		operationRouter(t, &redisStore{client: client}, teamID, operation),
	}
	first := request(t.Context(), routers[0], "/widgets/one", `{"value":"a"}`, []string{"key"})
	require.Equal(t, http.StatusServiceUnavailable, first.Code)
	owner := make(chan *httptest.ResponseRecorder, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		owner <- request(t.Context(), routers[1], "/widgets/one", `{"value":"a"}`, []string{"key"})
	}()
	defer func() { releaseOwner(); <-done }()
	select {
	case <-started:
	case <-done:
		t.Fatalf("retry did not execute: %s", (<-owner).Body.String())
	}
	var wg sync.WaitGroup
	responses := make([]*httptest.ResponseRecorder, 16)
	for i := range responses {
		wg.Go(func() {
			responses[i] = request(t.Context(), routers[i%2], "/widgets/one", `{"value":"a"}`, []string{"key"})
		})
	}
	wg.Wait()
	for _, response := range responses {
		assert.Equal(t, http.StatusConflict, response.Code)
		assert.Contains(t, response.Body.String(), "idempotency_in_progress")
		assert.Equal(t, "true", response.Header().Get(apierrors.ShouldRetryHeader))
	}
	releaseOwner()
	result := <-owner
	assert.Equal(t, http.StatusCreated, result.Code)
	replay := request(t.Context(), routers[0], "/widgets/one", `{"value":"a"}`, []string{"key"})
	assert.Equal(t, result.Code, replay.Code)
	assert.Equal(t, result.Body.String(), replay.Body.String())
	assert.EqualValues(t, 2, calls.Load())
}

func TestRetryableCompletionFailure(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	for _, afterWrite := range []bool{false, true} {
		t.Run(fmt.Sprintf("written=%t", afterWrite), func(t *testing.T) {
			t.Parallel()
			s := &redisStore{client: client}
			teamID := uuid.New()
			key := recordKey(teamID.String(), "key")
			calls := 0
			operation := func(c *gin.Context) *apierrors.APIError {
				calls++
				if calls == 1 {
					return &apierrors.APIError{Code: http.StatusServiceUnavailable, Retryable: apierrors.CanRetry}
				}
				c.Status(http.StatusCreated)

				return nil
			}
			fault := faultStore{store: s, completeErr: context.DeadlineExceeded, completeAfterWrite: afterWrite}
			r := operationRouter(t, fault, teamID, operation)
			first := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, http.StatusServiceUnavailable, first.Code)
			r = operationRouter(t, s, teamID, operation)
			retry := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			if afterWrite {
				assert.Equal(t, http.StatusCreated, retry.Code)
				assert.Equal(t, 2, calls)
			} else {
				assert.Equal(t, http.StatusConflict, retry.Code)
				require.NoError(t, client.HSet(t.Context(), key, "expected_response_at", 1).Err())
				retry = request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
				assert.Equal(t, http.StatusUnprocessableEntity, retry.Code)
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestUncertainRetryDoesNotReplayPreviousFailure(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	for _, panicAfterCreation := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panicAfterCreation), func(t *testing.T) {
			t.Parallel()
			s := &redisStore{client: client}
			teamID := uuid.New()
			key := recordKey(teamID.String(), "key")
			calls := 0
			operation := func(c *gin.Context) *apierrors.APIError {
				calls++
				if calls == 1 {
					return &apierrors.APIError{Code: http.StatusServiceUnavailable, Retryable: apierrors.CanRetry}
				}
				if panicAfterCreation {
					panic("failed after creation")
				}
				c.Status(http.StatusCreated)

				return nil
			}
			r := operationRouter(t, s, teamID, operation)
			first := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			require.Equal(t, http.StatusServiceUnavailable, first.Code)
			r = operationRouter(t, faultStore{store: s, completeErr: errors.New("write failed")}, teamID, operation)
			request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			r = operationRouter(t, s, teamID, operation)
			pending := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, http.StatusConflict, pending.Code)
			assert.Contains(t, pending.Body.String(), "idempotency_in_progress")
			require.NoError(t, client.HSet(t.Context(), key, "expected_response_at", 1).Err())
			unknown := request(t.Context(), r, "/widgets/one", `{"value":"a"}`, []string{"key"})
			assert.Equal(t, http.StatusUnprocessableEntity, unknown.Code)
			assert.Equal(t, "false", unknown.Header().Get(apierrors.ShouldRetryHeader))
			assert.Equal(t, 2, calls)
		})
	}
}

func TestRetryableReservationOwnership(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	key := recordKey(uuid.NewString(), "key")
	deadline := time.Now().Add(time.Minute)
	_, err := s.reserve(t.Context(), key, "hash", "first", 3600, deadline)
	require.NoError(t, err)
	failure := cachedResponse{Status: http.StatusServiceUnavailable}
	require.NoError(t, s.complete(t.Context(), key, "first", failure, true))
	expiresAt, err := client.PExpireTime(t.Context(), key).Result()
	require.NoError(t, err)
	second, err := s.reserve(t.Context(), key, "hash", "second", 3600, deadline)
	require.NoError(t, err)
	require.Equal(t, stateOwner, second.state)
	require.ErrorIs(t, s.complete(t.Context(), key, "first", failure, true), errOwnershipLost)
	require.NoError(t, s.release(t.Context(), key, "first"))
	pending, err := s.reserve(t.Context(), key, "hash", "observer", 3600, deadline)
	require.NoError(t, err)
	assert.Equal(t, statePending, pending.state)
	require.NoError(t, s.release(t.Context(), key, "second"))
	mismatch, err := s.reserve(t.Context(), key, "changed", "observer", 3600, deadline)
	require.NoError(t, err)
	assert.Equal(t, stateMismatch, mismatch.state)
	third, err := s.reserve(t.Context(), key, "hash", "third", 3600, deadline)
	require.NoError(t, err)
	require.Equal(t, stateOwner, third.state)
	require.ErrorIs(t, s.complete(t.Context(), key, "second", failure, true), errOwnershipLost)
	require.NoError(t, s.release(t.Context(), key, "second"))
	success := cachedResponse{Status: http.StatusCreated, Body: []byte("created")}
	require.NoError(t, s.complete(t.Context(), key, "third", success, false))
	replayed, err := s.reserve(t.Context(), key, "hash", "observer", 3600, deadline)
	require.NoError(t, err)
	assert.Equal(t, stateComplete, replayed.state)
	assert.Equal(t, success, replayed.response)
	afterExpiry, err := client.PExpireTime(t.Context(), key).Result()
	require.NoError(t, err)
	assert.Equal(t, expiresAt, afterExpiry)
}

func TestRetryMustFitRemainingRetention(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	key := recordKey(uuid.NewString(), "key")
	_, err := s.reserve(t.Context(), key, "hash", "first", 3600, time.Now().Add(time.Minute))
	require.NoError(t, err)
	failure := cachedResponse{Status: http.StatusServiceUnavailable, Body: []byte("no capacity")}
	require.NoError(t, s.complete(t.Context(), key, "first", failure, true))
	require.NoError(t, client.PExpire(t.Context(), key, 30*time.Second).Err())
	expiresAt, err := client.PExpireTime(t.Context(), key).Result()
	require.NoError(t, err)
	result, err := s.reserve(t.Context(), key, "hash", "second", 3600, time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Equal(t, stateComplete, result.state)
	assert.Equal(t, failure, result.response)
	afterExpiry, err := client.PExpireTime(t.Context(), key).Result()
	require.NoError(t, err)
	assert.Equal(t, expiresAt, afterExpiry)
}
