package idempotency

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

type cancelAfterReserveStore struct {
	store

	cancel context.CancelFunc
}

func (s cancelAfterReserveStore) reserve(ctx context.Context, key, digest, owner string, retentionSeconds int, responseDeadline time.Time) (record, error) {
	result, err := s.store.reserve(ctx, key, digest, owner, retentionSeconds, responseDeadline)
	if err == nil && result.state == stateOwner {
		s.cancel()
	}

	return result, err
}

func TestCancellationAfterReservationReleasesUnusedKey(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	flags, _ := testFlags(t, false, 86400)
	team := uuid.New()
	key := recordKey(team.String(), "key")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	handler := func(c *gin.Context) {
		calls++
		c.Status(http.StatusCreated)
	}
	r := testRouter(t, flags, cancelAfterReserveStore{store: s, cancel: cancel}, team, time.Minute, handler)
	first := request(ctx, r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusServiceUnavailable, first.Code, first.Body.String())
	require.Zero(t, calls)
	exists, err := client.Exists(t.Context(), key).Result()
	require.NoError(t, err)
	require.Zero(t, exists, "a confirmed reservation must be released when execution never started")

	r = testRouter(t, flags, s, team, time.Minute, handler)
	retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusCreated, retry.Code, retry.Body.String())
	require.Equal(t, 1, calls)
}

type cleanupObservation struct {
	contextErr      error
	requestValue    any
	remaining       time.Duration
	afterCompletion bool
	err             error
}

type requestContextKey struct{}

type gatedReservationClient struct {
	redis.UniversalClient

	started      chan struct{}
	allowWrite   chan struct{}
	written      chan struct{}
	allowReturn  chan struct{}
	reserveErr   error
	reserveReply []any
	returned     atomic.Bool
	releases     chan cleanupObservation
}

func (c *gatedReservationClient) EvalSha(ctx context.Context, sha string, keys []string, args ...any) *redis.Cmd {
	if sha == reserveScript.Hash() {
		close(c.started)
		<-c.allowWrite
		// Model a command already in flight whose socket I/O ignores caller cancellation.
		cmd := c.UniversalClient.EvalSha(context.WithoutCancel(ctx), sha, keys, args...)
		close(c.written)
		<-c.allowReturn
		if c.reserveErr != nil {
			cmd.SetErr(c.reserveErr)
		}
		if c.reserveReply != nil {
			cmd.SetVal(c.reserveReply)
		}
		c.returned.Store(true)

		return cmd
	}
	if sha == releaseScript.Hash() {
		deadline, _ := ctx.Deadline()
		observation := cleanupObservation{
			contextErr:      ctx.Err(),
			requestValue:    ctx.Value(requestContextKey{}),
			remaining:       time.Until(deadline),
			afterCompletion: c.returned.Load(),
		}
		cmd := c.UniversalClient.EvalSha(ctx, sha, keys, args...)
		observation.err = cmd.Err()
		c.releases <- observation

		return cmd
	}

	return c.UniversalClient.EvalSha(ctx, sha, keys, args...)
}

func TestLateReservationCleanupWaitsForRedis(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	require.NoError(t, reserveScript.Load(t.Context(), client).Err())
	require.NoError(t, releaseScript.Load(t.Context(), client).Err())
	for _, tc := range []struct {
		name         string
		replaceOwner bool
		reserveErr   error
	}{
		{name: "release unused reservation"},
		{name: "preserve replacement owner", replaceOwner: true},
		{name: "release lost acknowledgement", reserveErr: errors.New("reservation acknowledgement lost")},
		{name: "lost acknowledgement preserves replacement", replaceOwner: true, reserveErr: errors.New("reservation acknowledgement lost")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gated := &gatedReservationClient{
				UniversalClient: client,
				started:         make(chan struct{}),
				allowWrite:      make(chan struct{}),
				written:         make(chan struct{}),
				allowReturn:     make(chan struct{}),
				reserveErr:      tc.reserveErr,
				releases:        make(chan cleanupObservation, 2),
			}
			write := sync.OnceFunc(func() { close(gated.allowWrite) })
			finish := sync.OnceFunc(func() { close(gated.allowReturn) })
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), requestContextKey{}, "request"))
			requestDone := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				write()
				finish()
				select {
				case <-requestDone:
				case <-time.After(5 * time.Second):
					t.Error("cancelled request did not return")
				}
			})
			flags, _ := testFlags(t, false, 86400)
			team := uuid.New()
			key := recordKey(team.String(), "key")
			var calls atomic.Int64
			handler := func(c *gin.Context) {
				calls.Add(1)
				c.Status(http.StatusCreated)
			}
			r := testRouter(t, flags, &redisStore{client: gated}, team, time.Minute, handler)
			responses := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				defer close(requestDone)
				responses <- request(ctx, r, "/v2/sandboxes", validBody, []string{"key"})
			}()
			select {
			case <-gated.started:
			case <-time.After(5 * time.Second):
				t.Fatal("reservation did not start")
			}
			cancel()
			select {
			case response := <-responses:
				require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
			case <-time.After(2 * time.Second):
				t.Fatal("request waited for the outstanding reservation")
			}
			require.Zero(t, calls.Load())
			require.Zero(t, client.Exists(t.Context(), key).Val())
			write()
			select {
			case <-gated.written:
			case <-time.After(5 * time.Second):
				t.Fatal("reservation did not reach Redis")
			}
			reserved, err := client.HGetAll(t.Context(), key).Result()
			require.NoError(t, err)
			require.NotEmpty(t, reserved["owner"])
			select {
			case <-gated.releases:
				t.Fatal("cleanup ran before the reservation call returned")
			default:
			}
			var replacement map[string]string
			var replacementExpiry time.Duration
			if tc.replaceOwner {
				require.NoError(t, client.PExpire(t.Context(), key, time.Millisecond).Err())
				require.Eventually(t, func() bool { return client.Exists(t.Context(), key).Val() == 0 }, time.Second, time.Millisecond)
				newStore := &redisStore{client: client}
				result, err := newStore.reserve(t.Context(), key, reserved["fingerprint"], "new-owner", 86400, time.Now().Add(time.Minute))
				require.NoError(t, err)
				require.Equal(t, stateOwner, result.state)
				replacement, err = client.HGetAll(t.Context(), key).Result()
				require.NoError(t, err)
				replacementExpiry, err = client.PExpireTime(t.Context(), key).Result()
				require.NoError(t, err)
			}
			finish()
			select {
			case observation := <-gated.releases:
				require.True(t, observation.afterCompletion)
				require.Equal(t, "request", observation.requestValue)
				require.NoError(t, observation.contextErr)
				require.NoError(t, observation.err)
				require.Positive(t, observation.remaining)
				require.LessOrEqual(t, observation.remaining, reservationCleanupTimeout)
			case <-time.After(5 * time.Second):
				t.Fatal("unused late reservation was not released")
			}
			if tc.replaceOwner {
				after, err := client.HGetAll(t.Context(), key).Result()
				require.NoError(t, err)
				require.Equal(t, replacement, after)
				expiresAt, err := client.PExpireTime(t.Context(), key).Result()
				require.NoError(t, err)
				require.Equal(t, replacementExpiry, expiresAt)
			} else {
				require.Zero(t, client.Exists(t.Context(), key).Val())
				r = testRouter(t, flags, &redisStore{client: client}, team, time.Minute, handler)
				retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
				require.Equal(t, http.StatusCreated, retry.Code, retry.Body.String())
				require.EqualValues(t, 1, calls.Load())
			}
		})
	}
}

func TestReservationReplyFailureReleasesUnusedKey(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	require.NoError(t, reserveScript.Load(t.Context(), client).Err())
	require.NoError(t, releaseScript.Load(t.Context(), client).Err())
	for _, tc := range []struct {
		name  string
		err   error
		reply []any
	}{
		{name: "lost acknowledgement", err: errors.New("connection reset after reservation")},
		{name: "read timeout", err: context.DeadlineExceeded},
		{name: "invalid response", reply: []any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ready := make(chan struct{})
			close(ready)
			gated := &gatedReservationClient{
				UniversalClient: client,
				started:         make(chan struct{}),
				allowWrite:      ready,
				written:         make(chan struct{}),
				allowReturn:     ready,
				reserveErr:      tc.err,
				reserveReply:    tc.reply,
				releases:        make(chan cleanupObservation, 2),
			}
			flags, _ := testFlags(t, false, 86400)
			team := uuid.New()
			key := recordKey(team.String(), "key")
			calls := 0
			handler := func(c *gin.Context) {
				calls++
				c.Status(http.StatusCreated)
			}
			r := testRouter(t, flags, &redisStore{client: gated}, team, time.Minute, handler)
			first := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
			require.Equal(t, http.StatusServiceUnavailable, first.Code, first.Body.String())
			require.Zero(t, calls)
			exists, err := client.Exists(t.Context(), key).Result()
			require.NoError(t, err)
			require.Zero(t, exists, "a failed reservation reply must not strand an unused key")
			select {
			case observation := <-gated.releases:
				require.True(t, observation.afterCompletion)
				require.NoError(t, observation.contextErr)
				require.NoError(t, observation.err)
				require.Positive(t, observation.remaining)
				require.LessOrEqual(t, observation.remaining, reservationCleanupTimeout)
			default:
				t.Fatal("reservation error returned without cleanup")
			}
			r = testRouter(t, flags, &redisStore{client: client}, team, time.Minute, handler)
			retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
			require.Equal(t, http.StatusCreated, retry.Code, retry.Body.String())
			require.Equal(t, 1, calls)
		})
	}
}

func TestRedisReleaseProtectsOtherReservations(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	for _, state := range []string{"missing", "pending", "different owner", "complete", "empty response"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			key := recordKey(uuid.NewString(), "key")
			if state != "missing" {
				_, err := s.reserve(t.Context(), key, "hash", "owner", 86400, time.Now().Add(time.Minute))
				require.NoError(t, err)
			}
			if state == "complete" {
				require.NoError(t, s.complete(t.Context(), key, "owner", cachedResponse{Status: http.StatusCreated}))
			}
			if state == "empty response" {
				require.NoError(t, client.HSet(t.Context(), key, "response", "").Err())
			}
			before, err := client.HGetAll(t.Context(), key).Result()
			require.NoError(t, err)
			expiresAt, err := client.PExpireTime(t.Context(), key).Result()
			require.NoError(t, err)
			owner := "owner"
			if state == "different owner" {
				owner = "someone-else"
			}
			require.NoError(t, s.release(t.Context(), key, owner))
			if state == "pending" || state == "missing" {
				require.Zero(t, client.Exists(t.Context(), key).Val())
			} else {
				after, err := client.HGetAll(t.Context(), key).Result()
				require.NoError(t, err)
				require.Equal(t, before, after)
				afterExpiry, err := client.PExpireTime(t.Context(), key).Result()
				require.NoError(t, err)
				require.Equal(t, expiresAt, afterExpiry)
			}
		})
	}
}

func TestCancellationAfterExecutionStartsKeepsResponse(t *testing.T) {
	t.Parallel()
	client := redisutils.SetupInstance(t)
	s := &redisStore{client: client}
	flags, _ := testFlags(t, false, 86400)
	team := uuid.New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	r := testRouter(t, flags, s, team, time.Minute, func(c *gin.Context) {
		calls++
		cancel()
		c.Data(http.StatusCreated, "application/json", []byte(`{"sandboxID":"original"}`))
	})
	original := request(ctx, r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, http.StatusCreated, original.Code, original.Body.String())
	retry := request(t.Context(), r, "/v2/sandboxes", validBody, []string{"key"})
	require.Equal(t, original.Code, retry.Code)
	require.Equal(t, original.Body.String(), retry.Body.String())
	require.Equal(t, 1, calls)
}
