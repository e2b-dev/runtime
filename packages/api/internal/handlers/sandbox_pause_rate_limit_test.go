package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/pauselimit"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// pauseRateLimitBackend serves one sandbox record and pauses it, refusing a
// pin that names another execution as StartRemoving does.
type pauseRateLimitBackend struct {
	mu        sync.Mutex
	record    *sandbox.Sandbox
	lookupErr error
	// resumes replace the record, one after each lookup: a resume landing
	// between the limiter check and the pause.
	resumes []string
	paused  int
	// removeErr answers every pause that passes the pin.
	removeErr error
}

func (b *pauseRateLimitBackend) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lookupErr != nil {
		return sandbox.Sandbox{}, b.lookupErr
	}
	record := b.record
	if len(b.resumes) > 0 {
		b.record = &sandbox.Sandbox{ExecutionID: b.resumes[0], State: sandbox.StateRunning}
		b.resumes = b.resumes[1:]
	}
	if record == nil {
		return sandbox.Sandbox{}, sandbox.ErrNotFound
	}

	return *record, nil
}

func (b *pauseRateLimitBackend) RemoveSandbox(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if opts.ExpectExecutionID != "" && b.record != nil && b.record.ExecutionID != opts.ExpectExecutionID {
		return orchestrator.ErrExecutionChanged
	}
	if b.removeErr != nil {
		return b.removeErr
	}
	b.paused++

	return nil
}

func (b *pauseRateLimitBackend) pausedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.paused
}

func runningExecution(executionID string) *pauseRateLimitBackend {
	return &pauseRateLimitBackend{record: &sandbox.Sandbox{ExecutionID: executionID, State: sandbox.StateRunning}}
}

func newPauseLimiter(t *testing.T, client redis.UniversalClient, flagValue any) *pauselimit.Limiter {
	t.Helper()

	td := ldtestdata.DataSource()
	if flagValue != nil {
		td.Update(td.Flag(featureflags.SandboxPauseRateLimitFlag.Key()).ValueForAll(ldvalue.CopyArbitraryValue(flagValue)))
	}
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	limiter, err := pauselimit.New(client, ff, noop.NewMeterProvider())
	require.NoError(t, err)

	return limiter
}

func postLimitedPause(t *testing.T, limiter *pauselimit.Limiter, backend *pauseRateLimitBackend, sandboxID string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes/"+sandboxID+"/pause", nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: uuid.New(), Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store := &APIStore{pauseLimiter: limiter, pauseBackendOverride: backend}
	store.PostSandboxesSandboxIDPause(ginCtx, sandboxID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func TestPauseRateLimit(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	twoPerMinute := map[string]int{"limit": 2, "period_s": 60}
	onePerMinute := map[string]int{"limit": 1, "period_s": 60}

	t.Run("pauses beyond the limit are refused before the sandbox is touched", func(t *testing.T) {
		t.Parallel()

		limiter := newPauseLimiter(t, client, twoPerMinute)
		sandboxID := "i" + id.Generate()

		for _, executionID := range []string{"first", "second"} {
			assert.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, runningExecution(executionID), sandboxID).Code)
		}

		refused := runningExecution("third")
		recorder := postLimitedPause(t, limiter, refused, sandboxID)

		assert.Equal(t, http.StatusTooManyRequests, recorder.Code)
		assert.Zero(t, refused.pausedCount())
		retryAfter, err := strconv.Atoi(recorder.Header().Get("Retry-After"))
		require.NoError(t, err)
		assert.InDelta(t, 30, retryAfter, 1, "two pauses a minute refill one slot every 30 seconds")

		other := runningExecution("third")
		assert.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, other, "i"+id.Generate()).Code, "each sandbox has its own budget")
	})

	t.Run("concurrent pauses of one execution use one slot", func(t *testing.T) {
		t.Parallel()

		limiter := newPauseLimiter(t, client, onePerMinute)
		sandboxID := "i" + id.Generate()
		backend := runningExecution("first")

		var wg sync.WaitGroup
		codes := make([]int, 8)
		for i := range codes {
			wg.Go(func() { codes[i] = postLimitedPause(t, limiter, backend, sandboxID).Code })
		}
		wg.Wait()

		for _, code := range codes {
			assert.Equal(t, http.StatusNoContent, code)
		}
		for range 2 {
			assert.Equal(t, http.StatusTooManyRequests, postLimitedPause(t, limiter, runningExecution("second"), sandboxID).Code)
		}
	})

	t.Run("lookups a new pause cannot start from use no slot", func(t *testing.T) {
		t.Parallel()

		uncounted := map[string]*pauseRateLimitBackend{
			"lookup failed": {lookupErr: errors.New("redis timeout")},
			"pausing":       {record: &sandbox.Sandbox{ExecutionID: "pausing", State: sandbox.StatePausing}},
			"killing":       {record: &sandbox.Sandbox{ExecutionID: "killing", State: sandbox.StateKilling}},
		}
		for name, backend := range uncounted {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				limiter := newPauseLimiter(t, client, onePerMinute)
				sandboxID := "i" + id.Generate()

				assert.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, backend, sandboxID).Code)
				assert.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, runningExecution("running"), sandboxID).Code)
			})
		}
	})

	// After the first execution used the only slot, a resume lands between a
	// request's check and its pause: the request must not pause the new
	// execution without counting it.
	t.Run("a resume between the check and the pause is checked again", func(t *testing.T) {
		t.Parallel()

		lookups := map[string]*sandbox.Sandbox{
			"deduplicated with the counted pause": {ExecutionID: "first", State: sandbox.StateRunning},
			"joining the counted pause":           {ExecutionID: "first", State: sandbox.StatePausing},
			"missing after the counted pause":     nil,
		}
		for name, seen := range lookups {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				limiter := newPauseLimiter(t, client, onePerMinute)
				sandboxID := "i" + id.Generate()
				require.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, runningExecution("first"), sandboxID).Code)

				raced := &pauseRateLimitBackend{record: seen, resumes: []string{"second"}}

				assert.Equal(t, http.StatusTooManyRequests, postLimitedPause(t, limiter, raced, sandboxID).Code)
				assert.Zero(t, raced.pausedCount())
			})
		}
	})

	t.Run("an execution replaced on every attempt is a conflict", func(t *testing.T) {
		t.Parallel()

		limiter := newPauseLimiter(t, client, map[string]int{"limit": 10, "period_s": 60})
		backend := runningExecution("first")
		backend.resumes = []string{"second", "third"}

		assert.Equal(t, http.StatusConflict, postLimitedPause(t, limiter, backend, "i"+id.Generate()).Code)
		assert.Zero(t, backend.pausedCount())
	})

	// The SDKs read a 409 from pause as already paused and return without an
	// error, which would hide that the sandbox was restored and still runs.
	t.Run("a filesystem-only pause the guest refuses is a 400, not a 409", func(t *testing.T) {
		t.Parallel()

		limiter := newPauseLimiter(t, client, nil)
		backend := runningExecution("first")
		backend.removeErr = orchestrator.PauseUnsupportedError{}

		recorder := postLimitedPause(t, limiter, backend, "i"+id.Generate())
		assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assert.Contains(t, recorder.Body.String(), "pause it with memory instead")
	})

	t.Run("an unset flag leaves pauses unlimited", func(t *testing.T) {
		t.Parallel()

		limiter := newPauseLimiter(t, client, nil)
		sandboxID := "i" + id.Generate()

		for i := range 3 {
			assert.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, runningExecution(strconv.Itoa(i)), sandboxID).Code)
		}
	})

	t.Run("an unavailable limiter lets the pause through", func(t *testing.T) {
		t.Parallel()

		unreachable := redis.NewClient(&redis.Options{Addr: "localhost:1"})
		t.Cleanup(func() { _ = unreachable.Close() })
		limiter := newPauseLimiter(t, unreachable, onePerMinute)
		backend := runningExecution("first")

		assert.Equal(t, http.StatusNoContent, postLimitedPause(t, limiter, backend, "i"+id.Generate()).Code)
		assert.Equal(t, 1, backend.pausedCount())
	})
}
