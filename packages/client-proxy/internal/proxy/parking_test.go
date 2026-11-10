package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var errStillTransitioning = status.Error(codes.FailedPrecondition, proxygrpc.SandboxStillTransitioningMessage)

// sequenceResumer answers its calls with the listed errors in order, then with
// the node IP.
type sequenceResumer struct {
	errs  []error
	route *catalog.SandboxInfo
	calls atomic.Int32
}

func (s *sequenceResumer) Resume(_ context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	call := int(s.calls.Add(1)) - 1
	if call < len(s.errs) {
		return nil, s.errs[call]
	}

	return s.route, nil
}

func newFFWithParking(t *testing.T, enabled bool, budgetMs int, lotSize int) *featureflags.Client {
	t.Helper()

	ff, _ := newFFWithParkingSource(t, enabled, budgetMs, lotSize)

	return ff
}

// newFFWithParkingSource also returns the data source, so a test can flip
// the parking flag while a flight is in progress.
func newFFWithParkingSource(t *testing.T, enabled bool, budgetMs int, lotSize int) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ClientProxyResumeParkingFlag.Key()).VariationForAll(enabled))
	source.Update(source.Flag(featureflags.ClientProxyResumeParkingBudgetMs.Key()).ValueForAll(ldvalue.Int(budgetMs)))
	source.Update(source.Flag(featureflags.ClientProxyResumeParkingLotSize.Key()).ValueForAll(ldvalue.Int(lotSize)))

	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff, source
}

func (m metricReader) parkingWaitSamples(t *testing.T, outcomes ...string) uint64 {
	t.Helper()

	var samples uint64
	for _, scope := range m.collect(t).ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != string(telemetry.ClientProxyResumeParkingWaitDurationName) {
				continue
			}

			histogram, ok := metric.Data.(metricdata.Histogram[int64])
			require.True(t, ok, "%s is not a histogram", metric.Name)
			for _, point := range histogram.DataPoints {
				outcome, _ := point.Attributes.Value("outcome")
				if len(outcomes) == 0 || outcome.AsString() == outcomes[0] {
					samples += point.Count
				}
			}
		}
	}

	return samples
}

func (m metricReader) parkingRejected(t *testing.T) int64 {
	t.Helper()

	return m.counter(t, string(telemetry.ClientProxyResumeParkingRejectedCounterName))
}

func (m metricReader) parkingActive(t *testing.T) int64 {
	t.Helper()

	return m.counter(t, string(telemetry.ClientProxyResumeParkingActiveName))
}

type parkedProxy struct {
	metrics metricReader
	handler http.Handler
	closer  ResumeCloser
}

func newParkedProxy(t *testing.T, ff *featureflags.Client, resumer SandboxResumer) parkedProxy {
	t.Helper()

	metrics := newMetricReader(t)
	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))
	p, closer, err := NewClientProxy(metrics.provider, "parking-"+t.Name(), 0, 5007, c, resumer, ff, nil)
	require.NoError(t, err)

	return parkedProxy{metrics: metrics, handler: p.Handler, closer: closer}
}

// getInBackground serves one request on its own goroutine and delivers the
// recorder once the handler has written its answer.
func (p parkedProxy) getInBackground(t *testing.T) <-chan *httptest.ResponseRecorder {
	t.Helper()

	page := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		page <- p.get(t)
	}()

	return page
}

func (p parkedProxy) get(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()

	return p.getPort(t, uint64(consts.DefaultEnvdServerPort))
}

func (p parkedProxy) getPort(t *testing.T, port uint64) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("http://%d-sbx.e2b.app/", port), nil)
	rr := httptest.NewRecorder()
	p.handler.ServeHTTP(rr, req)

	return rr
}

// captureLogs replaces the global logger for the test and returns what was
// logged through it.
func captureLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()

	core, logs := observer.New(zap.DebugLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	return logs
}

func warningsAndAbove(logs *observer.ObservedLogs) []observer.LoggedEntry {
	return logs.Filter(func(entry observer.LoggedEntry) bool { return entry.Level >= zapcore.WarnLevel }).All()
}

// fakeClock advances only when a test tells it to, so a test's budget
// arithmetic follows simulated time rather than the runner's load.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// advancingSleep returns a sleep that moves the clock instead of waiting:
// by the asked duration, or by fixed when it is set.
func advancingSleep(clock *fakeClock, fixed time.Duration, sleeps *atomic.Int32) func(context.Context, time.Duration) bool {
	return func(_ context.Context, d time.Duration) bool {
		if sleeps != nil {
			sleeps.Add(1)
		}
		if fixed != 0 {
			d = fixed
		}
		clock.Advance(d)

		return true
	}
}

// A caller that leaves while parked is answered without noise: nothing is
// logged at warn or above, and its connection gets a 408 and no page.
//
//nolint:paralleltest // replaces the global logger to capture what the handler logs
func TestResumeFlights_CallerThatLeavesWhileParkedIsAnsweredQuietly(t *testing.T) {
	logs := captureLogs(t)
	p := newParkedProxy(t, newFFWithParking(t, true, 15000, 512), stillTransitioningForever())

	ctx, leave := context.WithCancel(t.Context())
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://49983-sbx.e2b.app/", nil)
	rr := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		defer close(served)
		p.handler.ServeHTTP(rr, req)
	}()
	require.Eventually(t, func() bool { return p.metrics.parkingActive(t) == 1 }, 5*time.Second, 5*time.Millisecond)

	leave()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the handler did not return after the caller left")
	}

	require.Empty(t, warningsAndAbove(logs))
	require.Equal(t, http.StatusRequestTimeout, rr.Code)
	require.NotContains(t, rr.Body.String(), "sbx", "a page was rendered for a caller that had left")
	require.EqualValues(t, 1, p.metrics.parkingWaitSamples(t, parkingOutcomeCanceled))
}

// placementStatus is the status the API sends for a 503 placement failure:
// Unavailable, with its semantic error code as an ErrorInfo detail.
func placementStatus(t *testing.T, code string) error {
	t.Helper()

	return placementStatusWithCode(t, codes.Unavailable, code)
}

// A permanent placement refusal (a feature no orchestrator supports) is not
// retried even with the flag on: one call, no parking, and the page says no
// node can run the sandbox, with the code in the body and no Retry-After. A
// capacity shortage and a "no compatible node" answer, which the API also
// gives for a node pool that is only briefly excluded, are still retried.
func TestResumeFlights_PermanentPlacementRefusalIsNotRetried(t *testing.T) {
	t.Parallel()

	t.Run(proxygrpc.PlacementFeatureUnsupported+" refused", func(t *testing.T) {
		t.Parallel()

		resumer := &sequenceResumer{errs: []error{placementStatus(t, proxygrpc.PlacementFeatureUnsupported), placementStatus(t, proxygrpc.PlacementFeatureUnsupported)}, route: testRoute}
		p := newParkedProxy(t, newFFWithParking(t, true, 15000, 512), resumer)

		rr := p.get(t)
		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
		require.Empty(t, rr.Header().Get("Retry-After"))
		require.Contains(t, rr.Body.String(), "no node in this cluster can run it")
		require.Contains(t, rr.Body.String(), `"error_code":"`+proxygrpc.PlacementFeatureUnsupported+`"`)
		require.NotContains(t, rr.Body.String(), "Failed to place sandbox")
		require.EqualValues(t, 1, resumer.calls.Load())
		require.Zero(t, p.metrics.parkingWaitSamples(t))
	})

	// A snapshot taken over an unresponsive guest agent is refused its memory
	// resume for good: the page is the API's 409 with the code, so the caller
	// learns to resume without memory, and no retry is spent on it.
	t.Run(proxygrpc.SnapshotGuestAgentUnresponsive+" refused", func(t *testing.T) {
		t.Parallel()

		refusal := placementStatusWithHTTPStatus(t, codes.AlreadyExists, proxygrpc.SnapshotGuestAgentUnresponsive, http.StatusConflict)
		resumer := &sequenceResumer{errs: []error{refusal, refusal}, route: testRoute}
		p := newParkedProxy(t, newFFWithParking(t, true, 15000, 512), resumer)

		rr := p.get(t)
		require.Equal(t, http.StatusConflict, rr.Code)
		require.Empty(t, rr.Header().Get("Retry-After"))
		require.Contains(t, rr.Body.String(), `"error_code":"`+proxygrpc.SnapshotGuestAgentUnresponsive+`"`)
		require.EqualValues(t, 1, resumer.calls.Load())
		require.Zero(t, p.metrics.parkingWaitSamples(t))
	})

	for _, code := range []string{proxygrpc.PlacementCapacityUnavailable, proxygrpc.PlacementNoCompatibleNode} {
		t.Run(code+" retried", func(t *testing.T) {
			t.Parallel()

			metrics := newMetricReader(t)
			resumer := &sequenceResumer{errs: []error{placementStatus(t, code)}, route: testRoute}
			flights := newTestResumeFlights(t, metrics, resumer)
			flights.featureFlags = newFFWithParking(t, true, 15000, 512)

			route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
			require.NoError(t, err)
			require.Same(t, testRoute, route)
			require.EqualValues(t, 2, resumer.calls.Load())
			require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
		})
	}
}

// A placement the API's carried code says to retry (a create that failed
// on the node, a placement timeout) is retried like a shortage, whatever
// gRPC code its HTTP status mapped to: the caller parks and the retry
// serves it. Without the code those statuses are terminal.
func TestResumeFlights_RetryablePlacementCodeParksAndRetries(t *testing.T) {
	t.Parallel()

	for name, failure := range map[string]error{
		"create failed as Internal":             placementStatusWithCode(t, codes.Internal, proxygrpc.PlacementCreateFailed),
		"placement timeout as DeadlineExceeded": placementStatusWithCode(t, codes.DeadlineExceeded, proxygrpc.PlacementTimeout),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			metrics := newMetricReader(t)
			resumer := &sequenceResumer{errs: []error{failure}, route: testRoute}
			flights := newTestResumeFlights(t, metrics, resumer)
			flights.featureFlags = newFFWithParking(t, true, 15000, 512)

			route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
			require.NoError(t, err)
			require.Same(t, testRoute, route)
			require.EqualValues(t, 2, resumer.calls.Load())
			require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
		})
	}

	t.Run("the same codes without the placement code are terminal", func(t *testing.T) {
		t.Parallel()

		require.False(t, retryableResumeError(status.Error(codes.Internal, "boom")))
		require.False(t, retryableResumeError(status.Error(codes.DeadlineExceeded, "orchestrator did not answer")))
		require.False(t, retryableResumeError(placementStatusWithCode(t, codes.Internal, proxygrpc.PlacementCreateRefused)))
	})
}

// panickingResumer panics inside the resume call, as a bug in the gRPC
// client or its instrumentation would.
type panickingResumer struct{}

func (panickingResumer) Resume(_ context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	panic("resumer bug")
}

// A panic inside the resumer ends that flight, not the process: the caller
// gets the capacity page, the registry is left clean and a later caller
// starts afresh.
//
//nolint:paralleltest // replaces the global logger to capture what the flight logs
func TestResumeFlights_PanicInTheResumerEndsTheFlightWithTheCapacityPage(t *testing.T) {
	logs := captureLogs(t)
	p := newParkedProxy(t, newFFWithParking(t, false, 15000, 512), panickingResumer{})

	rr := p.get(t)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Contains(t, rr.Body.String(), "cannot be resumed right now")
	require.NotEmpty(t, logs.FilterMessage("resume flight panicked").All())

	flights, ok := p.closer.(*resumeFlights)
	require.True(t, ok)
	require.Zero(t, flightsInRegistry(flights))
	require.Zero(t, detachedFlights(flights))
	require.NoError(t, flights.Close(t.Context()))
}

// A still-transitioning answer is retried once the flag is on, and the
// caller is served without ever seeing the 409.
func TestResumeFlights_StillTransitioningIsRetriedThenServed(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &sequenceResumer{errs: []error{errStillTransitioning}, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.NoError(t, err)
	require.Same(t, testRoute, route)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t))
	require.Zero(t, metrics.parkingActive(t))
	require.Zero(t, metrics.parkingRejected(t))
}

// exhaustingBudget is a budget the fake clock exhausts after exactly one
// retry whatever the jitter: the first backoff (at most 300 ms) fits and is
// slept as exhaustingStep, which leaves the floor plus 200 ms, so the retry
// starts; the second backoff (at least 300 ms) would eat the floor.
const (
	exhaustingBudget = 2500 * time.Millisecond
	exhaustingStep   = 300 * time.Millisecond
)

// onFakeClock moves the proxy's flights onto a fake clock whose backoff
// sleep waits for a waiter to park and then advances by step, so the attempt
// count follows the budget arithmetic rather than the runner's scheduling.
func (p parkedProxy) onFakeClock(t *testing.T, step time.Duration) {
	t.Helper()

	flights, ok := p.closer.(*resumeFlights)
	require.True(t, ok)
	clock := newFakeClock()
	flights.now = clock.Now
	flights.sleep = parkedThenAdvancing(flights, clock, step)
}

// Still transitioning until the budget runs out renders the 409 with
// Retry-After and counts the wait as budget_exhausted; the budget allows
// exactly one retry.
func TestResumeFlights_BudgetExhaustedRendersStillTransitioning(t *testing.T) {
	t.Parallel()

	resumer := stillTransitioningForever()
	p := newParkedProxy(t, newFFWithParking(t, true, int(exhaustingBudget.Milliseconds()), 512), resumer)
	p.onFakeClock(t, exhaustingStep)

	rr := p.get(t)
	// The envd port renders the still-transitioning page as 503.
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Equal(t, "2", rr.Header().Get("Retry-After"))
	require.Contains(t, rr.Body.String(), `still transitioning`)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, p.metrics.parkingWaitSamples(t, parkingOutcomeBudgetExhausted))
	require.EqualValues(t, 1, p.metrics.parkingWaitSamples(t))
	require.Zero(t, p.metrics.parkingActive(t))
}

// The same budget exhaustion on a user port keeps the 409 with Retry-After.
func TestResumeFlights_BudgetExhaustedRendersStillTransitioningOnAUserPort(t *testing.T) {
	t.Parallel()

	resumer := stillTransitioningForever()
	p := newParkedProxy(t, newFFWithParking(t, true, int(exhaustingBudget.Milliseconds()), 512), resumer)
	p.onFakeClock(t, exhaustingStep)

	rr := p.getPort(t, 3000)
	require.Equal(t, http.StatusConflict, rr.Code)
	require.Equal(t, "2", rr.Header().Get("Retry-After"))
	require.Contains(t, rr.Body.String(), `still transitioning`)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, p.metrics.parkingWaitSamples(t, parkingOutcomeBudgetExhausted))
}

// A non-retryable answer after a retried one is the final answer.
func TestResumeFlights_NotFoundAfterUnavailableWins(t *testing.T) {
	t.Parallel()

	resumer := &sequenceResumer{errs: []error{
		status.Error(codes.Unavailable, "no capacity"),
		status.Error(codes.NotFound, "snapshot not found"),
	}}
	p := newParkedProxy(t, newFFWithParking(t, true, 15000, 512), resumer)

	rr := p.get(t)
	require.Equal(t, http.StatusBadGateway, rr.Code)
	require.Contains(t, rr.Body.String(), `"message":"The sandbox was not found"`)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, p.metrics.parkingWaitSamples(t, parkingOutcomeError))
	require.EqualValues(t, 1, p.metrics.parkingWaitSamples(t))
}

// The team limit is never retried and never parks.
func TestResumeFlights_ResourceExhaustedIsNeverRetried(t *testing.T) {
	t.Parallel()

	resumer := &sequenceResumer{errs: []error{status.Error(codes.ResourceExhausted, "rate limit")}}
	p := newParkedProxy(t, newFFWithParking(t, true, 15000, 512), resumer)

	rr := p.get(t)
	require.Equal(t, http.StatusTooManyRequests, rr.Code)
	require.Contains(t, rr.Body.String(), `"message":"rate limit"`)
	require.EqualValues(t, 1, resumer.calls.Load())
	require.Zero(t, p.metrics.parkingWaitSamples(t))
	require.Zero(t, p.metrics.parkingRejected(t))
	require.Zero(t, p.metrics.parkingActive(t))
}

// A full lot sheds the caller at its park transition with the 503
// capacity page, no Retry-After, so clients that honour the header do
// not return in one 2 s wave, and the shed error_code, so the answer is
// told from a capacity 503 without the header.
func TestResumeFlights_FullLotShedsWithCapacityPage(t *testing.T) {
	t.Parallel()

	resumer := stillTransitioningForever()
	p := newParkedProxy(t, newFFWithParking(t, true, 15000, 0), resumer)

	rr := p.get(t)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Empty(t, rr.Header().Get("Retry-After"))
	require.Empty(t, rr.Header().Get("Access-Control-Expose-Headers"))
	require.Contains(t, rr.Body.String(), `cannot be resumed right now`)
	require.Contains(t, rr.Body.String(), `"error_code":"proxy_parking_lot_full"`)
	require.EqualValues(t, 1, p.metrics.parkingRejected(t))
	require.Zero(t, p.metrics.parkingWaitSamples(t))
	require.Zero(t, p.metrics.parkingActive(t))
}

// With the flag off the first retryable failure is today's answer.
func TestResumeFlights_FlagOffReturnsFirstFailure(t *testing.T) {
	t.Parallel()

	resumer := &sequenceResumer{errs: []error{errStillTransitioning}, route: testRoute}
	p := newParkedProxy(t, newFFWithParking(t, false, 15000, 512), resumer)

	rr := p.get(t)
	// The envd port: the SDK reads a 409 as "already exists", so a
	// sandbox still pausing answers the same page as 503 there.
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Equal(t, "2", rr.Header().Get("Retry-After"))
	require.EqualValues(t, 1, resumer.calls.Load())
	require.Zero(t, p.metrics.parkingWaitSamples(t))
	require.Zero(t, p.metrics.parkingRejected(t))
}

// On a user port a sandbox still pausing answers 409 with Retry-After; on
// the envd port it answers 503 with Retry-After, because the SDK maps an
// envd 409 to "already exists" and a make_dir would report success.
func TestHandlePausedSandbox_StillTransitioningByPort(t *testing.T) {
	t.Parallel()

	t.Run("user port", func(t *testing.T) {
		t.Parallel()

		flights := newTestResumeFlights(t, newMetricReader(t), &sequenceResumer{errs: []error{errStillTransitioning}, route: testRoute})
		flights.featureFlags = newFFWithParking(t, false, 15000, 512)

		_, res, err := handlePausedSandbox(t.Context(), "sbx", 3000, "token", "", flights)
		require.Equal(t, autoResumeErrored, res)
		var transitioning *reverseproxy.SandboxStillTransitioningError
		require.ErrorAs(t, err, &transitioning)
		require.False(t, transitioning.Unavailable)
	})

	t.Run("envd port", func(t *testing.T) {
		t.Parallel()

		flights := newTestResumeFlights(t, newMetricReader(t), &sequenceResumer{errs: []error{errStillTransitioning}, route: testRoute})
		flights.featureFlags = newFFWithParking(t, false, 15000, 512)

		_, res, err := handlePausedSandbox(t.Context(), "sbx", uint64(consts.DefaultEnvdServerPort), "token", "", flights)
		require.Equal(t, autoResumeErrored, res)
		var transitioning *reverseproxy.SandboxStillTransitioningError
		require.ErrorAs(t, err, &transitioning)
		require.True(t, transitioning.Unavailable)
	})
}

// The gRPC resumer wraps the status it received; the mapping must still see
// the message the API sent.
func TestHandlePausedSandbox_ReadsStatusThroughWrapping(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("grpc resume: %w", errStillTransitioning)
	_, res, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", stubResumer{err: wrapped})
	require.Equal(t, autoResumeErrored, res)

	var transitioningErr *reverseproxy.SandboxStillTransitioningError
	require.ErrorAs(t, err, &transitioningErr)

	wrapped = fmt.Errorf("grpc resume: %w", status.Error(codes.ResourceExhausted, "rate limit"))
	_, res, err = handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", stubResumer{err: wrapped})
	require.Equal(t, autoResumeResourceExhausted, res)

	var exhaustedErr *reverseproxy.SandboxResourceExhaustedError
	require.ErrorAs(t, err, &exhaustedErr)
	require.Equal(t, "rate limit", exhaustedErr.Message)
	require.True(t, retryableResumeError(fmt.Errorf("grpc resume: %w", errStillTransitioning)))
	require.False(t, retryableResumeError(fmt.Errorf("grpc resume: %w", status.Error(codes.FailedPrecondition, "other"))))
}

// A flight that has already finished when its caller reaches the park
// transition is served even though the lot is full.
func TestResumeFlights_ReadyResultIsServedWhenLotIsFull(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	flights := newTestResumeFlights(t, metrics, &recordingResumer{})
	flights.featureFlags = newFFWithParking(t, true, 15000, 1)
	require.True(t, flights.lot.tryEnter(1), "another sandbox's caller holds the only slot")

	f := &resumeFlight{done: make(chan struct{}), retrying: make(chan struct{}), route: testRoute}
	close(f.retrying)
	close(f.done)

	for range 100 {
		route, err := flights.await(t.Context(), f, resumeFlightRoleJoiner, "sbx")
		require.NoError(t, err)
		require.Same(t, testRoute, route)
	}

	require.Zero(t, metrics.parkingRejected(t))
	require.EqualValues(t, 100, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}

// The parking flag is read by every caller at its own park transition, and
// a joiner that may not park still waits for an attempt under way that is
// about to end: one arriving after the flag went off, while the leader's
// retry is within the wait bound of its ceiling, is served by that retry
// rather than answered with the first failure, however old, as if no
// retry were running. The ceiling is real and far; the clock the bound is
// measured on is the test's, moved to within the bound before the joiner
// arrives.
func TestResumeFlights_FlagOffJoinerWaitsForTheAttemptUnderWay(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.attemptCeiling = 10 * time.Second
	clock := newFakeClock()
	flights.now = clock.Now
	ff, source := newFFWithParkingSource(t, true, 15000, 512)
	flights.featureFlags = ff

	leaderResult := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 2 }, 5*time.Second, 5*time.Millisecond)

	clock.Advance(flights.attemptCeiling - flagOffWaitBound)
	source.Update(source.Flag(featureflags.ClientProxyResumeParkingFlag.Key()).VariationForAll(false))

	type pausedResult struct {
		route *catalog.SandboxInfo
		err   error
	}
	joinerResult := make(chan pausedResult, 1)
	go func() {
		route, _, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
		joinerResult <- pausedResult{route: route, err: err}
	}()
	// The joiner is attached and holds no lot slot while the retry runs.
	require.Eventually(t, func() bool { return waitersInRegistry(flights) == 2 }, 5*time.Second, 5*time.Millisecond)
	require.EqualValues(t, 1, metrics.parkingActive(t))

	close(resumer.release)
	joined := <-joinerResult
	require.NoError(t, joined.err)
	require.Same(t, testRoute, joined.route)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))

	led := <-leaderResult
	require.NoError(t, led.err)
	require.Same(t, testRoute, led.route)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
}

// An attempt under way with long to run is not waited for: a joiner whose
// flag is off takes the first failure at once, so turning the flag off
// still fails fast and holds no callers without a lot slot; the leader's
// retry goes on and serves the leader.
func TestResumeFlights_FlagOffJoinerDoesNotWaitForAnAttemptWithLongToRun(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.attemptCeiling = 10 * time.Second
	ff, source := newFFWithParkingSource(t, true, 15000, 512)
	flights.featureFlags = ff

	leaderResult := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 2 }, 5*time.Second, 5*time.Millisecond)

	source.Update(source.Flag(featureflags.ClientProxyResumeParkingFlag.Key()).VariationForAll(false))

	joinerResult := make(chan error, 1)
	go func() {
		_, _, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
		joinerResult <- err
	}()

	var err error
	select {
	case err = <-joinerResult:
	case <-time.After(5 * time.Second):
		close(resumer.release)
		require.FailNow(t, "the joiner waited for an attempt with long to run")
	}
	var transitioningErr *reverseproxy.SandboxStillTransitioningError
	require.ErrorAs(t, err, &transitioningErr)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))
	require.Equal(t, 1, waitersInRegistry(flights), "the joiner is still attached")

	close(resumer.release)
	led := <-leaderResult
	require.NoError(t, led.err)
	require.Same(t, testRoute, led.route)
	require.EqualValues(t, 2, resumer.calls.Load())
}

// With no attempt under way (the flight is in its backoff), a joiner whose
// flag is off is answered with the first failure at once, without waiting
// for a retry that it holds no slot for; the flight goes on for the parked
// leader.
func TestResumeFlights_FlagOffJoinerBetweenAttemptsTakesTheFirstFailureAtOnce(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := stillTransitioningForever()
	flights := newTestResumeFlights(t, metrics, resumer)
	ff, source := newFFWithParkingSource(t, true, 15000, 512)
	flights.featureFlags = ff
	sleeping := make(chan struct{})
	var sleepingOnce sync.Once
	flights.sleep = func(ctx context.Context, _ time.Duration) bool {
		sleepingOnce.Do(func() { close(sleeping) })
		<-ctx.Done()

		return false
	}

	leaderResult := resumeInBackground(flights, t.Context())
	<-sleeping
	require.Eventually(t, func() bool { return metrics.parkingActive(t) == 1 }, 5*time.Second, 5*time.Millisecond)

	source.Update(source.Flag(featureflags.ClientProxyResumeParkingFlag.Key()).VariationForAll(false))

	_, res, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
	require.Equal(t, autoResumeErrored, res)
	var transitioningErr *reverseproxy.SandboxStillTransitioningError
	require.ErrorAs(t, err, &transitioningErr)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))
	require.EqualValues(t, 1, resumer.calls.Load())
	require.Equal(t, 1, flightsInRegistry(flights), "the joiner's answer ended the leader's flight")

	require.NoError(t, flights.Close(t.Context()))
	led := <-leaderResult
	require.ErrorIs(t, led.err, errFlightClosed)
	require.ErrorIs(t, led.err, errStillTransitioning)
	require.EqualValues(t, 1, resumer.calls.Load())
}

// A flag-off joiner's wait is for the attempt it found under way, not for
// whichever attempt is under way when it wakes: when that attempt ends and
// the next begins before the joiner re-checks (the retry starts for a
// parked waiter as soon as the backoff is over), the joiner returns with
// the first failure instead of waiting through the next attempt too, over
// and over, without a lot slot. The end and the next begin are made one
// step here, as the race would have it.
func TestResumeFlights_FlagOffJoinerDoesNotWaitForTheNextAttempt(t *testing.T) {
	t.Parallel()

	flights := newTestResumeFlights(t, newMetricReader(t), &sequenceResumer{route: testRoute})
	flights.attemptCeiling = flagOffWaitBound // the joiner waits for an attempt ending within the bound
	f := &resumeFlight{done: make(chan struct{}), retrying: make(chan struct{}), changed: make(chan struct{}), waiters: 2, parked: 1}
	require.NoError(t, flights.beginAttempt(t.Context(), f, true, time.Now().Add(time.Minute)))

	joinerCtx := newAwaitingContext(t.Context())
	waited := make(chan bool, 1)
	go func() { waited <- flights.awaitAttempt(joinerCtx, f) }()
	select {
	case <-joinerCtx.awaiting: // the joiner holds the first attempt's number and is in its wait
	case ended := <-waited:
		require.FailNow(t, "the joiner did not wait for the attempt under way", "%v", ended)
	}

	flights.mu.Lock()
	// The number the joiner captured is the first attempt's: no other
	// attempt has begun since, and the joiner's select is reached only
	// after the capture (awaitingContext).
	require.Equal(t, 1, f.attempts)
	require.True(t, f.attempting)
	f.attempting = false
	f.notify()
	f.attempting = true
	f.attempts++
	flights.mu.Unlock()

	select {
	case ended := <-waited:
		require.True(t, ended)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the joiner waited for the next attempt")
	}
}

// A leader whose flag is off leaves with the first failure without ending
// the retries for a joiner whose flag is on: the joiner, attached before the
// failure, parks and is served by the retry. The first attempt is held until
// the joiner is in, since a flight ends with its last waiter.
func TestResumeFlights_LeaderFlagOffDoesNotEndTheRetriesForAJoinerFlagOn(t *testing.T) {
	t.Parallel()

	const callerKind ldcontext.Kind = "caller"

	metrics := newMetricReader(t)
	resumer := &heldFirstResumer{first: make(chan struct{}), second: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	ff, source := newFFWithParkingSource(t, false, 15000, 512)
	source.Update(source.Flag(featureflags.ClientProxyResumeParkingFlag.Key()).
		FallthroughVariation(false).
		VariationForKey(callerKind, "joiner", true))
	flights.featureFlags = ff

	leaderResult := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	joinerCtx := featureflags.AddToContext(t.Context(), ldcontext.NewWithKind(callerKind, "joiner"))
	joinerResult := resumeInBackground(flights, joinerCtx)
	require.Eventually(t, func() bool { return waitersInRegistry(flights) == 2 }, 5*time.Second, 5*time.Millisecond)

	close(resumer.first)
	led := <-leaderResult
	require.ErrorIs(t, led.err, errStillTransitioning)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultError))
	require.Equal(t, 1, flightsInRegistry(flights), "the leader's answer ended the flight")
	require.Eventually(t, func() bool { return metrics.parkingActive(t) == 1 }, 5*time.Second, 5*time.Millisecond)

	require.Eventually(t, func() bool { return resumer.calls.Load() == 2 }, 5*time.Second, 5*time.Millisecond)
	close(resumer.second)

	joined := <-joinerResult
	require.NoError(t, joined.err)
	require.Same(t, testRoute, joined.route)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}

// heldFirstResumer holds its first call until first is closed and then fails
// it with firstErr; it holds every later call until second is closed and
// then serves the node IP.
type heldFirstResumer struct {
	first    chan struct{}
	second   chan struct{}
	firstErr error
	route    *catalog.SandboxInfo
	calls    atomic.Int32
}

func (h *heldFirstResumer) Resume(ctx context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	gate, route, err := h.second, h.route, error(nil)
	if h.calls.Add(1) == 1 {
		gate, route, err = h.first, nil, h.firstErr
	}

	select {
	case <-gate:
		return route, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A budget shorter than the attempt floor is answered within the budget:
// no retry starts, and the hanging attempt the floor used to grant never runs.
func TestResumeFlights_ShortBudgetAnswersWithinTheBudget(t *testing.T) {
	t.Parallel()

	const budget = 300 * time.Millisecond

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, int(budget.Milliseconds()), 512)
	flights.attemptCeiling = 10 * time.Second
	clock := newFakeClock()
	flights.now = clock.Now
	var sleeps atomic.Int32
	flights.sleep = advancingSleep(clock, 0, &sleeps)

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)

	var exhausted *resumeBudgetExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.ErrorIs(t, err, errStillTransitioning)
	require.Zero(t, sleeps.Load(), "a backoff was slept for a budget below the floor")
	require.EqualValues(t, 1, resumer.calls.Load())
	require.Zero(t, metrics.parkingActive(t))
}

// A backoff that would leave less than the floor is not slept: the budget
// is reported exhausted at once, since the attempt after the sleep could
// not start. The budget leaves the floor plus less than the smallest
// jittered backoff, so no jitter value makes room.
func TestResumeFlights_BudgetIsExhaustedWithoutASleepThatWouldEatTheFloor(t *testing.T) {
	t.Parallel()

	budget := resumeRetryAttemptFloor + time.Duration(float64(resumeRetryInitialBackoff)*(1-resumeRetryBackoffJitter)) - 50*time.Millisecond

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, int(budget.Milliseconds()), 512)
	flights.attemptCeiling = 10 * time.Second
	var sleeps atomic.Int32
	flights.sleep = func(ctx context.Context, d time.Duration) bool {
		sleeps.Add(1)

		return sleepUnlessDone(ctx, d)
	}

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)

	var exhausted *resumeBudgetExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.ErrorIs(t, err, errStillTransitioning)
	require.Zero(t, sleeps.Load())
	require.EqualValues(t, 1, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultError))
}

// The floor is checked again once the backoff sleep is over: a sleep that
// ran longer than it was asked to leaves less than the floor, and no second
// attempt starts. The budget leaves room for the longest jittered backoff,
// so the sleep is taken; the clock is the test's own, moved past the
// budget's slack by the sleep, so the outcome does not depend on the host.
func TestResumeFlights_NoAttemptStartsBelowTheFloorAfterTheSleep(t *testing.T) {
	t.Parallel()

	slack := time.Duration(float64(resumeRetryInitialBackoff)*(1+resumeRetryBackoffJitter)) + 50*time.Millisecond
	budget := resumeRetryAttemptFloor + slack

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, int(budget.Milliseconds()), 512)
	flights.attemptCeiling = 10 * time.Second
	clock := newFakeClock()
	flights.now = clock.Now
	var sleeps atomic.Int32
	advance := advancingSleep(clock, slack+100*time.Millisecond, &sleeps)
	flights.sleep = func(ctx context.Context, d time.Duration) bool {
		// The caller parks during the backoff; the clock moves past the
		// slack once it has, so the re-check, not the wait for a parked
		// waiter, is what refuses the attempt.
		for deadline := time.Now().Add(5 * time.Second); parkedWaiters(flights) == 0 && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}

		return advance(ctx, d)
	}

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)

	var exhausted *resumeBudgetExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.ErrorIs(t, err, errStillTransitioning)
	require.EqualValues(t, 1, sleeps.Load())
	require.EqualValues(t, 1, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeBudgetExhausted))
}

// retryDeadlineResumer fails its first call retryably and answers every
// later one at once, recording how long the call's context had left.
type retryDeadlineResumer struct {
	firstErr error
	route    *catalog.SandboxInfo
	calls    atomic.Int32
	left     atomic.Int64 // nanoseconds the retry's context had left; -1 for none
}

func (d *retryDeadlineResumer) Resume(ctx context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	if d.calls.Add(1) == 1 {
		return nil, d.firstErr
	}

	left := int64(-1)
	if deadline, ok := ctx.Deadline(); ok {
		left = int64(time.Until(deadline))
	}
	d.left.Store(left)

	return d.route, nil
}

// A retry started with the floor of the budget left runs under the ceiling,
// not under the budget left, while its caller waits: the control plane sees
// the ceiling as its deadline, so a resume that takes longer than the 2 s
// left still serves the parked waiter instead of being cut mid-create, which
// no later retry could have survived either. The budget is on the fake
// clock (2.5 s, one 500 ms backoff slept, exactly the floor left at the
// retry); the deadline the retry carries is real.
func TestResumeFlights_RetryRunsUnderTheCeilingNotTheBudgetLeft(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &retryDeadlineResumer{firstErr: errStillTransitioning, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 2500, 512)
	flights.attemptCeiling = 10 * time.Second
	clock := newFakeClock()
	flights.now = clock.Now
	flights.sleep = parkedThenAdvancing(flights, clock, 500*time.Millisecond)

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.NoError(t, err)
	require.Same(t, testRoute, route)
	require.EqualValues(t, 2, resumer.calls.Load())
	left := time.Duration(resumer.left.Load())
	require.Greater(t, left, resumeRetryAttemptFloor, "the retry was capped at the budget left")
	require.LessOrEqual(t, left, flights.attemptCeiling)
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
	require.Zero(t, flightsInRegistry(flights))
}

// recordingAfter is an after whose timers the test fires: each call records
// the duration asked for and hands back a channel the test closes.
type recordingAfter struct {
	mu     sync.Mutex
	asked  []time.Duration
	timers []chan time.Time
}

func (a *recordingAfter) after(d time.Duration) (<-chan time.Time, func()) {
	a.mu.Lock()
	defer a.mu.Unlock()

	timer := make(chan time.Time)
	a.asked = append(a.asked, d)
	a.timers = append(a.timers, timer)

	return timer, func() {}
}

func (a *recordingAfter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.timers)
}

// fireLast fires the most recent timer and returns the duration it was
// asked for.
func (a *recordingAfter) fireLast() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()

	last := len(a.timers) - 1
	close(a.timers[last])

	return a.asked[last]
}

// A retry whose every caller has left is cut at the budget's end rather
// than run to the ceiling, so a callerless retry does not hold a detached
// slot for the ceiling; a caller that joins before the cut keeps the
// attempt, and is served by it. The cut's timer is the test's, and the
// backoff waits for the leader to park, so the retry starts without a
// floor timer of its own: the only timer recorded is the cut.
func TestResumeFlights_DetachedRetryIsCutAtTheBudgetsEnd(t *testing.T) {
	t.Parallel()

	newDetachedRetry := func(t *testing.T) (*resumeFlights, *gatedResumer, *recordingAfter, metricReader) {
		t.Helper()

		metrics := newMetricReader(t)
		resumer := &gatedResumer{release: make(chan struct{}), firstErr: errStillTransitioning, route: testRoute}
		flights := newTestResumeFlights(t, metrics, resumer)
		flights.featureFlags = newFFWithParking(t, true, 15000, 512)
		timers := &recordingAfter{}
		flights.after = timers.after
		clock := newFakeClock()
		flights.now = clock.Now
		flights.sleep = parkedThenAdvancing(flights, clock, 0)

		leaderCtx, leave := context.WithCancel(t.Context())
		leader := resumeInBackground(flights, leaderCtx)
		require.Eventually(t, func() bool { return resumer.calls.Load() == 2 }, 5*time.Second, 5*time.Millisecond)
		require.Zero(t, timers.count(), "a timer ran while the leader was still waiting")

		leave()
		require.ErrorIs(t, (<-leader).err, context.Canceled)
		require.Equal(t, 1, detachedFlights(flights))
		// The cut is armed for the budget left, and the attempt goes on.
		require.Eventually(t, func() bool { return timers.count() == 1 }, 5*time.Second, 5*time.Millisecond)
		require.EqualValues(t, 1, resumer.returned.Load(), "the retry was cut when the leader left")

		return flights, resumer, timers, metrics
	}

	t.Run("cut when nobody joined", func(t *testing.T) {
		t.Parallel()

		flights, resumer, timers, _ := newDetachedRetry(t)

		asked := timers.fireLast()
		require.Positive(t, asked)
		require.LessOrEqual(t, asked, 15*time.Second)
		require.Equal(t, 1, timers.count())
		require.Eventually(t, func() bool { return resumer.returned.Load() == 2 }, 5*time.Second, 5*time.Millisecond,
			"the callerless retry ran on past the budget")
		require.Eventually(t, func() bool { return flightsInRegistry(flights) == 0 }, 5*time.Second, 5*time.Millisecond)
		require.Zero(t, detachedFlights(flights))
		require.NoError(t, flights.Close(t.Context()))
		require.EqualValues(t, 2, resumer.calls.Load())
	})

	t.Run("kept when a caller joined", func(t *testing.T) {
		t.Parallel()

		flights, resumer, timers, metrics := newDetachedRetry(t)

		joiner := resumeInBackground(flights, t.Context())
		require.Eventually(t, func() bool { return waitersInRegistry(flights) == 1 }, 5*time.Second, 5*time.Millisecond)
		require.Zero(t, detachedFlights(flights))

		// The timer that fires after the join cuts nothing.
		timers.fireLast()
		close(resumer.release)
		joined := <-joiner
		require.NoError(t, joined.err)
		require.Same(t, testRoute, joined.route)
		require.EqualValues(t, 2, resumer.calls.Load())
		require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
	})
}

// parkedThenAdvancing is advancingSleep that first waits for a waiter to
// park, so the retry after the sleep finds one and the clock's position is
// what the test arranged, not what the runner's scheduling made of it.
func parkedThenAdvancing(flights *resumeFlights, clock *fakeClock, fixed time.Duration) func(context.Context, time.Duration) bool {
	advance := advancingSleep(clock, fixed, nil)

	return func(ctx context.Context, d time.Duration) bool {
		for deadline := time.Now().Add(5 * time.Second); parkedWaiters(flights) == 0 && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}

		return advance(ctx, d)
	}
}

// ceilingCutResumer fails retryably at once, lets the second attempt run
// into its ceiling, and serves the third at once.
type ceilingCutResumer struct {
	calls atomic.Int32
}

func (c *ceilingCutResumer) Resume(ctx context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	switch c.calls.Add(1) {
	case 1:
		return nil, errStillTransitioning
	case 2:
		<-ctx.Done()

		return nil, ctx.Err()
	default:
		return testRoute, nil
	}
}

// An attempt cut by the ceiling while budget remains is retried; it is not
// reported as the budget running out. The ceiling is real time, the budget
// is on the fake clock, and the stub alone decides which attempt the ceiling
// cuts and which one serves, so no step of the test races the flight's
// deadlines: a late test goroutine cannot let further attempts spend the
// budget before the serving one.
func TestResumeFlights_CeilingCutRetriesWhileBudgetRemains(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &ceilingCutResumer{}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 5000, 512)
	flights.attemptCeiling = 50 * time.Millisecond
	clock := newFakeClock()
	flights.now = clock.Now
	flights.sleep = parkedThenAdvancing(flights, clock, 0)

	served := <-resumeInBackground(flights, t.Context())
	require.NoError(t, served.err)
	require.Same(t, testRoute, served.route)
	require.EqualValues(t, 3, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
}

// deadlineResumer fails retryably once, then answers each attempt with the
// team limit only as the attempt's deadline expires.
type deadlineResumer struct {
	calls atomic.Int32
}

func (d *deadlineResumer) Resume(ctx context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	if d.calls.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "no capacity")
	}

	<-ctx.Done()

	return nil, status.Error(codes.ResourceExhausted, "rate limit")
}

// A status from the control plane that arrives as the attempt's deadline
// expires is the answer; the earlier retryable error does not replace it.
func TestResumeFlights_TerminalStatusRacingTheDeadlineWins(t *testing.T) {
	t.Parallel()

	resumer := &deadlineResumer{}
	flights := newTestResumeFlights(t, newMetricReader(t), resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)
	flights.attemptCeiling = 50 * time.Millisecond

	_, res, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
	require.Equal(t, autoResumeResourceExhausted, res)

	var exhaustedErr *reverseproxy.SandboxResourceExhaustedError
	require.ErrorAs(t, err, &exhaustedErr)
	require.Equal(t, "rate limit", exhaustedErr.Message)
	require.EqualValues(t, 2, resumer.calls.Load())
}

// carriedCodeAtDeadlineResumer answers its first call, only as the attempt's
// deadline expires, with the API's placement timeout (a 504 mapped to
// DeadlineExceeded with the code carried), then serves the node IP.
type carriedCodeAtDeadlineResumer struct {
	t     *testing.T
	calls atomic.Int32
}

func (d *carriedCodeAtDeadlineResumer) Resume(ctx context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	if d.calls.Add(1) > 1 {
		return testRoute, nil
	}

	<-ctx.Done()

	return nil, placementStatusWithCode(d.t, codes.DeadlineExceeded, proxygrpc.PlacementTimeout)
}

// A placement timeout the API sent as the attempt's deadline expired is the
// control plane's answer, told apart from the DeadlineExceeded the gRPC
// client makes of an ended context by the code only the API carries: the
// caller parks and the retry serves it, rather than the capacity page at
// once.
func TestResumeFlights_CarriedCodeRacingTheDeadlineParksAndRetries(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &carriedCodeAtDeadlineResumer{t: t}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)
	flights.attemptCeiling = 50 * time.Millisecond

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.NoError(t, err)
	require.Same(t, testRoute, route)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))
}

// A DeadlineExceeded the control plane sent while the attempt context was
// still alive is its answer, not a cut attempt, and is not retried.
func TestResumeFlights_RemoteDeadlineExceededIsNotRetried(t *testing.T) {
	t.Parallel()

	remote := status.Error(codes.DeadlineExceeded, "orchestrator did not answer")
	metrics := newMetricReader(t)
	resumer := &sequenceResumer{errs: []error{status.Error(codes.Unavailable, "no capacity"), remote}, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)
	require.ErrorIs(t, err, remote)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeError))
	require.Zero(t, flightsInRegistry(flights))
}

func stillTransitioningForever() *sequenceResumer {
	resumer := &sequenceResumer{errs: make([]error, 1000)}
	for i := range resumer.errs {
		resumer.errs[i] = errStillTransitioning
	}

	return resumer
}

func flightsInRegistry(flights *resumeFlights) int {
	flights.mu.Lock()
	defer flights.mu.Unlock()

	return len(flights.flights)
}

func parkedWaiters(flights *resumeFlights) int {
	flights.mu.Lock()
	defer flights.mu.Unlock()

	parked := 0
	for _, f := range flights.flights {
		parked += f.parked
	}

	return parked
}

func detachedFlights(flights *resumeFlights) int {
	flights.mu.Lock()
	defer flights.mu.Unlock()

	return flights.detached
}

func waitersInRegistry(flights *resumeFlights) int {
	flights.mu.Lock()
	defer flights.mu.Unlock()

	waiters := 0
	for _, f := range flights.flights {
		waiters += f.waiters
	}

	return waiters
}

// A flight whose every waiter has left stops before its next attempt and
// leaves the registry.
func TestResumeFlights_StopsWhenEveryWaiterLeavesDuringBackoff(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := stillTransitioningForever()
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	callerCtx, leave := context.WithCancel(t.Context())
	result := resumeInBackground(flights, callerCtx)
	require.Eventually(t, func() bool { return metrics.parkingActive(t) == 1 }, 5*time.Second, 5*time.Millisecond)

	atLeave := resumer.calls.Load()
	leave()
	require.ErrorIs(t, (<-result).err, context.Canceled)
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeCanceled))

	// The last detach between attempts unregisters and cancels the flight;
	// Close waits for its goroutine, so the call count is final after it.
	// An attempt that was under way when the caller left may finish; no
	// further one starts.
	require.Eventually(t, func() bool { return flightsInRegistry(flights) == 0 }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, flights.Close(t.Context()))
	require.LessOrEqual(t, resumer.calls.Load(), atLeave+1, "a retry started after the only caller left")
}

// A caller whose context ended before it called Resume is answered with
// that error at once: it starts no flight, so the control plane is never
// asked to resume for a client that is gone, and nothing stays in the
// registry.
func TestResumeFlights_CallerAlreadyGoneAtArrivalStartsNoFlight(t *testing.T) {
	t.Parallel()

	const callers = 500

	metrics := newMetricReader(t)
	resumer := &sequenceResumer{route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	gone, cancel := context.WithCancel(t.Context())
	cancel()

	results := make([]<-chan resumeResult, 0, callers)
	for range callers {
		results = append(results, resumeInBackground(flights, gone))
	}
	for _, result := range results {
		r := <-result
		require.Nil(t, r.route)
		require.ErrorIs(t, r.err, context.Canceled)
	}

	require.Zero(t, resumer.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
	require.EqualValues(t, callers, metrics.flightCount(t, resumeFlightRoleNone, resumeFlightResultCanceled))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled), "a caller that started no flight counted as its leader")
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultCanceled))
	require.Zero(t, metrics.parkingWaitSamples(t))
}

// run, given a registered flight whose every waiter has left, makes no
// attempt and leaves the registry; given one with a waiter still attached,
// it makes the attempt. This is the unit of the attempt's own guard; the
// caller side is TestResumeFlights_CallerAlreadyGoneAtArrivalStartsNoFlight.
func TestResumeFlights_RunMakesNoAttemptForAFlightWithoutWaiters(t *testing.T) {
	t.Parallel()

	resumer := &sequenceResumer{route: testRoute}
	flights := newTestResumeFlights(t, newMetricReader(t), resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)
	key := newResumeFlightKey("sbx", 8000, "token", "")

	abandoned := &resumeFlight{key: key, done: make(chan struct{}), retrying: make(chan struct{}), cancel: func(error) {}, changed: make(chan struct{})}
	flights.flights[key] = abandoned
	flights.running.Add(1)
	flights.run(t.Context(), abandoned, 15*time.Second, "sbx", 8000, "token", "")
	<-abandoned.done
	require.ErrorIs(t, abandoned.err, errFlightAbandoned)
	require.Zero(t, resumer.calls.Load())
	require.Zero(t, flightsInRegistry(flights))

	attached := &resumeFlight{key: key, done: make(chan struct{}), retrying: make(chan struct{}), cancel: func(error) {}, waiters: 1, changed: make(chan struct{})}
	flights.flights[key] = attached
	flights.running.Add(1)
	flights.run(t.Context(), attached, 15*time.Second, "sbx", 8000, "token", "")
	<-attached.done
	require.NoError(t, attached.err)
	require.Same(t, testRoute, attached.route)
	require.EqualValues(t, 1, resumer.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
}

// The last waiter to leave does not end the attempt under way: it completes,
// a caller arriving meanwhile joins it and is served by the one call, and
// the flight leaves the registry once it is over. No retry starts for a
// flight whose waiters have all left.
func TestResumeFlights_LastWaiterLeavingLetsTheAttemptFinishForTheNextCaller(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, false, 15000, 512)

	callerCtx, leave := context.WithCancel(t.Context())
	result := resumeInBackground(flights, callerCtx)
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	leave()
	require.ErrorIs(t, (<-result).err, context.Canceled)
	require.Equal(t, 1, flightsInRegistry(flights), "the flight left the registry while its attempt was under way")
	require.Zero(t, resumer.returned.Load(), "the attempt was cut off when its only caller left")
	require.Equal(t, 1, detachedFlights(flights))

	later := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return waitersInRegistry(flights) == 1 }, 5*time.Second, 5*time.Millisecond)
	require.Zero(t, detachedFlights(flights), "a caller joined the flight but it still counts as detached")

	close(resumer.release)
	joined := <-later
	require.NoError(t, joined.err)
	require.Same(t, testRoute, joined.route)
	require.EqualValues(t, 1, resumer.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}

// Beyond the detached bound the last waiter's departure cancels the attempt
// under way and unregisters the flight, so callerless resumes cannot pile up
// past the bound; a later caller starts afresh.
func TestResumeFlights_LastWaiterLeavingCancelsTheAttemptBeyondTheDetachedBound(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, false, 15000, 512)
	flights.maxDetached = 0

	callerCtx, leave := context.WithCancel(t.Context())
	result := resumeInBackground(flights, callerCtx)
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	leave()
	require.ErrorIs(t, (<-result).err, context.Canceled)
	require.Eventually(t, func() bool { return resumer.returned.Load() == 1 }, 5*time.Second, 5*time.Millisecond,
		"the attempt kept waiting for the control plane after its only caller left")
	require.Eventually(t, func() bool { return flightsInRegistry(flights) == 0 }, 5*time.Second, 5*time.Millisecond)
	require.Zero(t, detachedFlights(flights))
	require.NoError(t, flights.Close(t.Context()))
	require.EqualValues(t, 1, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled))
	require.Zero(t, metrics.parkingWaitSamples(t))
}

// A waiter that is attached but has not parked holds no retry: after the
// first retryable failure the flight waits for a parked waiter, so a caller
// who decides against parking never has an attempt started on its behalf,
// however late its goroutine runs. The late waiter is attached by hand here
// and leaves through detach, as a caller would. The backoff sleep is the
// sync point: once it has run, only the wait for a parked waiter stands
// between the flight and its next attempt, and the departure ends the
// flight with a final call count.
func TestResumeFlights_NoRetryStartsWithoutAParkedWaiter(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := stillTransitioningForever()
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, false, 15000, 512)
	slept := make(chan struct{})
	var sleptOnce sync.Once
	flights.sleep = func(_ context.Context, _ time.Duration) bool {
		sleptOnce.Do(func() { close(slept) })

		return true
	}
	key := newResumeFlightKey("sbx", 8000, "token", "")

	ctx, cancel := context.WithCancelCause(context.WithoutCancel(t.Context()))
	late := &resumeFlight{key: key, done: make(chan struct{}), retrying: make(chan struct{}), cancel: cancel, waiters: 1, changed: make(chan struct{})}
	flights.mu.Lock()
	flights.flights[key] = late
	flights.running.Add(1)
	flights.mu.Unlock()
	go flights.run(ctx, late, 15*time.Second, "sbx", 8000, "token", "")

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)
	require.ErrorIs(t, err, errStillTransitioning)
	require.EqualValues(t, 1, resumer.calls.Load())

	<-slept
	require.Equal(t, 1, flightsInRegistry(flights))
	require.Equal(t, 1, waitersInRegistry(flights))

	// The flight is past its backoff with the late waiter still attached.
	// Its departure ends the flight; the call count is final once done
	// closes, so a retry that had started without a parked waiter would
	// show here as a second call.
	flights.detach(late)
	<-late.done
	require.ErrorIs(t, late.err, errFlightAbandoned)
	require.Zero(t, flightsInRegistry(flights))
	require.Zero(t, detachedFlights(flights))
	require.EqualValues(t, 1, resumer.calls.Load(), "a retry started without a parked waiter")
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))
}

// A retry starts only in the critical section that finds a parked waiter:
// with one attached waiter that has not decided and no parked one,
// beginAttempt waits and marks nothing; with a parked waiter it marks the
// attempt; with no waiter at all it ends the flight; a first attempt needs
// only an attached waiter.
func TestResumeFlights_RetryNeedsAParkedWaiterAtItsStart(t *testing.T) {
	t.Parallel()

	flights := newTestResumeFlights(t, newMetricReader(t), &sequenceResumer{route: testRoute})
	key := newResumeFlightKey("sbx", 8000, "token", "")
	f := &resumeFlight{key: key, done: make(chan struct{}), retrying: make(chan struct{}), cancel: func(error) {}, waiters: 1, changed: make(chan struct{})}
	flights.flights[key] = f

	ended, end := context.WithCancelCause(t.Context())
	waiting := make(chan error, 1)
	go func() { waiting <- flights.beginAttempt(ended, f, true, time.Now().Add(time.Minute)) }()
	select {
	case err := <-waiting:
		require.FailNow(t, "a retry started with an undecided waiter only", "%v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.False(t, f.attempting)

	flights.park(f)
	require.NoError(t, <-waiting)
	require.True(t, f.attempting)
	require.True(t, flights.endAttempt(f))
	flights.unpark(f)

	go func() { waiting <- flights.beginAttempt(ended, f, true, time.Now().Add(time.Minute)) }()
	end(errFlightAbandoned)
	require.ErrorIs(t, <-waiting, errFlightAbandoned)
	require.False(t, f.attempting)

	require.NoError(t, flights.beginAttempt(t.Context(), f, false, time.Time{}))
	require.True(t, flights.endAttempt(f))

	// A wait whose budget has less than the floor left ends as exhausted
	// instead of outliving it.
	require.ErrorIs(t, flights.beginAttempt(t.Context(), f, true, time.Now().Add(resumeRetryAttemptFloor/2)), errBudgetFloor)
	require.False(t, f.attempting)

	// A wait for a parked waiter that reaches the floor ends the same way;
	// the timer is the test's.
	floorReached := make(chan time.Time)
	flights.after = func(_ time.Duration) (<-chan time.Time, func()) { return floorReached, func() {} }
	go func() { waiting <- flights.beginAttempt(t.Context(), f, true, time.Now().Add(time.Minute)) }()
	close(floorReached)
	require.ErrorIs(t, <-waiting, errBudgetFloor)
	require.False(t, f.attempting)
	flights.after = timerAfter

	f.waiters = 0
	require.ErrorIs(t, flights.beginAttempt(t.Context(), f, true, time.Now().Add(time.Minute)), errFlightAbandoned)
	require.Zero(t, flightsInRegistry(flights))
}

// The parked waiter leaves during the backoff while an undecided waiter is
// still attached: the retry waits for a parked waiter that never comes, no
// second call is made, and the undecided waiter's departure ends the flight.
func TestResumeFlights_ParkedWaiterLeavingBeforeTheRetryStartsNoAttempt(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := stillTransitioningForever()
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)
	key := newResumeFlightKey("sbx", 8000, "token", "")

	parkedCtx, leave := context.WithCancel(t.Context())
	slept := make(chan struct{})
	var sleptOnce sync.Once
	var hookMissedSync atomic.Bool
	waitParked := func(want int) {
		for deadline := time.Now().Add(5 * time.Second); parkedWaiters(flights) != want; {
			if !time.Now().Before(deadline) {
				hookMissedSync.Store(true)

				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	flights.sleep = func(_ context.Context, _ time.Duration) bool {
		// The waiter parks while the flight sleeps and has left its
		// slot (read under the registry's mutex) before the sleep ends,
		// so the retry that follows finds no parked waiter.
		waitParked(1)
		leave()
		waitParked(0)
		sleptOnce.Do(func() { close(slept) })

		return true
	}

	ctx, cancel := context.WithCancelCause(context.WithoutCancel(t.Context()))
	undecided := &resumeFlight{key: key, done: make(chan struct{}), retrying: make(chan struct{}), cancel: cancel, waiters: 1, changed: make(chan struct{})}
	flights.mu.Lock()
	flights.flights[key] = undecided
	flights.running.Add(1)
	flights.mu.Unlock()
	go flights.run(ctx, undecided, 15*time.Second, "sbx", 8000, "token", "")

	parked := resumeInBackground(flights, parkedCtx)
	require.ErrorIs(t, (<-parked).err, context.Canceled)

	<-slept
	require.False(t, hookMissedSync.Load(), "the sleep hook did not see the waiter park and leave")
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeCanceled))
	require.Equal(t, 1, flightsInRegistry(flights))

	flights.detach(undecided)
	<-undecided.done
	require.ErrorIs(t, undecided.err, errFlightAbandoned)
	require.EqualValues(t, 1, resumer.calls.Load(), "a retry started after the only parked waiter had left")
	require.Zero(t, flightsInRegistry(flights))
}

// Close ends a parked flight without waiting its backoff out: the sleep
// under way is cut, its parked callers record a canceled wait and a
// canceled flight while keeping the last retryable answer, and a Resume
// afterwards is answered with the unavailable page without an attempt and
// counted as canceled too.
func TestResumeFlights_CloseEndsParkedFlights(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := stillTransitioningForever()
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)
	var sleepsCut atomic.Int32
	flights.sleep = func(ctx context.Context, d time.Duration) bool {
		slept := sleepUnlessDone(ctx, d)
		if !slept {
			sleepsCut.Add(1)
		}

		return slept
	}

	leader := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return metrics.parkingActive(t) == 1 }, 5*time.Second, 5*time.Millisecond)
	joiner := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return metrics.parkingActive(t) == 2 }, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, flights.Close(t.Context()))

	for _, result := range []<-chan resumeResult{leader, joiner} {
		select {
		case r := <-result:
			require.ErrorIs(t, r.err, errFlightClosed)
			require.ErrorIs(t, r.err, errStillTransitioning)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "the parked flight outlived Close")
		}
	}
	require.Zero(t, flightsInRegistry(flights))
	require.EqualValues(t, 1, sleepsCut.Load(), "Close waited the backoff out instead of cutting it")
	require.EqualValues(t, 2, metrics.parkingWaitSamples(t, parkingOutcomeCanceled))
	require.EqualValues(t, 2, metrics.parkingWaitSamples(t))
	require.Zero(t, metrics.parkingActive(t))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultCanceled))
	calls := resumer.calls.Load()

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)

	var capacityErr *reverseproxy.SandboxCapacityError
	require.ErrorAs(t, err, &capacityErr)
	require.Equal(t, calls, resumer.calls.Load())
	require.Zero(t, flightsInRegistry(flights))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleNone, resumeFlightResultCanceled))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled), "a caller rejected after Close counted as a leader")
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultError))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))
}

// A token the proxy could not obtain during a retry is a transient failure:
// the flight backs off and tries again within its budget, the next attempt
// serves, and the token error is logged at warn with the sandbox id so the
// real cause is on record.
//
//nolint:paralleltest // replaces the global logger to capture what the flight logs
func TestResumeFlights_TokenFailureDuringARetryIsRetried(t *testing.T) {
	logs := captureLogs(t)

	retryable := status.Error(codes.Unavailable, "no capacity")
	tokenErr := fmt.Errorf("%w: token endpoint down", errResumeToken)
	metrics := newMetricReader(t)
	resumer := &sequenceResumer{errs: []error{retryable, tokenErr}, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	route, err := flights.Resume(t.Context(), "sbx-token", 8000, "token", "")
	require.NoError(t, err)
	require.Same(t, testRoute, route)
	require.EqualValues(t, 3, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeServed))

	logged := logs.FilterField(logger.WithSandboxID("sbx-token")).FilterLevelExact(zapcore.WarnLevel).All()
	require.Len(t, logged, 1)
	require.Equal(t, "resume token unavailable, retrying", logged[0].Message)
	require.Equal(t, tokenErr.Error(), logged[0].ContextMap()["error"])
}

// Token failures that outlast the budget end the flight as budget
// exhaustion with the last retryable error as the answer, never with the
// token error. The budget leaves room for exactly one backoff: the first
// is slept whatever its jitter, the second would eat the floor.
func TestResumeFlights_TokenFailuresEndWithTheBudget(t *testing.T) {
	t.Parallel()

	retryable := status.Error(codes.Unavailable, "no capacity")
	tokenErr := fmt.Errorf("%w: token endpoint down", errResumeToken)
	metrics := newMetricReader(t)
	resumer := &sequenceResumer{errs: []error{retryable, tokenErr, tokenErr, tokenErr, tokenErr}, route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 2450, 512)
	clock := newFakeClock()
	flights.now = clock.Now
	flights.sleep = advancingSleep(clock, 0, nil)

	route, err := flights.Resume(t.Context(), "sbx-token", 8000, "token", "")
	require.Nil(t, route)
	var exhausted *resumeBudgetExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.ErrorIs(t, err, retryable)
	require.NotErrorIs(t, err, errResumeToken)
	require.EqualValues(t, 2, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.parkingWaitSamples(t, parkingOutcomeBudgetExhausted))
}

// statusGatedResumer is gatedResumer answering a cancelled attempt the way
// a gRPC client does, with a Canceled status instead of the context error.
type statusGatedResumer struct {
	gatedResumer
}

func (g *statusGatedResumer) Resume(ctx context.Context, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	route, err := g.gatedResumer.Resume(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
	if err != nil && ctx.Err() != nil {
		return nil, fmt.Errorf("grpc resume: %w", status.FromContextError(ctx.Err()).Err())
	}

	return route, err
}

type closedResumerCase struct {
	name    string
	resumer SandboxResumer
	calls   *atomic.Int32
}

// Close during a flight's first attempt renders the unavailable page, not
// the not-found page, whether the resumer reports the cancellation as the
// context error or as a gRPC status.
func TestResumeFlights_CloseDuringTheFirstAttemptRendersTheUnavailablePage(t *testing.T) {
	t.Parallel()

	plain := &gatedResumer{release: make(chan struct{}), route: testRoute}
	withStatus := &statusGatedResumer{gatedResumer{release: make(chan struct{}), route: testRoute}}
	tests := []closedResumerCase{
		{name: "context error", resumer: plain, calls: &plain.calls},
		{name: "grpc status", resumer: withStatus, calls: &withStatus.calls},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flights := newTestResumeFlights(t, newMetricReader(t), tt.resumer)
			flights.featureFlags = newFFWithParking(t, true, 15000, 512)

			result := make(chan error, 1)
			go func() {
				_, _, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
				result <- err
			}()
			require.Eventually(t, func() bool { return tt.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

			require.NoError(t, flights.Close(t.Context()))

			var capacityErr *reverseproxy.SandboxCapacityError
			require.ErrorAs(t, <-result, &capacityErr)
			require.Equal(t, "sbx", capacityErr.SandboxID())
			require.Zero(t, flightsInRegistry(flights))

			_, _, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
			require.ErrorAs(t, err, &capacityErr)
			require.EqualValues(t, 1, tt.calls.Load())
		})
	}
}

// Close during a flight's first attempt counts every waiter canceled on the
// flight counter, as a Close during a parked wait does; none of them is an
// error, and none parked.
func TestResumeFlights_CloseDuringTheFirstAttemptCountsEveryWaiterCanceled(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	leader := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
	joiner := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return waitersInRegistry(flights) == 2 }, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, flights.Close(t.Context()))

	for _, result := range []<-chan resumeResult{leader, joiner} {
		select {
		case r := <-result:
			var capacityErr *reverseproxy.SandboxCapacityError
			require.ErrorAs(t, r.err, &capacityErr)
			require.Equal(t, "sbx", capacityErr.SandboxID())
			require.ErrorIs(t, r.err, errFlightClosed)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "the flight outlived Close")
		}
	}
	require.Zero(t, flightsInRegistry(flights))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultCanceled))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultError))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))
	require.Zero(t, metrics.parkingWaitSamples(t))
}

// The request Close ends during the first attempt gets the capacity page, a
// 503 with Retry-After, and its flight counts canceled.
func TestClientProxy_CloseDuringTheFirstAttemptAnswersWithTheCapacityPage(t *testing.T) {
	t.Parallel()

	resumer := &gatedResumer{release: make(chan struct{}), route: testRoute}
	p := newParkedProxy(t, newFFWithParking(t, true, 15000, 512), resumer)

	page := p.getInBackground(t)
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, p.closer.Close(t.Context()))

	select {
	case rr := <-page:
		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
		require.Equal(t, "2", rr.Header().Get("Retry-After"))
		require.Contains(t, rr.Body.String(), `cannot be resumed right now`)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the request outlived Close")
	}
	require.EqualValues(t, 1, p.metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled))
	require.Zero(t, p.metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultError))
}

// deafResumer blocks every Resume call until release is closed, whatever
// its context does, so a flight Close canceled stays in the registry until
// the test lets it go.
type deafResumer struct {
	release chan struct{}
	route   *catalog.SandboxInfo
	calls   atomic.Int32
}

func (d *deafResumer) Resume(_ context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	d.calls.Add(1)
	<-d.release

	return d.route, nil
}

// A Resume rejected after Close counts as a canceled caller of no flight
// even when the registry still holds a flight for its key: it joined
// nothing, and it led nothing.
func TestResumeFlights_RejectionAfterCloseIsNotCountedAsAJoiner(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &deafResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	leader := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	// Close waits for the flight the resumer holds; the registry is closed
	// to new callers as soon as Close has marked it.
	closing := closeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return isClosed(flights) }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, flightsInRegistry(flights))

	route, err := flights.Resume(t.Context(), "sbx", 8000, "token", "")
	require.Nil(t, route)

	var capacityErr *reverseproxy.SandboxCapacityError
	require.ErrorAs(t, err, &capacityErr)
	require.EqualValues(t, 1, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleNone, resumeFlightResultCanceled))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled), "a caller rejected after Close counted as a leader")
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultCanceled))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultError))

	close(resumer.release)
	select {
	case r := <-leader:
		require.Same(t, testRoute, r.route)
		require.NoError(t, r.err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the leader outlived the release")
	}
	require.NoError(t, <-closing)
	require.Zero(t, flightsInRegistry(flights))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultServed))
	require.Zero(t, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}

func closeInBackground(flights *resumeFlights, ctx context.Context) <-chan error {
	closed := make(chan error, 1)
	go func() {
		closed <- flights.Close(ctx)
	}()

	return closed
}

func isClosed(flights *resumeFlights) bool {
	flights.mu.Lock()
	defer flights.mu.Unlock()

	return flights.closed
}

// Close returns only once a flight that was inside its attempt has
// published its answer: the feature flags and the resumer connection,
// closed after the flights, are not pulled from under a running flight.
func TestResumeFlights_CloseWaitsForAFlightInsideItsAttempt(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &deafResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	leader := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	closing := closeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return isClosed(flights) }, 5*time.Second, 5*time.Millisecond)
	select {
	case err := <-closing:
		require.FailNow(t, "Close returned while the flight was still inside its attempt", "%v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.Equal(t, 1, flightsInRegistry(flights))

	close(resumer.release)
	select {
	case err := <-closing:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Close outlived the release")
	}
	require.Zero(t, flightsInRegistry(flights))
	served := <-leader
	require.NoError(t, served.err)
	require.Same(t, testRoute, served.route)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultServed))
}

// Close stops waiting when its own context ends, reporting the flights it
// left running; the flight still publishes once the resumer answers.
func TestResumeFlights_CloseStopsWaitingWithItsContext(t *testing.T) {
	t.Parallel()

	resumer := &deafResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, newMetricReader(t), resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	leader := resumeInBackground(flights, t.Context())
	require.Eventually(t, func() bool { return resumer.calls.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	closeCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := flights.Close(closeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "resume flights still running")
	require.Equal(t, 1, flightsInRegistry(flights))

	close(resumer.release)
	select {
	case served := <-leader:
		require.NoError(t, served.err)
		require.Same(t, testRoute, served.route)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the leader outlived the release")
	}
	require.Zero(t, flightsInRegistry(flights))
}

func TestNewClientProxy_ReturnsResumeCloserOnlyWithResumer(t *testing.T) {
	t.Parallel()

	c := catalog.NewRedisSandboxCatalog(redis_utils.SetupInstance(t))

	_, closer, err := NewClientProxy(newMetricReader(t).provider, "closer-none", 0, 5007, c, nil, newFF(t), nil)
	require.NoError(t, err)
	require.Nil(t, closer)

	_, closer, err = NewClientProxy(newMetricReader(t).provider, "closer-some", 0, 5007, c, stubResumer{}, newFF(t), nil)
	require.NoError(t, err)
	require.NotNil(t, closer)
	require.NoError(t, closer.Close(t.Context()))
}
