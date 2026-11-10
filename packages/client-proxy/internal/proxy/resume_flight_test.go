package proxy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// testRoute is what the test resumers answer with: a full-router route with
// every placement field set and no orchestrator IP. Tests compare it by
// identity, so a flight that dropped or rebuilt the route fails them.
var testRoute = &catalog.SandboxInfo{
	ExecutionID:      "execution",
	NetworkPlacement: &catalog.NetworkPlacement{NetworkMode: catalog.NetworkModeFullRouter, RouterID: "router", IngressAddress: "10.8.0.2", IngressPort: 5010},
}

// gatedResumer blocks every Resume call until release is closed or the call's
// context ends, and counts the calls it received. With firstErr set the first
// call fails with it at once instead of waiting.
type gatedResumer struct {
	release  chan struct{}
	firstErr error
	route    *catalog.SandboxInfo
	err      error
	calls    atomic.Int32
	returned atomic.Int32 // calls that have ended, however they ended
}

func (g *gatedResumer) Resume(ctx context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	defer g.returned.Add(1)

	if g.calls.Add(1) == 1 && g.firstErr != nil {
		return nil, g.firstErr
	}

	select {
	case <-g.release:
		return g.route, g.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// awaitingContext reports, once, when a caller starts waiting on it, which the
// flight does only after it has attached to the registry and awaitAttempt only
// in its select, after it captured the attempt number and changed under the mutex.
type awaitingContext struct {
	context.Context //nolint:containedctx // the type is a context, wrapped to observe when it is waited on

	once     sync.Once
	awaiting chan struct{}
}

func newAwaitingContext(ctx context.Context) *awaitingContext {
	return &awaitingContext{Context: ctx, awaiting: make(chan struct{})}
}

func (c *awaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.awaiting) })

	return c.Context.Done()
}

type resumeResult struct {
	route *catalog.SandboxInfo
	err   error
}

// resumeInBackground calls Resume on its own goroutine and delivers the result
// on the returned channel, so the assertions stay on the test goroutine.
func resumeInBackground(flights *resumeFlights, ctx context.Context) <-chan resumeResult {
	return resumeInBackgroundWith(flights, ctx, 8000, "token", "")
}

func resumeInBackgroundWith(flights *resumeFlights, ctx context.Context, port uint64, trafficAccessToken string, envdAccessToken string) <-chan resumeResult {
	result := make(chan resumeResult, 1)
	go func() {
		route, err := flights.Resume(ctx, "sbx", port, trafficAccessToken, envdAccessToken)
		result <- resumeResult{route: route, err: err}
	}()

	return result
}

// tokenResumer blocks like gatedResumer and then answers by the traffic
// token: the valid one is served, any other is denied.
type tokenResumer struct {
	release    chan struct{}
	validToken string
	route      *catalog.SandboxInfo
	calls      atomic.Int32
}

func (g *tokenResumer) Resume(ctx context.Context, _ string, _ uint64, trafficAccessToken string, _ string) (*catalog.SandboxInfo, error) {
	g.calls.Add(1)

	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if trafficAccessToken != g.validToken {
		return nil, status.Error(codes.PermissionDenied, "invalid traffic access token")
	}

	return g.route, nil
}

type metricReader struct {
	provider *sdkmetric.MeterProvider
	meter    metric.Meter
	reader   *sdkmetric.ManualReader
}

func newMetricReader(t *testing.T) metricReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.WithoutCancel(t.Context())) })

	return metricReader{provider: provider, meter: provider.Meter("github.com/e2b-dev/infra/packages/client-proxy/internal/proxy"), reader: reader}
}

func (m metricReader) collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, m.reader.Collect(t.Context(), &collected))

	return collected
}

func (m metricReader) counter(t *testing.T, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()

	want := attribute.NewSet(attrs...)
	for _, scope := range m.collect(t).ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}

			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not a counter", name)
			for _, point := range sum.DataPoints {
				if point.Attributes.Equals(&want) {
					return point.Value
				}
			}
		}
	}

	return 0
}

func (m metricReader) flightCount(t *testing.T, role string, result string) int64 {
	t.Helper()

	return m.counter(t, string(telemetry.ClientProxyResumeFlightCounterName),
		attribute.String("role", role), attribute.String("result", result))
}

func newTestResumeFlights(t *testing.T, metrics metricReader, resumer SandboxResumer) *resumeFlights {
	t.Helper()

	flights, err := newResumeFlights(metrics.meter, resumer, newFF(t))
	require.NoError(t, err)

	return flights
}

// Concurrent callers for one paused sandbox share a single Resume call
// and every one of them receives its node IP.
func TestResumeFlights_ConcurrentCallersShareOneResume(t *testing.T) {
	t.Parallel()

	const callers = 10

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)

	results := make([]<-chan resumeResult, 0, callers)
	contexts := make([]*awaitingContext, 0, callers)
	for range callers {
		ctx := newAwaitingContext(t.Context())
		contexts = append(contexts, ctx)
		results = append(results, resumeInBackground(flights, ctx))
	}

	for _, ctx := range contexts {
		<-ctx.awaiting
	}
	close(resumer.release)

	for _, result := range results {
		r := <-result
		require.NoError(t, r.err)
		require.Same(t, testRoute, r.route)
	}

	require.EqualValues(t, 1, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultServed))
	require.EqualValues(t, callers-1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}

// The leader's request ends mid-flight; the flight completes and serves
// the callers that joined it.
func TestResumeFlights_LeaderCancelDoesNotFailJoiners(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{}), route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)

	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leader := newAwaitingContext(leaderCtx)
	leaderResult := resumeInBackground(flights, leader)
	<-leader.awaiting

	joiner := newAwaitingContext(t.Context())
	joinerResult := resumeInBackground(flights, joiner)
	<-joiner.awaiting

	cancelLeader()
	require.ErrorIs(t, (<-leaderResult).err, context.Canceled)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultCanceled))

	close(resumer.release)
	joined := <-joinerResult
	require.NoError(t, joined.err)
	require.Same(t, testRoute, joined.route)
	require.EqualValues(t, 1, resumer.calls.Load())
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}

// A resume that succeeds on its first attempt is served straight away
// and never touches the parking lot.
func TestResumeFlights_FirstAttemptServes(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &recordingResumer{}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.featureFlags = newFFWithParking(t, true, 15000, 512)

	route, err := flights.Resume(t.Context(), "sbx", 49983, "token", "envd-token")
	require.NoError(t, err)
	require.Same(t, testRoute, route)
	require.Equal(t, "sbx", resumer.sandboxID)
	require.EqualValues(t, 49983, resumer.sandboxPort)
	require.Equal(t, "token", resumer.trafficAccessToken)
	require.Equal(t, "envd-token", resumer.envdAccessToken)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultServed))
	require.Zero(t, metrics.parkingWaitSamples(t))
	require.Zero(t, metrics.counter(t, string(telemetry.ClientProxyResumeParkingActiveName)))
}

// An attempt that never answers ends at the ceiling with the unavailable
// page, not the not-found page, and the registry forgets the flight before
// its callers learn the result.
func TestResumeFlights_AttemptCeilingEndsAnUnansweredResumeWithTheUnavailablePage(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &gatedResumer{release: make(chan struct{})}
	flights := newTestResumeFlights(t, metrics, resumer)
	flights.attemptCeiling = 50 * time.Millisecond

	start := time.Now()
	route, res, err := handlePausedSandbox(t.Context(), "sbx", 8000, "token", "", flights)
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, autoResumeErrored, res)
	require.Nil(t, route)

	var capacityErr *reverseproxy.SandboxCapacityError
	require.ErrorAs(t, err, &capacityErr)
	require.Equal(t, "sbx", capacityErr.SandboxID())
	require.Equal(t, errAttemptDeadline.Error(), capacityErr.Message)

	flights.mu.Lock()
	defer flights.mu.Unlock()
	require.Empty(t, flights.flights)
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultError))
}

// A leader carrying an invalid token does not decide the answer for a
// concurrent caller carrying the valid one: the joiner is served, the leader
// is denied.
func TestResumeFlights_InvalidLeaderTokenDoesNotDenyValidJoiner(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &tokenResumer{release: make(chan struct{}), validToken: "valid", route: testRoute}
	flights := newTestResumeFlights(t, metrics, resumer)

	type pausedResult struct {
		route *catalog.SandboxInfo
		res   autoResumeResult
		err   error
	}
	resumeThroughHandler := func(ctx context.Context, token string) <-chan pausedResult {
		result := make(chan pausedResult, 1)
		go func() {
			route, res, err := handlePausedSandbox(ctx, "sbx", 8000, token, "", flights)
			result <- pausedResult{route: route, res: res, err: err}
		}()

		return result
	}

	leader := newAwaitingContext(t.Context())
	leaderResult := resumeThroughHandler(leader, "wrong")
	<-leader.awaiting

	joiner := newAwaitingContext(t.Context())
	joinerResult := resumeThroughHandler(joiner, "valid")
	<-joiner.awaiting

	close(resumer.release)

	denied := <-leaderResult
	require.Equal(t, autoResumePermissionDenied, denied.res)
	var deniedErr *reverseproxy.SandboxResumePermissionDeniedError
	require.ErrorAs(t, denied.err, &deniedErr)

	served := <-joinerResult
	require.NoError(t, served.err)
	require.Equal(t, autoResumeSucceeded, served.res)
	require.Same(t, testRoute, served.route)
	require.EqualValues(t, 2, resumer.calls.Load())
}

// credentialResumer blocks like gatedResumer and then answers each call with
// a route of its own, naming the port and tokens the call carried.
type credentialResumer struct {
	release chan struct{}
	calls   atomic.Int32
}

func (c *credentialResumer) Resume(ctx context.Context, _ string, port uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	c.calls.Add(1)

	select {
	case <-c.release:
		return credentialRoute(port, trafficAccessToken, envdAccessToken), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func credentialRoute(port uint64, trafficAccessToken string, envdAccessToken string) *catalog.SandboxInfo {
	return &catalog.SandboxInfo{
		ExecutionID:      fmt.Sprintf("%d/%s/%s", port, trafficAccessToken, envdAccessToken),
		NetworkPlacement: &catalog.NetworkPlacement{NetworkMode: catalog.NetworkModeFullRouter, RouterID: "router", IngressAddress: "10.8.0.2", IngressPort: 5010},
	}
}

// Callers that differ in port or in either token run their own resume and
// receive the route answered for their own credentials; only identical
// credentials on the same port share a flight and its route.
func TestResumeFlights_DifferentPortOrCredentialsDoNotShare(t *testing.T) {
	t.Parallel()

	metrics := newMetricReader(t)
	resumer := &credentialResumer{release: make(chan struct{})}
	flights := newTestResumeFlights(t, metrics, resumer)

	callers := []struct {
		port    uint64
		traffic string
		envd    string
	}{
		{port: 8000, traffic: "token", envd: ""},
		{port: 49983, traffic: "token", envd: ""},
		{port: 8000, traffic: "other", envd: ""},
		{port: 8000, traffic: "token", envd: "envd"},
		{port: 8000, traffic: "token", envd: ""},
	}

	results := make([]<-chan resumeResult, 0, len(callers))
	for _, c := range callers {
		ctx := newAwaitingContext(t.Context())
		results = append(results, resumeInBackgroundWith(flights, ctx, c.port, c.traffic, c.envd))
		<-ctx.awaiting
	}
	close(resumer.release)

	routes := make([]*catalog.SandboxInfo, 0, len(results))
	for i, result := range results {
		r := <-result
		require.NoError(t, r.err)
		require.Equal(t, credentialRoute(callers[i].port, callers[i].traffic, callers[i].envd), r.route)
		routes = append(routes, r.route)
	}
	require.Same(t, routes[0], routes[4], "identical credentials share the flight's route")

	require.EqualValues(t, 4, resumer.calls.Load())
	require.EqualValues(t, 4, metrics.flightCount(t, resumeFlightRoleLeader, resumeFlightResultServed))
	require.EqualValues(t, 1, metrics.flightCount(t, resumeFlightRoleJoiner, resumeFlightResultServed))
}
