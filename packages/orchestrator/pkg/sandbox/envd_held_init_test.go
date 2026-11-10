//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// heldFlags serves envd-init-held-request at on.
func heldFlags(t *testing.T, on bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.EnvdInitHeldRequestFlag.Key()).VariationForAll(on))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// heldRequest is one /init the envd double received.
type heldRequest struct {
	remoteAddr string
	expect     string
	// body is the decoded body, zero until the handler read it; bodyRead is when the
	// handler started reading it, zero if it never did.
	body     envd.PostInitJSONBody
	bodyRead time.Time
	// rawBody is what the handler read, empty if it never did.
	rawBody []byte
}

// heldEnvd is an envd double that records every /init it receives and answers it with
// what its handler says.
type heldEnvd struct {
	t    *testing.T
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []heldRequest
	// all is the path of every request, /init or not, in arrival order.
	all []string
	// init answers the n-th /init. read reads and records the request's body; a handler
	// that never calls it leaves the body unread.
	init func(w http.ResponseWriter, n int, read func() envd.PostInitJSONBody)
	// closed receives once per connection the server saw close.
	closed chan struct{}
}

// newHeldEnvd answers like an envd at 0.10.0: it reads the body, then answers 204 with its
// version, a zero unobserved-handler count and a zero clock correction, and X-Envd-Thaw:
// skip when the body asked to skip the thaw.
func newHeldEnvd(t *testing.T) *heldEnvd {
	t.Helper()

	e := &heldEnvd{t: t}
	e.init = func(w http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
		body := read()
		w.Header().Set("X-Envd-Version", "0.10.0")
		w.Header().Set(envdUnobservedHeader, "0")
		w.Header().Set(envdClockCorrectionHeader, "0")
		if body.Thaw == envd.Skip {
			w.Header().Set(envdThawHeader, string(envd.Skip))
		}
		w.WriteHeader(http.StatusNoContent)
	}
	e.closed = make(chan struct{}, 64)
	e.srv = httptest.NewUnstartedServer(http.HandlerFunc(e.serve))
	e.srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			e.closed <- struct{}{}
		}
	}
	e.srv.Start()
	t.Cleanup(e.srv.Close)

	return e
}

func (e *heldEnvd) serve(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	e.all = append(e.all, r.URL.Path)
	e.mu.Unlock()

	if r.URL.Path != "/init" {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	// The request is recorded, and its ordinal taken, on arrival: a handler that drops the
	// connection lets the client retry at once, and the retry must see the dropped request
	// already counted.
	e.mu.Lock()
	idx := len(e.reqs)
	e.reqs = append(e.reqs, heldRequest{remoteAddr: r.RemoteAddr, expect: r.Header.Get("Expect")})
	e.mu.Unlock()

	read := func() envd.PostInitJSONBody {
		start := time.Now()
		raw, err := io.ReadAll(r.Body)
		var body envd.PostInitJSONBody
		if err == nil {
			err = json.Unmarshal(raw, &body)
		}
		if err != nil {
			e.t.Errorf("read the /init body: %v", err) // a handler goroutine: no FailNow here
		}

		e.mu.Lock()
		e.reqs[idx].bodyRead = start
		e.reqs[idx].rawBody = raw
		e.reqs[idx].body = body
		e.mu.Unlock()

		return body
	}
	e.init(w, idx+1, read)
}

// paths is the path of every request the double received, in arrival order.
func (e *heldEnvd) paths() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]string(nil), e.all...)
}

func (e *heldEnvd) requests() []heldRequest {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]heldRequest(nil), e.reqs...)
}

// newHeldSandbox is a sandbox whose baked envd version is base plus a unique build tag,
// pointed at url, with envd-init-held-request at on. It returns the sandbox and its
// version.
func newHeldSandbox(t *testing.T, url, base string, on bool) (*Sandbox, string) {
	t.Helper()

	version := uniqueEnvdVersion(base)
	s := &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second, envdServerURLOverride: url},
			Config:         NewConfig(Config{Envd: EnvdMetadata{Version: version}, RamMB: testSandboxRamMB}),
			Runtime:        sandboxtypes.RuntimeMetadata{SandboxID: "sbx-" + version},
		},
		featureFlags: heldFlags(t, on),
		egressProxy:  network.NewNoopEgressProxy(),
		networkHook:  NoopNetworkAssignHook{},
	}

	return s, version
}

// initHeld runs a first customer start on s, bounded by 5 s.
func initHeld(t *testing.T, s *Sandbox) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	return s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{budget: 5 * time.Second})
}

func TestHeldInitSendsOneInitWithItsBodyWithheld(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, version := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	require.NoError(t, initHeld(t, s))

	reqs := e.requests()
	require.Len(t, reqs, 1, "one /init, no re-stamp")
	assert.Equal(t, "100-continue", reqs[0].expect)
	assert.Empty(t, reqs[0].body.Thaw, "a customer start sends no thaw field")

	assert.Equal(t, map[string]int64{"success/": 1}, exitsByType(counterPointsFor(t, telemetry.EnvdInitCalls, version)))
	calls := counterPointsFor(t, telemetry.EnvdInitCalls, version)
	assert.True(t, hasAttr(calls[0].Attributes, "loop", "held"), "%v", calls[0].Attributes.ToSlice())
	assert.Contains(t, s.waitForEnvdDurationAttrs(StartTypeResume, nil), attribute.String("loop", "held"))

	ages := timestampAgePoints(t, version)
	require.Len(t, ages, 1)
	assert.True(t, hasAttr(ages[0].Attributes, "loop", "held"))

	corrections := floatHistogramPointsFor(t, telemetry.EnvdInitClockCorrectionHistogramName, version)
	require.Len(t, corrections, 1)
	assert.True(t, hasAttr(corrections[0].Attributes, "loop", "held"))

	unobserved := counterPointsFor(t, telemetry.EnvdInitUnobservedHandlers, version)
	require.Len(t, unobserved, 1, "a zero is recorded, so the series exists")
	assert.True(t, hasAttr(unobserved[0].Attributes, "loop", "held"))
}

func TestLegacyInitSendsItsBodyWithItsHeaders(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", false)
	require.NoError(t, initHeld(t, s))

	reqs := e.requests()
	require.Len(t, reqs, 1)
	assert.Empty(t, reqs[0].expect, "the legacy loop sends no Expect")
	assert.Equal(t, envdInitLoopLegacy, s.envdInitLoop())
}

// TestHeldInitMintsItsTimestampWhenEnvdReadsTheBody: the timestamp envd receives is
// minted only once its handler asks for the body, so whatever the request waited for before
// the handler ran is not in its age.
func TestHeldInitMintsItsTimestampWhenEnvdReadsTheBody(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	require.NoError(t, initHeld(t, s))

	reqs := e.requests()
	require.Len(t, reqs, 1)
	assert.False(t, reqs[0].body.Timestamp.Before(reqs[0].bodyRead),
		"the timestamp (%s) is minted after the handler asked for the body (%s)",
		reqs[0].body.Timestamp.Format(time.RFC3339Nano), reqs[0].bodyRead.Format(time.RFC3339Nano))
}

// TestHeldInitStatusBeforeTheBodyEndsTheAttempt: a final status written before the body is
// read is the attempt's response, and the body is never sent.
func TestHeldInitStatusBeforeTheBodyEndsTheAttempt(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	e.init = func(w http.ResponseWriter, _ int, _ func() envd.PostInitJSONBody) {
		w.WriteHeader(http.StatusUnauthorized)
	}
	s, version := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	err := initHeld(t, s)
	require.ErrorContains(t, err, "unexpected status code: 401")

	reqs := e.requests()
	require.Len(t, reqs, 1, "a served status is no transport failure: nothing is re-sent")
	assert.True(t, reqs[0].bodyRead.IsZero(), "the handler never read a body")
	assert.Empty(t, timestampAgePoints(t, version), "a failed start records no timestamp age")
}

// TestHeldInitWaitsOnAServerThatNeverReadsTheBody: the body waits for envd as the response
// would, and the start ends at its budget with today's error.
func TestHeldInitWaitsOnAServerThatNeverReadsTheBody(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	e.init = func(_ http.ResponseWriter, _ int, _ func() envd.PostInitJSONBody) {
		<-release
	}
	s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", true)

	ctx, cancel := context.WithCancelCause(t.Context())
	errc := make(chan error, 1)
	go func() {
		errc <- s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{budget: 5 * time.Second})
	}()
	require.Eventually(t, func() bool { return len(e.requests()) == 1 }, 5*time.Second, time.Millisecond)
	cancel(ErrWaitForEnvdTimeout)

	err := <-errc
	require.ErrorIs(t, err, ErrWaitForEnvdTimeout)
	reqs := e.requests()
	require.Len(t, reqs, 1)
	assert.True(t, reqs[0].bodyRead.IsZero())
}

// TestHeldInitAbandonedBeforeThe100ContinueDiedWriting: an attempt the start gives up on
// while envd has not yet asked for the body wrote no body, so it is counted in the write
// phase, not the response phase net/http's request-written event would suggest.
func TestHeldInitAbandonedBeforeThe100ContinueDiedWriting(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	e.init = func(_ http.ResponseWriter, _ int, _ func() envd.PostInitJSONBody) {
		<-release
	}
	s, version := newHeldSandbox(t, e.srv.URL, "0.10.0", true)

	ctx, cancel := context.WithCancelCause(t.Context())
	errc := make(chan error, 1)
	go func() {
		errc <- s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{budget: 5 * time.Second})
	}()
	require.Eventually(t, func() bool { return len(e.requests()) == 1 }, 5*time.Second, time.Millisecond)
	cancel(ErrWaitForEnvdTimeout)
	require.ErrorIs(t, <-errc, ErrWaitForEnvdTimeout)

	points := counterPointsFor(t, telemetry.EnvdInitCalls, version)
	require.Len(t, points, 1, "one terminal exit, no transient attempt")
	phase, ok := points[0].Attributes.Value("phase")
	require.True(t, ok)
	assert.Equal(t, string(envdInitPhaseWrite), phase.AsString())
}

func TestHeldInitGate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		flag     bool
		baked    string
		reported string
		held     bool
	}{
		{name: "flag off", flag: false, baked: "0.10.0", held: false},
		{name: "at the release", flag: true, baked: "0.10.0", held: true},
		{name: "below the release", flag: true, baked: "0.9.99", held: false},
		{name: "a reported version admits a stale baked one", flag: true, baked: "0.9.0", reported: "0.10.0", held: true},
		{name: "a reported version outranks the baked one", flag: true, baked: "0.10.0", reported: "0.9.0", held: false},
		{name: "an over-long report above the release falls back to the baked one", flag: true, baked: "0.9.0", reported: "0.11.0+" + strings.Repeat("a", envdVersionMaxBytes), held: false},
		{name: "an unparseable baked version", flag: true, baked: "", held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _ := newHeldSandbox(t, e.srv.URL, tc.baked, tc.flag)
			if tc.baked == "" {
				s.Config.Envd.Version = ""
			}
			if tc.reported != "" {
				s.setLiveEnvdVersion(tc.reported)
			}
			require.NoError(t, initHeld(t, s))

			reqs := e.requests()
			require.Len(t, reqs, 1)
			if tc.held {
				assert.Equal(t, envdInitLoopHeld, s.envdInitLoop())
				assert.Equal(t, "100-continue", reqs[0].expect)
			} else {
				assert.Equal(t, envdInitLoopLegacy, s.envdInitLoop())
				assert.Empty(t, reqs[0].expect)
			}
		})
	}
}

// TestHeldInitResendsOnlyAfterATransportFailure drops the first /init's connection after
// envd read it; the held loop re-sends once, on a new connection.
func TestHeldInitResendsOnlyAfterATransportFailure(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	served := e.init
	e.init = func(w http.ResponseWriter, n int, read func() envd.PostInitJSONBody) {
		if n == 1 {
			read()
			conn, _, err := http.NewResponseController(w).Hijack()
			if assert.NoError(t, err) {
				conn.Close()
			}

			return
		}
		served(w, n, read)
	}
	s, version := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	require.NoError(t, initHeld(t, s))

	reqs := e.requests()
	require.Len(t, reqs, 2)
	assert.NotEqual(t, reqs[0].remoteAddr, reqs[1].remoteAddr, "the re-send opens a new connection")
	assert.Equal(t, map[string]int64{"transient/response": 1, "success/": 1},
		exitsByType(counterPointsFor(t, telemetry.EnvdInitCalls, version)))
}

// TestHeldInitHoldsOneRequestPastTheAttemptTimeout: the held loop sets no per-attempt
// deadline, so a /init slower than envd-init-request-timeout-milliseconds is waited for.
func TestHeldInitHoldsOneRequestPastTheAttemptTimeout(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	served := e.init
	release := make(chan struct{})
	e.init = func(w http.ResponseWriter, n int, read func() envd.PostInitJSONBody) {
		<-release
		served(w, n, read)
	}
	s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	s.internalConfig.EnvdInitRequestTimeout = time.Millisecond

	errc := make(chan error, 1)
	go func() { errc <- initHeld(t, s) }()
	// 50 times the attempt timeout: a loop that abandoned the attempt would have sent more.
	time.Sleep(50 * time.Millisecond)
	close(release)
	require.NoError(t, <-errc)

	assert.Len(t, e.requests(), 1, "one /init, held")
}

func TestHeldInitClosesItsConnectionOnReturn(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", true)
	require.NoError(t, initHeld(t, s))

	select {
	case <-e.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the held connection is still open after initEnvd returned")
	}
}

func TestEnvdInitConnectTimeout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		flag int
		want time.Duration
	}{
		{flag: 0, want: 5 * time.Second},
		{flag: -1, want: 5 * time.Second},
		{flag: 999, want: time.Second},
		{flag: 1000, want: time.Second},
		{flag: 3000, want: 3 * time.Second},
	} {
		t.Run(strconv.Itoa(tc.flag), func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(featureflags.EnvdInitConnectTimeoutMilliseconds.Key()).ValueForAll(ldvalue.Int(tc.flag)))
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			s, _ := newHeldSandbox(t, "http://127.0.0.1:1", "0.10.0", true)
			s.featureFlags = ff
			assert.Equal(t, tc.want, s.envdInitConnectTimeout(t.Context()))
		})
	}
}

func TestHeldInitClientWaitsForTheBodyRequestUpToTheBudget(t *testing.T) {
	t.Parallel()

	h, err := newHeldInitClient("http://127.0.0.1:1", time.Second, 60*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 60*time.Second, h.transport.ExpectContinueTimeout)

	// A zero timeout would send the body with the headers; no stand-in holds it to envd's
	// ask.
	_, err = newHeldInitClient("http://127.0.0.1:1", time.Second, 0)
	require.ErrorIs(t, err, errNoHeldBudget)
	_, err = newHeldInitClient("http://127.0.0.1:1", time.Second, -time.Second)
	require.ErrorIs(t, err, errNoHeldBudget)
}

// TestHeldInitWithoutABudgetFails: a held start given no envd-wait budget fails before it
// writes anything, rather than sending its body unasked after the connect deadline.
func TestHeldInitWithoutABudgetFails(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, _ := newHeldSandbox(t, e.srv.URL, "0.10.0", true)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{})

	require.ErrorIs(t, err, errNoHeldBudget)
	assert.Empty(t, e.requests(), "nothing was sent")
}

func TestMintedInitBodyMintsOnceOnItsFirstRead(t *testing.T) {
	t.Parallel()

	b := newMintedInitBody(envd.PostInitJSONBody{LifecycleID: "x"})
	_, ok := b.mintedAt()
	assert.False(t, ok, "nothing is minted before the body is read")

	var out []byte
	buf := make([]byte, 7)
	for {
		n, err := b.Read(buf)
		out = append(out, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	minted, ok := b.mintedAt()
	require.True(t, ok)
	var body envd.PostInitJSONBody
	require.NoError(t, json.Unmarshal(out, &body), "small reads still yield one body")
	assert.True(t, body.Timestamp.Equal(minted), "the body carries the minted timestamp")
	assert.Equal(t, "x", body.LifecycleID)
}

func TestRecordSlowDial(t *testing.T) { //nolint:paralleltest // reads a delta of a counter with no per-test key
	read := func() map[string]int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, testMetricReader.Collect(t.Context(), &rm))
		out := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != string(telemetry.EnvdInitSlowDials) {
					continue
				}
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					phase, _ := dp.Attributes.Value("phase")
					out[phase.AsString()] += dp.Value
				}
			}
		}

		return out
	}

	s := &Sandbox{Metadata: &Metadata{Config: NewConfig(Config{})}}
	before := read()
	s.recordSlowDial(t.Context(), true, envdInitSlowDial+time.Millisecond, "success")
	s.recordSlowDial(t.Context(), true, envdInitSlowDial, "success")
	s.recordSlowDial(t.Context(), false, time.Second, "connect")
	after := read()

	assert.Equal(t, before["success"]+1, after["success"], "a dial over 500 ms on a counted start")
	assert.Equal(t, before["connect"], after["connect"], "nothing on a start that is not counted")
}

func TestHeldInitGateReadsTheDeliveredVersionFirst(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		delivered string
		reported  string
		held      bool
	}{
		{name: "an upgrade to the release on an old report and baked version", delivered: "0.10.0", reported: "0.9.0", held: true},
		{name: "the mark outranks a newer report", delivered: "0.9.5", reported: "0.10.0", held: false},
		{name: "no mark: the report decides", reported: "0.10.0", held: true},
		{name: "a fresh handle: the baked version decides", held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _ := newHeldSandbox(t, e.srv.URL, "0.9.0", true)
			if tc.reported != "" {
				s.setLiveEnvdVersion(tc.reported)
			}
			if tc.delivered != "" {
				s.MarkEnvdDelivered(tc.delivered)
			}
			require.NoError(t, initHeld(t, s))

			want := envdInitLoopLegacy
			if tc.held {
				want = envdInitLoopHeld
			}
			assert.Equal(t, want, s.envdInitLoop())
		})
	}
}

// TestRebootGateReadsTheOfflineVersion: a reboot whose pre-boot swapped envd onto the
// rootfs routes its first /init by that binary, ahead of the template-baked version and
// the guest's report; a live upgrade's mark, made later in the life, outranks it.
func TestRebootGateReadsTheOfflineVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		offline   string
		delivered string
		reported  string
		held      bool
	}{
		{name: "an offline upgrade to the release on an older template", offline: "0.10.0", held: true},
		{name: "the offline version outranks the report", offline: "0.10.0", reported: "0.9.0", held: true},
		{name: "the mark outranks the offline version", offline: "0.10.0", delivered: "0.9.5", held: false},
		{name: "no offline upgrade: the baked version decides", held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _ := newHeldSandbox(t, e.srv.URL, "0.9.0", true)
			if tc.offline != "" {
				s.offlineEnvdVersion.Store(&tc.offline)
			}
			if tc.reported != "" {
				s.setLiveEnvdVersion(tc.reported)
			}
			if tc.delivered != "" {
				s.MarkEnvdDelivered(tc.delivered)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, s.initEnvd(ctx, StartTypeReboot, true, envdInitOptions{budget: 5 * time.Second}))

			want := envdInitLoopLegacy
			if tc.held {
				want = envdInitLoopHeld
			}
			assert.Equal(t, want, s.envdInitLoop())
		})
	}
}

// TestPostUpgradeRecheckRoutesByTheMark: the re-check is the exec'd process's first /init
// and the resume's own /init reported the old process, so only the mark routes it held. It
// sends no thaw field and, being no first start, records nothing.
func TestPostUpgradeRecheckRoutesByTheMark(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, version := newHeldSandbox(t, e.srv.URL, "0.9.0", true)
	s.process = &fc.Process{Exit: utils.NewErrorOnce()}
	s.setLiveEnvdVersion("0.9.0")
	// The re-check is never a handle's first start.
	s.startupRecorded.Store(true)
	s.MarkEnvdDelivered("0.10.0")

	require.NoError(t, s.WaitForEnvd(t.Context(), StartTypeResume, 5*time.Second))

	reqs := e.requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "100-continue", reqs[0].expect)
	assert.Empty(t, reqs[0].body.Thaw, "no thaw field")
	assert.Equal(t, envdInitLoopHeld, s.envdInitLoop())
	assert.Empty(t, counterPointsFor(t, telemetry.EnvdInitCalls, version))
}

func TestRunningVersionLabelKeepsTheDeliveredVersion(t *testing.T) {
	t.Parallel()

	s := &Sandbox{Metadata: &Metadata{Config: NewConfig(Config{Envd: EnvdMetadata{Version: "0.9.0"}})}}
	s.setLiveEnvdVersion("0.10.0")
	require.Equal(t, envdRunningVersionOther, s.envdRunningVersionAttr().Value.AsString(),
		"a report the orchestrator did not deliver is not a label value")

	s.MarkEnvdDelivered("0.10.0")
	assert.Equal(t, "0.10.0", s.envdRunningVersionAttr().Value.AsString(),
		"the version a confirmed live upgrade delivered is")
}

// harvestThawPoints reads the harvest.thaw points for one running-version label value.
func harvestThawPoints(t *testing.T, version string) map[bool]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	out := map[bool]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.PauseResumePrefetchHarvestThaw) {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if !hasAttr(dp.Attributes, envdRunningVersionKey, version) {
					continue
				}
				skipped, _ := dp.Attributes.Value("skipped")
				out[skipped.AsBool()] += dp.Value
			}
		}
	}

	return out
}

// TestThrowawaySkipsTheThaw: a held throwaway sends thaw: "skip", records nothing in the init
// instruments, and counts in harvest.thaw by whether envd answered that it skipped; on the
// legacy loop it sends nothing and counts nothing.
func TestThrowawaySkipsTheThaw(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// baked is the throwaway's baked envd version; honours says whether the double
		// answers X-Envd-Thaw: skip.
		baked    string
		honours  bool
		wantThaw envd.PostInitJSONBodyThaw
		want     map[bool]int64
	}{
		{name: "held, envd skips", baked: "0.10.0", honours: true, wantThaw: envd.Skip, want: map[bool]int64{true: 1}},
		{name: "held, envd thaws inline anyway", baked: "0.10.0", honours: false, wantThaw: envd.Skip, want: map[bool]int64{false: 1}},
		{name: "legacy", baked: "0.9.0", honours: true, wantThaw: "", want: map[bool]int64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			if !tc.honours {
				e.init = func(w http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
					read()
					w.WriteHeader(http.StatusNoContent)
				}
			}
			s, version := newHeldSandbox(t, e.srv.URL, tc.baked, true)
			s.skipThaw = true
			s.skipStartupMetrics = true
			require.NoError(t, initHeld(t, s))

			reqs := e.requests()
			require.Len(t, reqs, 1)
			assert.Equal(t, tc.wantThaw, reqs[0].body.Thaw)
			assert.Equal(t, tc.want, harvestThawPoints(t, version), "harvest.thaw by skipped, under the gate version")
			assert.Empty(t, counterPointsFor(t, telemetry.EnvdInitCalls, version), "a throwaway records nothing in the init instruments")
			assert.Empty(t, floatHistogramPointsFor(t, telemetry.EnvdInitClockCorrectionHistogramName, version))
		})
	}
}

// TestHarvestThawLabelIsAVersionTheOrchestratorHolds: the label is the gate version when the
// orchestrator holds it, else other.
func TestHarvestThawLabelIsAVersionTheOrchestratorHolds(t *testing.T) {
	t.Parallel()

	e := newHeldEnvd(t)
	s, _ := newHeldSandbox(t, e.srv.URL, "0.9.0", true)
	inherited := uniqueEnvdVersion("0.10.0")
	s.inheritedEnvdVersion = inherited
	s.skipThaw = true
	s.skipStartupMetrics = true
	require.NoError(t, initHeld(t, s))

	assert.Empty(t, harvestThawPoints(t, inherited), "an inherited version the orchestrator does not hold is not a label value")

	e2 := newHeldEnvd(t)
	s2, _ := newHeldSandbox(t, e2.srv.URL, "0.9.0", true)
	held := uniqueEnvdVersion("0.10.0")
	s2.inheritedEnvdVersion = held
	s2.offlineEnvdVersion.Store(&held)
	s2.skipThaw = true
	s2.skipStartupMetrics = true
	require.NoError(t, initHeld(t, s2))

	assert.Equal(t, map[bool]int64{true: 1}, harvestThawPoints(t, held))
}

func TestThrowawayGateReadsTheInheritedVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		baked     string
		inherited string
		held      bool
	}{
		{name: "an upgraded source admits a stale baked version", baked: "0.9.0", inherited: "0.10.0", held: true},
		{name: "the inherited version outranks the baked one", baked: "0.10.0", inherited: "0.9.0", held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _ := newHeldSandbox(t, e.srv.URL, tc.baked, true)
			s.inheritedEnvdVersion = tc.inherited
			s.skipThaw = true
			require.NoError(t, initHeld(t, s))

			want := envdInitLoopLegacy
			if tc.held {
				want = envdInitLoopHeld
			}
			assert.Equal(t, want, s.envdInitLoop())
		})
	}
}

func TestRunningEnvdVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		delivered string
		offline   string
		reported  string
		want      string
	}{
		{name: "the report outranks the mark", delivered: "0.10.1", reported: "0.10.0", want: "0.10.0"},
		{name: "a restarted rootfs envd outranks the mark", delivered: "0.10.0", reported: "0.9.0", want: "0.9.0"},
		{name: "else the mark", delivered: "0.10.1", want: "0.10.1"},
		{name: "the mark outranks the offline version", delivered: "0.10.1", offline: "0.10.0", want: "0.10.1"},
		{name: "else the offline version", offline: "0.10.0", want: "0.10.0"},
		{name: "an admissible report", reported: "0.10.0", want: "0.10.0"},
		{name: "an inadmissible report falls back to the mark", delivered: "0.10.1", reported: "not-a-version", want: "0.10.1"},
		{name: "never an inadmissible one", reported: "not-a-version", want: ""},
		{name: "never the baked one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, _ := newHeldSandbox(t, "http://127.0.0.1:1", "0.10.0", true)
			if tc.reported != "" {
				s.setLiveEnvdVersion(tc.reported)
			}
			if tc.offline != "" {
				s.offlineEnvdVersion.Store(&tc.offline)
			}
			if tc.delivered != "" {
				s.MarkEnvdDelivered(tc.delivered)
			}
			assert.Equal(t, tc.want, s.RunningEnvdVersion())
		})
	}
}

// TestThrowawayInheritsTheEnvdThatAnsweredTheLastInit: a live upgrade to the release was
// confirmed, the delivered binary died and the guest restarted the rootfs one, which
// answered the re-check below the release. The life's own gate keeps routing by the mark,
// a pause records the rootfs binary's version, and a throwaway harvested from that life
// inherits the same, so it takes the legacy loop and asks for no thaw skip the resumed
// envd would ignore.
func TestThrowawayInheritsTheEnvdThatAnsweredTheLastInit(t *testing.T) {
	t.Parallel()

	source, _ := newHeldSandbox(t, "http://127.0.0.1:1", "0.9.0", true)
	source.MarkEnvdDelivered("0.10.0")
	source.setLiveEnvdVersion("0.9.5")
	require.Equal(t, "0.10.0", source.envdGateVersion(), "the life's own gate keeps the mark")
	recorded := metadata.Template{Version: metadata.CurrentVersion}.
		WithRunningEnvdVersion(source.snapshotEnvdVersion(false))
	require.Equal(t, "0.9.5", recorded.RunningEnvdVersion)

	e := newHeldEnvd(t)
	throwaway, _ := newHeldSandbox(t, e.srv.URL, "0.9.0", true)
	throwaway.inheritedEnvdVersion = resumeGateEnvdVersion(source.RunningEnvdVersion(), recorded.RunningEnvdVersion)
	throwaway.skipThaw = true
	require.NoError(t, initHeld(t, throwaway))

	assert.Equal(t, "0.9.5", throwaway.inheritedEnvdVersion, "the throwaway is given what the pause recorded")
	assert.Equal(t, envdInitLoopLegacy, throwaway.envdInitLoop())
	reqs := e.requests()
	require.Len(t, reqs, 1)
	assert.Empty(t, reqs[0].body.Thaw, "no thaw skip for an envd that ignores it")
}

func TestSnapshotEnvdVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		delivered      string
		reported       string
		inherited      string
		reportBaked    bool
		filesystemOnly bool
		want           string
	}{
		{name: "the report outranks the mark", delivered: "0.10.1", reported: "0.10.0", want: "0.10.0"},
		{name: "a restarted rootfs envd outranks the mark", delivered: "0.10.1", reportBaked: true},
		{name: "else the mark", delivered: "0.10.1", want: "0.10.1"},
		{name: "the report", reported: "0.10.0", want: "0.10.0"},
		{name: "else the version the life was resumed with", inherited: "0.10.0", want: "0.10.0"},
		{name: "a report outranks the version the life was resumed with", reported: "0.9.0", inherited: "0.10.0", want: "0.9.0"},
		{name: "nothing for the baked version", reportBaked: true},
		{name: "nothing for a fresh handle"},
		{name: "nothing for a filesystem-only snapshot", delivered: "0.10.1", filesystemOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, _ := newHeldSandbox(t, "http://127.0.0.1:1", "0.9.0", true)
			s.inheritedEnvdVersion = tc.inherited
			switch {
			case tc.reportBaked:
				s.setLiveEnvdVersion(s.Config.Envd.Version)
			case tc.reported != "":
				s.setLiveEnvdVersion(tc.reported)
			}
			if tc.delivered != "" {
				s.MarkEnvdDelivered(tc.delivered)
			}
			assert.Equal(t, tc.want, s.snapshotEnvdVersion(tc.filesystemOnly))
		})
	}
}

func TestResumeGateEnvdVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, given, recorded, want string
	}{
		{name: "the given version outranks the recorded one", given: "0.10.1", recorded: "0.10.0", want: "0.10.1"},
		{name: "else the recorded one", recorded: "0.10.0", want: "0.10.0"},
		{name: "never an inadmissible recorded one", recorded: "not-a-version", want: ""},
		{name: "nothing recorded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, resumeGateEnvdVersion(tc.given, tc.recorded))
		})
	}
}

// A life live-upgraded past the release records its version for a memory pause; a fresh
// handle given that version takes the held request on its first /init, before its envd has
// reported anything, although its template-baked version is below the release.
func TestResumedLifeGatesOnTheVersionItsSnapshotRecorded(t *testing.T) {
	t.Parallel()

	paused, _ := newHeldSandbox(t, "http://127.0.0.1:1", "0.9.0", true)
	paused.MarkEnvdDelivered("0.10.0")
	recorded := metadata.Template{Version: metadata.CurrentVersion}.
		WithRunningEnvdVersion(paused.snapshotEnvdVersion(false))

	for _, tc := range []struct {
		name string
		meta metadata.Template
		held bool
	}{
		{name: "a memory snapshot of the upgraded life", meta: recorded, held: true},
		{name: "a snapshot from before the field", meta: metadata.Template{Version: metadata.CurrentVersion}, held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			s, _ := newHeldSandbox(t, e.srv.URL, "0.9.0", true)
			s.inheritedEnvdVersion = resumeGateEnvdVersion("", tc.meta.RunningEnvdVersion)
			require.NoError(t, initHeld(t, s))

			want := envdInitLoopLegacy
			if tc.held {
				want = envdInitLoopHeld
			}
			assert.Equal(t, want, s.envdInitLoop())
		})
	}
}

// TestEnvdInitCABundleFollowsTheExpectedEnvd: each /init carries the bundle for the envd
// expected at that moment, and the next call follows what the last one reported.
func TestEnvdInitCABundleFollowsTheExpectedEnvd(t *testing.T) {
	t.Parallel()

	const (
		bundle = "BUNDLE\n"
		roots  = "ROOTS\n"
	)
	for _, tc := range []struct {
		name                      string
		baked, offline, inherited string
		// upgrade runs a post-upgrade re-check, marked delivered when delivered is set.
		upgrade   bool
		delivered string
		throwaway bool
		// answers is the version envd reports on each /init, its last one repeated.
		answers []string
		// roots is whether each /init carried the roots, in order.
		roots []bool
	}{
		{name: "a compatible template", baked: "0.10.2", answers: []string{"0.10.2"}, roots: []bool{true, true}},
		{name: "an older template", baked: "0.10.0", answers: []string{"0.10.0"}, roots: []bool{false, false}},
		{name: "a confirmed live upgrade", baked: "0.10.0", upgrade: true, delivered: "0.10.2", answers: []string{"0.10.0", "0.10.2"}, roots: []bool{false, false, true}},
		{name: "a delivered binary the guest replaced with its rootfs one", baked: "0.10.0", upgrade: true, delivered: "0.10.2", answers: []string{"0.10.0"}, roots: []bool{false, false, false}},
		{name: "a snapshot that recorded its upgraded envd", baked: "0.10.0", inherited: "0.10.2", answers: []string{"0.10.2"}, roots: []bool{true, true}},
		{name: "a snapshot that recorded an older envd than its template", baked: "0.10.2", inherited: "0.10.0", answers: []string{"0.10.0"}, roots: []bool{false, false}},
		{name: "an upgraded snapshot from before the record", baked: "0.10.0", answers: []string{"0.10.2"}, roots: []bool{false, true}},
		{name: "a throwaway of an upgraded snapshot from before the record", baked: "0.10.0", throwaway: true, answers: []string{"0.10.2"}, roots: []bool{false, true}},
		{name: "an offline upgrade", baked: "0.10.0", offline: "0.10.2", answers: []string{"0.10.2"}, roots: []bool{true, true}},
		{name: "an envd that reports no admissible version", baked: "0.10.2", answers: []string{"not-a-version"}, roots: []bool{true, true}},
	} {
		for _, held := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/held=%t", tc.name, held), func(t *testing.T) {
				t.Parallel()

				e := newHeldEnvd(t)
				e.init = func(w http.ResponseWriter, n int, read func() envd.PostInitJSONBody) {
					read()
					w.Header().Set("X-Envd-Version", tc.answers[min(n, len(tc.answers))-1])
					w.WriteHeader(http.StatusNoContent)
				}
				s, version := newHeldSandbox(t, e.srv.URL, tc.baked, held)
				s.egressProxy = &mockEgressProxy{bundle: bundle, roots: roots}
				s.inheritedEnvdVersion = tc.inherited
				if tc.offline != "" {
					s.offlineEnvdVersion.Store(&tc.offline)
				}
				s.skipThaw = tc.throwaway
				s.skipStartupMetrics = tc.throwaway

				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				opts := envdInitOptions{budget: 5 * time.Second}
				require.NoError(t, s.initEnvd(ctx, StartTypeResume, true, opts))
				if tc.upgrade {
					if tc.delivered != "" {
						s.MarkEnvdDelivered(tc.delivered)
					}
					require.NoError(t, s.initEnvd(ctx, StartTypeResume, false, opts))
				}
				require.NoError(t, s.initEnvd(ctx, StartTypeResume, false, opts))

				var got []bool
				wantThaw := envd.PostInitJSONBodyThaw("")
				if held && tc.throwaway {
					wantThaw = envd.Skip
				}
				for _, r := range e.requests() {
					assert.Contains(t, []string{bundle, bundle + roots}, r.body.CaBundle)
					got = append(got, r.body.CaBundle == bundle+roots)
					assert.Equal(t, wantThaw, r.body.Thaw, "each /init keeps the start's thaw")
				}
				assert.Equal(t, tc.roots, got)

				wantCalls := map[string]int64{"success/": 1}
				if tc.throwaway {
					wantCalls = map[string]int64{}
				}
				assert.Equal(t, wantCalls, exitsByType(counterPointsFor(t, telemetry.EnvdInitCalls, version)),
					"a re-check is no start of its own")
			})
		}
	}
}

// envdReadyHook is a network-assign hook whose OnEnvdReady is ready.
type envdReadyHook struct {
	NoopNetworkAssignHook

	ready func(envdVersion string) error
}

func (h envdReadyHook) OnEnvdReady(_ context.Context, _ *Sandbox, envdVersion string) error {
	return h.ready(envdVersion)
}

// TestEnvdInitFailsWhenTheNetworkRefusesItsEnvd: the network-assign hook hears the version
// envd reported when it is admissible, and its refusal fails the start as network_refused.
func TestEnvdInitFailsWhenTheNetworkRefusesItsEnvd(t *testing.T) {
	t.Parallel()

	refusal := errors.New("refused")
	for _, tc := range []struct {
		name   string
		answer string
		want   string
	}{
		{name: "an admissible report", answer: "0.10.2", want: "0.10.2"},
		{name: "no report", answer: ""},
		{name: "a malformed report", answer: "not-a-version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newHeldEnvd(t)
			e.init = func(w http.ResponseWriter, _ int, read func() envd.PostInitJSONBody) {
				read()
				if tc.answer != "" {
					w.Header().Set("X-Envd-Version", tc.answer)
				}
				w.WriteHeader(http.StatusNoContent)
			}
			s, version := newHeldSandbox(t, e.srv.URL, "0.10.2", false)
			var heard []string
			responseCaptured := false
			s.networkHook = envdReadyHook{ready: func(envdVersion string) error {
				assert.True(t, responseCaptured, "startup faults must be captured before the network-ready hook")
				heard = append(heard, envdVersion)

				return refusal
			}}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{
				budget:     5 * time.Second,
				onResponse: func() { responseCaptured = true },
			})
			require.ErrorIs(t, err, errEnvdRefusedByNetwork)
			require.ErrorIs(t, err, refusal)
			assert.True(t, responseCaptured)
			assert.Equal(t, []string{tc.want}, heard)
			assert.Equal(t, map[string]int64{"network_refused/response": 1},
				exitsByType(counterPointsFor(t, telemetry.EnvdInitCalls, version)))
		})
	}
}
