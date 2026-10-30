//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// initTestSeq makes each fixture's baked envd version unique per invocation, so a test
// reads only its own points from the package's cumulative metric reader under -count.
var initTestSeq atomic.Int64

// uniqueEnvdVersion is base with a semver build tag unique to this invocation; the tag
// leaves the version's ordering alone.
func uniqueEnvdVersion(base string) string {
	return fmt.Sprintf("%s+t%d", base, initTestSeq.Add(1))
}

// counterPointsFor reads a counter's points for one baked envd version.
func counterPointsFor(t *testing.T, name telemetry.CounterType, version string) []metricdata.DataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	var out []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not an int64 sum", m.Name)
			for _, dp := range sum.DataPoints {
				if hasAttr(dp.Attributes, telemetry.WithEnvdVersion("").Key, version) {
					out = append(out, dp)
				}
			}
		}
	}

	return out
}

// timestampAgePoints reads the timestamp-age histogram's points for one baked envd version.
func timestampAgePoints(t *testing.T, version string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()

	return floatHistogramPointsFor(t, telemetry.EnvdInitTimestampAgeHistogramName, version)
}

// floatHistogramPointsFor reads a float64 histogram's points for one baked envd version.
func floatHistogramPointsFor(t *testing.T, name telemetry.HistogramType, version string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	var out []metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(name) {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "%s is not a float64 histogram", m.Name)
			for _, dp := range h.DataPoints {
				if hasAttr(dp.Attributes, telemetry.WithEnvdVersion("").Key, version) {
					out = append(out, dp)
				}
			}
		}
	}

	return out
}

// exitsByType sums init.calls points by exit_type and phase, as "exit_type/phase".
func exitsByType(points []metricdata.DataPoint[int64]) map[string]int64 {
	out := map[string]int64{}
	for _, dp := range points {
		exit, _ := dp.Attributes.Value("exit_type")
		phase, _ := dp.Attributes.Value("phase")
		out[exit.AsString()+"/"+phase.AsString()] += dp.Value
	}

	return out
}

// loggedAttempts is the attempt count the failed start with baked version logged.
func loggedAttempts(t *testing.T, version string) int64 {
	t.Helper()

	var found []int64
	for _, e := range testLogObserver.FilterMessage("failed to init envd after retries").All() {
		fields := e.ContextMap()
		if fields["envd.version"] == version {
			n, ok := fields["attempts"].(int64)
			require.True(t, ok, "attempts field: %v", fields)
			found = append(found, n)
		}
	}
	require.Len(t, found, 1, "one failure log for %s", version)

	return found[0]
}

// newInitTestSandbox is a legacy-loop sandbox (no feature flags) with a unique baked
// version, pointed at url, abandoning each attempt at attempt.
func newInitTestSandbox(url string, attempt time.Duration) (*Sandbox, string) {
	version := uniqueEnvdVersion("0.9.0")

	return &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: attempt, envdServerURLOverride: url},
			Config:         NewConfig(Config{Envd: EnvdMetadata{Version: version}, RamMB: testSandboxRamMB}),
			Runtime:        sandboxtypes.RuntimeMetadata{SandboxID: "sbx-" + version},
		},
		egressProxy: network.NewNoopEgressProxy(),
		networkHook: NoopNetworkAssignHook{},
	}, version
}

// stallingEnvd answers /init with 204 and reportedVersion, after holding the first
// stalls requests until their client gives up.
func stallingEnvd(t *testing.T, stalls int64, reportedVersion string) *httptest.Server {
	t.Helper()

	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server notices the client leaving only once the body has been read.
		_, _ = io.Copy(io.Discard, r.Body)
		if n.Add(1) <= stalls {
			<-r.Context().Done()

			return
		}
		if reportedVersion != "" {
			w.Header().Set("X-Envd-Version", reportedVersion)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	return srv
}

// droppingEnvd answers /init with 204, after closing the connection of each of the first
// drops requests once it has read the request, so each of those attempts fails in its
// response phase whatever its deadline.
func droppingEnvd(t *testing.T, drops int64) *httptest.Server {
	t.Helper()

	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if n.Add(1) <= drops {
			conn, _, err := http.NewResponseController(w).Hijack()
			if assert.NoError(t, err) {
				conn.Close()
			}

			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestInitCallsRecordOneTerminalExitPerStart(t *testing.T) {
	t.Parallel()

	t.Run("a start served on its third attempt", func(t *testing.T) {
		t.Parallel()

		srv := droppingEnvd(t, 2)
		s, version := newInitTestSandbox(srv.URL, 5*time.Second)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		require.NoError(t, s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{}))

		points := counterPointsFor(t, telemetry.EnvdInitCalls, version)
		assert.Equal(t, map[string]int64{"transient/response": 2, "success/": 1}, exitsByType(points),
			"two delivered attempts whose connection dropped and one terminal success")
		for _, dp := range points {
			assert.True(t, hasAttr(dp.Attributes, "loop", "legacy"), "every exit carries the loop: %v", dp.Attributes.ToSlice())
		}
		assert.Contains(t, s.waitForEnvdDurationAttrs(StartTypeResume, nil), attribute.String("loop", "legacy"),
			"the duration histogram carries the same loop")

		ages := timestampAgePoints(t, version)
		require.Len(t, ages, 1, "one timestamp age per successful start")
		assert.Equal(t, uint64(1), ages[0].Count)
		assert.Equal(t, 5, ages[0].Attributes.Len(), "envd.version, timeout_ms, start_type, loop and team: %v", ages[0].Attributes.ToSlice())
	})

	t.Run("a start that never got a response ends on its last attempt", func(t *testing.T) {
		t.Parallel()

		srv := stallingEnvd(t, 1<<30, "")
		s, version := newInitTestSandbox(srv.URL, 20*time.Millisecond)
		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()
		require.Error(t, s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{}))

		exits := exitsByType(counterPointsFor(t, telemetry.EnvdInitCalls, version))
		var terminal, total int64
		for key, n := range exits {
			assert.False(t, strings.HasSuffix(key, "/"), "a failed exit carries a phase: %s", key)
			total += n
			if !strings.HasPrefix(key, "transient/") {
				terminal += n
			}
		}
		assert.Equal(t, int64(1), terminal, "exactly one terminal exit: %v", exits)
		assert.Equal(t, loggedAttempts(t, version), total, "every attempt is one exit: %v", exits)
		assert.NotContains(t, exits, "success/")
		assert.Empty(t, timestampAgePoints(t, version),
			"a failed start applied no timestamp")
	})

	t.Run("a throwaway records nothing", func(t *testing.T) {
		t.Parallel()

		srv := stallingEnvd(t, 0, "")
		s, version := newInitTestSandbox(srv.URL, time.Second)
		s.skipStartupMetrics = true
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{}))

		assert.Empty(t, counterPointsFor(t, telemetry.EnvdInitCalls, version))
		assert.Empty(t, timestampAgePoints(t, version))
	})

	t.Run("a later init on the same handle records nothing", func(t *testing.T) {
		t.Parallel()

		srv := stallingEnvd(t, 0, "")
		s, version := newInitTestSandbox(srv.URL, time.Second)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, s.initEnvd(ctx, StartTypeResume, false, envdInitOptions{}))

		assert.Empty(t, counterPointsFor(t, telemetry.EnvdInitCalls, version))
		assert.Empty(t, timestampAgePoints(t, version))
	})
}

func TestInitInstrumentsCarryOnlyAnAdmissibleRunningVersion(t *testing.T) {
	t.Parallel()

	const baked = "<baked>"
	atBound := "0.10.3-" + strings.Repeat("a", envdVersionMaxBytes-7)
	for _, tc := range []struct {
		name     string
		reported string
		offline  string
		target   string
		want     string
	}{
		{name: "the baked version", reported: baked, want: baked},
		{name: "an offline-upgraded version", reported: "0.10.3", offline: "0.10.3", want: "0.10.3"},
		{name: "a version this node's upgrade target resolved to", reported: "0.10.3", target: "0.10.3", want: "0.10.3"},
		{name: "a version the orchestrator does not hold", reported: "0.10.3", want: envdRunningVersionOther},
		{name: "not held, at the byte bound", reported: atBound, offline: "0.10.3", want: envdRunningVersionOther},
		{name: "nothing reported", reported: "", want: envdRunningVersionUnknown},
		{name: "not semver", reported: "latest", want: envdRunningVersionUnknown},
		{name: "held, but one byte over the bound", reported: atBound + "a", offline: atBound + "a", want: envdRunningVersionUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, version := newInitTestSandbox("", time.Second)
			// The baked version is unique per sandbox; a case naming it reports that one.
			reported, want := tc.reported, tc.want
			if reported == baked {
				reported, want = version, version
			}
			s.internalConfig.envdServerURLOverride = stallingEnvd(t, 0, reported).URL
			if tc.offline != "" {
				s.offlineEnvdVersion.Store(&tc.offline)
			}
			s.envdTargets = &envdTargetVersions{}
			if tc.target != "" {
				probe := s.envdTargets.recording(func(context.Context, string) (string, error) { return tc.target, nil })
				_, err := probe(t.Context(), "envd")
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{}))

			points := counterPointsFor(t, telemetry.EnvdInitCalls, version)
			require.Len(t, points, 1)
			assert.True(t, hasAttr(points[0].Attributes, envdRunningVersionKey, want),
				"init.calls: %v", points[0].Attributes.ToSlice())
			assert.True(t, hasAttr(points[0].Attributes, telemetry.WithEnvdVersion("").Key, version),
				"envd.version stays the baked version")
			assert.Contains(t, s.waitForEnvdDurationAttrs(StartTypeResume, nil), envdRunningVersionKey.String(want),
				"the duration histogram carries the same running version")
		})
	}
}

// reportingEnvd answers /init with status and headers, delay after it has read the body.
func reportingEnvd(t *testing.T, status int, delay time.Duration, headers map[string]string) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(delay)
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestInitRecordsEnvdsClockCorrectionAndUnobservedCount(t *testing.T) {
	t.Parallel()

	// The stub answers 30 ms after it read the body, so the served timestamp is at least
	// that old and a 20 ms correction is inside it.
	const answerAfter = 30 * time.Millisecond
	for _, tc := range []struct {
		name           string
		status         int
		skipStartup    bool
		headers        map[string]string
		wantCorrection []float64
		wantUnobserved []int64
	}{
		{
			name:           "both reported",
			status:         http.StatusNoContent,
			headers:        map[string]string{envdClockCorrectionHeader: "20", envdUnobservedHeader: "3"},
			wantCorrection: []float64{20},
			wantUnobserved: []int64{3},
		},
		{
			name:           "zero unobserved handlers is still a report",
			status:         http.StatusNoContent,
			headers:        map[string]string{envdUnobservedHeader: "0"},
			wantUnobserved: []int64{0},
		},
		{
			name:   "a correction longer than the timestamp's age, and a count above the bound",
			status: http.StatusNoContent,
			headers: map[string]string{
				envdClockCorrectionHeader: "600000",
				envdUnobservedHeader:      fmt.Sprint(envdInitGuestCountMax + 1),
			},
		},
		{
			name:    "negative and malformed values",
			status:  http.StatusNoContent,
			headers: map[string]string{envdClockCorrectionHeader: "-1", envdUnobservedHeader: "x"},
		},
		{name: "an envd that sends neither", status: http.StatusNoContent},
		{
			name:    "a failed start",
			status:  http.StatusInternalServerError,
			headers: map[string]string{envdClockCorrectionHeader: "20", envdUnobservedHeader: "3"},
		},
		{
			name:        "a throwaway",
			status:      http.StatusNoContent,
			skipStartup: true,
			headers:     map[string]string{envdClockCorrectionHeader: "20", envdUnobservedHeader: "3"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := reportingEnvd(t, tc.status, answerAfter, tc.headers)
			s, version := newInitTestSandbox(srv.URL, 5*time.Second)
			s.skipStartupMetrics = tc.skipStartup
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := s.initEnvd(ctx, StartTypeResume, true, envdInitOptions{})
			if tc.status == http.StatusNoContent {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			var corrections []float64
			for _, dp := range floatHistogramPointsFor(t, telemetry.EnvdInitClockCorrectionHistogramName, version) {
				assert.Equal(t, uint64(1), dp.Count)
				corrections = append(corrections, dp.Sum)
				assertReportAttrs(t, dp.Attributes)
			}
			assert.Equal(t, tc.wantCorrection, corrections, "clock_correction")

			var unobserved []int64
			for _, dp := range counterPointsFor(t, telemetry.EnvdInitUnobservedHandlers, version) {
				unobserved = append(unobserved, dp.Value)
				assertReportAttrs(t, dp.Attributes)
			}
			assert.Equal(t, tc.wantUnobserved, unobserved, "unobserved_handlers")
		})
	}
}

// assertReportAttrs checks an instrument read off envd's report carries start_type, loop and
// the baked version, and not the legacy loop's window.
func assertReportAttrs(t *testing.T, attrs attribute.Set) {
	t.Helper()

	assert.True(t, hasAttr(attrs, "start_type", string(StartTypeResume)), "%v", attrs.ToSlice())
	assert.True(t, hasAttr(attrs, "loop", "legacy"), "%v", attrs.ToSlice())
	_, ok := attrs.Value("timeout_ms")
	assert.False(t, ok, "no timeout_ms: %v", attrs.ToSlice())
	assert.Equal(t, 4, attrs.Len(), "envd.version, start_type, loop and team: %v", attrs.ToSlice())
}
