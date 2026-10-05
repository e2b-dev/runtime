//go:build linux

package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// TestDecodeEnvdMemoryProtection pins the admissibility bound on the guest-written
// header: what decodes, and what is refused before anything reads it.
func TestDecodeEnvdMemoryProtection(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		header  string
		want    EnvdMemoryProtection
		wantErr string
	}{
		"the four fields": {
			header: `{"request":134217728,"low":268435456,"floor":67108864,"partial":true}`,
			want:   EnvdMemoryProtection{Request: 134217728, Low: 268435456, Floor: 67108864, Partial: true},
		},
		"a field this envd omits is its zero value": {
			header: `{"request":1}`,
			want:   EnvdMemoryProtection{Request: 1},
		},
		"a field this orchestrator does not know is ignored": {
			header: `{"floor":5,"newer":"thing"}`,
			want:   EnvdMemoryProtection{Floor: 5},
		},
		"negative":    {header: `{"floor":-1}`, wantErr: "cannot unmarshal number -1"},
		"non-numeric": {header: `{"floor":"lots"}`, wantErr: "cannot unmarshal string"},
		"fractional":  {header: `{"request":1.5}`, wantErr: "cannot unmarshal number 1.5"},
		// The sentinel envd sends for an unbounded request. Admitting exactly MaxInt64 is
		// the contract that makes it usable: one off-by-one in the guard and every start
		// on an unbounded chain loses its whole report instead.
		"at the int64 boundary": {header: `{"request":9223372036854775807,"floor":9223372036854775807}`, want: EnvdMemoryProtection{Request: math.MaxInt64, Floor: math.MaxInt64}},
		"over int64":            {header: `{"low":9223372036854775808}`, wantErr: "over the int64 range"},
		"not json":              {header: `floor=1`, wantErr: "invalid character"},
		"empty":                 {header: ``, wantErr: "unexpected end of JSON input"},
		"at the byte cap":       {header: `{"floor":7,"pad":"` + strings.Repeat("x", envdMemoryHeaderMaxBytes-len(`{"floor":7,"pad":""}`)) + `"}`, want: EnvdMemoryProtection{Floor: 7}},
		"over the byte cap":     {header: `{"floor":7,"pad":"` + strings.Repeat("x", envdMemoryHeaderMaxBytes) + `"}`, wantErr: "over the 1024-byte cap"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeEnvdMemoryProtection(tc.header)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Equal(t, EnvdMemoryProtection{}, got, "a refused header yields no report")

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestEnvdMemoryProtectionCohort pins the three cohorts a report maps to, and that a
// partial read is not absorbed by either measured one.
func TestEnvdMemoryProtectionCohort(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		p    EnvdMemoryProtection
		want envdProtectionCohort
	}{
		"a request at every level": {
			p:    EnvdMemoryProtection{Request: 50 * 1 << 20, Floor: 50 * 1 << 20},
			want: envdProtectionProtected,
		},
		"a level without one": {
			p:    EnvdMemoryProtection{Request: 50 * 1 << 20, Floor: 0},
			want: envdProtectionUnprotected,
		},
		"envd in the root cgroup, nothing to read": {
			p:    EnvdMemoryProtection{},
			want: envdProtectionUnprotected,
		},
		"a chain envd could not read": {
			p:    EnvdMemoryProtection{Partial: true},
			want: envdProtectionUnknown,
		},
		// An unreadable memory.min contributes 0 to the minimum and takes the floor with
		// it, so a positive floor alongside partial can only mean memory.low failed —
		// which the cohort does not rest on. This is the case that fixes the order of
		// the two conditions.
		"a positive floor with only memory.low unread": {
			p:    EnvdMemoryProtection{Request: 50 * 1 << 20, Floor: 50 * 1 << 20, Partial: true},
			want: envdProtectionProtected,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, tc.p.protection())
		})
	}
}

// testSandboxRamMB is the RAM the test sandboxes claim, and so the cap the histogram
// applies. Not a round power of two, so a sample landing on it cannot be mistaken for a
// value that arrived that way.
const testSandboxRamMB = 3000

// TestEnvdMemoryProtectionMiB pins the histogram's value conversion: MiB, truncated, and
// capped at the sandbox's RAM so that one unbounded request cannot cost its whole series
// the bucket resolution the instrument is read at.
func TestEnvdMemoryProtectionMiB(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		bytes uint64
		ramMB int64
		want  int64
	}{
		"an ordinary request":              {bytes: 50 << 20, ramMB: 4096, want: 50},
		"truncates, does not round":        {bytes: 50<<20 + (1 << 19), ramMB: 4096, want: 50},
		"below a MiB":                      {bytes: 4096, ramMB: 4096, want: 0},
		"exactly the sandbox's RAM":        {bytes: 4096 << 20, ramMB: 4096, want: 4096},
		"an unbounded request caps at RAM": {bytes: math.MaxInt64, ramMB: 4096, want: 4096},
		"above RAM but not unbounded caps": {bytes: 8192 << 20, ramMB: 4096, want: 4096},
		// Capping at 0 would file an unprotected magnitude beside a protected cohort.
		"an unknown RAM does not cap": {bytes: math.MaxInt64, ramMB: 0, want: math.MaxInt64 >> 20},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, envdMemoryProtectionMiB(tc.bytes, tc.ramMB))
		})
	}
}

// newMemoryTestSandbox builds a Sandbox whose /init goes to url, with an envd version and
// sandbox id unique to this call so the process-wide recorders can be filtered to it.
func newMemoryTestSandbox(t *testing.T, url string) (*Sandbox, string) {
	t.Helper()

	b := make([]byte, 8)
	_, err := rand.Read(b)
	require.NoError(t, err)
	id := hex.EncodeToString(b)

	s := &Sandbox{Metadata: &Metadata{
		internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second, envdServerURLOverride: url},
		Config:         NewConfig(Config{Envd: EnvdMetadata{Version: "test-" + id}, RamMB: testSandboxRamMB}),
		Runtime:        sandboxtypes.RuntimeMetadata{SandboxID: "sbx-" + id},
	}}

	return s, id
}

// envdServer answers /init with 204 and, when header is non-empty, X-Envd-Memory.
func envdServer(t *testing.T, header string) *httptest.Server {
	t.Helper()

	return envdServerStatus(t, header, http.StatusNoContent)
}

// envdServerStatus is envdServer answering with status instead of 204.
func envdServerStatus(t *testing.T, header string, status int) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if header != "" {
			w.Header().Set(envdMemoryHeader, header)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func hasAttr(set attribute.Set, key attribute.Key, want string) bool {
	v, ok := set.Value(key)

	return ok && v.Emit() == want
}

// protectionPoints reads the protection histogram's points for one envd version, by kind.
func protectionPoints(t *testing.T, version string) map[string]metricdata.HistogramDataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	out := map[string]metricdata.HistogramDataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.EnvdMemoryProtectionHistogramName) {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok, "%s is not an int64 histogram", m.Name)
			for _, dp := range h.DataPoints {
				if !hasAttr(dp.Attributes, telemetry.WithEnvdVersion("").Key, version) {
					continue
				}
				kind, ok := dp.Attributes.Value("kind")
				require.True(t, ok, "protection point without a kind")
				_, dup := out[kind.AsString()]
				require.False(t, dup, "two %s points for one version", kind.AsString())
				out[kind.AsString()] = dp
			}
		}
	}

	return out
}

// initCallsPoints reads envd.init.calls points for one envd version.
func initCallsPoints(t *testing.T, version string) []metricdata.DataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	var out []metricdata.DataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.EnvdInitCalls) {
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

// envdInitSpan finds the one envd-init span recorded for an envd version.
func envdInitSpan(t *testing.T, version string) map[attribute.Key]attribute.Value {
	t.Helper()

	var found []sdktrace.ReadOnlySpan
	for _, sp := range testSpanRecorder.Ended() {
		if sp.Name() != "envd-init" {
			continue
		}
		for _, kv := range sp.Attributes() {
			if kv.Key == telemetry.WithEnvdVersion("").Key && kv.Value.Emit() == version {
				found = append(found, sp)
			}
		}
	}
	require.Len(t, found, 1, "expected exactly one envd-init span for %s", version)

	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range found[0].Attributes() {
		attrs[kv.Key] = kv.Value
	}

	return attrs
}

// decodeWarnings counts the malformed-header warnings logged for one sandbox.
func decodeWarnings(sandboxID string) int {
	n := 0
	for _, e := range testLogObserver.FilterMessage("could not decode the envd memory protection header").All() {
		for _, f := range e.Context {
			if f.String == sandboxID {
				n++
			}
		}
	}

	return n
}

const (
	protectedHeader   = `{"request":134217728,"low":268435456,"floor":134217728,"partial":false}`
	unprotectedHeader = `{"request":52428800,"low":0,"floor":0,"partial":false}`
	partialHeader     = `{"request":0,"low":0,"floor":0,"partial":true}`
	unboundedHeader   = `{"request":9223372036854775807,"low":9223372036854775807,"floor":9223372036854775807,"partial":false}`
)

// TestInitEnvdRecordsMemoryProtection drives the real initEnvd against an envd double
// and pins what each /init records: the protection histogram and the cohort attribute
// only on the first WaitForEnvd of a start (recordMetrics), the span
// attributes on every call, and nothing at all for an absent or malformed header.
//
// Not parallel: overrides the package-level sandboxHttpClient, as the sibling init tests do.
func TestInitEnvdRecordsMemoryProtection(t *testing.T) { //nolint:paralleltest
	orig := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: 5 * time.Second}
	t.Cleanup(func() { sandboxHttpClient = orig })

	versionKey := telemetry.WithEnvdVersion("").Key

	run := func(t *testing.T, header string, startType StartType, recordMetrics bool) (*Sandbox, string) {
		t.Helper()

		srv := envdServer(t, header)
		s, id := newMemoryTestSandbox(t, srv.URL)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, s.initEnvd(ctx, startType, recordMetrics))

		return s, id
	}

	t.Run("protected chain on the first start", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, protectedHeader, StartTypeResume, true)
		version := "test-" + id

		points := protectionPoints(t, version)
		require.Len(t, points, 2, "kind=request and kind=floor")
		for kind, want := range map[string]int64{"request": 128, "floor": 128} {
			dp := points[kind]
			assert.Equal(t, uint64(1), dp.Count, "%s: one sample per start", kind)
			assert.Equal(t, want, dp.Sum, "%s in MiB", kind)
			// Exactly the enumerated attributes: nothing per sandbox, team or template.
			assert.Equal(t, 3, dp.Attributes.Len(), "%s attributes: %v", kind, dp.Attributes.ToSlice())
			assert.True(t, hasAttr(dp.Attributes, "start_type", "resume"))
			assert.True(t, hasAttr(dp.Attributes, versionKey, version))
		}

		calls := initCallsPoints(t, version)
		require.Len(t, calls, 1, "one success point, no transient")
		assert.True(t, hasAttr(calls[0].Attributes, "exit_type", "success"))
		assert.True(t, hasAttr(calls[0].Attributes, "protection", "protected"), "init.calls carries the cohort: %v", calls[0].Attributes.ToSlice())

		span := envdInitSpan(t, version)
		assert.Equal(t, int64(134217728), span["envd.memory.request"].AsInt64())
		assert.Equal(t, int64(268435456), span["envd.memory.low"].AsInt64())
		assert.Equal(t, int64(134217728), span["envd.memory.floor"].AsInt64())
		assert.False(t, span["envd.memory.partial"].AsBool())

		assert.Equal(t, []attribute.KeyValue{attribute.String("protection", "protected")}, s.envdProtectionAttrs())
		assert.Contains(t, s.waitForEnvdDurationAttrs(StartTypeResume, nil), attribute.String("protection", "protected"),
			"the duration histogram is recorded with the same cohort")
	})

	t.Run("a floor of zero is counted, as unprotected", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, unprotectedHeader, StartTypeResume, true)
		version := "test-" + id

		points := protectionPoints(t, version)
		require.Len(t, points, 2)
		assert.Equal(t, uint64(1), points["floor"].Count, "the zero is a sample, not an absence")
		assert.Equal(t, int64(0), points["floor"].Sum)
		assert.Equal(t, int64(50), points["request"].Sum)

		calls := initCallsPoints(t, version)
		require.Len(t, calls, 1)
		assert.True(t, hasAttr(calls[0].Attributes, "protection", "unprotected"))
		assert.Equal(t, []attribute.KeyValue{attribute.String("protection", "unprotected")}, s.envdProtectionAttrs())
	})

	t.Run("a partial read is recorded with its zeros and cohorted as unknown", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, partialHeader, StartTypeCreate, true)
		version := "test-" + id

		points := protectionPoints(t, version)
		require.Len(t, points, 2)
		assert.Equal(t, int64(0), points["floor"].Sum)
		assert.True(t, hasAttr(points["floor"].Attributes, "start_type", "create"))
		assert.True(t, envdInitSpan(t, version)["envd.memory.partial"].AsBool())

		// The zeros are what envd could not read, so they must not be counted as a
		// chain that was read and found unprotected.
		calls := initCallsPoints(t, version)
		require.Len(t, calls, 1)
		assert.True(t, hasAttr(calls[0].Attributes, "protection", "unknown"),
			"init.calls carries the unknown cohort: %v", calls[0].Attributes.ToSlice())
		assert.Equal(t, []attribute.KeyValue{attribute.String("protection", "unknown")}, s.envdProtectionAttrs())
		assert.Contains(t, s.waitForEnvdDurationAttrs(StartTypeCreate, nil), attribute.String("protection", "unknown"))
	})

	t.Run("a re-check records no metric but keeps the span attributes", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, protectedHeader, StartTypeResume, false)
		version := "test-" + id

		assert.Empty(t, protectionPoints(t, version), "the histogram follows the init.calls predicate")
		assert.Empty(t, initCallsPoints(t, version))
		assert.Equal(t, int64(134217728), envdInitSpan(t, version)["envd.memory.floor"].AsInt64())
		assert.Equal(t, []attribute.KeyValue{attribute.String("protection", "protected")}, s.envdProtectionAttrs(),
			"the report is retained even when nothing is recorded")
	})

	t.Run("an envd predating the header is counted as nothing", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, "", StartTypeResume, true)
		version := "test-" + id

		assert.Empty(t, protectionPoints(t, version))
		calls := initCallsPoints(t, version)
		require.Len(t, calls, 1, "init.calls is still recorded")
		_, has := calls[0].Attributes.Value("protection")
		assert.False(t, has, "no cohort attribute rather than a guessed one: %v", calls[0].Attributes.ToSlice())
		_, has = envdInitSpan(t, version)["envd.memory.floor"]
		assert.False(t, has)
		assert.Nil(t, s.envdProtectionAttrs())
		assert.NotContains(t, s.waitForEnvdDurationAttrs(StartTypeResume, nil), attribute.String("protection", "unprotected"))
		assert.Zero(t, decodeWarnings("sbx-"+id))
	})

	// envd sets the header before any WriteHeader, so a start that fails on the status still
	// reports its chain; the report is recorded and labels the failed start's duration.
	t.Run("a non-204 that carries the header is still the start's report", func(t *testing.T) { //nolint:paralleltest
		srv := envdServerStatus(t, protectedHeader, http.StatusServiceUnavailable)
		s, id := newMemoryTestSandbox(t, srv.URL)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err := s.initEnvd(ctx, StartTypeResume, true)
		require.ErrorContains(t, err, "unexpected status code")
		version := "test-" + id

		require.Len(t, protectionPoints(t, version), 2, "the chain's configuration is a fact whatever the start's outcome")
		assert.Equal(t, []attribute.KeyValue{attribute.String("protection", "protected")}, s.envdProtectionAttrs())
		assert.Contains(t, s.waitForEnvdDurationAttrs(StartTypeResume, err), attribute.String("protection", "protected"),
			"the failed start's duration carries its cohort")

		// init.calls counts one per start that answered, whatever it answered with, so
		// this start is a success there while its duration says exit_type=other. The
		// two disagree by design and the disagreement is pinned here rather than left
		// to be rediscovered from a dashboard.
		calls := initCallsPoints(t, version)
		require.Len(t, calls, 1, "one point: the 503 was not retried")
		assert.True(t, hasAttr(calls[0].Attributes, "exit_type", "success"),
			"init.calls counts a responded start: %v", calls[0].Attributes.ToSlice())
		assert.True(t, hasAttr(calls[0].Attributes, "protection", "protected"))
		assert.True(t, hasAttr(
			attribute.NewSet(s.waitForEnvdDurationAttrs(StartTypeResume, err)...), "exit_type", "other"),
			"the same start's duration is not a success")
	})

	// An unbounded chain is protected, and its magnitude reaches the histogram capped at
	// the sandbox's RAM: uncapped, the 8.8e12 MiB sample would drop every other sample in
	// the series to ~19% buckets. The raw byte count stays on the span.
	t.Run("an unbounded chain caps at the sandbox's RAM", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, unboundedHeader, StartTypeResume, true)
		version := "test-" + id

		points := protectionPoints(t, version)
		require.Len(t, points, 2)
		for _, kind := range []string{"request", "floor"} {
			assert.Equal(t, int64(testSandboxRamMB), points[kind].Sum,
				"%s is capped at the sandbox's RAM, not recorded as 8796093022207 MiB", kind)
		}

		assert.Equal(t, int64(math.MaxInt64), envdInitSpan(t, version)["envd.memory.request"].AsInt64(),
			"the span keeps the exact value the guest reported")
		assert.Equal(t, []attribute.KeyValue{attribute.String("protection", "protected")}, s.envdProtectionAttrs())
	})

	t.Run("a malformed header warns and records nothing", func(t *testing.T) { //nolint:paralleltest
		s, id := run(t, `{"floor":-1}`, StartTypeResume, true)
		version := "test-" + id

		assert.Empty(t, protectionPoints(t, version))
		calls := initCallsPoints(t, version)
		require.Len(t, calls, 1)
		_, has := calls[0].Attributes.Value("protection")
		assert.False(t, has)
		_, has = envdInitSpan(t, version)["envd.memory.floor"]
		assert.False(t, has)
		assert.Nil(t, s.envdProtectionAttrs())
		assert.Equal(t, 1, decodeWarnings("sbx-"+id))
	})
}
