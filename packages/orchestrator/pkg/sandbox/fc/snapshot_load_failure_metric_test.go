//go:build linux

package fc

import (
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type stubSnapfile string

func (s stubSnapfile) Path() string { return string(s) }
func (s stubSnapfile) Close() error { return nil }

// The same refusal, as Firecracker words it, replayed by the stub.
const msrFaultResponse = `{"fault_message":"Load snapshot error: Failed to restore from snapshot: Failed to build microVM from snapshot: Failed to restore vCPUs: Failed to run action on vcpu: Failed to set all KVM MSRs for this vCPU. Only a partial write was done."}`

// The classifier is unit-tested on its own; this covers the wiring instead —
// that a refusal on the real loadSnapshot path reaches the counter, under the
// reason the fault deserves, with the attribute key an alert will group by. A
// dropped Add or a renamed attribute is invisible in the classifier's tests
// and would only show up as an alert that never fires.
//
//nolint:paralleltest // otel.SetMeterProvider mutates process state
func TestLoadSnapshotRecordsRefusal(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	// Not t.TempDir(): its path embeds the full test name and overflows the
	// 108-char unix sun_path limit.
	dir, err := os.MkdirTemp("", "fc") //nolint:usetesting // t.TempDir embeds the test name and overflows the unix sun_path limit
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := dir + "/fc.sock"
	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /snapshot/load", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(msrFaultResponse))
	})
	server := &http.Server{Handler: mux} //nolint:gosec // no timeouts needed for a stub on a private socket
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	// uffdReady is never closed: the load fails before it is waited on.
	err = newApiClient(socket).loadSnapshot(t.Context(), dir+"/uffd.sock", make(chan struct{}), stubSnapfile(dir+"/snapfile"), false, false, "abc123")
	require.Error(t, err)

	assert.Equal(t, int64(1), counterValue(t, reader, string(telemetry.SandboxFCSnapshotLoadFailures), "reason", string(snapshotLoadVcpuMSR)))
	assert.Equal(t, int64(1), counterValue(t, reader, string(telemetry.SandboxFCSnapshotLoadFailures), "cpu_template", "abc123"))
}

// counterValue sums the data points of one counter carrying the given
// attribute, and fails when the counter was never recorded at all.
func counterValue(t *testing.T, reader sdkmetric.Reader, name, attrKey, attrValue string) int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			if recorded.Name != name {
				continue
			}

			sum, ok := recorded.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "%s is not an int64 sum: %T", name, recorded.Data)

			var total int64
			for _, point := range sum.DataPoints {
				if value, found := point.Attributes.Value(attribute.Key(attrKey)); found && value.Emit() == attrValue {
					total += point.Value
				}
			}

			return total
		}
	}

	t.Fatalf("%s was never recorded", name)

	return 0
}
