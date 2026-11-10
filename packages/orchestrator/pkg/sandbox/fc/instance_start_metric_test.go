//go:build linux

package fc

import (
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// A stalled boot is only visible if startVM records the answer it got,
// whichever it was, under the success attribute a slow-boot alert splits by.
//
//nolint:paralleltest // otel.SetMeterProvider mutates process state
func TestStartVMRecordsInstanceStartDuration(t *testing.T) {
	reader := sharedMeterReader()
	before := instanceStartCounts(t, reader)

	for _, status := range []int{http.StatusNoContent, http.StatusBadRequest} {
		socket := stubInstanceStart(t, status)
		err := newApiClient(socket).startVM(t.Context())
		if status == http.StatusNoContent {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}

	after := instanceStartCounts(t, reader)
	assert.Equal(t, uint64(1), after[true]-before[true], "successful starts")
	assert.Equal(t, uint64(1), after[false]-before[false], "refused starts")
}

// stubInstanceStart serves PUT /actions with status on a fresh unix socket.
func stubInstanceStart(t *testing.T, status int) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "fc") //nolint:usetesting // t.TempDir embeds the test name and overflows the unix sun_path limit
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := dir + "/fc.sock"
	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /actions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status != http.StatusNoContent {
			_, _ = w.Write([]byte(`{"fault_message":"refused"}`))
		}
	})
	server := &http.Server{Handler: mux} //nolint:gosec // no timeouts needed for a stub on a private socket
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	return socket
}

// instanceStartCounts is the InstanceStart histogram's sample count by success.
func instanceStartCounts(t *testing.T, reader sdkmetric.Reader) map[bool]uint64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	counts := map[bool]uint64{}
	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			if recorded.Name != string(telemetry.SandboxFCInstanceStartDuration) {
				continue
			}
			require.NotEmpty(t, recorded.Description)
			require.NotEmpty(t, recorded.Unit)

			hist, ok := recorded.Data.(metricdata.Histogram[int64])
			require.Truef(t, ok, "not an int64 histogram: %T", recorded.Data)
			for _, point := range hist.DataPoints {
				success, _ := point.Attributes.Value(attribute.Key("success"))
				counts[success.AsBool()] += point.Count
			}
		}
	}

	return counts
}
