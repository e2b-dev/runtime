//go:build linux

package fc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/testutils"
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
	reader := sharedMeterReader()

	dir := testutils.SocketDir(t)

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
	err = newApiClient(socket).loadSnapshot(t.Context(), dir+"/uffd.sock", make(chan struct{}), stubSnapfile(dir+"/snapfile"), false, false, false, nil, "abc123", "team-1")
	require.Error(t, err)

	assert.Equal(t, int64(1), counterValue(t, reader, string(telemetry.SandboxFCSnapshotLoadFailures), "reason", string(snapshotLoadVcpuMSR)))
	assert.Equal(t, int64(1), counterValue(t, reader, string(telemetry.SandboxFCSnapshotLoadFailures), "cpu_template", "abc123"))
}

// The memory handler exiting cancels the start while Firecracker is still
// loading. The HTTP client has to hand the cancel's cause on as an error, not
// only its text, or the load is counted as a transport failure.
//
//nolint:paralleltest // otel.SetMeterProvider mutates process state
func TestLoadSnapshotRecordsUffdExitDuringLoad(t *testing.T) {
	reader := sharedMeterReader()

	dir := testutils.SocketDir(t)

	socket := dir + "/fc.sock"
	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	loading := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /snapshot/load", func(_ http.ResponseWriter, r *http.Request) {
		// The server notices the client leaving only once the body is read.
		_, _ = io.Copy(io.Discard, r.Body)
		close(loading)
		<-r.Context().Done()
	})
	server := &http.Server{Handler: mux} //nolint:gosec // no timeouts needed for a stub on a private socket
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	go func() {
		<-loading
		mmapErr := fmt.Errorf("mmap memfd: %w", block.GuestMemoryError{Err: unix.ENOMEM})
		cancel(UffdExitError{Err: errors.Join(fmt.Errorf("failed to wrap memfd: %w", mmapErr))})
	}()

	err = newApiClient(socket).loadSnapshot(ctx, dir+"/uffd.sock", make(chan struct{}), stubSnapfile(dir+"/snapfile"), false, false, false, nil, "", "team-1")
	require.ErrorIs(t, err, unix.ENOMEM)

	assert.Equal(t, int64(1), counterValue(t, reader, string(telemetry.SandboxFCSnapshotLoadFailures), "reason", string(snapshotLoadGuestMemory)))
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
