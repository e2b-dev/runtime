package server

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func TestCreate_RefusesInvalidVcpuLimits(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]*orchestrator.SandboxConfig{
		"zero vcpu without maximum":     {Vcpu: 0},
		"negative vcpu without maximum": {Vcpu: -1},
		"maximum below vcpu":            {Vcpu: 4, MaxVcpus: 2},
		"maximum above firecracker's":   {Vcpu: 2, MaxVcpus: 33},
		"vcpu above firecracker's":      {Vcpu: 33},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := duplicateCreateTestServer()
			cfg.SandboxId = "sbx-vcpu"
			cfg.ExecutionId = "exec-vcpu"
			cfg.Snapshot = true

			_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{Sandbox: cfg})
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, st.Code())
			// The refusal is for good on every node, which the detail says;
			// a bad request is the client's fault.
			refusal := createRefusalDetail(t, st)
			assert.Equal(t, st.Message(), refusal.GetMessage())
			assert.EqualValues(t, http.StatusBadRequest, refusal.GetHttpStatus())
			assert.Equal(t, 0, s.sandboxFactory.Sandboxes.Count(), "nothing is reserved for a refused request")
		})
	}
}

// createRefusalDetail returns the SANDBOX_CREATE_REFUSED detail the status
// carries, failing the test when it carries none.
func createRefusalDetail(t *testing.T, st *status.Status) *orchestrator.UserError {
	t.Helper()

	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED {
			return userErr
		}
	}
	require.FailNow(t, "the refusal carries no SANDBOX_CREATE_REFUSED detail", "%v", st.Err())

	return nil
}

// A detail that cannot be attached (a status with code OK takes none) is
// logged with the refusal rather than dropped silently; the status still
// answers.
//
//nolint:paralleltest // replaces the global logger to capture what is logged
func TestCreateRefused_LogsADetailItCannotAttach(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	err := createRefused(t.Context(), codes.OK, "vcpu %d must be at least 1", 0)
	require.NoError(t, err, "a status with code OK is no error")
	logged := logs.FilterMessage("failed to attach the create refusal detail").All()
	require.Len(t, logged, 1)
	assert.Equal(t, "vcpu 0 must be at least 1", logged[0].ContextMap()["refusal"])

	refused := createRefused(t.Context(), codes.InvalidArgument, "vcpu %d must be at least 1", 0)
	st, ok := status.FromError(refused)
	require.True(t, ok)
	assert.EqualValues(t, http.StatusBadRequest, createRefusalDetail(t, st).GetHttpStatus())
	require.Len(t, logs.All(), 1, "a detail that attached was logged as a failure")
}

func TestValidateColdBootVcpus(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateColdBootVcpus(3, 4, 0), "the active target may be odd when the VM is even-sized")
	require.NoError(t, validateColdBootVcpus(2, 2, 4), "an explicit maximum may shrink the VM")
	require.NoError(t, validateColdBootVcpus(2, 0, 4), "an omitted maximum inherits the recorded capacity")
	if runtime.GOARCH != "arm64" {
		assert.Equal(t, codes.InvalidArgument, status.Code(validateColdBootVcpus(2, 3, 4)), "validate the explicit VM size, not the old one")
		assert.Equal(t, codes.InvalidArgument, status.Code(validateColdBootVcpus(2, 3, 0)))
		assert.Equal(t, codes.InvalidArgument, status.Code(validateColdBootVcpus(3, 0, 0)))
	}
}

// A region the start cannot carry is refused for good, with its reason named in the refusal and
// counted, so a firecracker-versions line below the hotplug release is an alertable signal.
func TestMemoryHotplugRefusalIsNamedAndCounted(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server").Int64Counter("refused")
	require.NoError(t, err)
	s := &Server{memoryHotplugRefused: counter}

	refusal := s.memoryHotplugRefusal(t.Context(), fmt.Errorf("resume: %w", fc.ErrMemoryHotplugFirecracker), sandboxtypes.RuntimeMetadata{})
	require.Error(t, refusal)
	st := status.Convert(refusal)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "memory hotplug refused (firecracker)")
	createRefusalDetail(t, st)

	_, ok := s.countMemoryHotplugRefusal(t.Context(), fmt.Errorf("boot: %w", fc.ErrMemoryHotplugKernel), memoryHotplugPathReboot)
	assert.True(t, ok)
	require.NoError(t, s.memoryHotplugRefusal(t.Context(), errors.New("network slot unavailable"), sandboxtypes.RuntimeMetadata{}))
	assert.False(t, guestCannotRun(fc.ErrMemoryHotplugFirecracker), "the hotplug refusal has its own branch")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				reason, _ := dp.Attributes.Value("reason")
				path, _ := dp.Attributes.Value("path")
				got[reason.AsString()+"/"+path.AsString()] += dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"firecracker/start": 1, "kernel/reboot": 1}, got)
}
