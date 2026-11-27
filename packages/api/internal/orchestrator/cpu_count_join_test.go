package orchestrator

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

// newCPUJoinTestOrchestrator is newJoinTestOrchestrator with the cpuCount
// decisions it can count (ignored) counted into the returned reader.
func newCPUJoinTestOrchestrator(t *testing.T, resizable bool) (*Orchestrator, *joinSignal, *sdkmetric.ManualReader) {
	t.Helper()

	o, signal, _ := newJoinTestOrchestrator(t, resizable)
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Int64Counter("api.sandbox.cpu_count_override")
	require.NoError(t, err)
	o.cpuCountOverrideCounter = counter

	return o, signal, reader
}

// countJoinerRestarts counts o's api.sandbox.start_joiner_restart into the
// returned reader.
func countJoinerRestarts(t *testing.T, o *Orchestrator) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Int64Counter("api.sandbox.start_joiner_restart")
	require.NoError(t, err)
	o.joinerRestartCounter = counter

	return reader
}

// resizableCPUBuild runs 2 vCPUs and can be resized up to 4.
func resizableCPUBuild() queries.EnvBuild {
	build := testBuild()
	build.Vcpu = 2
	build.MaxVcpu = new(int64(4))

	return build
}

// A connect that joins another request's start ignores its cpuCount, whatever
// the count and whether the team may choose one: never a 400 or a 409 for a
// count that does not apply. (The connect handler refuses a count the snapshot
// or the team cannot take before it gets here; this pins the start's own rule.) It gets the sandbox that start produced, at that
// start's count, and counts as ignored once the start succeeded.
func TestCreateSandbox_ConnectJoinIgnoresItsCPUCount(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		resizable bool
		requested *int32
	}{
		{name: "no count", resizable: true},
		{name: "a count", resizable: true, requested: new(int32(3))},
		{name: "above the maximum", resizable: true, requested: new(int32(8))},
		{name: "resizable templates off", requested: new(int32(3))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, signal, reader := newCPUJoinTestOrchestrator(t, tt.resizable)
			team := memoryTeam()
			sandboxID := newSandboxID()
			build := resizableCPUBuild()
			h := holdStart(t.Context(), o, team, sandboxID, meta(build), nil, time.Minute)
			joiner, starter := joinHeld(t, signal, h, func() createResult {
				return resume(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(build), 2*time.Hour, false, WithCPUCount(tt.requested), Connect())
			})

			require.Nil(t, starter.apiErr)
			require.Nil(t, joiner.apiErr)
			assert.Equal(t, int64(2), joiner.sbx.VCpu, "the joined start's count, not the requested one")
			assert.WithinDuration(t, time.Now().Add(2*time.Hour), joiner.sbx.EndTime, time.Minute, "the connect's own timeout")

			wantIgnored := int64(0)
			if tt.requested != nil {
				wantIgnored = 1
			}
			assert.Equal(t, wantIgnored, metricCount(t, reader, "api.sandbox.cpu_count_override", map[string]string{"outcome": cpuCountOverrideIgnored, "route": MemoryRouteConnect}))
		})
	}
}

// A connect's cpuCount is checked only when the connect holds the start.
// Holding it, a count the snapshot or the team cannot run is refused with the
// check's code, before any node is asked.
func TestCreateSandbox_ConnectCPUCountCheckedWhenItStarts(t *testing.T) {
	t.Parallel()

	// wideBuild can be resized past memoryTeam's limit of 8 vCPUs.
	wideBuild := resizableCPUBuild()
	wideBuild.MaxVcpu = new(int64(16))
	for _, tt := range []struct {
		name      string
		resizable bool
		build     queries.EnvBuild
		requested int32
		wantCode  string
	}{
		{name: "resizable templates off", build: resizableCPUBuild(), requested: 3, wantCode: errCodeSandboxCpuCountDisabled},
		{name: "fixed-size snapshot", resizable: true, build: testBuild(), requested: 3, wantCode: errCodeSandboxCpuCountNotResizable},
		{name: "above the maximum", resizable: true, build: resizableCPUBuild(), requested: 8, wantCode: errCodeSandboxCpuCountOutOfRange},
		{name: "above the team limit", resizable: true, build: wideBuild, requested: 12, wantCode: errCodeSandboxCpuCountTeamLimit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, _, reader := newCPUJoinTestOrchestrator(t, tt.resizable)

			r := resume(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, memoryTeam(), newSandboxID(), meta(tt.build), time.Hour, false, WithCPUCount(&tt.requested), Connect())

			require.NotNil(t, r.apiErr)
			assert.Equal(t, http.StatusBadRequest, r.apiErr.Code)
			assert.Equal(t, tt.wantCode, r.apiErr.ErrorCode)
			assert.True(t, StartErrorKeepsRetryHint(r.apiErr), "a refused count keeps its no-retry hint")
			assert.Equal(t, int64(1), metricCount(t, reader, "api.sandbox.cpu_count_override", map[string]string{"outcome": cpuCountOverrideRejected, "route": MemoryRouteConnect, "error_code": tt.wantCode}))
		})
	}
}

// A connect that holds the start resumes the sandbox at its cpuCount.
func TestCreateSandbox_ConnectResumesAtItsCPUCount(t *testing.T) {
	t.Parallel()

	o, _, reader := newCPUJoinTestOrchestrator(t, true)

	r := resume(t.Context(), o, memoryTeam(), newSandboxID(), meta(resizableCPUBuild()), time.Hour, false, WithCPUCount(new(int32(3))), Connect())

	require.Nil(t, r.apiErr)
	assert.Equal(t, int64(3), r.sbx.VCpu)
	assert.Zero(t, metricCount(t, reader, "api.sandbox.cpu_count_override", map[string]string{"outcome": cpuCountOverrideIgnored}))
}

// When a start that named a cpuCount fails, a request that joined it does not
// take a failure that may be the count's: it starts the sandbox again on its
// own terms, here at the build's count.
func TestCreateSandbox_CPUSizedStartFailureRestartsItsJoiners(t *testing.T) {
	t.Parallel()

	o, signal, _ := newCPUJoinTestOrchestrator(t, true)
	restarts := countJoinerRestarts(t, o)
	team := memoryTeam()
	sandboxID := newSandboxID()
	build := resizableCPUBuild()
	h := holdStart(t.Context(), o, team, sandboxID, meta(build), nil, time.Hour, WithCPUCount(new(int32(8))), Connect())
	joiner, starter := joinHeld(t, signal, h, func() createResult {
		return resume(t.Context(), o, team, sandboxID, meta(build), time.Hour, false)
	})

	require.NotNil(t, starter.apiErr, "the sized start fails its own count check")
	assert.Equal(t, errCodeSandboxCpuCountOutOfRange, starter.apiErr.ErrorCode)
	require.Nil(t, joiner.apiErr, "the joiner starts the sandbox itself")
	assert.Equal(t, sandboxID, joiner.sbx.SandboxID)
	assert.Equal(t, int64(2), joiner.sbx.VCpu)
	assert.Equal(t, int64(1), metricCount(t, restarts, "api.sandbox.start_joiner_restart", map[string]string{"outcome": joinerRestarted}))
}

// A start that named a cpuCount and failed for any reason but the count's
// refusal hands its joiners its own error, and they do not start the sandbox
// again: the failure may have come after the sandbox was created,
// while it is still being removed.
func TestCreateSandbox_CPUSizedStartOtherFailureReachesItsJoiners(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		options []CreateOption
	}{
		{name: "resume naming cpuCount", options: []CreateOption{WithCPUCount(new(int32(3)))}},
		{name: "connect naming cpuCount", options: []CreateOption{WithCPUCount(new(int32(3))), Connect()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, signal, _ := newCPUJoinTestOrchestrator(t, true)
			team := memoryTeam()
			sandboxID := newSandboxID()
			startErr := &api.APIError{Code: http.StatusInternalServerError, ErrorCode: "test_start_failed", ClientMsg: "start failed", Err: errors.New("start failed"), Retryable: apierrors.NoRetry}
			h := holdStart(t.Context(), o, team, sandboxID, SandboxMetadata{}, startErr, time.Hour, tt.options...)
			joiner, starter := joinHeld(t, signal, h, func() createResult {
				return resume(t.Context(), o, team, sandboxID, meta(resizableCPUBuild()), time.Hour, false, Connect())
			})

			require.NotNil(t, starter.apiErr)
			require.NotNil(t, joiner.apiErr, "the joiner does not start the sandbox again")
			assert.Equal(t, "test_start_failed", joiner.apiErr.ErrorCode)
		})
	}
}

// A start that named a count or a size and found no node for it created no
// sandbox, and the cause may be the count or size: its joiners start the
// sandbox again on their own terms.
func TestCreateSandbox_SizedStartWithoutANodeRestartsItsJoiners(t *testing.T) {
	t.Parallel()

	for _, named := range []struct {
		name   string
		option CreateOption
	}{
		{name: "cpuCount", option: WithCPUCount(new(int32(4)))},
		{name: "memoryMB", option: WithMemoryMB(new(int32(2048)))},
	} {
		for _, code := range []string{errCodeCapacityUnavailable, errCodeNoCompatibleNode, errCodeFeatureUnsupported} {
			t.Run(named.name+"/"+code, func(t *testing.T) {
				t.Parallel()

				o, signal, _ := newCPUJoinTestOrchestrator(t, true)
				team := memoryTeam()
				sandboxID := newSandboxID()
				placementErr := &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: code, ClientMsg: "no node", Err: errors.New("no node")}
				h := holdStart(t.Context(), o, team, sandboxID, SandboxMetadata{}, placementErr, time.Hour, named.option, Connect())
				joiner, starter := joinHeld(t, signal, h, func() createResult {
					return resume(t.Context(), o, team, sandboxID, meta(resizableCPUBuild()), time.Hour, false)
				})

				require.NotNil(t, starter.apiErr)
				require.Nil(t, joiner.apiErr, "the joiner starts the sandbox itself")
				assert.Equal(t, int64(2), joiner.sbx.VCpu)
			})
		}
	}
}

// A refusal of a field the start did not name is not the start's own: a start
// that named only cpuCount and failed with a memory refusal hands its joiners
// that refusal, and they do not start the sandbox again.
func TestCreateSandbox_RefusalOfAnUnnamedFieldReachesItsJoiners(t *testing.T) {
	t.Parallel()

	o, signal, _ := newCPUJoinTestOrchestrator(t, true)
	team := memoryTeam()
	sandboxID := newSandboxID()
	memoryRefusal := &api.APIError{Code: http.StatusBadRequest, ErrorCode: errCodeSandboxMemoryTeamLimit, ClientMsg: "memory refused", Err: errors.New("memory refused")}
	h := holdStart(t.Context(), o, team, sandboxID, SandboxMetadata{}, memoryRefusal, time.Hour, WithCPUCount(new(int32(3))), Connect())
	joiner, starter := joinHeld(t, signal, h, func() createResult {
		return resume(t.Context(), o, team, sandboxID, meta(resizableCPUBuild()), time.Hour, false)
	})

	require.NotNil(t, starter.apiErr)
	require.NotNil(t, joiner.apiErr, "the joiner does not start the sandbox again")
	assert.Equal(t, errCodeSandboxMemoryTeamLimit, joiner.apiErr.ErrorCode)
}

// A start that named no size and found no node hands its joiners that
// failure: they would ask for the same resources.
func TestCreateSandbox_UnsizedStartWithoutANodeReachesItsJoiners(t *testing.T) {
	t.Parallel()

	o, signal, _ := newCPUJoinTestOrchestrator(t, true)
	team := memoryTeam()
	sandboxID := newSandboxID()
	placementErr := &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: errCodeCapacityUnavailable, ClientMsg: "no node", Err: errors.New("no node")}
	h := holdStart(t.Context(), o, team, sandboxID, SandboxMetadata{}, placementErr, time.Hour, Connect())
	joiner, starter := joinHeld(t, signal, h, func() createResult {
		return resume(t.Context(), o, team, sandboxID, meta(resizableCPUBuild()), time.Hour, false)
	})

	require.NotNil(t, starter.apiErr)
	require.NotNil(t, joiner.apiErr, "the joiner does not start the sandbox again")
	assert.Equal(t, errCodeCapacityUnavailable, joiner.apiErr.ErrorCode)
}

// Every way a named cpuCount is refused or lost before the start resolves it
// is counted under its route, with its code: the early feature-off refusal of a
// resume, a resume that meets a start in flight, and a connect whose joined
// start failed.
func TestCreateSandbox_CPUCountOutcomesCarryRouteAndCode(t *testing.T) {
	t.Parallel()

	t.Run("feature off, refused before the start", func(t *testing.T) {
		t.Parallel()

		o, _, reader := newCPUJoinTestOrchestrator(t, false)
		ctx := WithMemoryRoute(t.Context(), MemoryRouteResume)

		r := resume(ctx, o, memoryTeam(), newSandboxID(), meta(resizableCPUBuild()), time.Hour, false, WithCPUCount(new(int32(3))))

		require.NotNil(t, r.apiErr)
		assert.Equal(t, int64(1), metricCount(t, reader, "api.sandbox.cpu_count_override", map[string]string{"outcome": cpuCountOverrideRejected, "route": MemoryRouteResume, "error_code": errCodeSandboxCpuCountDisabled}))
	})

	t.Run("resume meets a start in flight", func(t *testing.T) {
		t.Parallel()

		o, signal, reader := newCPUJoinTestOrchestrator(t, true)
		team := memoryTeam()
		sandboxID := newSandboxID()
		h := holdStart(t.Context(), o, team, sandboxID, meta(resizableCPUBuild()), nil, time.Hour)
		joiner, starter := joinHeld(t, signal, h, func() createResult {
			return resume(WithMemoryRoute(t.Context(), MemoryRouteResume), o, team, sandboxID, meta(resizableCPUBuild()), time.Hour, false, WithCPUCount(new(int32(3))))
		})

		require.Nil(t, starter.apiErr)
		require.NotNil(t, joiner.apiErr)
		assert.Equal(t, int64(1), metricCount(t, reader, "api.sandbox.cpu_count_override", map[string]string{"outcome": cpuCountOverrideRejected, "route": MemoryRouteResume, "error_code": ErrCodeStartInFlight}))
	})

	t.Run("connect joins a start that fails", func(t *testing.T) {
		t.Parallel()

		o, signal, reader := newCPUJoinTestOrchestrator(t, true)
		team := memoryTeam()
		sandboxID := newSandboxID()
		startErr := &api.APIError{Code: http.StatusInternalServerError, ErrorCode: "test_start_failed", ClientMsg: "start failed", Err: errors.New("start failed")}
		h := holdStart(t.Context(), o, team, sandboxID, SandboxMetadata{}, startErr, time.Hour)
		joiner, starter := joinHeld(t, signal, h, func() createResult {
			return resume(WithMemoryRoute(t.Context(), MemoryRouteConnect), o, team, sandboxID, meta(resizableCPUBuild()), time.Hour, false, WithCPUCount(new(int32(3))), Connect())
		})

		require.NotNil(t, starter.apiErr)
		require.NotNil(t, joiner.apiErr)
		assert.Equal(t, int64(1), metricCount(t, reader, "api.sandbox.cpu_count_override", map[string]string{"outcome": cpuCountOverrideFailed, "route": MemoryRouteConnect, "error_code": "test_start_failed"}))
	})
}
