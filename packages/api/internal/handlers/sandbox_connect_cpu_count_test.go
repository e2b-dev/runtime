package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

// cpuCount, like memoryMB, applies only when the connect resumes the sandbox:
// a running sandbox keeps its count whatever count is named, nothing is
// checked, not even whether the team may choose a count, and a named count is
// reported ignored.
func TestConnectV2_CPUCountOnARunningSandboxIsIgnored(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		resizable bool
		requested *api.CPUCount
	}{
		{name: "no count", resizable: true},
		{name: "its own count", resizable: true, requested: new(api.CPUCount(2))},
		{name: "a larger count", resizable: true, requested: new(api.CPUCount(64))},
		{name: "resizable templates off", requested: new(api.CPUCount(4))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			teamID := uuid.New()
			sandboxID := "i" + id.Generate()
			stub := &keepAliveRecorder{sbx: sandbox.Sandbox{
				SandboxID: sandboxID, TeamID: teamID, State: sandbox.StateRunning,
				StartTime: time.Now(), EndTime: time.Now().Add(time.Hour),
				VCpu: 2, RamMB: 2048, MemoryMB: 4096,
			}}
			starts := &startRecorder{}
			store := &APIStore{featureFlags: connectMemoryFlags(t, tt.resizable), connectBackendOverride: stub, createBackendOverride: starts, templateSpawnCounter: testSpawnCounter(t)}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{CpuCount: tt.requested})

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			assert.Equal(t, 1, stub.calls, "the timeout is extended once")
			assert.Zero(t, starts.calls, "a running sandbox is not started")
			wantIgnored := 0
			if tt.requested != nil {
				wantIgnored = 1
			}
			assert.Equal(t, wantIgnored, stub.cpuIgnored, "a named count is reported ignored")
			assert.Zero(t, stub.ignored, "no memory size was named")
			requireSandboxShape(t, recorder, 2, 4096)
		})
	}
}

// A connect refuses a cpuCount the paused snapshot or the team can never run
// before it reserves the start, as resume does, so the refused count never
// holds the start and no request joined to it has to start the sandbox again.
func TestConnectV2_CPUCountRefusedBeforeTheStart(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		resizable bool
		maxVcpu   *int64
		coldBoot  bool
		requested api.CPUCount
		wantCode  string
	}{
		{name: "fixed-size snapshot", resizable: true, requested: 4, wantCode: "sandbox_cpu_count_not_resizable"},
		{name: "feature off", maxVcpu: new(int64(4)), requested: 4, wantCode: "sandbox_cpu_count_disabled"},
		{name: "above the maximum", resizable: true, maxVcpu: new(int64(4)), requested: 6, wantCode: "sandbox_cpu_count_out_of_range"},
		{name: "above the team limit", resizable: true, maxVcpu: new(int64(16)), requested: 12, wantCode: "sandbox_cpu_count_team_limit"},
		{name: "cold boot", resizable: true, coldBoot: true, requested: 4, wantCode: "sandbox_cpu_count_not_resizable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, teamID, sandboxID := newPausedSnapshotStore(t, tt.resizable, nil, 0, currentEnvd, tt.maxVcpu)
			starts := &startRecorder{}
			store.createBackendOverride, store.templateSpawnCounter = starts, testSpawnCounter(t)
			body := api.PostV2SandboxesSandboxIDConnectJSONRequestBody{CpuCount: &tt.requested}
			if tt.coldBoot {
				body.Memory = new(false)
			}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, body)

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			requireSizeRefused(t, recorder, tt.wantCode)
			assert.Zero(t, starts.calls, "a refused count never takes the start")
		})
	}
}

// A count the paused snapshot can run goes to the start with the connect's
// options, and the answer reports the sandbox's shape.
func TestConnectV2_CPUCountAcceptedGoesToTheStart(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		coldBoot bool
	}{
		{name: "memory restore"},
		{name: "cold boot", coldBoot: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, teamID, sandboxID := newPausedSnapshotStore(t, true, nil, 0, currentEnvd, new(int64(4)))
			starts := &startRecorder{}
			store.createBackendOverride, store.templateSpawnCounter = starts, testSpawnCounter(t)
			body := api.PostV2SandboxesSandboxIDConnectJSONRequestBody{CpuCount: new(api.CPUCount(4))}
			if tt.coldBoot {
				body.Memory = new(false)
			}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, body)

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
			require.Equal(t, 1, starts.calls)
			assert.Equal(t, tt.coldBoot, starts.coldBoot)
			// WithMemoryMB, WithCPUCount and Connect. This counts the options; the
			// orchestrator tests pin what the count does at the start.
			assert.Equal(t, 3, starts.options)
			requireSandboxShape(t, recorder, 2, 2048)
		})
	}
}

// requireSandboxShape checks that a connect answers with the vCPUs and memory
// of the sandbox it returns.
func requireSandboxShape(t *testing.T, recorder *httptest.ResponseRecorder, cpuCount api.CPUCount, memoryMB api.MemoryMB) {
	t.Helper()

	var sbx api.Sandbox
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &sbx))
	require.NotNil(t, sbx.CpuCount)
	require.NotNil(t, sbx.MemoryMB)
	assert.Equal(t, cpuCount, *sbx.CpuCount)
	assert.Equal(t, memoryMB, *sbx.MemoryMB)
}
