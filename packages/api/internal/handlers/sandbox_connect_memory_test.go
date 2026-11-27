package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func connectMemoryRequest(t *testing.T, teamID uuid.UUID, sandboxID string, body api.PostV2SandboxesSandboxIDConnectJSONRequestBody) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/sandboxes/"+sandboxID+"/connect", bytes.NewReader(raw))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: teamID, Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24, MaxVcpu: 8, MaxRamMb: 16384},
	})

	return recorder, ginCtx
}

// memoryMB, like memory, applies only when the connect resumes the sandbox: a
// running sandbox keeps its size whatever size is named, the connect extends its
// timeout as a connect without a size does, nothing is checked or read, not
// even whether the team may choose a size, and a named size is reported ignored.
func TestConnectV2_MemoryOnARunningSandboxIsIgnored(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		resizable bool
		requested *api.MemoryMB
	}{
		{name: "no size", resizable: true},
		{name: "its own size", resizable: true, requested: new(api.MemoryMB(4096))},
		{name: "a larger size", resizable: true, requested: new(api.MemoryMB(6144))},
		{name: "a smaller size", resizable: true, requested: new(api.MemoryMB(2048))},
		{name: "resizable templates off", requested: new(api.MemoryMB(6144))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			teamID := uuid.New()
			sandboxID := "i" + id.Generate()
			stub := &keepAliveRecorder{sbx: sandbox.Sandbox{
				SandboxID: sandboxID, TeamID: teamID, State: sandbox.StateRunning,
				StartTime: time.Now(), EndTime: time.Now().Add(time.Hour),
				RamMB: 2048, MemoryMB: 4096,
			}}
			starts := &startRecorder{}
			store := &APIStore{featureFlags: connectMemoryFlags(t, tt.resizable), connectBackendOverride: stub, createBackendOverride: starts, templateSpawnCounter: testSpawnCounter(t)}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{MemoryMB: tt.requested})

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			assert.Equal(t, 1, stub.calls, "the timeout is extended once")
			assert.Zero(t, starts.calls, "a running sandbox is not started")
			wantIgnored := 0
			if tt.requested != nil {
				wantIgnored = 1
			}
			assert.Equal(t, wantIgnored, stub.ignored, "a named size is reported ignored")
		})
	}
}

func connectMemoryFlags(t *testing.T, resizable bool) *featureflags.Client {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).ValueForAll(ldvalue.Bool(resizable)))
	source.Update(source.Flag(featureflags.FsOnlyResumeAPIFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(t.Context())) })

	return flags
}

// newPausedSnapshotStore seeds a paused sandbox booted with 2048 MiB and 2
// vCPUs whose build can grow to maxRAMMB (fixed when nil) and be resized up to
// maxVcpu vCPUs (fixed when nil), was paused at pausedMB (unknown when 0) and
// baked envdVersion, and returns a store whose connect reads it.
func newPausedSnapshotStore(t *testing.T, resizable bool, maxRAMMB *int64, pausedMB int64, envdVersion string, maxVcpu *int64) (*APIStore, uuid.UUID, string) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	sandboxID := "i" + id.Generate()
	totalDiskSize := int64(1024)
	row, err := db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
		TemplateID:         id.Generate(),
		TeamID:             teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     testutils.CreateTestTemplate(t, db, teamID),
		StartedAt:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:               2,
		RamMb:              2048,
		TotalDiskSizeMb:    &totalDiskSize,
		Metadata:           dbtypes.JSONBStringMap{},
		KernelVersion:      "6.1.0",
		FirecrackerVersion: "1.4.0",
		EnvdVersion:        &envdVersion,
		OriginNodeID:       "test-node",
		StatusGroup:        dbtypes.BuildStatusGroupReady,
		Config:             &dbtypes.PausedSandboxConfig{},
		StartMemoryMb:      startMemoryMB(pausedMB),
	})
	require.NoError(t, err)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.env_builds SET max_ram_mb = $1, max_vcpu = $2 WHERE id = $3", maxRAMMB, maxVcpu, row.BuildID))

	return &APIStore{
		snapshotCache:          snapshotcache.NewSnapshotCache(db.SqlcClient, redis_utils.SetupInstance(t)),
		featureFlags:           connectMemoryFlags(t, resizable),
		connectBackendOverride: notFoundResumeBackend{},
	}, teamID, sandboxID
}

const currentEnvd = "0.10.202611200046-e581f4205ef"

// A connect refuses a size the paused snapshot or the team can never take
// before it reserves the start, as resume does, so the refused size never
// holds the start and no request joined to it has to start the sandbox again.
func TestConnectV2_MemoryRefusedBeforeTheStart(t *testing.T) {
	t.Parallel()

	grows := new(int64(8192))
	for _, tt := range []struct {
		name      string
		resizable bool
		maxRAMMB  *int64
		pausedMB  int64
		envd      string
		coldBoot  bool
		requested api.MemoryMB
		wantCode  string
	}{
		{name: "fixed-size snapshot", resizable: true, envd: currentEnvd, requested: 4096, wantCode: "sandbox_memory_not_resizable"},
		{name: "feature off", maxRAMMB: grows, envd: currentEnvd, requested: 4096, wantCode: "sandbox_memory_disabled"},
		{name: "above the maximum", resizable: true, maxRAMMB: grows, envd: currentEnvd, requested: 9216, wantCode: "sandbox_memory_out_of_range"},
		{name: "shrink below the paused size", resizable: true, maxRAMMB: grows, pausedMB: 4096, envd: currentEnvd, requested: 3072, wantCode: "sandbox_memory_shrink_unsupported"},
		{name: "cold boot shrink below the paused size", resizable: true, maxRAMMB: grows, pausedMB: 4096, envd: currentEnvd, coldBoot: true, requested: 3072, wantCode: "sandbox_memory_shrink_unsupported"},
		{name: "growth on an outdated envd", resizable: true, maxRAMMB: grows, envd: "0.10.2", requested: 4096, wantCode: "sandbox_memory_envd_outdated"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, teamID, sandboxID := newPausedSnapshotStore(t, tt.resizable, tt.maxRAMMB, tt.pausedMB, tt.envd, nil)
			starts := &startRecorder{}
			store.createBackendOverride, store.templateSpawnCounter = starts, testSpawnCounter(t)
			body := api.PostV2SandboxesSandboxIDConnectJSONRequestBody{MemoryMB: &tt.requested}
			if tt.coldBoot {
				body.Memory = new(false)
			}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, body)

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			requireSizeRefused(t, recorder, tt.wantCode)
			assert.Zero(t, starts.calls, "a refused size never takes the start")
		})
	}
}

// testSpawnCounter is a template spawn counter that never flushes during a test.
func testSpawnCounter(t *testing.T) *utils.TemplateSpawnCounter {
	t.Helper()

	return utils.NewTemplateSpawnCounter(t.Context(), time.Hour, nil)
}

// startRecorder stands in for the orchestrator's start and records what it was
// asked for. The memory decisions the options carry are the orchestrator's,
// tested there.
type startRecorder struct {
	calls    int
	isResume bool
	coldBoot bool
	timeout  time.Duration
	options  int
}

func (r *startRecorder) CreateSandbox(_ context.Context, sandboxID, _ string, _ *authtypes.Team, _ orchestrator.SandboxDataFetcher, _, _ time.Time, timeout time.Duration, isResume, demandFilesystemBoot bool, _ sandbox.CreationMetadata, options ...orchestrator.CreateOption) (sandbox.Sandbox, *api.APIError) {
	r.calls++
	r.isResume = isResume
	r.coldBoot = demandFilesystemBoot
	r.timeout = timeout
	r.options = len(options)

	return sandbox.Sandbox{SandboxID: sandboxID, State: sandbox.StateRunning, VCpu: 2, RamMB: 2048}, nil
}

// A connect to a paused sandbox resumes it through the orchestrator's start,
// on a memory restore and on a cold boot alike, for the connect's timeout.
func TestConnectV2_MemoryAcceptedGoesToTheStart(t *testing.T) {
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

			store, teamID, sandboxID := newPausedSnapshotStore(t, true, new(int64(8192)), 2048, currentEnvd, nil)
			starts := &startRecorder{}
			store.createBackendOverride, store.templateSpawnCounter = starts, testSpawnCounter(t)
			body := api.PostV2SandboxesSandboxIDConnectJSONRequestBody{MemoryMB: new(api.MemoryMB(4096)), Timeout: new(int32(600))}
			if tt.coldBoot {
				body.Memory = new(false)
			}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, body)

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
			require.Equal(t, 1, starts.calls)
			assert.True(t, starts.isResume)
			assert.Equal(t, tt.coldBoot, starts.coldBoot)
			assert.Equal(t, 600*time.Second, starts.timeout)
			assert.Positive(t, starts.options)
		})
	}
}

// transitionStub answers keep-alives from a script: first with the sandbox
// pausing, then, after the state change, with what then plays out.
type transitionStub struct {
	running   sandbox.Sandbox
	paused    bool  // the pause went through: the record is gone
	waitErr   error // what the state-change wait returns
	keepAlive int
	extended  int
	ignored   int
}

func (s *transitionStub) KeepAliveFor(_ context.Context, _ uuid.UUID, sandboxID string, _ time.Duration, _ bool) (*sandbox.Sandbox, *api.APIError) {
	s.keepAlive++
	if s.keepAlive == 1 {
		return nil, &api.APIError{Code: http.StatusConflict, Err: &sandbox.NotRunningError{SandboxID: sandboxID, State: sandbox.StatePausing}}
	}
	if s.paused {
		return nil, &api.APIError{Code: http.StatusNotFound, Err: sandbox.ErrNotFound}
	}
	s.extended++

	return &s.running, nil
}

func (s *transitionStub) WaitForStateChange(context.Context, uuid.UUID, string) error {
	return s.waitErr
}

func (s *transitionStub) RecordConnectMemoryIgnored(context.Context) {
	s.ignored++
}

func (s *transitionStub) RecordConnectCPUCountIgnored(context.Context) {}

// A connect that finds the sandbox pausing waits for the pause. When the pause
// goes through, the connect resumes it at the requested size; when the pause is
// refused and the sandbox restored, it is running again, keeps its size and the
// named one is ignored.
func TestConnectV2_MemoryAcrossAPause(t *testing.T) {
	t.Parallel()

	t.Run("pausing, paused, resumed at the size", func(t *testing.T) {
		t.Parallel()

		store, teamID, sandboxID := newPausedSnapshotStore(t, true, new(int64(8192)), 2048, currentEnvd, nil)
		starts := &startRecorder{}
		store.createBackendOverride, store.templateSpawnCounter = starts, testSpawnCounter(t)
		stub := &transitionStub{paused: true}
		store.connectBackendOverride = stub
		recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{MemoryMB: new(api.MemoryMB(4096))})

		store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

		require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
		assert.Equal(t, 1, starts.calls, "the paused sandbox is resumed")
		assert.Zero(t, stub.ignored, "a connect that resumes does not ignore its size")
	})

	for _, requested := range []api.MemoryMB{4096, 6144} {
		t.Run(fmt.Sprintf("refused pause, restored, %d MiB named", requested), func(t *testing.T) {
			t.Parallel()

			teamID := uuid.New()
			sandboxID := "i" + id.Generate()
			stub := &transitionStub{
				running: sandbox.Sandbox{SandboxID: sandboxID, TeamID: teamID, State: sandbox.StateRunning, RamMB: 2048, MemoryMB: 4096},
				waitErr: sandbox.ErrTransitionRestored,
			}
			starts := &startRecorder{}
			store := &APIStore{featureFlags: connectMemoryFlags(t, true), connectBackendOverride: stub, createBackendOverride: starts, templateSpawnCounter: testSpawnCounter(t)}
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{MemoryMB: &requested})

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			assert.Zero(t, starts.calls, "a restored sandbox is not started again")
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			assert.Equal(t, 1, stub.extended)
			assert.Equal(t, 1, stub.ignored, "the size named for the running sandbox is reported ignored")
		})
	}
}

// failingStart is an orchestrator start that fails with one error.
type failingStart struct{ err *api.APIError }

func (f failingStart) CreateSandbox(context.Context, string, string, *authtypes.Team, orchestrator.SandboxDataFetcher, time.Time, time.Time, time.Duration, bool, bool, sandbox.CreationMetadata, ...orchestrator.CreateOption) (sandbox.Sandbox, *api.APIError) {
	return sandbox.Sandbox{}, f.err
}

// A start's refusal of the size keeps its no-retry hint and a failed joined
// sized start keeps its retry hint, through connect and resume alike; any other
// start error leaves the hint unset, as before.
func TestStartErrorsKeepTheirRetryHint(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		err       *api.APIError
		wantRetry string
	}{
		{name: "memory size refused", err: &api.APIError{Code: http.StatusBadRequest, ErrorCode: "sandbox_memory_out_of_range", ClientMsg: "out of range", Err: errors.New("out of range"), Retryable: apierrors.NoRetry}, wantRetry: "false"},
		{name: "joined sized start failed", err: &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: "sandbox_sized_start_failed", ClientMsg: "retry", Err: errors.New("sized start failed"), Retryable: apierrors.CanRetry}, wantRetry: "true"},
		{name: "other start error", err: &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "failed", Err: errors.New("failed"), Retryable: apierrors.CanRetry}, wantRetry: ""},
	} {
		t.Run(tt.name+"/connect", func(t *testing.T) {
			t.Parallel()

			store, teamID, sandboxID := newPausedSnapshotStore(t, true, new(int64(8192)), 2048, currentEnvd, nil)
			store.createBackendOverride, store.templateSpawnCounter = failingStart{err: tt.err}, testSpawnCounter(t)
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{MemoryMB: new(api.MemoryMB(4096))})

			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			require.Equal(t, tt.err.Code, recorder.Code, recorder.Body.String())
			assert.Equal(t, tt.wantRetry, recorder.Header().Get(apierrors.ShouldRetryHeader))
		})
		t.Run(tt.name+"/resume", func(t *testing.T) {
			t.Parallel()

			store, teamID, sandboxID := newPausedSnapshotStore(t, true, new(int64(8192)), 2048, currentEnvd, nil)
			store.createBackendOverride, store.templateSpawnCounter = failingStart{err: tt.err}, testSpawnCounter(t)
			store.resumeBackendOverride = notFoundResumeBackend{}
			raw, err := json.Marshal(api.PostSandboxesSandboxIDResumeJSONRequestBody{})
			require.NoError(t, err)
			recorder, ginCtx := connectMemoryRequest(t, teamID, sandboxID, api.PostV2SandboxesSandboxIDConnectJSONRequestBody{})
			ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes/"+sandboxID+"/resume", bytes.NewReader(raw))
			ginCtx.Request.Header.Set("Content-Type", "application/json")

			store.PostSandboxesSandboxIDResume(ginCtx, sandboxID)

			require.Equal(t, tt.err.Code, recorder.Code, recorder.Body.String())
			assert.Equal(t, tt.wantRetry, recorder.Header().Get(apierrors.ShouldRetryHeader))
		})
	}
}
