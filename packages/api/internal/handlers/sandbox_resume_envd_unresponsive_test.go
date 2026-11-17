package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// notFoundResumeBackend answers every lookup as a sandbox the store does not
// hold, so a resume reads the snapshot.
type notFoundResumeBackend struct{}

func (notFoundResumeBackend) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return sandbox.Sandbox{}, sandbox.ErrNotFound
}

func (notFoundResumeBackend) WaitForStateChange(context.Context, uuid.UUID, string) error {
	return nil
}

func (notFoundResumeBackend) KeepAliveFor(context.Context, uuid.UUID, string, time.Duration, bool) (*sandbox.Sandbox, *api.APIError) {
	return nil, &api.APIError{Code: http.StatusNotFound, ClientMsg: "not found", Err: sandbox.ErrNotFound}
}

// newEnvdUnresponsiveSnapshotStore seeds a paused sandbox whose only ready
// build is a memory snapshot the node took over an unresponsive guest agent,
// and returns a store whose resume paths read it.
func newEnvdUnresponsiveSnapshotStore(t *testing.T, backend *autoResumeBackendStub) (*APIStore, uuid.UUID, string) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID := "i" + id.Generate()

	totalDiskSize := int64(1024)
	envdVersion := "v1.0.0"
	row, err := db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
		TemplateID:         id.Generate(),
		TeamID:             teamID,
		SandboxID:          sandboxID,
		BaseTemplateID:     baseTemplateID,
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
		Config: &dbtypes.PausedSandboxConfig{
			AutoResume: &dbtypes.SandboxAutoResumeConfig{Policy: dbtypes.SandboxAutoResumeAny},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.SqlcClient.MarkSnapshotBuildEnvdUnresponsive(t.Context(), row.BuildID))

	store := &APIStore{
		snapshotCache:             snapshotcache.NewSnapshotCache(db.SqlcClient, redisClient),
		authService:               teamAuthServiceStub{team: &authtypes.Team{Team: &dbtypes.Team{ID: teamID, Slug: "test-team"}}},
		autoResumeBackendOverride: backend,
		resumeBackendOverride:     notFoundResumeBackend{},
		connectBackendOverride:    notFoundResumeBackend{},
	}

	return store, teamID, sandboxID
}

func newTeamRequest(t *testing.T, teamID uuid.UUID, method, path string) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), method, path, nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: teamID, Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	return recorder, ginCtx
}

func requireGuestAgentUnresponsiveConflict(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()

	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	require.NotNil(t, apiErr.ErrorCode)
	assert.Equal(t, errCodeSnapshotGuestAgentUnresponsive, *apiErr.ErrorCode)
	assert.Contains(t, apiErr.Message, "guest agent was not responding")
	assert.Contains(t, apiErr.Message, "memory: false")
}

// A plain resume of a snapshot taken over an unresponsive guest agent is
// refused before any node is asked, as a conflict the caller can act on.
func TestResume_SnapshotTakenOverUnresponsiveGuestIsRefused(t *testing.T) {
	t.Parallel()

	store, teamID, sandboxID := newEnvdUnresponsiveSnapshotStore(t, &autoResumeBackendStub{sbxErr: sandbox.ErrNotFound})
	recorder, ginCtx := newTeamRequest(t, teamID, http.MethodPost, "/sandboxes/"+sandboxID+"/resume")

	store.PostSandboxesSandboxIDResume(ginCtx, sandboxID)

	requireGuestAgentUnresponsiveConflict(t, recorder)
	gate, _ := ginCtx.Get(metricResumeGate)
	assert.Equal(t, "refused_guest_agent_unresponsive", gate, "the request metric carries the refusal")
}

// Connect resumes a paused sandbox the same way, and refuses the same way.
func TestConnectV2_SnapshotTakenOverUnresponsiveGuestIsRefused(t *testing.T) {
	t.Parallel()

	store, teamID, sandboxID := newEnvdUnresponsiveSnapshotStore(t, &autoResumeBackendStub{sbxErr: sandbox.ErrNotFound})
	recorder, ginCtx := newTeamRequest(t, teamID, http.MethodPost, "/v2/sandboxes/"+sandboxID+"/connect")

	store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

	requireGuestAgentUnresponsiveConflict(t, recorder)
	gate, _ := ginCtx.Get(metricResumeGate)
	assert.Equal(t, "refused_guest_agent_unresponsive", gate, "the request metric carries the refusal")
}

// Traffic never brings such a snapshot back: auto-resume restores memory, and
// the caller must ask for a resume without it. The answer carries the same
// code and HTTP status as the explicit resume, so the proxy's page is the 409
// with sandbox_snapshot_guest_agent_unresponsive and not a bare status.
func TestResumeSandbox_PausedSnapshotTakenOverUnresponsiveGuestIsRefused(t *testing.T) {
	t.Parallel()

	backend := &autoResumeBackendStub{sbxErr: sandbox.ErrNotFound}
	store, _, sandboxID := newEnvdUnresponsiveSnapshotStore(t, backend)
	svc := NewSandboxService(store, false, nil)

	_, err := svc.ResumeSandbox(t.Context(), &proxygrpc.SandboxResumeRequest{SandboxId: sandboxID})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, sharedutils.GRPCCodeFromHTTPStatus(http.StatusConflict), st.Code())
	assert.Contains(t, st.Message(), "guest agent was not responding")
	assert.Contains(t, st.Message(), "memory: false")
	var info *errdetails.ErrorInfo
	for _, detail := range st.Details() {
		if candidate, isInfo := detail.(*errdetails.ErrorInfo); isInfo {
			info = candidate
		}
	}
	require.NotNil(t, info, "the refusal must carry its error code as a detail")
	assert.Equal(t, errCodeSnapshotGuestAgentUnresponsive, info.GetReason())
	assert.Equal(t, proxygrpc.ResumeErrorDomain, info.GetDomain())
	assert.Equal(t, strconv.Itoa(http.StatusConflict), info.GetMetadata()[proxygrpc.ResumeErrorHTTPStatusKey])
	assert.Zero(t, backend.routed)
}

// The paused list shows the mark on the build a resume would restore, and
// omits the field for every other snapshot.
func TestSnapshotsToPaginatedSandboxes_ExposesTheGuestAgentMark(t *testing.T) {
	t.Parallel()

	envdVersion := "v1.0.0"
	diskSize := int64(1024)
	row := func(sandboxID string, mark *bool) queries.GetSnapshotsWithCursorRow {
		return queries.GetSnapshotsWithCursorRow{
			Snapshot:              queries.Snapshot{SandboxID: sandboxID, SandboxStartedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true}},
			BuildEnvdVersion:      &envdVersion,
			BuildTotalDiskSizeMb:  &diskSize,
			BuildEnvdUnresponsive: mark,
		}
	}

	got := snapshotsToPaginatedSandboxes(t.Context(), []queries.GetSnapshotsWithCursorRow{
		row("marked", new(true)),
		row("answered", new(false)),
		row("legacy", nil),
	})
	require.Len(t, got, 3)
	require.NotNil(t, got[0].EnvdUnresponsiveAtPause)
	assert.True(t, *got[0].EnvdUnresponsiveAtPause)
	assert.Nil(t, got[1].EnvdUnresponsiveAtPause)
	assert.Nil(t, got[2].EnvdUnresponsiveAtPause)
}
