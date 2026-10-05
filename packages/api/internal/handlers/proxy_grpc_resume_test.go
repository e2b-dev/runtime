package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// autoResumeBackendStub answers GetSandbox with a fixed sandbox or error and
// routes a running one to a fixed node IP.
type autoResumeBackendStub struct {
	sbx    sandbox.Sandbox
	sbxErr error
	nodeIP string

	routed int
}

func (s *autoResumeBackendStub) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return s.sbx, s.sbxErr
}

func (s *autoResumeBackendStub) HandleExistingSandboxAutoResume(context.Context, uuid.UUID, string, sandbox.Sandbox, time.Duration) (string, bool, error) {
	s.routed++

	return s.nodeIP, true, nil
}

type teamAuthServiceStub struct {
	sharedauth.Service

	team *authtypes.Team
}

func (s teamAuthServiceStub) GetTeamByID(context.Context, uuid.UUID) (*authtypes.Team, error) {
	return s.team, nil
}

// newFilesystemOnlyResumeService seeds a sandbox whose latest snapshot is
// filesystem-only and returns the auto-resume service over the given backend.
func newFilesystemOnlyResumeService(t *testing.T, backend *autoResumeBackendStub) (*SandboxService, string) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sandboxID := "i" + id.Generate()

	// A ready filesystem-only snapshot with auto-resume allowed, the state a
	// memory:false snapshot template leaves behind.
	totalDiskSize := int64(1024)
	envdVersion := "v1.0.0"
	_, err := db.SqlcClient.UpsertSnapshot(t.Context(), queries.UpsertSnapshotParams{
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
		Status:             dbtypes.BuildStatusSuccess,
		Config: &dbtypes.PausedSandboxConfig{
			AutoResume:     &dbtypes.SandboxAutoResumeConfig{Policy: dbtypes.SandboxAutoResumeAny},
			FilesystemOnly: true,
		},
	})
	require.NoError(t, err)

	store := &APIStore{
		snapshotCache:             snapshotcache.NewSnapshotCache(db.SqlcClient, redisClient),
		authService:               teamAuthServiceStub{team: &authtypes.Team{Team: &authqueries.Team{ID: teamID, Slug: "test-team"}}},
		autoResumeBackendOverride: backend,
	}

	return NewSandboxService(store, false, nil), sandboxID
}

// A source that is still running after a filesystem-only snapshot template is
// routed to its node; the snapshot kind is not consulted because no resume is
// needed.
func TestResumeSandbox_RunningSourceOfFilesystemOnlySnapshotIsRouted(t *testing.T) {
	t.Parallel()

	backend := &autoResumeBackendStub{sbx: sandbox.Sandbox{State: sandbox.StateRunning}, nodeIP: "10.0.0.1"}
	svc, sandboxID := newFilesystemOnlyResumeService(t, backend)

	resp, err := svc.ResumeSandbox(t.Context(), &proxygrpc.SandboxResumeRequest{SandboxId: sandboxID})
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1", resp.GetOrchestratorIp())
	assert.Equal(t, 1, backend.routed)
}

// Once a resume is actually needed, a filesystem-only snapshot is refused:
// cold-booting it implicitly would lose the memory the caller may expect.
func TestResumeSandbox_PausedFilesystemOnlySnapshotIsRefused(t *testing.T) {
	t.Parallel()

	backend := &autoResumeBackendStub{sbxErr: sandbox.ErrNotFound}
	svc, sandboxID := newFilesystemOnlyResumeService(t, backend)

	_, err := svc.ResumeSandbox(t.Context(), &proxygrpc.SandboxResumeRequest{SandboxId: sandboxID})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "filesystem-only snapshot must be resumed explicitly")
	assert.Zero(t, backend.routed)
}
