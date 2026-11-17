package handlers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/oauth"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// autoResumeBackendStub answers GetSandbox with a fixed sandbox or error and
// routes a running one with a fixed response.
type autoResumeBackendStub struct {
	sbx    sandbox.Sandbox
	sbxErr error
	resp   *proxygrpc.SandboxResumeResponse

	routed int
}

func (s *autoResumeBackendStub) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return s.sbx, s.sbxErr
}

func (s *autoResumeBackendStub) HandleExistingSandboxAutoResume(context.Context, uuid.UUID, string, sandbox.Sandbox, time.Duration) (*proxygrpc.SandboxResumeResponse, bool, error) {
	s.routed++

	return s.resp, true, nil
}

type teamAuthServiceStub struct {
	sharedauth.Service

	team *authtypes.Team
	err  error
}

func (s teamAuthServiceStub) GetTeamByID(context.Context, uuid.UUID) (*authtypes.Team, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.team, nil
}

// newFilesystemOnlyResumeService seeds a sandbox whose latest snapshot is
// filesystem-only and returns the auto-resume service over the given backend.
func newFilesystemOnlyResumeService(t *testing.T, backend *autoResumeBackendStub) (*SandboxService, string) {
	t.Helper()

	store, sandboxID := newFilesystemOnlyResumeStore(t, backend)

	return NewSandboxService(store, false, nil), sandboxID
}

// newFilesystemOnlyResumeStore seeds the sandbox newFilesystemOnlyResumeService
// serves and returns the store over the given backend.
func newFilesystemOnlyResumeStore(t *testing.T, backend *autoResumeBackendStub) (*APIStore, string) {
	t.Helper()

	return newResumeStore(t, backend, dbtypes.SandboxAutoResumeAny)
}

// newResumeStore seeds a sandbox whose latest snapshot is filesystem-only
// with the given auto-resume policy and returns the store over the given
// backend.
func newResumeStore(t *testing.T, backend *autoResumeBackendStub, autoResumePolicy dbtypes.SandboxAutoResumePolicy) (*APIStore, string) {
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
		StatusGroup:        dbtypes.BuildStatusGroupReady,
		Config: &dbtypes.PausedSandboxConfig{
			AutoResume:     &dbtypes.SandboxAutoResumeConfig{Policy: autoResumePolicy},
			FilesystemOnly: true,
		},
	})
	require.NoError(t, err)

	store := &APIStore{
		snapshotCache:             snapshotcache.NewSnapshotCache(db.SqlcClient, redisClient),
		authService:               teamAuthServiceStub{team: &authtypes.Team{Team: &dbtypes.Team{ID: teamID, Slug: "test-team"}}},
		autoResumeBackendOverride: backend,
	}

	return store, sandboxID
}

// A source that is still running after a filesystem-only snapshot template is
// routed to its node; the snapshot kind is not consulted because no resume is
// needed.
func TestResumeSandbox_RunningSourceOfFilesystemOnlySnapshotIsRouted(t *testing.T) {
	t.Parallel()

	backend := &autoResumeBackendStub{sbx: sandbox.Sandbox{State: sandbox.StateRunning}, resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.1"}}
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

// verifierStub answers every token with fixed claims or a fixed error.
type verifierStub struct {
	claims oauth.Claims
	err    error
}

func (v verifierStub) VerifyClaims(context.Context, string) (oauth.Claims, error) {
	return v.claims, v.err
}

// The edge service checks the calling proxy's credentials before it looks the
// sandbox up, and fails them as Unauthenticated even for a sandbox it holds:
// PermissionDenied would tell the proxy the sandbox is here and refused.
func TestResumeSandbox_EdgeCallerCredentialFailuresAreUnauthenticated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		md       metadata.MD
		verifier oauth.Verifier
	}{
		{
			name:     "missing bearer token",
			md:       metadata.MD{},
			verifier: verifierStub{claims: oauth.Claims{Scopes: []string{oauth.RequiredScope}}},
		},
		{
			name:     "invalid bearer token",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"),
			verifier: verifierStub{err: errors.New("invalid token")},
		},
		{
			name:     "token without the required scope",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"),
			verifier: verifierStub{claims: oauth.Claims{Scopes: []string{"other"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := &autoResumeBackendStub{sbx: sandbox.Sandbox{State: sandbox.StateRunning}, resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.1"}}
			store, sandboxID := newFilesystemOnlyResumeStore(t, backend)
			svc := NewSandboxService(store, true, tt.verifier)

			ctx := metadata.NewIncomingContext(t.Context(), tt.md)
			_, err := svc.ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{SandboxId: sandboxID})
			require.Error(t, err)
			assert.Equal(t, codes.Unauthenticated, status.Code(err))
			assert.Zero(t, backend.routed)
		})
	}
}

// resumeCase is one edge client-proxy's ResumeSandbox call against a seeded
// sandbox.
type resumeCase struct {
	callerOrg     string
	policy        dbtypes.SandboxAutoResumePolicy
	teamClusterID *uuid.UUID
	teamErr       error
	// unknownID asks for a sandbox ID that was never seeded.
	unknownID bool
}

type resumeOutcome struct {
	code    codes.Code
	message string
	routed  bool
}

// resumeWith seeds a running sandbox as c describes, on a control plane whose
// clusters are pool, and resumes it.
func resumeWith(t *testing.T, pool *clusters.Pool, c resumeCase) resumeOutcome {
	t.Helper()

	backend := &autoResumeBackendStub{sbx: sandbox.Sandbox{State: sandbox.StateRunning}, resp: &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.1"}}
	store, sandboxID := newResumeStore(t, backend, c.policy)
	team := store.authService.(teamAuthServiceStub).team
	team.ClusterID = c.teamClusterID
	store.authService = teamAuthServiceStub{team: team, err: c.teamErr}
	store.clusters = pool
	if c.unknownID {
		sandboxID = "i" + id.Generate()
	}

	svc := NewSandboxService(store, true, verifierStub{claims: oauth.Claims{OrgID: c.callerOrg, Scopes: []string{oauth.RequiredScope}}})
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"))

	_, err := svc.ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{SandboxId: sandboxID})

	return resumeOutcome{code: status.Code(err), message: status.Convert(err).Message(), routed: backend.routed > 0}
}

// The snapshot lookup spans every cluster, so an edge caller is routed only to
// sandboxes on its org's clusters, and until it is known to be allowed to see
// a sandbox, every outcome answers exactly as an org mismatch does: NotFound,
// not PermissionDenied, which client-proxy reads as "this sandbox is here".
// Otherwise the answer tells the caller whether the ID exists on another org's
// cluster, how it is configured, or which cluster holds it.
func TestResumeSandbox_EdgeCallerSeesOnlyItsOrgsSandboxes(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	unknownClusterID := uuid.New()
	pool := clusters.NewTestPool(clusters.NewCluster(clusterID, nil, "org-a", nil, nil, nil))

	orgMismatch := resumeWith(t, pool, resumeCase{callerOrg: "org-b", policy: dbtypes.SandboxAutoResumeAny, teamClusterID: &clusterID})
	require.Equal(t, codes.NotFound, orgMismatch.code)
	require.False(t, orgMismatch.routed)

	tests := []struct {
		name string
		c    resumeCase
	}{
		{name: "unknown sandbox ID", c: resumeCase{policy: dbtypes.SandboxAutoResumeAny, teamClusterID: &clusterID, unknownID: true}},
		{name: "sandbox on the main cluster, which no org owns", c: resumeCase{policy: dbtypes.SandboxAutoResumeAny}},
		{name: "another org's sandbox with auto-resume off", c: resumeCase{policy: dbtypes.SandboxAutoResumeOff, teamClusterID: &clusterID}},
		{name: "sandbox whose cluster this API does not know", c: resumeCase{policy: dbtypes.SandboxAutoResumeAny, teamClusterID: &unknownClusterID}},
		{name: "sandbox whose team is banned", c: resumeCase{policy: dbtypes.SandboxAutoResumeAny, teamClusterID: &clusterID, teamErr: &sharedauth.TeamForbiddenError{Message: "team is banned"}}},
		{name: "sandbox whose team is gone", c: resumeCase{policy: dbtypes.SandboxAutoResumeAny, teamClusterID: &clusterID, teamErr: fmt.Errorf("failed to get team from team ID: %w", pgx.ErrNoRows)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.c.callerOrg = "org-b"
			assert.Equal(t, orgMismatch, resumeWith(t, pool, tt.c))
		})
	}

	t.Run("the sandbox's own org routes it", func(t *testing.T) {
		t.Parallel()

		own := resumeWith(t, pool, resumeCase{callerOrg: "org-a", policy: dbtypes.SandboxAutoResumeAny, teamClusterID: &clusterID})

		assert.Equal(t, codes.OK, own.code)
		assert.True(t, own.routed)
	})

	// Once the caller is allowed to see the sandbox, it learns why it is
	// refused.
	t.Run("the sandbox's own org is told auto-resume is off", func(t *testing.T) {
		t.Parallel()

		own := resumeWith(t, pool, resumeCase{callerOrg: "org-a", policy: dbtypes.SandboxAutoResumeOff, teamClusterID: &clusterID})

		assert.Equal(t, codes.NotFound, own.code)
		assert.NotEqual(t, orgMismatch.message, own.message)
		assert.False(t, own.routed)
	})
}

// The placement reaches a client over a real TCP gRPC connection, and a
// client built before network_placement existed still reads orchestrator_ip
// from the same response.
func TestResumeSandbox_PlacementSurvivesTheWire(t *testing.T) {
	t.Parallel()

	backend := &autoResumeBackendStub{sbx: sandbox.Sandbox{State: sandbox.StateRunning}}
	svc, sandboxID := newFilesystemOnlyResumeService(t, backend)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proxygrpc.RegisterSandboxServiceServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := proxygrpc.NewSandboxServiceClient(conn)
	req := &proxygrpc.SandboxResumeRequest{SandboxId: sandboxID}

	for _, want := range []*proxygrpc.SandboxResumeResponse{
		{OrchestratorIp: "10.0.0.1"},
		{NetworkPlacement: &orchestrator.SandboxNetworkPlacement{
			ExecutionId: "execution-b", NetworkMode: "full-router", RouterId: "router-b", IngressAddress: "10.8.0.2", IngressPort: 5010,
		}},
	} {
		backend.resp = want

		got, err := client.ResumeSandbox(t.Context(), req)
		require.NoError(t, err)
		require.Truef(t, proto.Equal(want, got), "want %v, got %v", want, got)

		// orchestrator_ip is field 1, a string, exactly as StringValue encodes value.
		old := &wrapperspb.StringValue{}
		require.NoError(t, conn.Invoke(t.Context(), proxygrpc.SandboxService_ResumeSandbox_FullMethodName, req, old))
		assert.Equal(t, want.GetOrchestratorIp(), old.GetValue())
	}
}
