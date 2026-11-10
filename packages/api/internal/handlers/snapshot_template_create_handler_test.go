package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// snapshotBackendStub stands in for the orchestrator: a Running sandbox on a
// node whose support answer is fixed, and a record of every transition asked
// for. CreateSnapshotTemplate is the only call that moves the sandbox out of
// Running, so "never called" is "still Running".
type snapshotBackendStub struct {
	supportErr    error
	createErr     error
	supportChecks int
	created       []orchestrator.SnapshotTemplateOpts
	// onCreate, when set, runs before createErr is answered: the real backend
	// persists the template and alias before the node refuses.
	onCreate func()
}

func (s *snapshotBackendStub) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return sandbox.Sandbox{State: sandbox.StateRunning, EnvdVersion: sharedutils.MinEnvdVersionForSnapshot}, nil
}

func (s *snapshotBackendStub) EnsureSnapshotSupport(_ context.Context, _ sandbox.Sandbox, opts orchestrator.SnapshotTemplateOpts) error {
	s.supportChecks++
	if opts.FilesystemOnly {
		return s.supportErr
	}

	return nil
}

func (s *snapshotBackendStub) CreateSnapshotTemplate(_ context.Context, _ uuid.UUID, _ string, opts orchestrator.SnapshotTemplateOpts) (orchestrator.SnapshotTemplateResult, error) {
	s.created = append(s.created, opts)
	if s.onCreate != nil {
		s.onCreate()
	}
	if s.createErr != nil {
		return orchestrator.SnapshotTemplateResult{}, s.createErr
	}

	return orchestrator.SnapshotTemplateResult{TemplateID: "tpl"}, nil
}

func postSnapshot(t *testing.T, backend *snapshotBackendStub, flagOn bool, body string, cache *templatecache.TemplateCache) *httptest.ResponseRecorder {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.FilesystemOnlyCheckpointFlag.Key()).VariationForAll(flagOn))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	sandboxID := "i" + id.Generate()
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes/"+sandboxID+"/snapshots", bytes.NewBufferString(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: uuid.New(), Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store := &APIStore{snapshotBackendOverride: backend, featureFlags: ff, templateCache: cache}
	store.PostSandboxesSandboxIDSnapshots(ginCtx, sandboxID, api.PostSandboxesSandboxIDSnapshotsParams{})

	return recorder
}

func snapshotErrorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()

	var body struct {
		ErrorCode string `json:"error_code"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	return body.ErrorCode
}

// A body that does not parse is answered 400 with Should-Retry: false; the
// router's schema validator usually rejects it first.
func TestSnapshotTemplate_MalformedBodyIs400WithoutRetry(t *testing.T) {
	t.Parallel()

	backend := &snapshotBackendStub{}
	recorder := postSnapshot(t, backend, true, `{`, nil)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	assert.Equal(t, "false", recorder.Header().Get(apierrors.ShouldRetryHeader))
	assert.Empty(t, backend.created)
}

// memory:false on a node below the orchestrator version floor is answered 409
// with its own error code, and the sandbox never leaves Running: the
// transition is never asked for.
func TestSnapshotTemplate_NodeBelowFloorIs409BeforeTransition(t *testing.T) {
	t.Parallel()

	backend := &snapshotBackendStub{supportErr: orchestrator.NodeFeatureMissingError{
		NodeID: "old", NodeVersion: "0.16.202609291011", Feature: "filesystem-only-checkpoint", MinVersion: "0.16.202609301732",
	}}
	recorder := postSnapshot(t, backend, true, `{"memory": false}`, nil)

	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	assert.Equal(t, errCodeFilesystemOnlySnapshotUnsupportedNode, snapshotErrorCode(t, recorder))
	assert.Empty(t, backend.created, "a refused request must not start a snapshot")
}

// memory:false while the flag is off is answered 400 before the node is even
// consulted, and again without a transition.
func TestSnapshotTemplate_FlagOffIs400BeforeTransition(t *testing.T) {
	t.Parallel()

	backend := &snapshotBackendStub{supportErr: orchestrator.NodeFeatureMissingError{NodeID: "old"}}
	recorder := postSnapshot(t, backend, false, `{"memory": false}`, nil)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	assert.Equal(t, errCodeFilesystemOnlySnapshotDisabled, snapshotErrorCode(t, recorder))
	assert.Zero(t, backend.supportChecks, "the flag refusal comes before the node check")
	assert.Empty(t, backend.created)
}

// A snapshot refused because another checkpoint of the sandbox is in flight
// is a 503 asking for a plain retry, never the generic 500.
func TestSnapshotTemplate_CheckpointInProgressIs503(t *testing.T) {
	t.Parallel()

	backend := &snapshotBackendStub{createErr: orchestrator.PauseRefusedCheckpointInProgressError{}}
	recorder := postSnapshot(t, backend, false, `{}`, nil)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	assert.Contains(t, recorder.Body.String(), "another snapshot of it is in progress, please retry")
}

// A snapshot refused because the sandbox's guest agent did not answer the
// node's health probe is a 503 naming the guest agent, never the busy-node
// text it also matches.
func TestSnapshotTemplate_EnvdUnresponsiveIs503(t *testing.T) {
	t.Parallel()

	backend := &snapshotBackendStub{createErr: orchestrator.PauseRefusedEnvdUnresponsiveError{}}
	recorder := postSnapshot(t, backend, false, `{}`, nil)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	assert.Contains(t, recorder.Body.String(), "guest agent is not responding, please retry")
}

// A named snapshot refused for a checkpoint in flight still clears the cached
// alias miss: the template and alias were persisted before the node was asked,
// so the retry the 503 invites must find them instead of creating a duplicate.
// The miss is seeded in the cache first and the alias persisted during the
// refused create, so the only way the final lookup finds the template is the
// handler clearing the miss on the refusal path.
func TestSnapshotTemplate_RefusedNamedSnapshotClearsTheCachedMiss(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cache := templatecache.NewTemplateCache(db.SqlcClient, redis_utils.SetupInstance(t))
	const alias, namespace = "ckpt-refused-alias", "test-team"
	ns := namespace

	_, err := cache.ResolveAlias(t.Context(), alias, namespace)
	require.ErrorIs(t, err, templatecache.ErrTemplateNotFound, "the miss is cached before the request")

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	backend := &snapshotBackendStub{
		createErr: orchestrator.PauseRefusedCheckpointInProgressError{},
		onCreate:  func() { testutils.CreateTestTemplateAliasWithName(t, db, templateID, alias, &ns) },
	}
	recorder := postSnapshot(t, backend, false, `{"name": "`+alias+`"}`, cache)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	require.Len(t, backend.created, 1)
	require.NotNil(t, backend.created[0].Alias)
	assert.Equal(t, alias, *backend.created[0].Alias)

	info, err := cache.ResolveAlias(t.Context(), alias, namespace)
	require.NoError(t, err, "the cached miss must be gone so a retry finds the persisted alias")
	assert.Equal(t, templateID, info.TemplateID)
}

// A memory snapshot (memory omitted) passes both gates untouched, flag off and
// node old alike, and reaches the transition as a memory snapshot.
func TestSnapshotTemplate_MemorySnapshotIgnoresBothGates(t *testing.T) {
	t.Parallel()

	// The success path invalidates the template cache, so this case alone
	// needs a real one.
	db := testutils.SetupDatabase(t)
	cache := templatecache.NewTemplateCache(db.SqlcClient, redis_utils.SetupInstance(t))

	backend := &snapshotBackendStub{supportErr: orchestrator.NodeFeatureMissingError{NodeID: "old"}}
	recorder := postSnapshot(t, backend, false, `{}`, cache)

	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
	require.Len(t, backend.created, 1)
	assert.False(t, backend.created[0].FilesystemOnly)
}

// memory:false that the node refuses as unsupported for the guest is answered 409 with the sandbox left running, never the
// generic 500. The pause handler answers the same refusal 400, since the SDKs read a 409 from pause as already paused.
func TestSnapshotTemplate_UnsupportedRefusalIs409(t *testing.T) {
	t.Parallel()

	backend := &snapshotBackendStub{createErr: orchestrator.PauseUnsupportedError{}}
	recorder := postSnapshot(t, backend, true, `{"memory": false}`, nil)

	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	assert.Contains(t, recorder.Body.String(), "does not support it")
	require.Len(t, backend.created, 1)
	assert.True(t, backend.created[0].FilesystemOnly)
}
