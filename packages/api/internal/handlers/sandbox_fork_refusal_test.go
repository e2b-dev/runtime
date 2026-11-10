package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// forkBackendStub is a Running sandbox whose checkpoint answers a fixed error.
type forkBackendStub struct {
	checkpointErr error
}

func (s forkBackendStub) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return sandbox.Sandbox{State: sandbox.StateRunning, EnvdVersion: sharedutils.MinEnvdVersionForSnapshot}, nil
}

func (s forkBackendStub) CheckpointSandbox(context.Context, uuid.UUID, string) error {
	return s.checkpointErr
}

func postFork(t *testing.T, checkpointErr error) *httptest.ResponseRecorder {
	t.Helper()

	sandboxID := "i" + id.Generate()
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes/"+sandboxID+"/fork", bytes.NewBufferString(`{"count": 1}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: uuid.New(), Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24, SandboxConcurrency: 10},
	})

	store := &APIStore{forkBackendOverride: forkBackendStub{checkpointErr: checkpointErr}}
	store.PostSandboxesSandboxIDFork(ginCtx, sandboxID)

	return recorder
}

func forkErrorMessage(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))

	return apiErr.Message
}

// A fork refused because another checkpoint of the sandbox is in flight is a
// 503 asking for a plain retry; the sandbox is still running.
func TestFork_CheckpointInProgressMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postFork(t, orchestrator.PauseRefusedCheckpointInProgressError{})

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	assert.Contains(t, forkErrorMessage(t, recorder), "being snapshotted, please retry")
}

// A fork refused because the sandbox's guest agent did not answer the node's
// health probe is a 503 naming the guest agent; the sandbox is still running.
func TestFork_EnvdUnresponsiveMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postFork(t, orchestrator.PauseRefusedEnvdUnresponsiveError{})

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	assert.Contains(t, forkErrorMessage(t, recorder), "guest agent is not responding, please retry")
}

// A busy-node refusal keeps its own 503 text.
func TestFork_RetryableRefusalMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postFork(t, orchestrator.PauseQueueExhaustedError{})

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	assert.Contains(t, forkErrorMessage(t, recorder), "node is busy, please retry")
}

// Anything else is still the generic failure.
func TestFork_GenericFailureStays500(t *testing.T) {
	t.Parallel()

	recorder := postFork(t, errors.New("node exploded"))

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
}
