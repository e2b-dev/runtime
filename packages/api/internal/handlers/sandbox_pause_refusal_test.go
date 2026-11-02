package handlers

import (
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
)

// pauseBackendStub returns a fixed error from RemoveSandbox; GetSandbox is
// never reached on the paths under test.
type pauseBackendStub struct {
	err error
}

func (s pauseBackendStub) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return sandbox.Sandbox{}, errors.New("not used")
}

func (s pauseBackendStub) RemoveSandbox(context.Context, uuid.UUID, string, sandbox.RemoveOpts) error {
	return s.err
}

func postPause(t *testing.T, backendErr error) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes/"+testPauseSandboxID+"/pause", nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: uuid.New(), Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store := &APIStore{pauseBackendOverride: pauseBackendStub{err: backendErr}}
	store.PostSandboxesSandboxIDPause(ginCtx, testPauseSandboxID)

	return recorder
}

var testPauseSandboxID = "i" + id.Generate()

// A retryable pause refusal surfaces as 503 "node busy, please retry" — the
// same idiom the fork handler uses — never as a generic 500.
func TestPause_RetryableRefusalMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postPause(t, orchestrator.PauseQueueExhaustedError{})

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	assert.Contains(t, apiErr.Message, "node is busy, please retry")
}

// A pause refused for a checkpoint in flight is a 503 that names the
// snapshot and asks for a plain retry, not the busy-node text.
func TestPause_CheckpointInProgressMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postPause(t, orchestrator.PauseRefusedCheckpointInProgressError{})

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	assert.Contains(t, apiErr.Message, "being snapshotted, please retry")
}

// A pause refused because the guest agent did not answer is a 503 that names
// the guest agent and asks for a plain retry, not the busy-node text.
func TestPause_EnvdUnresponsiveMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postPause(t, orchestrator.PauseRefusedEnvdUnresponsiveError{})

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	assert.Contains(t, apiErr.Message, "guest agent is not responding, please retry")
}

// A wrapped refusal (the orchestrator chain wraps before returning) still maps.
func TestPause_WrappedRetryableRefusalMapsTo503(t *testing.T) {
	t.Parallel()

	recorder := postPause(t, errors.Join(errors.New("failed to auto pause sandbox"), orchestrator.PauseQueueExhaustedError{}))

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

// Every other failure keeps today's generic 500.
func TestPause_GenericFailureStays500(t *testing.T) {
	t.Parallel()

	recorder := postPause(t, errors.New("boom"))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}
