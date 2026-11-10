package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// pauseBackendStub returns a fixed error from RemoveSandbox and keeps what it
// was asked; GetSandbox is never reached on the paths under test.
type pauseBackendStub struct {
	err  error
	opts *sandbox.RemoveOpts
}

func (s pauseBackendStub) GetSandbox(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return sandbox.Sandbox{}, errors.New("not used")
}

func (s pauseBackendStub) RemoveSandbox(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
	if s.opts != nil {
		*s.opts = opts
	}

	return s.err
}

func postPause(t *testing.T, backendErr error) *httptest.ResponseRecorder {
	t.Helper()

	recorder, _, _ := postPauseWithBody(t, backendErr, "")

	return recorder
}

// postPauseWithBody sends the pause with body (none when empty) and returns
// what the backend was asked and the pause mode the request metric carries.
// gin writes the status of a bodiless answer only when the request ends, so
// a success reads it from the gin writer.
func postPauseWithBody(t *testing.T, backendErr error, body string) (*httptest.ResponseRecorder, sandbox.RemoveOpts, string) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes/"+testPauseSandboxID+"/pause", reader)
	if body != "" {
		ginCtx.Request.Header.Set("Content-Type", "application/json")
	}
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team:   &dbtypes.Team{ID: uuid.New(), Slug: "test-team"},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	var opts sandbox.RemoveOpts
	store := &APIStore{pauseBackendOverride: pauseBackendStub{err: backendErr, opts: &opts}}
	store.PostSandboxesSandboxIDPause(ginCtx, testPauseSandboxID)
	if ginCtx.Writer.Status() == http.StatusNoContent {
		ginCtx.Writer.WriteHeaderNow()
	}
	mode, _ := ginCtx.Get(metricPauseMode)
	modeLabel, _ := mode.(string)

	return recorder, opts, modeLabel
}

// What the request asks of the node: a plain pause is a memory pause that
// goes on over a hung guest; requireHealthyGuest makes it strict; memory:false
// asks for a filesystem-only snapshot.
func TestPause_BodyShapesTheRemoval(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		body   string
		fsOnly bool
		strict bool
		mode   string
	}{
		"no body":                    {body: "", fsOnly: false, strict: false, mode: "default"},
		"empty object":               {body: "{}", fsOnly: false, strict: false, mode: "default"},
		"healthy guest required":     {body: `{"requireHealthyGuest": true}`, fsOnly: false, strict: true, mode: "strict"},
		"healthy guest not required": {body: `{"requireHealthyGuest": false}`, fsOnly: false, strict: false, mode: "default"},
		"filesystem-only":            {body: `{"memory": false}`, fsOnly: true, strict: false, mode: "filesystem_only"},
		"filesystem-only, strict":    {body: `{"memory": false, "requireHealthyGuest": true}`, fsOnly: true, strict: true, mode: "filesystem_only"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder, opts, mode := postPauseWithBody(t, nil, c.body)

			require.Equal(t, http.StatusNoContent, recorder.Code)
			assert.Equal(t, sandbox.StateActionPause, opts.Action)
			assert.Equal(t, c.fsOnly, opts.FilesystemOnly)
			assert.Equal(t, c.strict, opts.RequireEnvdHealthy)
			assert.Equal(t, c.mode, mode, "the request metric carries the pause mode")
		})
	}
}

// The pause mode is set before the pause runs, so a refused pause carries it too.
func TestPause_RefusalCarriesThePauseMode(t *testing.T) {
	t.Parallel()

	recorder, _, mode := postPauseWithBody(t, orchestrator.PauseRefusedEnvdUnresponsiveError{}, `{"requireHealthyGuest": true}`)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Equal(t, "strict", mode)
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
// the guest agent and asks for a plain retry, not the busy-node text. Only a
// strict pause gets this answer, so it is sent as one.
func TestPause_EnvdUnresponsiveMapsTo503(t *testing.T) {
	t.Parallel()

	recorder, opts, _ := postPauseWithBody(t, orchestrator.PauseRefusedEnvdUnresponsiveError{}, `{"requireHealthyGuest": true}`)
	assert.True(t, opts.RequireEnvdHealthy)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	assert.Contains(t, apiErr.Message, "guest agent is not responding, please retry")
}

// A pause of a held sandbox, or one whose record lock expired before it
// wrote, is a 503 that asks for a retry: released SDKs read a pause 409 as
// already paused.
func TestPause_BusyMapsTo503(t *testing.T) {
	t.Parallel()

	for _, err := range []error{orchestrator.ErrSandboxLifecycleHeld, fmt.Errorf("sandbox %q: %w", testPauseSandboxID, sandbox.ErrLockLost)} {
		recorder := postPause(t, err)

		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, err)
		var apiErr api.Error
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
		assert.Contains(t, apiErr.Message, "busy with another operation, please retry")
	}
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
