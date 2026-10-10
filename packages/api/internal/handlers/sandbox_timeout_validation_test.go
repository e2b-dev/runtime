package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
)

func assertBadRequestTimeout(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, recorder.Code)

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))
	assert.Equal(t, int32(http.StatusBadRequest), apiErr.Code)
	assert.Equal(t, "Timeout must be greater than 0", apiErr.Message)
}

func minimalTeamInfo() *authtypes.Team {
	return &authtypes.Team{
		Team:   &authqueries.Team{ID: uuid.New()},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	}
}

// TestSandboxResume_RejectsNonPositiveTimeout verifies that POST /sandboxes/{id}/resume
// returns 400 for zero and negative timeout values. The check runs before any
// snapshot or orchestrator lookup.
func TestSandboxResume_RejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		timeout int32
	}{
		{"zero", 0},
		{"negative one", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			store := &APIStore{}

			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)

			body, err := json.Marshal(api.PostSandboxesSandboxIDResumeJSONRequestBody{
				Timeout: &tc.timeout,
			})
			require.NoError(t, err)

			ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes/abc/resume", bytes.NewReader(body))
			ginCtx.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, ginCtx, minimalTeamInfo())

			store.PostSandboxesSandboxIDResume(ginCtx, "abc123")

			assertBadRequestTimeout(t, recorder)
		})
	}
}

// TestSandboxFork_RejectsNonPositiveTimeout verifies that POST /sandboxes/{id}/fork
// returns 400 for zero and negative timeout values. The check runs before any
// sandbox-ID length check or orchestrator lookup.
func TestSandboxFork_RejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		timeout int32
	}{
		{"zero", 0},
		{"negative one", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			store := &APIStore{}

			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)

			body, err := json.Marshal(api.PostSandboxesSandboxIDForkJSONRequestBody{
				Timeout: &tc.timeout,
			})
			require.NoError(t, err)

			ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes/abc/fork", bytes.NewReader(body))
			ginCtx.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, ginCtx, minimalTeamInfo())

			store.PostSandboxesSandboxIDFork(ginCtx, "abc123")

			assertBadRequestTimeout(t, recorder)
		})
	}
}

// TestSandboxConnect_RejectsNonPositiveTimeout verifies that POST /sandboxes/{id}/connect
// returns 400 for zero and negative timeout values. The check runs before any
// sandbox-ID length check or snapshot lookup.
func TestSandboxConnect_RejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		timeout int32
	}{
		{"zero", 0},
		{"negative one", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			store := &APIStore{}

			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)

			body, err := json.Marshal(api.PostSandboxesSandboxIDConnectJSONRequestBody{
				Timeout: tc.timeout,
			})
			require.NoError(t, err)

			ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes/abc/connect", bytes.NewReader(body))
			ginCtx.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, ginCtx, minimalTeamInfo())

			//nolint:contextcheck // handler reads ctx from ginCtx.Request.Context().
			store.PostSandboxesSandboxIDConnect(ginCtx, "abc123")

			assertBadRequestTimeout(t, recorder)
		})
	}
}

