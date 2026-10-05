package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

// keepAliveRecorder answers with a running sandbox and records the requested keep-alive.
type keepAliveRecorder struct {
	sbx      sandbox.Sandbox
	duration time.Duration
	calls    int
}

func (s *keepAliveRecorder) KeepAliveFor(_ context.Context, _ uuid.UUID, _ string, duration time.Duration, _ bool) (*sandbox.Sandbox, *api.APIError) {
	s.calls++
	s.duration = duration

	return &s.sbx, nil
}

func (s *keepAliveRecorder) WaitForStateChange(context.Context, uuid.UUID, string) error {
	return nil
}

func newConnectV2Request(t *testing.T, sandboxID string, body io.Reader) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/sandboxes/"+sandboxID+"/connect", body)
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	return recorder, ginCtx
}

func TestConnectV2_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     io.Reader
		status   int
		duration time.Duration
	}{
		{name: "no body", body: http.NoBody, status: http.StatusOK, duration: sandbox.SandboxTimeoutDefaultV2},
		{name: "empty object", body: strings.NewReader(`{}`), status: http.StatusOK, duration: sandbox.SandboxTimeoutDefaultV2},
		{name: "explicit timeout", body: strings.NewReader(`{"timeout":60}`), status: http.StatusOK, duration: time.Minute},
		{name: "zero timeout", body: strings.NewReader(`{"timeout":0}`), status: http.StatusBadRequest},
		{name: "over team limit", body: strings.NewReader(`{"timeout":90000}`), status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			teamID := uuid.New()
			sandboxID := "i" + id.Generate()
			stub := &keepAliveRecorder{
				sbx: sandbox.Sandbox{SandboxID: sandboxID, TeamID: teamID, State: sandbox.StateRunning, StartTime: time.Now(), EndTime: time.Now().Add(time.Hour)},
			}

			recorder, ginCtx := newConnectV2Request(t, sandboxID, tt.body)
			auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
				Team:   &authqueries.Team{ID: teamID, Slug: "test-team"},
				Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
			})

			store := &APIStore{connectBackendOverride: stub}
			//nolint:contextcheck // handler reads ctx from ginCtx.Request.Context().
			store.PostV2SandboxesSandboxIDConnect(ginCtx, sandboxID)

			require.Equal(t, tt.status, recorder.Code, recorder.Body.String())
			if tt.status != http.StatusOK {
				assert.Zero(t, stub.calls)

				return
			}

			assert.Equal(t, 1, stub.calls)
			assert.Equal(t, tt.duration, stub.duration)
		})
	}
}

func TestConnectSandboxV2Schema_HasNoRequiredFields(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)

	schema := spec.Components.Schemas["ConnectSandboxV2"]
	require.NotNil(t, schema)
	assert.Empty(t, schema.Value.Required)
	assert.InDelta(t, float64(sandbox.SandboxTimeoutDefaultV2/time.Second), schema.Value.Properties["timeout"].Value.Default, 0)
	assert.InDelta(t, float64(1), *schema.Value.Properties["timeout"].Value.Min, 0)

	v2Create := spec.Components.Schemas["NewSandboxV2"]
	require.NotNil(t, v2Create)
	assert.InDelta(t, float64(sandbox.SandboxTimeoutDefaultV2/time.Second), v2Create.Value.Properties["timeout"].Value.Default, 0)
	assert.InDelta(t, float64(1), *v2Create.Value.Properties["timeout"].Value.Min, 0)

	connect := spec.Paths.Value("/v2/sandboxes/{sandboxID}/connect").Post
	require.NotNil(t, connect)
	assert.False(t, connect.RequestBody.Value.Required)
	assert.True(t, spec.Paths.Value("/sandboxes/{sandboxID}/connect").Post.Deprecated)
}
