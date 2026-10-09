package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
)

type fakeSandboxLifecycles struct {
	lifecycles map[string]clickhouse.SandboxLifecycle
	err        error
}

func (f fakeSandboxLifecycles) QuerySandboxLifecycle(_ context.Context, sandboxID string) (clickhouse.SandboxLifecycle, error) {
	if f.err != nil {
		return clickhouse.SandboxLifecycle{}, f.err
	}

	lifecycle, ok := f.lifecycles[sandboxID]
	if !ok {
		return clickhouse.SandboxLifecycle{}, fmt.Errorf("sandbox %q: %w", sandboxID, clickhouse.ErrSandboxNotFound)
	}

	return lifecycle, nil
}

func TestGetAdminSandboxesSandboxID(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	withEvent := func(eventType string) fakeSandboxLifecycles {
		return fakeSandboxLifecycles{lifecycles: map[string]clickhouse.SandboxLifecycle{
			"sbx1": {TeamID: teamID, EventType: eventType},
		}}
	}

	tests := map[string]struct {
		sandboxID  string
		lifecycles fakeSandboxLifecycles
		wantCode   int
		wantState  api.AdminSandboxState
	}{
		"created is running":                      {sandboxID: "sbx1", lifecycles: withEvent(events.SandboxCreatedEvent), wantCode: http.StatusOK, wantState: api.AdminSandboxStateRunning},
		"resumed is running":                      {sandboxID: "sbx1", lifecycles: withEvent(events.SandboxResumedEvent), wantCode: http.StatusOK, wantState: api.AdminSandboxStateRunning},
		"paused is paused":                        {sandboxID: "sbx1", lifecycles: withEvent(events.SandboxPausedEvent), wantCode: http.StatusOK, wantState: api.AdminSandboxStatePaused},
		"killed is killed":                        {sandboxID: "sbx1", lifecycles: withEvent(events.SandboxKilledEvent), wantCode: http.StatusOK, wantState: api.AdminSandboxStateKilled},
		"accepts the ID with its client suffix":   {sandboxID: "sbx1-client", lifecycles: withEvent(events.SandboxCreatedEvent), wantCode: http.StatusOK, wantState: api.AdminSandboxStateRunning},
		"event without a state is a server error": {sandboxID: "sbx1", lifecycles: withEvent(events.SandboxUpdatedEvent), wantCode: http.StatusInternalServerError},
		"unknown sandbox is not found":            {sandboxID: "sbx2", lifecycles: withEvent(events.SandboxCreatedEvent), wantCode: http.StatusNotFound},
		"malformed ID is rejected":                {sandboxID: "a-b-c", lifecycles: withEvent(events.SandboxCreatedEvent), wantCode: http.StatusBadRequest},
		"query failure is a server error":         {sandboxID: "sbx1", lifecycles: fakeSandboxLifecycles{err: errors.New("clickhouse unavailable")}, wantCode: http.StatusInternalServerError},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := &APIStore{sandboxLifecycles: tt.lifecycles}
			recorder := httptest.NewRecorder()
			ginContext, _ := gin.CreateTestContext(recorder)
			ginContext.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/admin/sandboxes/"+tt.sandboxID, nil)

			store.GetAdminSandboxesSandboxID(ginContext, tt.sandboxID)

			require.Equal(t, tt.wantCode, recorder.Code, recorder.Body.String())
			if tt.wantCode != http.StatusOK {
				return
			}

			var body api.AdminSandbox
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			assert.Equal(t, api.AdminSandbox{SandboxID: "sbx1", TeamID: teamID, State: tt.wantState}, body)
		})
	}
}
