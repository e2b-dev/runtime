package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestPostAdminTeamsBootstrapAlwaysFails(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/admin/teams/bootstrap", strings.NewReader(`{
		"name": "Bootstrap team",
		"email": "bootstrap-team@example.com"
	}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store := &APIStore{}
	store.PostAdminTeamsBootstrap(ginCtx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), retiredBootstrapMessage) {
		t.Fatalf("expected retired-operation message, got %s", recorder.Body.String())
	}
}
