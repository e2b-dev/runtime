package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

type ForbiddenErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func TestBannedTeam(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := setup.GetTestDBClient(t)
	c := setup.GetAPIClient()

	teamName := "test-team-banned"
	teamID := utils.CreateTeamWithUser(t, db, teamName, setup.UserID)
	apiKey := utils.CreateAPIKey(t, ctx, c, setup.UserID, teamID)

	err := db.AuthDb.TestsRawSQL(ctx, `
UPDATE teams SET is_banned = $1 WHERE id = $2
`, true, teamID)
	require.NoError(t, err)

	resp, err := c.GetSandboxesWithResponse(ctx, nil, setup.WithAPIKey(apiKey))
	require.NoError(t, err)

	var errResp ForbiddenErrorResponse
	err = json.Unmarshal(resp.Body, &errResp)
	require.NoError(t, err)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode())
	assert.Equal(t, http.StatusForbidden, errResp.Code)
	assert.Equal(t, "team is banned", errResp.Message)
}

// TestBlockedTeam verifies the blocked-team enforcement performed by
// middleware.EnforceBlockedTeam against the route allowlist
// (see packages/api/internal/middleware/blocked_team.go).
//
// Blocked teams are:
//   - allowed for read and delete operations (recovery + cleanup)
//   - denied for create and mutate operations (resource-consuming)
//
// Mutate / Delete subtests use a synthetic sandbox ID because the
// blocked-team check runs before the resource is resolved — we only
// care that the request was (not) rejected by the blocked-team policy.
func TestBlockedTeam(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := setup.GetTestDBClient(t)
	c := setup.GetAPIClient()

	teamName := "test-team-blocked"
	blockReason := "test-reason"

	teamID := utils.CreateTeamWithUser(t, db, teamName, setup.UserID)
	apiKey := utils.CreateAPIKey(t, ctx, c, setup.UserID, teamID)

	err := db.AuthDb.TestsRawSQL(ctx, `
UPDATE teams SET is_blocked = $1, blocked_reason = $2 WHERE id = $3
`, true, blockReason, teamID)
	require.NoError(t, err)

	assertBlocked := func(t *testing.T, body []byte, status int) {
		t.Helper()

		var errResp ForbiddenErrorResponse
		require.NoError(t, json.Unmarshal(body, &errResp))
		assert.Equal(t, http.StatusForbidden, status)
		assert.Equal(t, http.StatusForbidden, errResp.Code)
		assert.Equal(t, "team is blocked: test-reason", errResp.Message)
	}

	assertNotBlocked := func(t *testing.T, body []byte, status int) {
		t.Helper()

		assert.NotEqual(t, http.StatusForbidden, status, "request should not be rejected by blocked-team policy")
		assert.NotContains(t, string(body), "team is blocked", "response body should not surface blocked-team error")
	}

	t.Run("view is allowed (GET /sandboxes)", func(t *testing.T) {
		t.Parallel()

		resp, err := c.GetSandboxesWithResponse(ctx, nil, setup.WithAPIKey(apiKey))
		require.NoError(t, err)
		assertNotBlocked(t, resp.Body, resp.StatusCode())
	})

	t.Run("view is allowed (GET /v2/templates)", func(t *testing.T) {
		t.Parallel()

		resp, err := c.GetV2TemplatesWithResponse(ctx, &api.GetV2TemplatesParams{}, setup.WithAPIKey(apiKey))
		require.NoError(t, err)
		assertNotBlocked(t, resp.Body, resp.StatusCode())
	})

	t.Run("create is denied (POST /sandboxes)", func(t *testing.T) {
		t.Parallel()

		resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
			TemplateID: setup.SandboxTemplateID,
		}, setup.WithAPIKey(apiKey))
		require.NoError(t, err)
		assertBlocked(t, resp.Body, resp.StatusCode())
	})

	t.Run("mutate is denied (POST /sandboxes/:sandboxID/pause)", func(t *testing.T) {
		t.Parallel()

		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, "nonexistent-sandbox", api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey(apiKey))
		require.NoError(t, err)
		assertBlocked(t, resp.Body, resp.StatusCode())
	})

	t.Run("delete is allowed (DELETE /sandboxes/:sandboxID)", func(t *testing.T) {
		t.Parallel()

		resp, err := c.DeleteSandboxesSandboxIDWithResponse(ctx, "nonexistent-sandbox", setup.WithAPIKey(apiKey))
		require.NoError(t, err)
		assertNotBlocked(t, resp.Body, resp.StatusCode())
	})
}
