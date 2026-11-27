package api_templates

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	testutils "github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

// seedTemplate inserts a minimal template (env) row for the team with the given
// created_at offset (hours into the past), so listing order is deterministic.
// Registers cleanup that deletes the row before the team is torn down.
func seedTemplate(t *testing.T, db *setup.Database, teamID uuid.UUID, hoursAgo int) string {
	t.Helper()

	id := "tmpl-" + uuid.NewString()
	err := db.Db.TestsRawSQL(t.Context(),
		`INSERT INTO public.envs (id, team_id, public, created_at, updated_at, source)
		 VALUES ($1, $2, true, NOW() - ($3 || ' hours')::interval, NOW(), 'template')`,
		id, teamID, hoursAgo,
	)
	require.NoError(t, err, "failed to seed template")

	t.Cleanup(func() {
		_ = db.Db.TestsRawSQL(t.Context(), "DELETE FROM public.envs WHERE id = $1", id)
	})

	return id
}

func TestListTemplatesV2WithAPIKey(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	response, err := c.GetV2TemplatesWithResponse(
		t.Context(),
		&api.GetV2TemplatesParams{},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)

	assert.NotNil(t, *response.JSON200, "Expected templates list to not be nil")
}

func TestListTemplatesV2WithAPIKeyAndTeamID(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	response, err := c.GetV2TemplatesWithResponse(
		t.Context(),
		&api.GetV2TemplatesParams{
			TeamID: &setup.TeamID,
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)
}

func TestListTemplatesV2WithMismatchedTeamID(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	mismatchedTeamID := "00000000-0000-0000-0000-000000000000"

	response, err := c.GetV2TemplatesWithResponse(
		t.Context(),
		&api.GetV2TemplatesParams{
			TeamID: &mismatchedTeamID,
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode(),
		"Expected 400 Bad Request for mismatched team ID")
}

func TestListTemplatesV2WithInvalidAPIKey(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	response, err := c.GetV2TemplatesWithResponse(
		t.Context(),
		&api.GetV2TemplatesParams{},
		setup.WithAPIKey("invalid-api-key"),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, response.StatusCode())
	require.NotNil(t, response.JSON401)
}

// TestListTemplatesV2Pagination exercises the real pagination flow against an
// isolated team seeded with a known set of templates: page through the whole
// list with limit=1 by following X-Next-Token, and assert every page round-trips
// correctly — each template is returned exactly once, newest-first, the page
// size is honored, and the final page carries no token.
func TestListTemplatesV2Pagination(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	// Isolated team so the listing contains only our seeded templates.
	teamID := testutils.CreateTeam(t, db, "templates-v2-pagination")
	apiKey := testutils.CreateAPIKey(t, ctx, c, "", teamID)

	// Seed 3 templates with distinct created_at; expected newest-first order.
	newest := seedTemplate(t, db, teamID, 1)
	middle := seedTemplate(t, db, teamID, 2)
	oldest := seedTemplate(t, db, teamID, 3)
	want := []string{newest, middle, oldest}

	// Page through one item at a time, following the cursor to exhaustion.
	var limit int32 = 1
	var got []string
	var nextToken *string
	seen := make(map[string]bool)

	for range len(want) + 2 { // +2 headroom guards against a non-terminating cursor
		resp, err := c.GetV2TemplatesWithResponse(ctx,
			&api.GetV2TemplatesParams{Limit: &limit, NextToken: nextToken},
			setup.WithAPIKey(apiKey),
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)

		items := *resp.JSON200
		token := resp.HTTPResponse.Header.Get("X-Next-Token")

		if len(items) == 0 {
			assert.Empty(t, token, "empty page must not advertise more results")

			break
		}

		require.Len(t, items, int(limit), "page must contain exactly `limit` items while results remain")
		id := items[0].TemplateID
		require.False(t, seen[id], "template %s returned on more than one page", id)
		seen[id] = true
		got = append(got, id)

		if token == "" {
			break // last page
		}
		nextToken = &token
	}

	assert.Equal(t, want, got, "should page through all seeded templates exactly once, newest-first")
}
