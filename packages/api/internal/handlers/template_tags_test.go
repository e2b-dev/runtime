package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	apispec "github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestPostTemplatesTags_RejectsOtherTeamTemplate(t *testing.T) {
	t.Parallel()

	testDB := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	ownerTeamID := testutils.CreateTestTeam(t, testDB)
	templateID := testutils.CreateTestTemplate(t, testDB, ownerTeamID)

	otherTeamID := testutils.CreateTestTeam(t, testDB)
	otherTeamSlug := testutils.GetTeamSlug(t, ctx, testDB, otherTeamID)

	store := &APIStore{
		sqlcDB:        testDB.SqlcClient,
		authDB:        testDB.AuthDb,
		templateCache: templatecache.NewTemplateCache(testDB.SqlcClient, redis),
	}

	body, err := json.Marshal(apispec.AssignTemplateTagsRequest{
		Target: templateID,
		Tags:   []string{"v1.0.0"},
	})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/templates/tags", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, c, &types.Team{
		Team: &authqueries.Team{
			ID:   otherTeamID,
			Slug: otherTeamSlug,
		},
	})

	store.PostTemplatesTags(c)

	res, err := apispec.ParsePostTemplatesTagsResponse(w.Result())
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, res.StatusCode())
}
