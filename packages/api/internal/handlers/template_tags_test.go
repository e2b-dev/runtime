package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestPostTemplatesTags_RejectsOtherTeamBeforeTransaction(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"", ":default", ":v1.0.0", ":missing-tag"} {
		t.Run("target="+suffix, func(t *testing.T) {
			t.Parallel()

			redis := redis_utils.SetupInstance(t)
			const templateID = "foreign-template"
			const teamSlug = "requesting-team"
			alias, err := json.Marshal(templatecache.AliasInfo{
				TemplateID: templateID,
				TeamID:     uuid.New(),
			})
			require.NoError(t, err)
			require.NoError(t, redis.Set(t.Context(), "template:alias:"+teamSlug+"/"+templateID, `{"not_found":true}`, 5*time.Minute).Err())
			require.NoError(t, redis.Set(t.Context(), "template:alias:"+templateID, alias, 5*time.Minute).Err())

			// A nil database makes any transaction or tag lookup fail the test.
			store := &APIStore{templateCache: templatecache.NewTemplateCache(nil, redis)}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			body := fmt.Sprintf(`{"target":%q,"tags":["new-tag"]}`, templateID+suffix)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/templates/tags", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, c, &types.Team{Team: &dbtypes.Team{
				ID: uuid.New(), Slug: teamSlug,
			}})

			store.PostTemplatesTags(c)

			require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
			require.JSONEq(t, `{"code":403,"message":"You don't have access to sandbox template 'foreign-template'"}`, recorder.Body.String())
		})
	}
}
