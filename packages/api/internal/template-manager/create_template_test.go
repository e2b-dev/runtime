package template_manager

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestSetTemplateSource_FromTemplateUsesResolvedBuild(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "base-template", &teamSlug)

	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "ready")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "v1")

	cache := templatecache.NewTemplateCache(db.SqlcClient, redis)
	defer func() {
		require.NoError(t, cache.Close(ctx))
	}()

	tm := &TemplateManager{
		sqlcDB:        db.SqlcClient,
		templateCache: cache,
	}

	fromTemplate := id.WithTag(id.WithNamespace(teamSlug, "base-template"), "v1")
	template := &templatemanagergrpc.TemplateConfig{}

	err := setTemplateSource(ctx, tm, teamID, teamSlug, template, nil, &fromTemplate)
	require.NoError(t, err)

	require.NotNil(t, template.GetFromTemplate())
	assert.Equal(t, fromTemplate, template.GetFromTemplate().GetAlias())
	assert.Equal(t, buildID.String(), template.GetFromTemplate().GetBuildID())
}

func TestSetTemplateSource_FromTemplateMissingBuildReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		requesterTeam string
		public        bool
		wantMessage   func(ownerSlug, alias string) string
	}{
		{
			name:          "owner gets tag-specific not found",
			requesterTeam: "owner",
			public:        false,
			wantMessage: func(ownerSlug, alias string) string {
				return fmt.Sprintf(
					"tag 'v2' does not exist for template '%s'",
					id.WithNamespace(ownerSlug, alias),
				)
			},
		},
		{
			name:          "foreign team gets tag-specific not found for public template",
			requesterTeam: "foreign",
			public:        true,
			wantMessage: func(ownerSlug, alias string) string {
				return fmt.Sprintf(
					"tag 'v2' does not exist for template '%s'",
					id.WithNamespace(ownerSlug, alias),
				)
			},
		},
		{
			name:          "foreign team gets generic not found for private template",
			requesterTeam: "foreign",
			public:        false,
			wantMessage: func(ownerSlug, alias string) string {
				return fmt.Sprintf("template '%s' not found", id.WithNamespace(ownerSlug, alias))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			redis := redis_utils.SetupInstance(t)
			ctx := t.Context()

			ownerTeamID := testutils.CreateTestTeam(t, db)
			ownerTeamSlug := testutils.GetTeamSlug(t, ctx, db, ownerTeamID)
			requesterTeamID := ownerTeamID
			requesterTeamSlug := ownerTeamSlug

			if tt.requesterTeam == "foreign" {
				requesterTeamID = testutils.CreateTestTeam(t, db)
				requesterTeamSlug = testutils.GetTeamSlug(t, ctx, db, requesterTeamID)
			}

			templateID := testutils.CreateTestTemplate(t, db, ownerTeamID)
			setTemplatePublic(t, db, templateID, tt.public)
			testutils.CreateTestTemplateAliasWithName(t, db, templateID, "base-template-missing", &ownerTeamSlug)

			buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "ready")
			testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, id.DefaultTag)

			cache := templatecache.NewTemplateCache(db.SqlcClient, redis)
			defer func() {
				require.NoError(t, cache.Close(ctx))
			}()

			tm := &TemplateManager{
				sqlcDB:        db.SqlcClient,
				templateCache: cache,
			}

			fromTemplate := id.WithTag(id.WithNamespace(ownerTeamSlug, "base-template-missing"), "v2")
			template := &templatemanagergrpc.TemplateConfig{}

			err := setTemplateSource(ctx, tm, requesterTeamID, requesterTeamSlug, template, nil, &fromTemplate)
			require.Error(t, err)

			var fromTemplateErr *FromTemplateError
			require.ErrorAs(t, err, &fromTemplateErr)

			assert.Equal(t, tt.wantMessage(ownerTeamSlug, "base-template-missing"), err.Error())
			assert.Nil(t, template.GetFromTemplate())
		})
	}
}

func setTemplatePublic(t *testing.T, db *testutils.Database, templateID string, public bool) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(
		t.Context(),
		"UPDATE public.envs SET public = $2 WHERE id = $1",
		templateID,
		public,
	)
	require.NoError(t, err)
}

func TestSetTemplateSource_WithoutSourceIsRejected(t *testing.T) {
	t.Parallel()

	// The V1 path sent an empty fromImage and let the builder find the base itself.
	emptyImage := ""
	emptyTemplate := ""

	tests := []struct {
		name         string
		fromImage    *string
		fromTemplate *string
	}{
		{name: "neither source"},
		{name: "empty fromImage", fromImage: &emptyImage},
		{name: "empty fromTemplate", fromTemplate: &emptyTemplate},
		{name: "both empty", fromImage: &emptyImage, fromTemplate: &emptyTemplate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			template := &templatemanagergrpc.TemplateConfig{}
			err := setTemplateSource(t.Context(), nil, uuid.Nil, "team", template, tt.fromImage, tt.fromTemplate)

			require.EqualError(t, err, "must specify either fromImage or fromTemplate")
			assert.Nil(t, template.GetSource())
		})
	}
}
