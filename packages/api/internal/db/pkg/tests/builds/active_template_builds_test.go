package builds

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

func TestGetInProgressTemplateBuildsByTeam_ExcludesSameTemplateWithOverlappingTags(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	otherTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	err := db.SqlcClient.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    uuid.New(),
		TeamID:     teamID,
		TemplateID: templateID,
		Tags:       []string{"latest", "prod"},
	})
	require.NoError(t, err)

	err = db.SqlcClient.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    uuid.New(),
		TeamID:     teamID,
		TemplateID: templateID,
		Tags:       []string{"staging"},
	})
	require.NoError(t, err)

	err = db.SqlcClient.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    uuid.New(),
		TeamID:     teamID,
		TemplateID: otherTemplateID,
		Tags:       []string{"latest"},
	})
	require.NoError(t, err)

	count, err := db.SqlcClient.GetInProgressTemplateBuildsByTeam(ctx, queries.GetInProgressTemplateBuildsByTeamParams{
		TeamID:            teamID,
		ExcludeTemplateID: templateID,
		ExcludeTags:       []string{"latest"},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(2), count)
}

func TestGetInProgressTemplateBuildsByTeam_IgnoresRowsOlderThanDay(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := uuid.New()

	err := db.SqlcClient.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    buildID,
		TeamID:     teamID,
		TemplateID: templateID,
		Tags:       []string{"latest"},
	})
	require.NoError(t, err)

	err = db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.active_template_builds
		 SET created_at = NOW() - INTERVAL '2 days'
		 WHERE build_id = $1`,
		buildID,
	)
	require.NoError(t, err)

	count, err := db.SqlcClient.GetInProgressTemplateBuildsByTeam(ctx, queries.GetInProgressTemplateBuildsByTeamParams{
		TeamID:            teamID,
		ExcludeTemplateID: "other-template",
		ExcludeTags:       []string{"latest"},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(0), count)
}

func TestDeleteActiveTemplateBuild_RemovesActiveBuild(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := uuid.New()

	err := db.SqlcClient.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    buildID,
		TeamID:     teamID,
		TemplateID: templateID,
		Tags:       []string{"latest"},
	})
	require.NoError(t, err)

	err = db.SqlcClient.DeleteActiveTemplateBuild(ctx, buildID)
	require.NoError(t, err)

	count, err := db.SqlcClient.GetInProgressTemplateBuildsByTeam(ctx, queries.GetInProgressTemplateBuildsByTeamParams{
		TeamID:            teamID,
		ExcludeTemplateID: "other-template",
		ExcludeTags:       []string{"latest"},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(0), count)
}
