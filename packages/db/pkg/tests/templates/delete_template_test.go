package templates

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func envDeleted(t *testing.T, db *testutils.Database, envID string) bool {
	t.Helper()
	var deleted bool
	err := db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT deleted_at IS NOT NULL FROM public.envs WHERE id = $1",
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&deleted)
			}

			return nil
		},
		envID,
	)
	require.NoError(t, err)

	return deleted
}

func TestDeleteTemplate_SoftDeletesEnvAndPreservesStructure(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "default")
	alias := testutils.CreateTestTemplateAlias(t, db, templateID)

	_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: templateID, TeamID: teamID})
	require.NoError(t, err)
	_, err = db.SqlcClient.ReleaseTemplateAliases(ctx, templateID)
	require.NoError(t, err)

	assert.True(t, testutils.GetEnvByID(t, ctx, db, templateID), "env row must be preserved")
	assert.True(t, envDeleted(t, db, templateID), "env must be soft-deleted")
	assert.True(t, testutils.GetEnvBuildByID(t, ctx, db, buildID), "build row must be preserved")
	assert.NotEmpty(t, testutils.GetBuildAssignments(t, ctx, db, templateID), "build assignment must be preserved")

	templates, err := db.SqlcClient.GetTeamTemplates(ctx, queries.GetTeamTemplatesParams{TeamID: teamID})
	require.NoError(t, err)
	assert.Empty(t, templates, "soft-deleted template must not be listed")

	var aliasCount int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT COUNT(*) FROM public.env_aliases WHERE alias = $1",
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&aliasCount)
			}

			return nil
		},
		alias,
	))
	assert.Zero(t, aliasCount, "alias must be released for reuse")
}
