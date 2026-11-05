package builds

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// liveRootIDs runs the query and returns the build IDs as a set for assertions.
func liveRootIDs(t *testing.T, db *testutils.Database, teamID uuid.UUID, excludeTemplateID string) map[uuid.UUID]struct{} {
	t.Helper()

	rows, err := db.SqlcClient.GetLiveBuildRootsForTeamExcludingTemplate(t.Context(), queries.GetLiveBuildRootsForTeamExcludingTemplateParams{
		TeamID:     teamID,
		TemplateID: excludeTemplateID,
	})
	require.NoError(t, err)

	set := make(map[uuid.UUID]struct{}, len(rows))
	for _, id := range rows {
		set[id] = struct{}{}
	}

	return set
}

// A build assigned to ANOTHER active template of the team is a live root; the
// doomed template's own exclusive build is excluded (source 1 of the UNION).
func TestGetLiveBuildRoots_OtherActiveTemplateIsRoot(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	doomed := testutils.CreateTestTemplate(t, db, teamID)
	other := testutils.CreateTestTemplate(t, db, teamID)

	doomedBuild := testutils.CreateTestBuild(t, ctx, db, doomed, "uploaded")
	testutils.CreateTestBuildAssignment(t, ctx, db, doomed, doomedBuild, "default")

	otherBuild := testutils.CreateTestBuild(t, ctx, db, other, "uploaded")
	testutils.CreateTestBuildAssignment(t, ctx, db, other, otherBuild, "default")

	roots := liveRootIDs(t, db, teamID, doomed)

	assert.Contains(t, roots, otherBuild, "a build of another active template is a live root")
	assert.NotContains(t, roots, doomedBuild, "the doomed template's own assigned build is excluded")
}

// The team's in-progress / pending builds are live roots even with NO
// assignment row yet (source 2 of the UNION). This is the gap the plain
// assignment-join query missed: a concurrent build reusing the doomed
// template's exclusive ancestor would otherwise be invisible to the guard.
func TestGetLiveBuildRoots_InProgressBuildWithoutAssignmentIsRoot(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	doomed := testutils.CreateTestTemplate(t, db, teamID)
	other := testutils.CreateTestTemplate(t, db, teamID)

	// 'building' -> status_group 'in_progress'; 'pending' -> 'pending'.
	// Neither has an env_build_assignments row.
	building := testutils.CreateTestBuild(t, ctx, db, other, "building")
	pending := testutils.CreateTestBuild(t, ctx, db, other, "pending")

	roots := liveRootIDs(t, db, teamID, doomed)

	assert.Contains(t, roots, building, "an in_progress build with no assignment must still be a live root")
	assert.Contains(t, roots, pending, "a pending build with no assignment must still be a live root")
}

// A soft-deleted other template's build is NOT a live root (active_envs join),
// and a ready build with no assignment is NOT a root (not in-progress).
func TestGetLiveBuildRoots_SoftDeletedAndReadyUnassignedExcluded(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	doomed := testutils.CreateTestTemplate(t, db, teamID)
	gone := testutils.CreateTestTemplate(t, db, teamID)

	goneBuild := testutils.CreateTestBuild(t, ctx, db, gone, "uploaded")
	testutils.CreateTestBuildAssignment(t, ctx, db, gone, goneBuild, "default")
	// Soft-delete that template: its assigned build must drop out of live roots.
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, gone))

	// A terminal (ready) build with no assignment is not in-progress and has no
	// assignment, so neither UNION source includes it.
	readyUnassigned := testutils.CreateTestBuild(t, ctx, db, doomed, "uploaded")

	roots := liveRootIDs(t, db, teamID, doomed)

	assert.NotContains(t, roots, goneBuild, "a soft-deleted template's build is not a live root")
	assert.NotContains(t, roots, readyUnassigned, "a ready build with no assignment is not a live root")
}

// The query is team-scoped: another team's in-progress build is never a root.
func TestGetLiveBuildRoots_OtherTeamExcluded(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	doomed := testutils.CreateTestTemplate(t, db, teamID)

	otherTeam := testutils.CreateTestTeam(t, db)
	otherTeamTpl := testutils.CreateTestTemplate(t, db, otherTeam)
	otherTeamBuild := testutils.CreateTestBuild(t, ctx, db, otherTeamTpl, "building")

	roots := liveRootIDs(t, db, teamID, doomed)

	assert.NotContains(t, roots, otherTeamBuild, "another team's build must never be a live root")
}
