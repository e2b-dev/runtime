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

// A snapshot (paused sandbox / checkpoint) build is a live root for the team
// even when the template being deleted is its BASE template (source 3 of the
// UNION). A snapshot is a COW diff on top of the base, so its header mapping
// references the base template's builds; it must protect them so deleting the
// base cannot orphan a still-referenced ancestor. This is the checkpoint
// corruption guard.
func TestGetLiveBuildRoots_SnapshotBuildIsRoot(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	base := testutils.CreateTestTemplate(t, db, teamID)

	// A snapshot whose base_env_id = base, with its own env + build. The
	// snapshot build is the COW layer that references base's builds.
	snap := testutils.UpsertTestSnapshot(t, ctx, db, "", "sbx-snap-1", teamID, base)

	// Deleting the base template: the snapshot build must appear as a live root
	// so base's ancestor builds are kept.
	roots := liveRootIDs(t, db, teamID, base)
	assert.Contains(t, roots, snap.BuildID, "a snapshot build on this base must be a live root when the base is deleted")
}

// Source 3 is deliberately NOT gated on active_envs: a snapshot build still
// protects the base's ancestors even after the snapshot's OWN env is
// soft-deleted but its diff layer still lives in storage. This is exactly the
// window ExistsTemplateSnapshots (which joins active_envs) can let a base
// deletion through — without source 3 the base ancestor would fall out of the
// keep set and be deleted, corrupting the orphaned-but-present snapshot.
func TestGetLiveBuildRoots_SnapshotBuildIsRootEvenAfterSnapshotEnvSoftDeleted(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	base := testutils.CreateTestTemplate(t, db, teamID)
	snap := testutils.UpsertTestSnapshot(t, ctx, db, "", "sbx-snap-2", teamID, base)

	// Soft-delete the snapshot's OWN env (its diff layer is assumed still in
	// storage). An active_envs-gated source would now miss it.
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, snap.TemplateID))

	roots := liveRootIDs(t, db, teamID, base)
	assert.Contains(t, roots, snap.BuildID,
		"a snapshot build must remain a live root for its base even after the snapshot's own env is soft-deleted")
}

// A snapshot of ANOTHER team never protects this team's base (team-scoped).
func TestGetLiveBuildRoots_SnapshotOtherTeamExcluded(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	base := testutils.CreateTestTemplate(t, db, teamID)

	otherTeam := testutils.CreateTestTeam(t, db)
	otherBase := testutils.CreateTestTemplate(t, db, otherTeam)
	otherSnap := testutils.UpsertTestSnapshot(t, ctx, db, "", "sbx-snap-other", otherTeam, otherBase)

	roots := liveRootIDs(t, db, teamID, base)
	assert.NotContains(t, roots, otherSnap.BuildID, "another team's snapshot build must never be a live root here")
}
