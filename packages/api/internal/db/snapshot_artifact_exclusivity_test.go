package db

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// buildIDs extracts the build IDs from exclusive-builds rows for assertions.
func buildIDs(rows []queries.GetExclusiveBuildsForTemplateDeletionRow) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.BuildID)
	}

	return out
}

// Proves the artifact-cleanup exclusivity guard holds on the SNAPSHOT path:
// a snapshot is backed by its own template (deleteSnapshot soft-deletes it), so
// GetExclusiveBuildsForTemplateDeletion(snapshot.TemplateID) must return the
// builds exclusively owned by that snapshot-template — including after the
// template is soft-deleted — and must NOT return a build shared with another
// template. This is what gates which artifacts snapshot deletion removes.
func TestSnapshotArtifactExclusivity(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseEnvID := testutils.CreateTestTemplate(t, db, teamID)

	// Snapshot backed by its own template; UpsertSnapshot creates one build.
	_, snapTemplateID, snapshotBuildID := createTestSnapshot(t, db, teamID, baseEnvID)

	// An extra build exclusively owned by the snapshot's template.
	exclusiveBuildID := createTestEnvBuild(t, db, snapTemplateID)

	// A build shared with ANOTHER template: assigned to both the snapshot's
	// template and a second template -> must be excluded by the guard.
	otherTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sharedBuildID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_builds
		(id, status, vcpu, ram_mb, free_disk_size_mb, total_disk_size_mb, kernel_version, firecracker_version, envd_version, cluster_node_id, created_at, updated_at, version)
		VALUES ($1,'ready',2,2048,512,1024,'6.1.0','1.4.0','v1.0.0','test-node',NOW(),NOW(),1)`,
		sharedBuildID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag) VALUES ($1,$2,'default')`,
		snapTemplateID, sharedBuildID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag) VALUES ($1,$2,'default')`,
		otherTemplateID, sharedBuildID))

	// Soft-delete the snapshot's template, exactly as deleteSnapshot does, to
	// prove the query still resolves afterwards (it joins raw envs, not active).
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, snapTemplateID))

	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, snapTemplateID)
	require.NoError(t, err)

	got := buildIDs(rows)
	assert.Contains(t, got, snapshotBuildID, "the snapshot's own build is exclusive -> returned")
	assert.Contains(t, got, exclusiveBuildID, "the extra exclusive build is returned")
	assert.NotContains(t, got, sharedBuildID, "a build shared with another template must NOT be returned")

	// Every returned build carries the node id needed to route DeleteBuild.
	for _, r := range rows {
		require.NotNil(t, r.ClusterNodeID, "exclusive build must carry cluster_node_id for routing")
	}
}
