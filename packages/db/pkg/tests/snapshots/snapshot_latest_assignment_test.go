package snapshots

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestSnapshotCursorMetadataFilter(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	team := testutils.CreateTestTeam(t, db)
	template := testutils.CreateTestTemplate(t, db, team)
	for _, owner := range []string{"alice", "bob"} {
		id := "sandbox-" + owner
		testutils.UpsertTestSnapshot(t, ctx, db, "snapshot-"+owner, id, team, template)
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			"UPDATE snapshots SET metadata=$2::jsonb WHERE sandbox_id=$1", id, types.JSONBStringMap{"owner": owner}))
	}
	config, err := pgx.ParseConfig(db.ConnStr())
	require.NoError(t, err)
	config.DefaultQueryExecMode = pgx.QueryExecModeExec

	for _, tc := range []struct {
		name     string
		metadata types.JSONBStringMap
		count    int
	}{
		{name: "nil", count: 2},
		{name: "empty", metadata: types.JSONBStringMap{}, count: 2},
		{name: "matching", metadata: types.JSONBStringMap{"owner": "alice"}, count: 1},
		{name: "missing", metadata: types.JSONBStringMap{"owner": "nobody"}, count: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			conn, err := pgx.ConnectConfig(ctx, config)
			require.NoError(t, err)
			defer func() { require.NoError(t, conn.Close(ctx)) }()

			// Inspect the actual generated statements without pinning a scan type
			// or timings: empty filters must disappear from the custom plan.
			q := queries.New(&snapshotMetadataPlan{DBTX: conn, t: t, filtered: len(tc.metadata) > 0})
			end := pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true}
			begin := pgtype.Timestamptz{Time: time.Time{}, Valid: true}
			desc, err := q.GetSnapshotsWithCursor(ctx, queries.GetSnapshotsWithCursorParams{
				TeamID: team, Metadata: tc.metadata, Limit: 10, CursorTime: end,
			})
			require.NoError(t, err)
			require.Len(t, desc, tc.count)
			asc, err := q.GetSnapshotsWithCursorAsc(ctx, queries.GetSnapshotsWithCursorAscParams{
				TeamID: team, Metadata: tc.metadata, Limit: 10, CursorTime: begin,
			})
			require.NoError(t, err)
			require.Len(t, asc, tc.count)
			byTemplate, err := q.GetSnapshotsByTemplateWithCursor(ctx, queries.GetSnapshotsByTemplateWithCursorParams{
				TeamID: team, TemplateID: template, Metadata: tc.metadata, Limit: 10, CursorTime: end,
			})
			require.NoError(t, err)
			require.Len(t, byTemplate, tc.count)
			byTemplateAsc, err := q.GetSnapshotsByTemplateWithCursorAsc(ctx, queries.GetSnapshotsByTemplateWithCursorAscParams{
				TeamID: team, TemplateID: template, Metadata: tc.metadata, Limit: 10, CursorTime: begin,
			})
			require.NoError(t, err)
			require.Len(t, byTemplateAsc, tc.count)
			for _, row := range desc {
				if len(tc.metadata) > 0 {
					require.Equal(t, "alice", row.Snapshot.Metadata["owner"])
				}
			}
		})
	}
}

type snapshotMetadataPlan struct {
	queries.DBTX

	t        *testing.T
	filtered bool
}

func (p *snapshotMetadataPlan) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.t.Helper()
	var plan string
	require.NoError(p.t, p.DBTX.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&plan))
	if p.filtered {
		require.Contains(p.t, plan, "@>")
	} else {
		require.NotContains(p.t, plan, "@>", "an empty metadata filter must not leave a containment predicate in the plan")
	}

	return p.DBTX.Query(ctx, sql, args...)
}

// TestGetLastSnapshot_ReturnsLatestAssignment verifies that GetLastSnapshot returns
// the build from the most recent assignment (ordered by assignment created_at DESC).
func TestGetLastSnapshot_ReturnsLatestAssignment(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and base template for snapshot
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create first snapshot (creates template, build, and assignment)
	sandboxID := "sandbox-" + uuid.New().String()
	snapshotTemplateID := "snapshot-template-" + uuid.New().String()

	result1 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	build1ID := result1.BuildID

	// Small delay to ensure different created_at timestamps
	time.Sleep(10 * time.Millisecond)

	// Upsert again with same sandbox_id - creates a new build and assignment
	result2 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	build2ID := result2.BuildID

	// Verify we got different builds
	require.NotEqual(t, build1ID, build2ID, "Each upsert should create a new build")

	// Execute GetLastSnapshot - should return the latest build (build2)
	snapshot, err := db.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)

	assert.Equal(t, build2ID, snapshot.EnvBuild.ID,
		"GetLastSnapshot should return the build from the latest assignment")
}

// TestGetLastSnapshot_OnlyReturnsSuccessBuilds verifies that GetLastSnapshot only
// returns builds with status IN ('success', 'uploaded').
func TestGetLastSnapshot_OnlyReturnsSuccessBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and base template
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	sandboxID := "sandbox-" + uuid.New().String()
	snapshotTemplateID := "snapshot-template-" + uuid.New().String()

	// Create first snapshot with success status
	result1 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	successBuildID := result1.BuildID

	time.Sleep(10 * time.Millisecond)

	// Create second snapshot with snapshotting status (not success)
	testutils.UpsertTestSnapshotWithStatus(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID, types.BuildStatusSnapshotting)

	// GetLastSnapshot should return the success build, not the snapshotting one
	snapshot, err := db.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)

	assert.Equal(t, successBuildID, snapshot.EnvBuild.ID,
		"GetLastSnapshot should only return builds with status IN ('success', 'uploaded')")
}

// TestGetSnapshotsWithCursor_ReturnsLatestAssignment verifies that GetSnapshotsWithCursor
// returns the build from the most recent assignment for each snapshot.
func TestGetSnapshotsWithCursor_ReturnsLatestAssignment(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and base template
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	sandboxID := "sandbox-" + uuid.New().String()
	snapshotTemplateID := "snapshot-template-" + uuid.New().String()

	// Create first snapshot
	testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)

	time.Sleep(10 * time.Millisecond)

	// Upsert again - creates a new build (latest)
	result2 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	latestBuildID := result2.BuildID

	// Execute GetSnapshotsWithCursor
	results, err := db.SqlcClient.GetSnapshotsWithCursor(ctx, queries.GetSnapshotsWithCursorParams{
		TeamID:     teamID,
		Metadata:   types.JSONBStringMap{},
		CursorID:   "",
		CursorTime: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		Limit:      10,
	})
	require.NoError(t, err)
	require.Len(t, results, 1, "Should return 1 snapshot")

	assert.Equal(t, latestBuildID, results[0].BuildID,
		"GetSnapshotsWithCursor should return the build from the latest assignment")
}

// TestGetSnapshotsWithCursorAsc_OrdersOldestFirstAndPaginates verifies the ascending
// keyset query returns snapshots oldest-first and that its cursor predicate walks the
// pages without gaps or overlaps.
func TestGetSnapshotsWithCursorAsc_OrdersOldestFirstAndPaginates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create three snapshots with strictly increasing sandbox_started_at.
	oldestToNewest := make([]string, 0, 3)
	snapshotTemplateIDs := make([]string, 0, 3)
	for range 3 {
		sandboxID := "sandbox-" + uuid.New().String()
		snapshotTemplateID := "snapshot-template-" + uuid.New().String()
		testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
		oldestToNewest = append(oldestToNewest, sandboxID)
		snapshotTemplateIDs = append(snapshotTemplateIDs, snapshotTemplateID)
		time.Sleep(10 * time.Millisecond)
	}

	// First page (zero-time / MaxSandboxID cursor) mirrors the handler's first-page
	// defaults for ascending order.
	firstPageCursor := queries.GetSnapshotsWithCursorAscParams{
		TeamID:     teamID,
		Metadata:   types.JSONBStringMap{},
		CursorID:   "zzzzzzzzzzzzzzzzzzzz",
		CursorTime: pgtype.Timestamptz{Time: time.Time{}, Valid: true},
		Limit:      10,
	}

	all, err := db.SqlcClient.GetSnapshotsWithCursorAsc(ctx, firstPageCursor)
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, []string{
		all[0].Snapshot.SandboxID,
		all[1].Snapshot.SandboxID,
		all[2].Snapshot.SandboxID,
	}, oldestToNewest, "ascending query should return oldest sandbox first")

	// Walk the pages with limit 2: page one is the two oldest...
	page1Cursor := firstPageCursor
	page1Cursor.Limit = 2
	page1, err := db.SqlcClient.GetSnapshotsWithCursorAsc(ctx, page1Cursor)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, []string{page1[0].Snapshot.SandboxID, page1[1].Snapshot.SandboxID}, oldestToNewest[:2])

	// ...and the next page (cursor = last returned row) is the newest, with no overlap.
	last := page1[1].Snapshot
	page2, err := db.SqlcClient.GetSnapshotsWithCursorAsc(ctx, queries.GetSnapshotsWithCursorAscParams{
		TeamID:     teamID,
		Metadata:   types.JSONBStringMap{},
		CursorID:   last.SandboxID,
		CursorTime: last.SandboxStartedAt,
		Limit:      2,
	})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, oldestToNewest[2], page2[0].Snapshot.SandboxID)

	// The ascending query must return the same active set as the descending query.
	// Soft-deleting the oldest snapshot template leaves its snapshot row behind, so
	// only the active_envs join prevents it from reappearing in ascending results.
	_, err = db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{
		TemplateID: snapshotTemplateIDs[0],
		TeamID:     teamID,
	})
	require.NoError(t, err)

	active, err := db.SqlcClient.GetSnapshotsWithCursorAsc(ctx, firstPageCursor)
	require.NoError(t, err)
	require.Len(t, active, 2)
	activeIDs := []string{
		active[0].Snapshot.SandboxID,
		active[1].Snapshot.SandboxID,
	}
	assert.Equal(t, oldestToNewest[1:], activeIDs)
}

func TestGetSnapshotsByTemplateWithCursor_FiltersTemplateAndStartedAfter(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateA := testutils.CreateTestTemplate(t, db, teamID)
	templateB := testutils.CreateTestTemplate(t, db, teamID)
	startedAfter := time.Date(2026, 1, 2, 11, 0, 0, 0, time.UTC)

	createSnapshot := func(sandboxID, baseTemplateID string, startedAt time.Time) {
		t.Helper()
		testutils.UpsertTestSnapshot(
			t,
			ctx,
			db,
			"snapshot-template-"+uuid.New().String(),
			sandboxID,
			teamID,
			baseTemplateID,
		)
		require.NoError(t, db.SqlcClient.TestsRawSQL(
			ctx,
			"UPDATE public.snapshots SET sandbox_started_at = $1 WHERE sandbox_id = $2",
			startedAt,
			sandboxID,
		))
	}

	createSnapshot("template-a-old", templateA, startedAfter.Add(-time.Hour))
	createSnapshot("template-a-at-bound", templateA, startedAfter)
	createSnapshot("template-a-new", templateA, startedAfter.Add(time.Hour))
	createSnapshot("template-b-new", templateB, startedAfter.Add(2*time.Hour))

	desc, err := db.SqlcClient.GetSnapshotsByTemplateWithCursor(ctx, queries.GetSnapshotsByTemplateWithCursorParams{
		Limit:        10,
		TeamID:       teamID,
		TemplateID:   templateA,
		Metadata:     types.JSONBStringMap{},
		StartedAfter: startedAfter,
		CursorID:     "zzzzzzzzzzzzzzzzzzzz",
		CursorTime:   pgtype.Timestamptz{Time: startedAfter.Add(3 * time.Hour), Valid: true},
	})
	require.NoError(t, err)
	require.Len(t, desc, 2)
	assert.Equal(t, []string{"template-a-new", "template-a-at-bound"}, []string{
		desc[0].Snapshot.SandboxID,
		desc[1].Snapshot.SandboxID,
	})

	asc, err := db.SqlcClient.GetSnapshotsByTemplateWithCursorAsc(ctx, queries.GetSnapshotsByTemplateWithCursorAscParams{
		Limit:        10,
		TeamID:       teamID,
		TemplateID:   templateA,
		Metadata:     types.JSONBStringMap{},
		StartedAfter: startedAfter,
		CursorTime:   pgtype.Timestamptz{Time: time.Time{}, Valid: true},
		CursorID:     "zzzzzzzzzzzzzzzzzzzz",
	})
	require.NoError(t, err)
	require.Len(t, asc, 2)
	assert.Equal(t, []string{"template-a-at-bound", "template-a-new"}, []string{
		asc[0].Snapshot.SandboxID,
		asc[1].Snapshot.SandboxID,
	})
}

// TestGetLastSnapshot_BuildSharedWithOtherTemplate verifies that when a build is assigned
// to multiple templates, GetLastSnapshot still returns the correct build for this template.
func TestGetLastSnapshot_BuildSharedWithOtherTemplate(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and base template
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create snapshot with first build
	sandboxID := "sandbox-" + uuid.New().String()
	snapshotTemplateID := "snapshot-template-" + uuid.New().String()
	result1 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	build1ID := result1.BuildID

	time.Sleep(10 * time.Millisecond)

	// Create second build (latest for this snapshot)
	result2 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	build2ID := result2.BuildID

	// Now assign build1 to a DIFFERENT template (simulating shared build)
	otherTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	time.Sleep(10 * time.Millisecond)
	testutils.CreateTestBuildAssignment(t, ctx, db, otherTemplateID, build1ID, "default")

	// GetLastSnapshot should still return build2 (latest for THIS template),
	// not build1 even though build1 has a newer assignment to another template
	snapshot, err := db.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)

	assert.Equal(t, build2ID, snapshot.EnvBuild.ID,
		"GetLastSnapshot should return latest build for THIS template, ignoring assignments to other templates")
}

// TestGetLastSnapshot_IgnoresNonDefaultTags verifies that assignments with non-default
// tags are filtered out, even if they are more recent.
func TestGetLastSnapshot_IgnoresNonDefaultTags(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and base template
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create snapshot (creates build with 'default' tag)
	sandboxID := "sandbox-" + uuid.New().String()
	snapshotTemplateID := "snapshot-template-" + uuid.New().String()
	result1 := testutils.UpsertTestSnapshot(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)
	defaultBuildID := result1.BuildID

	time.Sleep(10 * time.Millisecond)

	// Create another build and assign it with a non-default tag (e.g., "v1")
	// This should be ignored by GetLastSnapshot
	otherBuildID := testutils.CreateTestBuild(t, ctx, db, result1.TemplateID, "success")
	// Delete the auto-created 'default' assignment from the trigger so the build only has 'v1' tag
	testutils.DeleteTriggerBuildAssignment(t, ctx, db, result1.TemplateID, otherBuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, result1.TemplateID, otherBuildID, "v1")

	// GetLastSnapshot should return the default-tagged build, not the v1-tagged one
	snapshot, err := db.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)

	assert.Equal(t, defaultBuildID, snapshot.EnvBuild.ID,
		"GetLastSnapshot should only consider 'default' tag assignments")
}

// TestGetLastSnapshot_AssignmentOrderDifferentFromBuildOrder verifies that when
// assignment created_at order differs from build created_at order, the assignment
// order takes precedence.
func TestGetLastSnapshot_AssignmentOrderDifferentFromBuildOrder(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and templates
	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	snapshotTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create builds: build1 (older), build2 (newer)
	build1ID := testutils.CreateTestBuild(t, ctx, db, snapshotTemplateID, "success")
	time.Sleep(10 * time.Millisecond)
	build2ID := testutils.CreateTestBuild(t, ctx, db, snapshotTemplateID, "success")

	// Assign in REVERSE order: build2 first, then build1
	// Even though build2 was created later, build1 has the latest assignment
	testutils.CreateTestBuildAssignment(t, ctx, db, snapshotTemplateID, build2ID, "default")
	time.Sleep(10 * time.Millisecond)
	testutils.CreateTestBuildAssignment(t, ctx, db, snapshotTemplateID, build1ID, "default")

	// Create snapshot record
	sandboxID := "sandbox-" + uuid.New().String()
	testutils.CreateSnapshotRecord(t, ctx, db, snapshotTemplateID, sandboxID, teamID, baseTemplateID)

	// GetLastSnapshot should return build1 (latest assignment), not build2 (latest build)
	snapshot, err := db.SqlcClient.GetLastSnapshot(ctx, sandboxID)
	require.NoError(t, err)

	assert.Equal(t, build1ID, snapshot.EnvBuild.ID,
		"GetLastSnapshot should use assignment order, not build creation order")
}
