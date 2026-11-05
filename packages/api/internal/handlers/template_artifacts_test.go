package handlers

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// noKeep is an empty reachability keep set: nothing is protected.
func noKeep() map[uuid.UUID]struct{} { return map[uuid.UUID]struct{}{} }

// rowWithNode builds a materialized build row (has a cluster node).
func rowWithNode(node string, cluster *uuid.UUID) queries.GetExclusiveBuildsForTemplateDeletionRow {
	return queries.GetExclusiveBuildsForTemplateDeletionRow{
		BuildID:       uuid.New(),
		ClusterNodeID: &node,
		ClusterID:     cluster,
	}
}

// rowNoNode builds a not-yet-materialized build row (nil cluster node).
func rowNoNode() queries.GetExclusiveBuildsForTemplateDeletionRow {
	return queries.GetExclusiveBuildsForTemplateDeletionRow{BuildID: uuid.New()}
}

// Builds with a node are deleted; builds without a node (not yet materialized)
// are skipped — never passed to the deleter.
func TestProcessExclusiveBuilds_SkipsNilNode(t *testing.T) {
	t.Parallel()

	withNode := rowWithNode("node-a", nil)
	nilNode := rowNoNode()

	var mu sync.Mutex
	var called []uuid.UUID
	deleteFn := func(_ context.Context, buildID uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		called = append(called, buildID)

		return nil
	}

	processExclusiveBuilds(t.Context(), "tpl", []queries.GetExclusiveBuildsForTemplateDeletionRow{withNode, nilNode}, noKeep(), deleteFn, artifactCleanupCounters{})

	assert.Equal(t, []uuid.UUID{withNode.BuildID}, called, "only the materialized build is deleted; nil-node build skipped")
}

// A build protected by the reachability keep set must never be passed to the
// deleter, even when it is materialized on a node. This is the regression guard
// against deleting a header-mapping ancestor still reconstructed by a live build.
func TestProcessExclusiveBuilds_GuardKeptNotDeleted(t *testing.T) {
	t.Parallel()

	protected := rowWithNode("node-a", nil)
	deletable := rowWithNode("node-b", nil)

	keep := map[uuid.UUID]struct{}{protected.BuildID: {}}

	var mu sync.Mutex
	var called []uuid.UUID
	deleteFn := func(_ context.Context, buildID uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		called = append(called, buildID)

		return nil
	}

	processExclusiveBuilds(t.Context(), "tpl", []queries.GetExclusiveBuildsForTemplateDeletionRow{protected, deletable}, keep, deleteFn, artifactCleanupCounters{})

	assert.Equal(t, []uuid.UUID{deletable.BuildID}, called, "protected build must not be deleted; only the unreferenced build is")
}

// A failure on one build must not stop the others (best-effort cleanup).
func TestProcessExclusiveBuilds_ErrorIsNonFatal(t *testing.T) {
	t.Parallel()

	b1 := rowWithNode("node-a", nil)
	b2 := rowWithNode("node-b", nil)
	b3 := rowWithNode("node-c", nil)

	var mu sync.Mutex
	attempted := map[uuid.UUID]bool{}
	deleteFn := func(_ context.Context, buildID uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		attempted[buildID] = true
		if buildID == b2.BuildID {
			return errors.New("storage unreachable")
		}

		return nil
	}

	processExclusiveBuilds(t.Context(), "tpl", []queries.GetExclusiveBuildsForTemplateDeletionRow{b1, b2, b3}, noKeep(), deleteFn, artifactCleanupCounters{})

	assert.True(t, attempted[b1.BuildID] && attempted[b2.BuildID] && attempted[b3.BuildID],
		"all builds attempted even though one failed")
}

// A nil ClusterID resolves to the local cluster fallback, not a panic.
func TestProcessExclusiveBuilds_NilClusterFallback(t *testing.T) {
	t.Parallel()

	explicit := uuid.New()
	withCluster := rowWithNode("node-a", &explicit)
	nilCluster := rowWithNode("node-b", nil)

	var mu sync.Mutex
	got := map[uuid.UUID]uuid.UUID{}
	deleteFn := func(_ context.Context, buildID uuid.UUID, _ string, clusterID uuid.UUID, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		got[buildID] = clusterID

		return nil
	}

	processExclusiveBuilds(t.Context(), "tpl", []queries.GetExclusiveBuildsForTemplateDeletionRow{withCluster, nilCluster}, noKeep(), deleteFn, artifactCleanupCounters{})

	assert.Equal(t, explicit, got[withCluster.BuildID], "explicit cluster id is passed through")
	assert.Equal(t, consts.LocalClusterID, got[nilCluster.BuildID], "nil cluster id falls back to local cluster")
}

// Empty input is a no-op and does not invoke the deleter.
func TestProcessExclusiveBuilds_Empty(t *testing.T) {
	t.Parallel()

	calls := 0
	deleteFn := func(_ context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
		calls++

		return nil
	}

	processExclusiveBuilds(t.Context(), "tpl", nil, noKeep(), deleteFn, artifactCleanupCounters{})
	require.Zero(t, calls)
}
