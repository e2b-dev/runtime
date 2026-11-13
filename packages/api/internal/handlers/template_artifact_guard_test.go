package handlers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubAncestorReader is a test buildAncestorReader. AncestorBuilds returns the
// build itself plus any ancestors configured for it; a non-nil err makes every
// lookup fail (to exercise the fail-closed path).
type stubAncestorReader struct {
	ancestors map[uuid.UUID][]uuid.UUID
	err       error
}

func (s stubAncestorReader) AncestorBuilds(_ context.Context, buildID uuid.UUID) ([]uuid.UUID, error) {
	if s.err != nil {
		return nil, s.err
	}

	out := []uuid.UUID{buildID}
	out = append(out, s.ancestors[buildID]...)

	return out, nil
}

// Reproduces the corruption scenario: a build that is DB-exclusive to the doomed
// template is nonetheless a header-mapping ancestor of another live build.
// Deleting it would orphan that ancestor and later surface as an unrecoverable
// I/O error. The guard must keep it (not delete it).
func TestReachabilityGuard_ProtectsReferencedAncestor(t *testing.T) {
	t.Parallel()

	sharedAncestor := uuid.New() // DB-exclusive to the doomed template...
	liveRoot := uuid.New()       // ...but a live build references it.

	reader := stubAncestorReader{
		ancestors: map[uuid.UUID][]uuid.UUID{
			liveRoot: {sharedAncestor},
		},
	}

	keep, err := reachableKeepSet(context.Background(), reader, []uuid.UUID{liveRoot})
	require.NoError(t, err)

	deletable, protected := partitionDeletableBuilds([]uuid.UUID{sharedAncestor}, keep)

	assert.Empty(t, deletable, "a build referenced by a live root's header closure must not be deletable")
	assert.Equal(t, []uuid.UUID{sharedAncestor}, protected)
}

// A build that no live root references is deletable.
func TestReachabilityGuard_OrphanIsDeletable(t *testing.T) {
	t.Parallel()

	orphan := uuid.New()
	liveRoot := uuid.New()
	unrelatedAncestor := uuid.New()

	reader := stubAncestorReader{
		ancestors: map[uuid.UUID][]uuid.UUID{
			liveRoot: {unrelatedAncestor},
		},
	}

	keep, err := reachableKeepSet(context.Background(), reader, []uuid.UUID{liveRoot})
	require.NoError(t, err)

	deletable, protected := partitionDeletableBuilds([]uuid.UUID{orphan}, keep)

	assert.Equal(t, []uuid.UUID{orphan}, deletable)
	assert.Empty(t, protected, "an unreferenced build should not be protected")
}

// If any live root's header cannot be read, keep-set resolution errors so the
// caller deletes nothing rather than risk deleting a build whose reachability is
// unknown. The error must classify as header_unreadable.
func TestReachabilityGuard_FailsClosedOnReadError(t *testing.T) {
	t.Parallel()

	reader := stubAncestorReader{err: errors.New("header missing/corrupt")}

	_, err := reachableKeepSet(context.Background(), reader, []uuid.UUID{uuid.New()})

	require.Error(t, err, "an unreadable live-root header must fail closed, not be treated as no-dependency")
	assert.ErrorIs(t, err, errHeaderUnreadable)
	assert.Equal(t, "header_unreadable", failClosedReason(err))
}

// The keep set is the union of every live root's ancestor closure.
func TestReachabilityGuard_UnionsMultipleRoots(t *testing.T) {
	t.Parallel()

	a1, a2 := uuid.New(), uuid.New()
	root1, root2 := uuid.New(), uuid.New()

	reader := stubAncestorReader{
		ancestors: map[uuid.UUID][]uuid.UUID{
			root1: {a1},
			root2: {a2},
		},
	}

	keep, err := reachableKeepSet(context.Background(), reader, []uuid.UUID{root1, root2})
	require.NoError(t, err)

	deletable, protected := partitionDeletableBuilds([]uuid.UUID{a1, a2, uuid.New()}, keep)

	assert.Len(t, protected, 2)
	assert.Len(t, deletable, 1)
	assert.Contains(t, keep, root1)
	assert.Contains(t, keep, root2)
	assert.Contains(t, keep, a1)
	assert.Contains(t, keep, a2)
}

// failClosedReason maps each sentinel to its low-cardinality label, and wrapped
// errors still classify correctly (so the fail_closed metric attribute is stable).
func TestFailClosedReason_Classification(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "reader_unconfigured", failClosedReason(errReaderUnconfigured))
	assert.Equal(t, "live_roots_query", failClosedReason(fmt.Errorf("%w: boom", errLiveRootsQuery)))
	assert.Equal(t, "header_unreadable", failClosedReason(fmt.Errorf("%w: boom", errHeaderUnreadable)))
	assert.Equal(t, "unknown", failClosedReason(errors.New("something else")))
}

// stubLiveRootSource is a test liveRootSource: returns configured roots, or err.
type stubLiveRootSource struct {
	label string
	roots []uuid.UUID
	err   error
}

func (s stubLiveRootSource) name() string { return s.label }

func (s stubLiveRootSource) liveRoots(_ context.Context, _ uuid.UUID, _ string) ([]uuid.UUID, error) {
	return s.roots, s.err
}

// collectLiveRoots unions roots across all sources and dedups.
func TestCollectLiveRoots_UnionsAndDedups(t *testing.T) {
	t.Parallel()

	a, b, c := uuid.New(), uuid.New(), uuid.New()
	sources := []liveRootSource{
		stubLiveRootSource{label: "db", roots: []uuid.UUID{a, b}},
		stubLiveRootSource{label: "snapshots", roots: []uuid.UUID{b, c}}, // b overlaps
	}

	roots, err := collectLiveRoots(context.Background(), sources, uuid.New(), "tpl")
	require.NoError(t, err)
	assert.ElementsMatch(t, []uuid.UUID{a, b, c}, roots, "roots are the deduped union of all sources")
}

// A single source error fails the whole collection closed (classified as
// live_roots_query), so the guard deletes nothing rather than expand an
// incomplete root set and risk deleting a still-referenced build.
func TestCollectLiveRoots_FailsClosedOnAnySourceError(t *testing.T) {
	t.Parallel()

	good := uuid.New()
	sources := []liveRootSource{
		stubLiveRootSource{label: "db", roots: []uuid.UUID{good}},
		stubLiveRootSource{label: "snapshots", err: errors.New("query boom")},
	}

	_, err := collectLiveRoots(context.Background(), sources, uuid.New(), "tpl")
	require.Error(t, err, "an error from any source must fail closed")
	assert.ErrorIs(t, err, errLiveRootsQuery)
	assert.Equal(t, "live_roots_query", failClosedReason(err))
}
