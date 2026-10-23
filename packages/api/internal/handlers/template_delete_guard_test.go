package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
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

// stubSandboxLister is a test runtimeSandboxLister.
type stubSandboxLister struct {
	sandboxes []sandbox.Sandbox
	err       error
}

func (s stubSandboxLister) GetSandboxes(_ context.Context, _ uuid.UUID, _ []sandbox.State) ([]sandbox.Sandbox, error) {
	return s.sandboxes, s.err
}

// TestReachabilityGuard_ProtectsReferencedAncestor reproduces the corruption
// scenario: a build that is DB-exclusive to the doomed template is nonetheless a
// header-mapping ancestor of another live template's build. Deleting it would
// orphan that ancestor and later surface as an unrecoverable I/O error. The
// guard must keep it (not delete it).
func TestReachabilityGuard_ProtectsReferencedAncestor(t *testing.T) {
	t.Parallel()

	sharedAncestor := uuid.New() // DB-exclusive to the doomed template...
	liveRoot := uuid.New()       // ...but a live template's build references it.

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

// TestReachabilityGuard_OrphanIsDeletable verifies a build that no live root
// references is deletable.
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

// TestReachabilityGuard_FailsClosedOnReadError verifies that if any live root's
// header cannot be read, the keep set resolution errors so the caller deletes
// nothing rather than risk deleting a build whose reachability is unknown.
func TestReachabilityGuard_FailsClosedOnReadError(t *testing.T) {
	t.Parallel()

	reader := stubAncestorReader{err: errors.New("header missing/corrupt")}

	_, err := reachableKeepSet(context.Background(), reader, []uuid.UUID{uuid.New()})

	require.Error(t, err, "an unreadable live-root header must fail closed, not be treated as no-dependency")
}

// TestReachabilityGuard_UnionsMultipleRoots verifies the keep set is the union
// of every live root's ancestor closure.
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

	// a1 and a2 both protected; an unrelated build is not.
	deletable, protected := partitionDeletableBuilds([]uuid.UUID{a1, a2, uuid.New()}, keep)

	assert.Len(t, protected, 2)
	assert.Len(t, deletable, 1)
	assert.Contains(t, keep, root1)
	assert.Contains(t, keep, root2)
	assert.Contains(t, keep, a1)
	assert.Contains(t, keep, a2)
}
