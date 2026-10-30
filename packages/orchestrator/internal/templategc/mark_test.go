package templategc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type stubReader struct {
	closures map[uuid.UUID][]uuid.UUID
	err      error
}

func (s stubReader) AncestorBuilds(_ context.Context, b uuid.UUID) ([]uuid.UUID, error) {
	if s.err != nil {
		return nil, s.err
	}

	out := []uuid.UUID{b}

	return append(out, s.closures[b]...), nil
}

func obj(buildID uuid.UUID, name string, size int64, mod time.Time) storage.ObjectInfo {
	return storage.ObjectInfo{Path: buildID.String() + "/" + name, Size: size, LastModified: mod}
}

// A build referenced only through a live root's header closure (never its own
// root) must be kept, not marked a candidate.
func TestMark_KeepsAncestorOfLiveRoot(t *testing.T) {
	t.Parallel()

	now := time.Now()
	old := now.Add(-100 * 24 * time.Hour)

	liveRoot := uuid.New()
	ancestor := uuid.New() // shared base layer, old, not a root itself

	reader := stubReader{closures: map[uuid.UUID][]uuid.UUID{liveRoot: {ancestor}}}

	in := MarkInput{
		LiveRoots: []uuid.UUID{liveRoot},
		Objects: []storage.ObjectInfo{
			obj(liveRoot, "rootfs.ext4.header", 10, old),
			obj(ancestor, "rootfs.ext4.header", 10, old),
			obj(ancestor, "memfile", 1000, old),
		},
		Now:   now,
		Grace: 60 * 24 * time.Hour,
	}

	m, err := Mark(context.Background(), reader, in)
	require.NoError(t, err)

	assert.Empty(t, m.Candidates, "an old ancestor referenced by a live root must not be a candidate")
	assert.Contains(t, m.Keep, ancestor)
	assert.Contains(t, m.Keep, liveRoot)
}

// An unreferenced, old build is a candidate.
func TestMark_OrphanOldBuildIsCandidate(t *testing.T) {
	t.Parallel()

	now := time.Now()
	old := now.Add(-100 * 24 * time.Hour)

	liveRoot := uuid.New()
	orphan := uuid.New()

	reader := stubReader{closures: map[uuid.UUID][]uuid.UUID{}}

	in := MarkInput{
		LiveRoots: []uuid.UUID{liveRoot},
		Objects: []storage.ObjectInfo{
			obj(liveRoot, "memfile", 1, old),
			obj(orphan, "memfile", 2048, old),
			obj(orphan, "rootfs.ext4.header", 20, old),
		},
		Now:   now,
		Grace: 60 * 24 * time.Hour,
	}

	m, err := Mark(context.Background(), reader, in)
	require.NoError(t, err)

	require.Len(t, m.Candidates, 1)
	assert.Equal(t, orphan, m.Candidates[0].BuildID)
	assert.Equal(t, 2, m.Candidates[0].ObjectCount)
	assert.Equal(t, int64(2068), m.Candidates[0].TotalBytes)
}

// An unreferenced build with a fresh object is held back (grace), protecting the
// whole prefix — a fresh object may be an in-flight upload.
func TestMark_RecentObjectProtectsWholePrefix(t *testing.T) {
	t.Parallel()

	now := time.Now()
	old := now.Add(-100 * 24 * time.Hour)
	fresh := now.Add(-1 * time.Hour)

	orphan := uuid.New()

	reader := stubReader{closures: map[uuid.UUID][]uuid.UUID{}}

	in := MarkInput{
		LiveRoots: nil,
		Objects: []storage.ObjectInfo{
			obj(orphan, "memfile", 10, old),
			obj(orphan, "memfile.header", 10, fresh), // one fresh object
		},
		Now:   now,
		Grace: 60 * 24 * time.Hour,
	}

	m, err := Mark(context.Background(), reader, in)
	require.NoError(t, err)

	assert.Empty(t, m.Candidates)
	require.Len(t, m.TooNew, 1)
	assert.Equal(t, orphan, m.TooNew[0].BuildID)
}

// If any live root's closure cannot be resolved, Mark fails closed.
func TestMark_FailsClosedOnUnreadableRoot(t *testing.T) {
	t.Parallel()

	reader := stubReader{err: errors.New("header corrupt")}

	in := MarkInput{
		LiveRoots: []uuid.UUID{uuid.New()},
		Objects:   []storage.ObjectInfo{obj(uuid.New(), "memfile", 1, time.Now().Add(-1000*time.Hour))},
		Now:       time.Now(),
		Grace:     60 * 24 * time.Hour,
	}

	_, err := Mark(context.Background(), reader, in)
	require.Error(t, err, "an unreadable live-root closure must fail closed, not produce candidates")
}

// Keys not under a UUID prefix are surfaced as unknown, never deleted.
func TestMark_UnknownKeysSurfaced(t *testing.T) {
	t.Parallel()

	now := time.Now()
	old := now.Add(-100 * 24 * time.Hour)

	in := MarkInput{
		LiveRoots: nil,
		Objects: []storage.ObjectInfo{
			{Path: "not-a-uuid/file", Size: 1, LastModified: old},
			{Path: "toplevel", Size: 1, LastModified: old},
		},
		Now:   now,
		Grace: 60 * 24 * time.Hour,
	}

	m, err := Mark(context.Background(), stubReader{}, in)
	require.NoError(t, err)

	assert.Empty(t, m.Candidates)
	assert.ElementsMatch(t, []string{"not-a-uuid/file", "toplevel"}, m.UnknownKeys)
}
