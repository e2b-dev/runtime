package templategc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingDeleter records every prefix it is asked to delete and can be told to
// fail for specific build IDs.
type recordingDeleter struct {
	deleted []uuid.UUID
	failFor map[uuid.UUID]bool
}

func (d *recordingDeleter) DeletePrefix(_ context.Context, buildID uuid.UUID) error {
	if d.failFor[buildID] {
		return errors.New("storage delete failed")
	}
	d.deleted = append(d.deleted, buildID)

	return nil
}

func bp(id uuid.UUID, bytes int64) BuildPrefix {
	return BuildPrefix{BuildID: id, ObjectCount: 1, TotalBytes: bytes}
}

// A confirmed sweep deletes exactly the intersection of approved IDs and the
// fresh candidate set, and sums reclaimed bytes.
func TestSweep_DeletesIntersectionConfirmed(t *testing.T) {
	t.Parallel()

	a, b := uuid.New(), uuid.New()
	fresh := &Manifest{Candidates: []BuildPrefix{bp(a, 100), bp(b, 200)}}

	d := &recordingDeleter{}
	res, err := Sweep(context.Background(), d, []uuid.UUID{a, b}, fresh, true)
	require.NoError(t, err)

	assert.ElementsMatch(t, []uuid.UUID{a, b}, d.deleted)
	assert.Len(t, res.Deleted, 2)
	assert.Equal(t, int64(300), res.ReclaimedBytes)
	assert.True(t, res.Confirmed)
	assert.Empty(t, res.SkippedChanged)
	assert.Empty(t, res.Failed)
}

// An approved build that is no longer a fresh candidate (became reachable or
// too-new since the mark) must be skipped, never deleted. This is the TOCTOU
// guard between mark and sweep.
func TestSweep_SkipsBuildThatChangedSinceMark(t *testing.T) {
	t.Parallel()

	stillOrphan := uuid.New()
	nowReachable := uuid.New() // approved earlier, but not in fresh candidates now

	fresh := &Manifest{Candidates: []BuildPrefix{bp(stillOrphan, 10)}}

	d := &recordingDeleter{}
	res, err := Sweep(context.Background(), d, []uuid.UUID{stillOrphan, nowReachable}, fresh, true)
	require.NoError(t, err)

	assert.Equal(t, []uuid.UUID{stillOrphan}, d.deleted, "only the still-orphan build is deleted")
	assert.Equal(t, []uuid.UUID{nowReachable}, res.SkippedChanged, "the changed build is skipped, not deleted")
}

// A dry run (confirm=false) computes what it would delete but never calls the
// deleter.
func TestSweep_DryRunDeletesNothing(t *testing.T) {
	t.Parallel()

	a := uuid.New()
	fresh := &Manifest{Candidates: []BuildPrefix{bp(a, 42)}}

	d := &recordingDeleter{}
	res, err := Sweep(context.Background(), d, []uuid.UUID{a}, fresh, false)
	require.NoError(t, err)

	assert.Empty(t, d.deleted, "dry run must not call the deleter")
	assert.Len(t, res.Deleted, 1, "but still reports what it would delete")
	assert.Equal(t, int64(42), res.ReclaimedBytes)
	assert.False(t, res.Confirmed)
}

// A per-build delete failure never aborts the others and is recorded.
func TestSweep_DeleteFailureIsNonFatal(t *testing.T) {
	t.Parallel()

	a, bad, c := uuid.New(), uuid.New(), uuid.New()
	fresh := &Manifest{Candidates: []BuildPrefix{bp(a, 1), bp(bad, 2), bp(c, 3)}}

	d := &recordingDeleter{failFor: map[uuid.UUID]bool{bad: true}}
	res, err := Sweep(context.Background(), d, []uuid.UUID{a, bad, c}, fresh, true)
	require.NoError(t, err)

	assert.ElementsMatch(t, []uuid.UUID{a, c}, d.deleted)
	require.Len(t, res.Failed, 1)
	assert.Equal(t, bad, res.Failed[0].BuildID)
	assert.Equal(t, int64(4), res.ReclaimedBytes, "failed build's bytes are not counted as reclaimed")
}

// Sweep inherits Mark's fail-closed guarantee: when the fresh manifest has no
// candidates (e.g. Mark failed closed on an unreadable root upstream), nothing
// is deleted even though builds were approved.
func TestSweep_FailClosedWhenNoFreshCandidates(t *testing.T) {
	t.Parallel()

	a := uuid.New()
	fresh := &Manifest{Candidates: nil}

	d := &recordingDeleter{}
	res, err := Sweep(context.Background(), d, []uuid.UUID{a}, fresh, true)
	require.NoError(t, err)

	assert.Empty(t, d.deleted)
	assert.Equal(t, []uuid.UUID{a}, res.SkippedChanged)
}

func TestSweep_NilManifestErrors(t *testing.T) {
	t.Parallel()

	_, err := Sweep(context.Background(), &recordingDeleter{}, []uuid.UUID{uuid.New()}, nil, true)
	require.Error(t, err)
}

// WriteOrphanMetrics emits parseable Prometheus gauges for both mark and sweep.
func TestWriteOrphanMetrics_EmitsGauges(t *testing.T) {
	t.Parallel()

	a := uuid.New()
	manifest := &Manifest{
		Candidates: []BuildPrefix{bp(a, 1000)},
		TooNew:     []BuildPrefix{bp(uuid.New(), 50)},
		Keep:       []uuid.UUID{uuid.New()},
	}
	sweep := &SweepResult{Deleted: []BuildPrefix{bp(a, 1000)}, ReclaimedBytes: 1000, Confirmed: true}

	var sb strings.Builder
	require.NoError(t, WriteOrphanMetrics(&sb, manifest, sweep, time.Unix(1700000000, 0)))
	out := sb.String()

	assert.Contains(t, out, "template_gc_orphan_builds 1")
	assert.Contains(t, out, "template_gc_orphan_bytes 1000")
	assert.Contains(t, out, "template_gc_too_new_bytes 50")
	assert.Contains(t, out, "template_gc_swept_bytes 1000")
	assert.Contains(t, out, "template_gc_sweep_confirmed 1")
	assert.Contains(t, out, "template_gc_last_run_timestamp_seconds 1700000000")
	assert.Contains(t, out, "# TYPE template_gc_orphan_bytes gauge")
}
