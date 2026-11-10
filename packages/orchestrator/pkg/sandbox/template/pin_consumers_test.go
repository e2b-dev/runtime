//go:build linux

package template

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// pinRefs is the number of outstanding pins on key's live entry.
func pinRefs(c *Cache, key string) int {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	if e, ok := c.pinned[key]; ok {
		return e.refs()
	}

	return 0
}

// A lookup never admits: a miss leaves the cache as it was, with nothing
// pinned and a release that is safe to call.
func TestLookupPinned_MissAdmitsNothing(t *testing.T) {
	t.Parallel()
	c, _ := newPinTestCache(time.Hour)

	got, release, ok := c.LookupPinned(t.Context(), "absent")
	require.False(t, ok)
	assert.Nil(t, got)
	release()

	assert.Zero(t, c.cache.Len())
	assert.Empty(t, c.pinned)
}

func TestLookupPinned_HitPinsUntilReleased(t *testing.T) {
	t.Parallel()
	c, _ := newPinTestCache(time.Hour)

	tmpl := newPinTestTemplate(t, "build-1")
	c.cache.Set(tmpl.key, tmpl, 0)

	got, release, ok := c.LookupPinned(t.Context(), tmpl.key)
	require.True(t, ok)
	assert.Same(t, tmpl, got)
	assert.Equal(t, 1, pinRefs(c, tmpl.key))

	release()
	release()
	assert.Zero(t, pinRefs(c, tmpl.key), "the release is idempotent")
}

// metadataWriteTemplate observes the cache's pins on itself while its
// metadata is written.
type metadataWriteTemplate struct {
	*pinTestTemplate

	c           *Cache
	refsAtWrite int
}

func (m *metadataWriteTemplate) UpdateMetadata(metadata.Template) error {
	m.refsAtWrite = pinRefs(m.c, m.key)

	return nil
}

func TestUpdateMetadata_WritesUnderAPin(t *testing.T) {
	t.Parallel()
	c, _ := newPinTestCache(time.Hour)

	tmpl := &metadataWriteTemplate{pinTestTemplate: newPinTestTemplate(t, "build-1"), c: c}
	c.cache.Set(tmpl.key, tmpl, 0)

	require.NoError(t, c.UpdateMetadata(t.Context(), tmpl.key, metadata.Template{}))
	assert.Equal(t, 1, tmpl.refsAtWrite, "the write runs under the pin")
	assert.Zero(t, pinRefs(c, tmpl.key), "and returns it")

	require.Error(t, c.UpdateMetadata(t.Context(), "absent", metadata.Template{}))
	assert.Empty(t, c.pinned, "a miss pins nothing")
}

// The provisional-header swap reads the template after AddSnapshot returns,
// so it holds a pin of its own beside the upload's, and returns it when done.
func TestAddSnapshot_SwapGoroutineHoldsItsOwnPin(t *testing.T) {
	t.Parallel()

	c := newDedupTestCache(t)
	c.pinned = make(map[string]*pinnedEntry)
	c.retired = make(map[*pinnedEntry]struct{})
	buildID := uuid.NewString()

	provisionalHdr := mustHeader(t, uuid.New())
	memfile := dedupTestDevice{build.NewFile(provisionalHdr, nil, build.Memfile, nil, blockmetrics.Metrics{})}
	residentTemplate(t, c, buildID, memfile)

	dedupedFuture := utils.NewSetOnce[*header.Header]()
	swapDone := make(chan struct{})

	finishUpload, err := c.AddSnapshot(t.Context(), buildID, SnapshotLineage{Origin: storage.ObjectOriginPause},
		dedupedFuture, resolvedHeader(mustHeader(t, uuid.New())),
		nil, nil,
		&build.NoDiff{}, &build.NoDiff{},
		provisionalHdr, &build.NoDiff{},
		time.Now(),
		func() { close(swapDone) },
	)
	require.NoError(t, err)
	assert.Equal(t, 2, pinRefs(c, buildID), "one pin for the upload, one for the swap")

	finishUpload(UploadLanded)
	assert.Equal(t, 1, pinRefs(c, buildID), "the swap still holds the entry")

	require.NoError(t, dedupedFuture.SetValue(mustHeader(t, uuid.New())))
	select {
	case <-swapDone:
	case <-time.After(5 * time.Second):
		t.Fatal("swap goroutine did not complete")
	}
	require.Eventually(t, func() bool { return pinRefs(c, buildID) == 0 }, 5*time.Second, time.Millisecond,
		"the swap returns its pin")
}

// Without a provisional header no goroutine outlives AddSnapshot, so the
// upload's pin is the only one.
func TestAddSnapshot_NoSwapPinWithoutProvisional(t *testing.T) {
	t.Parallel()

	c := newDedupTestCache(t)
	c.pinned = make(map[string]*pinnedEntry)
	c.retired = make(map[*pinnedEntry]struct{})
	buildID := uuid.NewString()

	memfile := dedupTestDevice{build.NewFile(mustHeader(t, uuid.New()), nil, build.Memfile, nil, blockmetrics.Metrics{})}
	residentTemplate(t, c, buildID, memfile)

	finishUpload, err := c.AddSnapshot(t.Context(), buildID, SnapshotLineage{Origin: storage.ObjectOriginPause},
		resolvedHeader(mustHeader(t, uuid.New())), resolvedHeader(mustHeader(t, uuid.New())),
		nil, nil,
		&build.NoDiff{}, &build.NoDiff{},
		nil, nil,
		time.Time{},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, pinRefs(c, buildID))

	finishUpload(UploadLanded)
	assert.Zero(t, pinRefs(c, buildID))
}
