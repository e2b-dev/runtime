//go:build linux

package sandbox

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockmocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Wait reads the ancestor's device after waiting on its upload future, so the
// pin must cover the whole wait, and come back once Wait returns.
func TestUploads_Wait_HoldsTheAncestorPinForTheWholeWait(t *testing.T) {
	t.Parallel()
	c, cache := newUploads(t)

	id := uuid.New()
	putPendingHeader(t, cache, id, build.Rootfs)
	fut, err := c.Start(id)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = c.Wait(t.Context(), id, build.Rootfs)
	}()

	require.Eventually(t, func() bool { return cache.pins.Load() == 1 }, time.Second, time.Millisecond,
		"the entry is pinned while Wait blocks on the future")

	require.NoError(t, fut.SetSuccess())
	<-done
	assert.Zero(t, cache.pins.Load(), "Wait returns its pin")
}

func TestUploads_Find_ReturnsThePinOnEveryPath(t *testing.T) {
	t.Parallel()

	t.Run("unsupported file type", func(t *testing.T) {
		t.Parallel()
		c, cache := newUploads(t)

		id := uuid.New()
		putHeader(t, cache, id, build.Memfile, false)

		_, _, err := c.Wait(t.Context(), id, build.DiffType("unknown"))
		require.Error(t, err)
		assert.Zero(t, cache.pins.Load())
	})

	t.Run("miss", func(t *testing.T) {
		t.Parallel()
		c, cache := newUploads(t)
		c.p2p = nil

		_, release, err := c.find(t.Context(), uuid.New(), build.Memfile)
		require.ErrorIs(t, err, ErrBuildNotInCache)
		release()
		assert.Zero(t, cache.pins.Load())
	})
}

func TestUpload_PublishReturnsItsPin(t *testing.T) {
	t.Parallel()
	c, cache := newUploads(t)

	id := uuid.New()
	final := &headers.Header{}
	tpl := templatemocks.NewMockTemplate(t)
	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().SwapHeader(final).Run(func(*headers.Header) {
		assert.Equal(t, int64(1), cache.pins.Load(), "the swap runs under the pin")
	}).Once()
	tpl.EXPECT().Rootfs().Return(dev, nil)
	cache.put(id.String(), tpl)

	u := &Upload{buildID: id, uploads: c}
	require.NoError(t, u.publish(t.Context(), build.Rootfs, final))
	assert.Zero(t, cache.pins.Load())
}

// templateFinish stands in for the cache's upload finisher: it counts the pin
// and records each outcome it is called with.
type templateFinish struct {
	mu       sync.Mutex
	pins     int
	outcomes []template.UploadOutcome
}

func newTemplateFinish() *templateFinish { return &templateFinish{pins: 1} }

func (f *templateFinish) finish(o template.UploadOutcome) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pins--
	f.outcomes = append(f.outcomes, o)
}

func (f *templateFinish) state() (int, []template.UploadOutcome) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.pins, f.outcomes
}

// newPinnedUpload registers an upload that owns a counted template pin.
func newPinnedUpload(t *testing.T) (*Upload, *Uploads, *templateFinish) {
	t.Helper()

	c, _ := newUploads(t)
	f := newTemplateFinish()

	snap := &Snapshot{BuildID: uuid.New(), FilesystemSnapshot: true, RootfsBlockSize: 4096}
	u, err := NewUpload(t.Context(), c, snap, nil, storage.CompressConfig{}, nil, storage.UseCasePause, nil, f.finish)
	require.NoError(t, err)

	return u, c, f
}

// Every terminal outcome returns the pin exactly once, and says how the upload
// ended: the cache releases only a landed layer, and counts only a failed one.
func TestUpload_TemplatePin(t *testing.T) {
	t.Parallel()

	t.Run("landed upload returns it as landed", func(t *testing.T) {
		t.Parallel()
		u, _, f := newPinnedUpload(t)

		u.Finish(t.Context(), nil)
		pins, outcomes := f.state()
		assert.Zero(t, pins)
		assert.Equal(t, []template.UploadOutcome{template.UploadLanded}, outcomes)
		require.NoError(t, u.Wait(t.Context()))
	})

	t.Run("failed upload returns it as failed", func(t *testing.T) {
		t.Parallel()
		u, _, f := newPinnedUpload(t)

		u.Finish(t.Context(), errors.New("storage down"))
		pins, outcomes := f.state()
		assert.Zero(t, pins, "the entry goes back to its TTL")
		assert.Equal(t, []template.UploadOutcome{template.UploadFailed}, outcomes, "and is never released")
	})

	t.Run("abandoned upload returns it as abandoned and fails its waiters", func(t *testing.T) {
		t.Parallel()
		u, c, f := newPinnedUpload(t)

		u.Abandon(t.Context(), errors.New("checkpoint failed"))
		pins, outcomes := f.state()
		assert.Zero(t, pins)
		assert.Equal(t, []template.UploadOutcome{template.UploadAbandoned}, outcomes)

		_, _, err := c.Wait(t.Context(), u.buildID, build.Rootfs)
		require.ErrorContains(t, err, "checkpoint failed", "a waiter wakes with the abandon error instead of blocking")
	})

	t.Run("only the first terminal call counts", func(t *testing.T) {
		t.Parallel()
		u, _, f := newPinnedUpload(t)

		u.Finish(t.Context(), errors.New("storage down"))
		u.Abandon(t.Context(), errors.New("late rollback"))
		pins, outcomes := f.state()
		assert.Zero(t, pins, "returned once, not twice")
		assert.Equal(t, []template.UploadOutcome{template.UploadFailed}, outcomes, "a rollback after a permanent failure must not relabel it")
		require.ErrorContains(t, u.Wait(t.Context()), "storage down")

		v, _, g := newPinnedUpload(t)
		v.Abandon(t.Context(), errors.New("abandoned"))
		v.Finish(t.Context(), nil)
		pins, outcomes = g.state()
		assert.Zero(t, pins, "returned once, not twice")
		assert.Equal(t, []template.UploadOutcome{template.UploadAbandoned}, outcomes, "a late success must not mark an abandoned layer landed")
		require.ErrorContains(t, v.Wait(t.Context()), "abandoned")
	})

	t.Run("NewUpload failure returns it as abandoned", func(t *testing.T) {
		t.Parallel()
		c, _ := newUploads(t)

		snap := &Snapshot{BuildID: uuid.New(), FilesystemSnapshot: true, RootfsBlockSize: 4096}
		_, err := c.Start(snap.BuildID) // a future already in flight
		require.NoError(t, err)

		f := newTemplateFinish()
		_, err = NewUpload(t.Context(), c, snap, nil, storage.CompressConfig{}, nil, storage.UseCasePause, nil, f.finish)
		require.Error(t, err)
		pins, outcomes := f.state()
		assert.Zero(t, pins)
		assert.Equal(t, []template.UploadOutcome{template.UploadAbandoned}, outcomes)
	})
}
