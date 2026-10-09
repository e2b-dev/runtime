package handlecache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
)

func assertStale(t *testing.T, err error) {
	t.Helper()

	var status *nfs.NFSStatusError
	require.ErrorAs(t, err, &status)
	assert.Equal(t, nfs.NFSStatusStale, status.NFSStatus)
}

func mint(t *testing.T, h *Handler, fs billy.Filesystem, n int) {
	t.Helper()

	for i := range n {
		h.ToHandle(t.Context(), fs, []string{"churn", fmt.Sprintf("file-%d", i)})
	}
}

// mount registers volumes as the chroot handler does when they are mounted.
func mount(h *Handler, volumes ...billy.Filesystem) {
	for _, v := range volumes {
		h.Register(v)
	}
}

func TestHeldHandleSurvivesAnotherVolumesChurn(t *testing.T) {
	t.Parallel()

	h := Wrap(nil, 8)
	volumeA, volumeB := memfs.New(), memfs.New()
	mount(h, volumeA, volumeB)

	held := h.ToHandle(t.Context(), volumeA, []string{".git", "index.lock"})
	mint(t, h, volumeB, 100)

	fs, path, err := h.FromHandle(t.Context(), held)
	require.NoError(t, err)
	assert.Same(t, volumeA, fs)
	assert.Equal(t, []string{".git", "index.lock"}, path)
}

func TestHandleGoesStaleAfterLimitNewPathsOnItsVolume(t *testing.T) {
	t.Parallel()

	h := Wrap(nil, 8)
	volume := memfs.New()
	mount(h, volume)

	held := h.ToHandle(t.Context(), volume, []string{".git", "index.lock"})
	mint(t, h, volume, 7)
	_, _, err := h.FromHandle(t.Context(), held)
	require.NoError(t, err, "within the limit")

	mint(t, h, volume, 16)
	_, _, err = h.FromHandle(t.Context(), held)
	assertStale(t, err)
}

func TestDirectoryOutlivesNewPathsWhileAFileInItIsUsed(t *testing.T) {
	t.Parallel()

	h := Wrap(nil, 8)
	volume := memfs.New()
	mount(h, volume)

	dir := h.ToHandle(t.Context(), volume, []string{"src", "pkg"})
	file := h.ToHandle(t.Context(), volume, []string{"src", "pkg", "main.go"})
	for i := range 100 {
		h.ToHandle(t.Context(), volume, []string{"churn", fmt.Sprintf("file-%d", i)})
		_, _, err := h.FromHandle(t.Context(), file)
		require.NoError(t, err)
	}

	_, path, err := h.FromHandle(t.Context(), dir)
	require.NoError(t, err)
	assert.Equal(t, []string{"src", "pkg"}, path)
}

type wrapper struct{ billy.Filesystem }

func (w wrapper) Unwrap() billy.Filesystem { return w.Filesystem }

func TestReleaseForgetsTheVolumesHandles(t *testing.T) {
	t.Parallel()

	h := Wrap(nil, 8)
	released, kept := memfs.New(), memfs.New()
	mount(h, released, kept)

	// middleware hands the cache a wrapper; the chroot handler releases what it mounted
	gone := h.ToHandle(t.Context(), wrapper{released}, []string{"file"})
	stays := h.ToHandle(t.Context(), kept, []string{"file"})

	h.Release(released)

	_, _, err := h.FromHandle(t.Context(), gone)
	assertStale(t, err)
	_, _, err = h.FromHandle(t.Context(), stays)
	require.NoError(t, err)
}

// A request still running when its sandbox is released asks for handles on
// the released filesystem afterwards. Those handles must be stale, and the
// filesystem must not get a cache back that nothing would ever release.
func TestHandlesAfterReleaseAreStaleAndRetainNothing(t *testing.T) {
	t.Parallel()

	h := Wrap(nil, 8)
	volume := memfs.New()
	mount(h, volume)
	h.Release(volume)

	late := h.ToHandle(t.Context(), wrapper{volume}, []string{"listed", "entry"})

	_, _, err := h.FromHandle(t.Context(), late)
	assertStale(t, err)
	assert.Empty(t, h.volumes)
	assert.Empty(t, h.byMounted)
}

func TestHandles(t *testing.T) {
	t.Parallel()

	t.Run("same path gives the same handle", func(t *testing.T) {
		t.Parallel()

		h := Wrap(nil, 8)
		volume := memfs.New()
		mount(h, volume)
		first := h.ToHandle(t.Context(), volume, []string{"a", "b"})
		second := h.ToHandle(t.Context(), volume, []string{"a", "b"})
		assert.Equal(t, first, second)
	})

	t.Run("same path on two volumes gives different handles", func(t *testing.T) {
		t.Parallel()

		h := Wrap(nil, 8)
		volumeA, volumeB := memfs.New(), memfs.New()
		mount(h, volumeA, volumeB)
		onA := h.ToHandle(t.Context(), volumeA, []string{"a"})
		onB := h.ToHandle(t.Context(), volumeB, []string{"a"})
		assert.NotEqual(t, onA, onB)
	})

	t.Run("invalidated handle is stale and the path gets a new one", func(t *testing.T) {
		t.Parallel()

		h := Wrap(nil, 8)
		volume := memfs.New()
		mount(h, volume)
		old := h.ToHandle(t.Context(), volume, []string{"a"})
		require.NoError(t, h.InvalidateHandle(t.Context(), volume, old))

		_, _, err := h.FromHandle(t.Context(), old)
		assertStale(t, err)
		assert.NotEqual(t, old, h.ToHandle(t.Context(), volume, []string{"a"}))
	})

	t.Run("handle from another volume's ID space is stale", func(t *testing.T) {
		t.Parallel()

		h := Wrap(nil, 8)
		volume := memfs.New()
		mount(h, volume)
		handle := h.ToHandle(t.Context(), volume, []string{"a"})
		handle[handleSize-1]++

		_, _, err := h.FromHandle(t.Context(), handle)
		assertStale(t, err)
	})

	t.Run("malformed handle is stale", func(t *testing.T) {
		t.Parallel()

		_, _, err := Wrap(nil, 8).FromHandle(t.Context(), []byte{1, 2, 3})
		assertStale(t, err)
	})
}

func TestConcurrentVolumes(t *testing.T) {
	t.Parallel()

	h := Wrap(nil, 64)

	var wg sync.WaitGroup
	for range 8 {
		volume := memfs.New()
		mount(h, volume)
		wg.Go(func() {
			for i := range 1000 {
				handle := h.ToHandle(t.Context(), volume, []string{"d", fmt.Sprintf("f-%d", i%100)})
				_, _, _ = h.FromHandle(t.Context(), handle)
				if i%10 == 0 {
					_ = h.InvalidateHandle(t.Context(), volume, handle)
				}
			}
			h.Release(volume)
		})
	}
	wg.Wait()
}

func BenchmarkFromHandle(b *testing.B) {
	for _, limit := range []int{1024, 16384, 65536} {
		b.Run(fmt.Sprint(limit), func(b *testing.B) {
			h := Wrap(nil, limit)
			volume := memfs.New()
			mount(h, volume)
			handles := make([][]byte, limit)
			for i := range handles {
				handles[i] = h.ToHandle(b.Context(), volume, []string{"vol", "d", fmt.Sprintf("f%d", i)})
			}

			b.ResetTimer()
			for i := range b.N {
				_, _, _ = h.FromHandle(b.Context(), handles[(i*7919)%limit])
			}
		})
	}
}
