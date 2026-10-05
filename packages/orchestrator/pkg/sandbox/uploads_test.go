//go:build linux

package sandbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	blockmocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type fakeCache struct {
	mu sync.Mutex
	m  map[string]template.Template

	// pins counts the pins LookupPinned handed out and nobody has returned.
	pins atomic.Int64
	// released is the builds WasReleased reports as released here.
	released map[string]bool
	// asks counts WasReleased calls per build: each is a touch of the real
	// cache's release record.
	asks map[string]int
}

func newFakeCache() *fakeCache {
	return &fakeCache{m: make(map[string]template.Template), released: make(map[string]bool), asks: make(map[string]int)}
}

func (f *fakeCache) LookupPinned(_ context.Context, buildID string) (template.Template, func(), bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.m[buildID]
	if !ok {
		return nil, func() {}, false
	}

	f.pins.Add(1)

	return t, sync.OnceFunc(func() { f.pins.Add(-1) }), true
}

func (f *fakeCache) WasReleased(buildID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.asks[buildID]++

	return f.released[buildID]
}

func (f *fakeCache) askCount(buildID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.asks[buildID]
}

func (f *fakeCache) release(buildID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, buildID)
	f.released[buildID] = true
}

func (f *fakeCache) put(buildID string, tpl template.Template) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[buildID] = tpl
}

func newUploads(t *testing.T) (*Uploads, *fakeCache) {
	t.Helper()
	cache := newFakeCache()
	futures := ttlcache.New(
		ttlcache.WithTTL[uuid.UUID, *utils.ErrorOnce](futureTTL),
	)
	go futures.Start()
	t.Cleanup(futures.Stop)

	return &Uploads{
		tc:      cache,
		futures: futures,
	}, cache
}

func putHeader(t *testing.T, cache *fakeCache, buildID uuid.UUID, fileType build.DiffType, pending bool) {
	t.Helper()
	tpl := templatemocks.NewMockTemplate(t)
	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(&headers.Header{
		Metadata:                &headers.Metadata{Version: headers.MetadataVersionV4},
		Builds:                  map[uuid.UUID]headers.BuildData{buildID: {}}, // self-entry → not stale
		IncompletePendingUpload: pending,
	}).Maybe()

	switch fileType {
	case build.Memfile:
		tpl.EXPECT().Memfile(mock.Anything).Return(dev, nil).Maybe()
	case build.Rootfs:
		tpl.EXPECT().Rootfs().Return(dev, nil).Maybe()
	}

	cache.put(buildID.String(), tpl)
}

func putPendingHeader(t *testing.T, cache *fakeCache, buildID uuid.UUID, fileType build.DiffType) {
	t.Helper()
	putHeader(t, cache, buildID, fileType, true)
}

func TestUploads_BeginDistinctIDsAreIndependent(t *testing.T) {
	t.Parallel()
	c, _ := newUploads(t)

	a := uuid.New()
	b := uuid.New()

	futA, err := c.Start(a)
	require.NoError(t, err)
	futB, err := c.Start(b)
	require.NoError(t, err)

	require.NotSame(t, futA, futB)
	require.NoError(t, futA.SetSuccess())

	select {
	case <-futB.Done():
		t.Fatal("futB should not be done after only futA fires")
	default:
	}
}

func TestUploads_Wait_BlocksUntilSet(t *testing.T) {
	t.Parallel()
	c, cache := newUploads(t)

	id := uuid.New()
	// Pending header → Wait reaches the future-wait branch instead of
	// short-circuiting on the cleared bit.
	putPendingHeader(t, cache, id, build.Memfile)
	fut, err := c.Start(id)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_, _, _ = c.Wait(t.Context(), id, build.Memfile)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Wait should block until the future fires")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, fut.SetSuccess())

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait should return after future fires")
	}
}

func TestUploads_Wait_PropagatesUploadError(t *testing.T) {
	t.Parallel()
	c, cache := newUploads(t)

	id := uuid.New()
	putPendingHeader(t, cache, id, build.Memfile)
	fut, err := c.Start(id)
	require.NoError(t, err)

	uploadErr := errors.New("upload exploded")
	require.NoError(t, fut.SetError(uploadErr))

	_, _, err = c.Wait(t.Context(), id, build.Memfile)
	require.ErrorIs(t, err, uploadErr)
}

func TestUploads_Wait_ContextCancellation(t *testing.T) {
	t.Parallel()
	c, _ := newUploads(t)

	id := uuid.New()
	_, err := c.Start(id) // never signaled
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		_, _, err := c.Wait(ctx, id, build.Memfile)
		errCh <- err
	}()

	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Wait should return on context cancel")
	}
}

func TestUploads_Wait_NoFuture_ReadsFromCache(t *testing.T) {
	t.Parallel()
	c, cache := newUploads(t)

	id := uuid.New()
	want := &headers.Header{
		Metadata: &headers.Metadata{Version: headers.MetadataVersionV4},
		Builds:   map[uuid.UUID]headers.BuildData{id: {}},
	}

	tpl := templatemocks.NewMockTemplate(t)
	dev := blockmocks.NewMockReadonlyDevice(t)
	dev.EXPECT().Header().Return(want)
	tpl.EXPECT().Rootfs().Return(dev, nil)
	cache.put(id.String(), tpl)

	got, _, err := c.Wait(t.Context(), id, build.Rootfs)
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestUploads_ConcurrentBeginsAndWaits(t *testing.T) {
	t.Parallel()
	c, cache := newUploads(t)

	const n = 10

	ids := make([]uuid.UUID, n)
	futs := make([]*utils.ErrorOnce, n)
	for i := range n {
		ids[i] = uuid.New()
		putPendingHeader(t, cache, ids[i], build.Memfile)
		fut, err := c.Start(ids[i])
		require.NoError(t, err)
		futs[i] = fut
	}

	var done atomic.Int32
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, _, err := c.Wait(t.Context(), ids[i], build.Memfile); err == nil {
				done.Add(1)
			}
		}(i)
	}

	for i := range n {
		require.NoError(t, futs[i].SetSuccess())
	}

	wg.Wait()
	assert.Equal(t, int32(n), done.Load())
}

// newFallbackFF returns a flags client serving
// SnapshotCacheAncestorStorageFallbackFlag at on. The value is set before the
// client starts: updating an ldtestdata source under a started client races
// the SDK's own start-up.
func newFallbackFF(t *testing.T, on bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SnapshotCacheAncestorStorageFallbackFlag.Key()).VariationForAll(on))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ff.Close(context.WithoutCancel(t.Context()))
	})

	return ff
}

// firedFuture starts id's upload future and fires it with uploadErr.
func firedFuture(t *testing.T, c *Uploads, id uuid.UUID, uploadErr error) {
	t.Helper()

	fut, err := c.Start(id)
	require.NoError(t, err)
	require.NoError(t, fut.SetError(uploadErr))
}

// A future that fired successfully for a build no longer in the cache fails
// the wait unless the fallback flag is on; with it on, Wait hands back the
// future_no_entry verdict and no header. The Uploads here has no P2P resolver
// and no persistence, so reaching the remote-storage poll would panic.
func TestUploads_Wait_FiredFutureWithoutEntry(t *testing.T) {
	t.Parallel()

	offFF := newFallbackFF(t, false)
	onFF := newFallbackFF(t, true)

	for _, tc := range []struct {
		name        string
		ff          *featureflags.Client
		wantVerdict AncestorVerdict
		wantErr     bool
	}{
		{
			name:        "nil client resolves to the fallback",
			ff:          nil,
			wantVerdict: map[bool]AncestorVerdict{false: verdictError, true: verdictFutureNoEntry}[featureflags.SnapshotCacheAncestorStorageFallbackFlag.Fallback()],
			wantErr:     !featureflags.SnapshotCacheAncestorStorageFallbackFlag.Fallback(),
		},
		{name: "flag off fails as before", ff: offFF, wantVerdict: verdictError, wantErr: true},
		{name: "flag on returns the verdict", ff: onFF, wantVerdict: verdictFutureNoEntry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newUploads(t)
			c.ff = tc.ff
			id := uuid.New()
			firedFuture(t, c, id, nil)

			h, verdict, err := c.Wait(t.Context(), id, build.Memfile)
			require.Nil(t, h)
			require.Equal(t, tc.wantVerdict, verdict)
			if tc.wantErr {
				require.ErrorContains(t, err, "not in template cache")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Uploads reads the flag through its client at each wait rather than
// capturing the value, so a client swapped in between two waits decides the
// second one.
func TestUploads_Wait_FallbackFlagReadPerWait(t *testing.T) {
	t.Parallel()

	c, _ := newUploads(t)
	c.ff = newFallbackFF(t, false)
	id := uuid.New()
	firedFuture(t, c, id, nil)

	_, verdict, err := c.Wait(t.Context(), id, build.Memfile)
	require.Error(t, err)
	require.Equal(t, verdictError, verdict)

	c.ff = newFallbackFF(t, true)

	_, verdict, err = c.Wait(t.Context(), id, build.Memfile)
	require.NoError(t, err)
	require.Equal(t, verdictFutureNoEntry, verdict)
}

// A future that fired with an error is a failed upload, not a missing entry:
// it still fails the wait with the fallback on.
func TestUploads_Wait_FailedFutureWithoutEntryStillFails(t *testing.T) {
	t.Parallel()

	ff := newFallbackFF(t, true)
	c, _ := newUploads(t)
	c.ff = ff
	id := uuid.New()
	uploadErr := errors.New("upload exploded")
	firedFuture(t, c, id, uploadErr)

	h, verdict, err := c.Wait(t.Context(), id, build.Memfile)
	require.Nil(t, h)
	require.Equal(t, verdictError, verdict)
	require.ErrorIs(t, err, uploadErr)
}
