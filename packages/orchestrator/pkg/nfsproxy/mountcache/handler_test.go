//go:build linux

package mountcache

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
)

type testFilesystem struct {
	billy.Filesystem

	key       MountKey
	admission *MountAdmission
}

func (f *testFilesystem) NFSCacheMountKey() MountKey {
	return f.key
}

func (f *testFilesystem) NFSCacheMountAdmission() *MountAdmission {
	return f.admission
}

type testHandler struct {
	mu         sync.Mutex
	keys       []MountKey
	admissions []*MountAdmission
	status     nfs.MountStatus
	nilFS      bool
}

type blockingTestHandler struct {
	*testHandler

	entered chan struct{}
	proceed chan struct{}
}

func (h *blockingTestHandler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	close(h.entered)
	<-h.proceed

	return h.testHandler.Mount(ctx, conn, request)
}

func (h *testHandler) Mount(context.Context, net.Conn, nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	h.mu.Lock()
	defer h.mu.Unlock()

	status := h.status
	if status == 0 {
		status = nfs.MountStatusOk
	}
	if status != nfs.MountStatusOk || h.nilFS {
		return status, nil, nil
	}

	key := MountKey{Owner: Owner{SandboxID: "", LifecycleID: ""}, VolumeID: uuid.Nil}
	if len(h.keys) > 0 {
		key = h.keys[0]
		h.keys = h.keys[1:]
	}

	var admission *MountAdmission
	if len(h.admissions) > 0 {
		admission = h.admissions[0]
		h.admissions = h.admissions[1:]
	}

	return status, &testFilesystem{Filesystem: memfs.New(), key: key, admission: admission}, nil
}

func (*testHandler) Change(context.Context, billy.Filesystem) billy.Change { return nil }

func (*testHandler) FSStat(context.Context, billy.Filesystem, *nfs.FSStat) error { return nil }

func (*testHandler) ToHandle(context.Context, billy.Filesystem, []string) []byte {
	panic("must be intercepted by the cache")
}

func (*testHandler) FromHandle(context.Context, []byte) (billy.Filesystem, []string, error) {
	panic("must be intercepted by the cache")
}

func (*testHandler) InvalidateHandle(context.Context, billy.Filesystem, []byte) error {
	panic("must be intercepted by the cache")
}

func (*testHandler) HandleLimit() int { return 1024 }

func newTestHandler(keys ...MountKey) *testHandler {
	return &testHandler{
		mu:         sync.Mutex{},
		keys:       keys,
		admissions: nil,
		status:     nfs.MountStatusOk,
		nilFS:      false,
	}
}

func mountKey(sandboxID, lifecycleID string, volumeID uuid.UUID) MountKey {
	return MountKey{
		Owner:    Owner{SandboxID: sandboxID, LifecycleID: lifecycleID},
		VolumeID: volumeID,
	}
}

func mount(tb testing.TB, handler *Handler) billy.Filesystem {
	tb.Helper()

	status, filesystem, _ := handler.Mount(tb.Context(), nil, nfs.MountRequest{}) //nolint:exhaustruct_v5 // empty request is sufficient for the fake inner handler
	require.Equal(tb, nfs.MountStatusOk, status)
	require.NotNil(tb, filesystem)

	return filesystem
}

func requireStaleHandle(tb testing.TB, err error) {
	tb.Helper()

	var statusErr *nfs.NFSStatusError
	require.ErrorAs(tb, err, &statusErr)
	require.Equal(tb, nfs.NFSStatusStale, statusErr.NFSStatus)
}

func TestHandlerIsolatesOrdinaryHandlesAcrossMounts(t *testing.T) {
	t.Parallel()

	keyA := mountKey("sandbox-a", "lifecycle-a", uuid.New())
	keyB := mountKey("sandbox-b", "lifecycle-b", uuid.New())
	handler := NewHandler(newTestHandler(keyA, keyB), 2)
	filesystemA := mount(t, handler)
	filesystemB := mount(t, handler)

	handleA := handler.ToHandle(t.Context(), filesystemA, []string{"a"})
	require.NotEmpty(t, handleA)

	// More than one shard's capacity in B must not consume A's LRU.
	for i := range 4 {
		handler.ToHandle(t.Context(), filesystemB, []string{fmt.Sprintf("b-%d", i)})
	}

	resolved, path, err := handler.FromHandle(t.Context(), handleA)
	require.NoError(t, err)
	require.Same(t, filesystemA, resolved)
	require.Equal(t, []string{"a"}, path)
}

func TestHandlerPinsRootOutsideOrdinaryLRU(t *testing.T) {
	t.Parallel()

	key := mountKey("sandbox-a", "lifecycle-a", uuid.New())
	handler := NewHandler(newTestHandler(key), 2)
	filesystem := mount(t, handler)

	root := handler.ToHandle(t.Context(), filesystem, nil)
	first := handler.ToHandle(t.Context(), filesystem, []string{"first"})
	for i := range 4 {
		handler.ToHandle(t.Context(), filesystem, []string{fmt.Sprintf("ordinary-%d", i)})
	}

	resolved, path, err := handler.FromHandle(t.Context(), root)
	require.NoError(t, err)
	require.Same(t, filesystem, resolved)
	require.Empty(t, path)

	_, _, err = handler.FromHandle(t.Context(), first)
	requireStaleHandle(t, err)
}

func TestHandlerReusesShardAcrossRemounts(t *testing.T) {
	t.Parallel()

	key := mountKey("sandbox-a", "lifecycle-a", uuid.New())
	handler := NewHandler(newTestHandler(key, key), 2)
	oldFilesystem := mount(t, handler)
	oldRoot := handler.ToHandle(t.Context(), oldFilesystem, nil)
	oldOrdinary := handler.ToHandle(t.Context(), oldFilesystem, []string{"old"})

	newFilesystem := mount(t, handler)
	newRoot := handler.ToHandle(t.Context(), newFilesystem, nil)

	require.Equal(t, oldRoot, newRoot, "one logical mount must have one stable root handle")
	require.Len(t, handler.shardsByKey, 1)
	require.Len(t, handler.shardsByID, 1)

	resolved, _, err := handler.FromHandle(t.Context(), oldRoot)
	require.NoError(t, err)
	require.Same(t, newFilesystem, resolved, "root resolves through the newest equivalent wrapper")

	resolved, path, err := handler.FromHandle(t.Context(), oldOrdinary)
	require.NoError(t, err)
	require.Same(t, oldFilesystem, resolved)
	require.Equal(t, []string{"old"}, path)
}

func TestHandlerRemoveOwnerIsLifecyclePreciseAndIdempotent(t *testing.T) {
	t.Parallel()

	oldKey := mountKey("sandbox-a", "lifecycle-old", uuid.New())
	newKey := mountKey("sandbox-a", "lifecycle-new", uuid.New())
	handler := NewHandler(newTestHandler(oldKey, newKey), 2)
	oldFilesystem := mount(t, handler)
	newFilesystem := mount(t, handler)
	oldHandle := handler.ToHandle(t.Context(), oldFilesystem, nil)
	newHandle := handler.ToHandle(t.Context(), newFilesystem, nil)

	handler.RemoveOwner(oldKey.Owner)
	handler.RemoveOwner(oldKey.Owner)

	_, _, err := handler.FromHandle(t.Context(), oldHandle)
	requireStaleHandle(t, err)
	resolved, _, err := handler.FromHandle(t.Context(), newHandle)
	require.NoError(t, err)
	require.Same(t, newFilesystem, resolved)
	require.Len(t, handler.shardsByKey, 1)
	require.Len(t, handler.keysByOwner, 1)
}

func TestHandlerDoesNotRegisterMountAfterOwnerRemoval(t *testing.T) {
	t.Parallel()

	key := mountKey("sandbox-a", "lifecycle-old", uuid.New())
	gate := NewLifecycleGate()
	admission, admitted := gate.Begin(key.Owner)
	require.True(t, admitted)

	inner := &blockingTestHandler{
		testHandler: &testHandler{
			mu:         sync.Mutex{},
			keys:       []MountKey{key},
			admissions: []*MountAdmission{admission},
			status:     nfs.MountStatusOk,
			nilFS:      false,
		},
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	handler := NewHandler(inner, 2)

	type mountResult struct {
		status     nfs.MountStatus
		filesystem billy.Filesystem
	}
	result := make(chan mountResult, 1)
	go func() {
		status, filesystem, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{}) //nolint:exhaustruct_v5 // empty request is sufficient for the fake inner handler
		result <- mountResult{status: status, filesystem: filesystem}
	}()

	<-inner.entered
	closed, ok := gate.OwnerClosed(key.Owner)
	require.True(t, ok)
	cleanupDone := make(chan struct{})
	go func() {
		gate.CloseAndWait(key.Owner)
		handler.RemoveOwner(key.Owner)
		close(cleanupDone)
	}()
	<-closed
	select {
	case <-cleanupDone:
		t.Fatal("cleanup returned before the admitted MOUNT completed")
	default:
	}

	close(inner.proceed)
	mounted := <-result
	<-cleanupDone

	assert.Equal(t, nfs.MountStatusErrAcces, mounted.status)
	assert.Nil(t, mounted.filesystem)
	assert.Empty(t, handler.shardsByKey)
	assert.Empty(t, handler.shardsByID)
	assert.Empty(t, handler.keysByOwner)
}

func TestHandlerRejectsSuccessfulMountWithoutValidOwner(t *testing.T) {
	t.Parallel()

	for name, key := range map[string]MountKey{
		"empty owner":  {Owner: Owner{SandboxID: "", LifecycleID: ""}, VolumeID: uuid.Nil},
		"empty volume": {Owner: Owner{SandboxID: "sandbox-a", LifecycleID: "lifecycle-a"}, VolumeID: uuid.Nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			handler := NewHandler(newTestHandler(key), 2)
			status, filesystem, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{}) //nolint:exhaustruct_v5 // empty request is sufficient for the fake inner handler
			assert.Equal(t, nfs.MountStatusErrServerFault, status)
			assert.Nil(t, filesystem)
			assert.Empty(t, handler.shardsByKey)
			assert.Empty(t, handler.keysByOwner)
		})
	}
}

func TestHandlerPassesThroughFailedMountWithoutShard(t *testing.T) {
	t.Parallel()

	handler := NewHandler(&testHandler{
		mu:         sync.Mutex{},
		keys:       nil,
		admissions: nil,
		status:     nfs.MountStatusErrAcces,
		nilFS:      false,
	}, 2)
	status, filesystem, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{}) //nolint:exhaustruct_v5 // empty request is sufficient for the fake inner handler
	assert.Equal(t, nfs.MountStatusErrAcces, status)
	assert.Nil(t, filesystem)
	assert.Empty(t, handler.shardsByKey)
}

func TestHandlerRejectsMalformedUnknownAndRemovedHandles(t *testing.T) {
	t.Parallel()

	key := mountKey("sandbox-a", "lifecycle-a", uuid.New())
	handler := NewHandler(newTestHandler(key), 2)
	filesystem := mount(t, handler)
	valid := handler.ToHandle(t.Context(), filesystem, nil)

	for _, handle := range [][]byte{nil, {1}, make([]byte, 2*uuidLength), append(valid, 0)} {
		_, _, err := handler.FromHandle(t.Context(), handle)
		requireStaleHandle(t, err)
	}

	handler.RemoveOwner(key.Owner)
	staleRoot := handler.ToHandle(t.Context(), filesystem, nil)
	require.Len(t, staleRoot, 2*uuidLength)
	_, _, err := handler.FromHandle(t.Context(), staleRoot)
	requireStaleHandle(t, err)
	_, _, err = handler.FromHandle(t.Context(), valid)
	requireStaleHandle(t, err)
}

func TestHandlerRejectsSuccessfulMountWithoutFilesystem(t *testing.T) {
	t.Parallel()

	handler := NewHandler(&testHandler{
		mu:     sync.Mutex{},
		status: nfs.MountStatusOk,
		nilFS:  true,
	}, 2)
	status, filesystem, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{}) //nolint:exhaustruct_v5 // empty request is sufficient for the fake inner handler
	assert.Equal(t, nfs.MountStatusErrServerFault, status)
	assert.Nil(t, filesystem)
	assert.Empty(t, handler.shardsByKey)
}

func TestHandlerRemovalInvalidatesCapturedShard(t *testing.T) {
	t.Parallel()

	key := mountKey("sandbox-a", "lifecycle-a", uuid.New())
	handler := NewHandler(newTestHandler(key), 2)
	filesystem := mount(t, handler)
	ordinary := handler.ToHandle(t.Context(), filesystem, []string{"file"})
	mountID, ok := filesystemMountID(filesystem)
	require.True(t, ok)
	handler.mu.RLock()
	entry := handler.shardsByID[mountID]
	handler.mu.RUnlock()
	require.NotNil(t, entry)

	handler.RemoveOwner(key.Owner)
	staleRoot := handler.ToHandle(t.Context(), filesystem, nil)
	require.Len(t, staleRoot, 2*uuidLength)
	_, _, err := handler.FromHandle(t.Context(), staleRoot)
	requireStaleHandle(t, err)
	_, _, err = entry.handler.FromHandle(t.Context(), ordinary[uuidLength:])
	requireStaleHandle(t, err)
}

type requestFilesystem struct {
	billy.Filesystem

	ctx context.Context //nolint:containedctx // regression deliberately models a request-bound wrapper
}

func (f *requestFilesystem) Unwrap() billy.Filesystem { return f.Filesystem }

func TestHandlerDoesNotCacheRequestWrappers(t *testing.T) {
	t.Parallel()
	for _, ordinaryFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(ordinaryFirst), func(t *testing.T) {
			t.Parallel()
			key := mountKey("sandbox-a", "lifecycle-a", uuid.New())
			handler := NewHandler(newTestHandler(key), 2)
			mounted := mount(t, handler)
			oldCtx, cancel := context.WithCancel(t.Context())
			cancel()
			oldRequest := &requestFilesystem{Filesystem: mounted, ctx: oldCtx}
			newRequest := &requestFilesystem{Filesystem: mounted, ctx: t.Context()}
			var root, ordinary []byte
			if ordinaryFirst {
				ordinary = handler.ToHandle(t.Context(), oldRequest, []string{"file"})
				root = handler.ToHandle(t.Context(), newRequest, nil)
			} else {
				root = handler.ToHandle(t.Context(), oldRequest, nil)
				ordinary = handler.ToHandle(t.Context(), newRequest, []string{"file"})
			}
			for _, handle := range [][]byte{root, ordinary} {
				resolved, _, err := handler.FromHandle(t.Context(), handle)
				require.NoError(t, err)
				require.Same(t, mounted, resolved, "no request context may become part of the cached filesystem identity")
			}
		})
	}
}

func TestHandlerConcurrentLookupAndCleanup(t *testing.T) {
	t.Parallel()

	key := mountKey("sandbox-a", "lifecycle-a", uuid.New())
	handler := NewHandler(newTestHandler(key), 16)
	filesystem := mount(t, handler)
	handle := handler.ToHandle(t.Context(), filesystem, []string{"file"})

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 500 {
				root := handler.ToHandle(t.Context(), filesystem, nil)
				if len(root) != 2*uuidLength {
					t.Errorf("root handle length = %d, want %d during concurrent cleanup", len(root), 2*uuidLength)

					return
				}
				_, _, err := handler.FromHandle(t.Context(), handle)
				if err != nil {
					var statusErr *nfs.NFSStatusError
					if !errors.As(err, &statusErr) {
						t.Errorf("expected NFS status error, got %T: %v", err, err)

						return
					}
					if statusErr.NFSStatus != nfs.NFSStatusStale {
						t.Errorf("status = %v, want stale", statusErr.NFSStatus)
					}
				}
			}
		})
	}
	wg.Go(func() { handler.RemoveOwner(key.Owner) })
	wg.Wait()
}

func BenchmarkHandlerRoundTrip(b *testing.B) {
	for _, mounts := range []int{1, 16, 100} {
		b.Run(fmt.Sprintf("mounts=%d", mounts), func(b *testing.B) {
			keys := make([]MountKey, mounts)
			for i := range mounts {
				keys[i] = mountKey(fmt.Sprintf("sandbox-%d", i), "lifecycle", uuid.New())
			}
			handler := NewHandler(newTestHandler(keys...), 1024)
			filesystems := make([]billy.Filesystem, mounts)
			handles := make([][]byte, mounts)
			for i := range mounts {
				filesystems[i] = mount(b, handler)
				handles[i] = handler.ToHandle(b.Context(), filesystems[i], []string{"file"})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				index := i % mounts
				resolved, _, err := handler.FromHandle(b.Context(), handles[index])
				if err != nil || resolved == nil {
					b.Fatalf("resolve: %v", err)
				}
			}
		})
	}
}
