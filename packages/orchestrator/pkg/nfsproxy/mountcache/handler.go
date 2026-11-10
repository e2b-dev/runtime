//go:build linux

package mountcache

import (
	"context"
	"net"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/willscott/go-nfs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/handlecache"
)

const uuidLength = len(uuid.UUID{})

type mountIdentity interface {
	NFSCacheMountID() uuid.UUID
}

type mountKeyProvider interface {
	NFSCacheMountKey() MountKey
}

type mountAdmissionProvider interface {
	NFSCacheMountAdmission() *MountAdmission
}

type filesystemUnwrapper interface {
	Unwrap() billy.Filesystem
}

type shard struct {
	id      uuid.UUID
	key     MountKey
	handler *handlecache.Handler
	root    billy.Filesystem
	mounted *mountedFS
}

// Handler gives every logical NFS mount an independent ordinary-handle LRU.
// Handles are prefixed by a stable shard ID, while the mount root is represented
// by a reserved local token outside the LRU so ordinary churn cannot evict it.
// Directory listings are owned by the proxy's outer directory-verifier handler.
type Handler struct {
	inner      nfs.Handler
	cacheLimit int

	mu          sync.RWMutex
	shardsByID  map[uuid.UUID]*shard
	shardsByKey map[MountKey]*shard
	keysByOwner map[Owner]map[MountKey]struct{}
	shardsGauge metric.Int64ObservableGauge
	ownersGauge metric.Int64ObservableGauge
}

var _ nfs.Handler = (*Handler)(nil)

// NewHandler wraps inner with lifecycle-scoped handle-cache shards.
func NewHandler(inner nfs.Handler, cacheLimit int) *Handler {
	h := &Handler{
		inner:       inner,
		cacheLimit:  cacheLimit,
		mu:          sync.RWMutex{},
		shardsByID:  make(map[uuid.UUID]*shard),
		shardsByKey: make(map[MountKey]*shard),
		keysByOwner: make(map[Owner]map[MountKey]struct{}),
		shardsGauge: nil,
		ownersGauge: nil,
	}

	meter := otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache")
	shardsGauge, shardsErr := meter.Int64ObservableGauge("nfs.mount_cache.shards", metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
		h.mu.RLock()
		defer h.mu.RUnlock()

		observer.Observe(int64(len(h.shardsByKey)))

		return nil
	}))
	if shardsErr == nil {
		h.shardsGauge = shardsGauge
	} else {
		nfs.Log.Warnf("failed to create NFS mount-cache shard gauge: %v", shardsErr)
	}

	ownersGauge, ownersErr := meter.Int64ObservableGauge("nfs.mount_cache.owners", metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
		h.mu.RLock()
		defer h.mu.RUnlock()

		observer.Observe(int64(len(h.keysByOwner)))

		return nil
	}))
	if ownersErr == nil {
		h.ownersGauge = ownersGauge
	} else {
		nfs.Log.Warnf("failed to create NFS mount-cache owner gauge: %v", ownersErr)
	}

	return h
}

func (h *Handler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	status, filesystem, auth := h.inner.Mount(ctx, conn, request)
	if status != nfs.MountStatusOk {
		return status, filesystem, auth
	}
	if filesystem == nil {
		nfs.Log.Warnf("successful NFS mount returned no filesystem")

		return nfs.MountStatusErrServerFault, nil, nil
	}

	admission := filesystemMountAdmission(filesystem)
	if admission != nil {
		defer admission.Done()
	}

	key, ok := filesystemMountKey(filesystem)
	if !ok || !key.valid() {
		nfs.Log.Warnf("successful NFS mount returned no valid cache owner")

		return nfs.MountStatusErrServerFault, nil, nil
	}

	var mounted *mountedFS
	register := func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		entry := h.shardsByKey[key]
		if entry == nil {
			entry = &shard{
				id:      uuid.New(),
				key:     key,
				handler: handlecache.Wrap(h.inner, h.cacheLimit),
				root:    nil,
			}
			h.shardsByID[entry.id] = entry
			h.shardsByKey[key] = entry
			if h.keysByOwner[key.Owner] == nil {
				h.keysByOwner[key.Owner] = make(map[MountKey]struct{})
			}
			h.keysByOwner[key.Owner][key] = struct{}{}
		}

		if entry.mounted == nil {
			entry.mounted = &mountedFS{Filesystem: filesystem, mountID: entry.id, key: key}
			entry.handler.Register(entry.mounted)
			entry.root = entry.mounted
		}
		mounted = entry.mounted
	}

	if admission != nil {
		if !admission.Register(register) {
			return nfs.MountStatusErrAcces, nil, nil
		}
	} else {
		register()
	}

	return status, mounted, auth
}

func (h *Handler) Change(ctx context.Context, filesystem billy.Filesystem) billy.Change {
	return h.inner.Change(ctx, filesystem)
}

func (h *Handler) FSStat(ctx context.Context, filesystem billy.Filesystem, stat *nfs.FSStat) error {
	return h.inner.FSStat(ctx, filesystem, stat)
}

func (h *Handler) ToHandle(ctx context.Context, filesystem billy.Filesystem, path []string) []byte {
	mountID, ok := filesystemMountID(filesystem)
	if !ok {
		return nil
	}

	h.mu.RLock()
	entry := h.shardsByID[mountID]
	h.mu.RUnlock()
	if entry == nil {
		// Removal makes root and child handles stale regardless of the path.
		// Keep the token shape valid so callers observe ESTALE.
		return encodeHandle(mountID, uuid.Nil[:])
	}

	if len(path) == 0 {
		return encodeHandle(mountID, uuid.Nil[:])
	}

	// Cache the mounted filesystem, never a transient middleware wrapper with
	// a request context. Root and ordinary handles use this same identity even
	// when an ordinary handle is allocated before the first root handle.
	localHandle := entry.handler.ToHandle(ctx, entry.mounted, path)
	if len(localHandle) != uuidLength {
		return nil
	}

	return encodeHandle(mountID, localHandle)
}

func (h *Handler) FromHandle(ctx context.Context, handle []byte) (billy.Filesystem, []string, error) {
	entry, localHandle, root, err := h.resolve(handle)
	if err != nil {
		return nil, []string{}, err
	}
	if isRootHandle(localHandle) {
		if root == nil {
			return nil, []string{}, staleHandle()
		}

		return root, []string{}, nil
	}

	return entry.handler.FromHandle(ctx, localHandle)
}

func (h *Handler) InvalidateHandle(ctx context.Context, filesystem billy.Filesystem, handle []byte) error {
	entry, localHandle, _, err := h.resolve(handle)
	if err != nil {
		return err
	}
	if isRootHandle(localHandle) {
		// The root is the mount's stable entry point. It is removed only with the
		// owning lifecycle, never by ordinary cache invalidation or LRU churn.

		return nil
	}

	return entry.handler.InvalidateHandle(ctx, filesystem, localHandle)
}

func (h *Handler) HandleLimit() int {
	return h.cacheLimit
}

// RemoveOwner drops all logical mount shards belonging to one sandbox
// lifecycle. It is idempotent; replacement lifecycles use a different owner.
func (h *Handler) RemoveOwner(owner Owner) {
	if !owner.valid() {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for key := range h.keysByOwner[owner] {
		entry := h.shardsByKey[key]
		delete(h.shardsByKey, key)
		if entry != nil {
			entry.handler.Release(entry.mounted)
			delete(h.shardsByID, entry.id)
		}
	}
	delete(h.keysByOwner, owner)
}

// Counts returns the shard and owner counts represented by the gauges.
func (h *Handler) Counts() (shards, owners int) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.shardsByKey), len(h.keysByOwner)
}

func (h *Handler) resolve(handle []byte) (*shard, []byte, billy.Filesystem, error) {
	if len(handle) != 2*uuidLength {
		return nil, nil, nil, staleHandle()
	}

	mountID, err := uuid.FromBytes(handle[:uuidLength])
	if err != nil {
		return nil, nil, nil, staleHandle()
	}

	h.mu.RLock()
	entry := h.shardsByID[mountID]
	var root billy.Filesystem
	if entry != nil {
		root = entry.root
	}
	h.mu.RUnlock()
	if entry == nil {
		return nil, nil, nil, staleHandle()
	}

	return entry, handle[uuidLength:], root, nil
}

func encodeHandle(mountID uuid.UUID, localHandle []byte) []byte {
	handle := make([]byte, 0, uuidLength+len(localHandle))
	handle = append(handle, mountID[:]...)
	handle = append(handle, localHandle...)

	return handle
}

func isRootHandle(handle []byte) bool {
	if len(handle) != uuidLength {
		return false
	}
	for _, b := range handle {
		if b != 0 {
			return false
		}
	}

	return true
}

func staleHandle() error {
	return &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale, WrappedErr: nil}
}

func filesystemMountID(filesystem billy.Filesystem) (uuid.UUID, bool) {
	for filesystem != nil {
		if identity, ok := filesystem.(mountIdentity); ok {
			return identity.NFSCacheMountID(), true
		}

		unwrapper, ok := filesystem.(filesystemUnwrapper)
		if !ok {
			return uuid.Nil, false
		}
		filesystem = unwrapper.Unwrap()
	}

	return uuid.Nil, false
}

func filesystemMountAdmission(filesystem billy.Filesystem) *MountAdmission {
	for filesystem != nil {
		if provider, ok := filesystem.(mountAdmissionProvider); ok {
			return provider.NFSCacheMountAdmission()
		}

		unwrapper, ok := filesystem.(filesystemUnwrapper)
		if !ok {
			return nil
		}
		filesystem = unwrapper.Unwrap()
	}

	return nil
}

func filesystemMountKey(filesystem billy.Filesystem) (MountKey, bool) {
	for filesystem != nil {
		if provider, ok := filesystem.(mountKeyProvider); ok {
			return provider.NFSCacheMountKey(), true
		}

		unwrapper, ok := filesystem.(filesystemUnwrapper)
		if !ok {
			return MountKey{Owner: Owner{SandboxID: "", LifecycleID: ""}, VolumeID: uuid.Nil}, false
		}
		filesystem = unwrapper.Unwrap()
	}

	return MountKey{Owner: Owner{SandboxID: "", LifecycleID: ""}, VolumeID: uuid.Nil}, false
}
