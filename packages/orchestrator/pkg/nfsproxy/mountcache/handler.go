package mountcache

import (
	"context"
	"net"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const uuidLength = len(uuid.UUID{})

type mountIdentity interface {
	NFSCacheMountID() uuid.UUID
}

type mountOwner interface {
	NFSCacheOwner() Owner
}

type filesystemUnwrapper interface {
	Unwrap() billy.Filesystem
}

type shard struct {
	handler nfs.Handler
}

type Owner struct {
	SandboxID   string
	LifecycleID string
}

// Handler gives every successful NFS mount an independent handle cache. Each
// opaque handle is prefixed with its mount ID so it can be routed to the right
// cache without sharing eviction capacity with other mounts.
//
// Handler intentionally does not implement nfs.CachingHandler. Directory
// verifier caching has no filesystem or mount argument and cannot be safely
// isolated by mount.
type Handler struct {
	inner      nfs.Handler
	cacheLimit int

	mu            sync.RWMutex
	shards        map[uuid.UUID]*shard
	shardsByOwner map[Owner]map[uuid.UUID]struct{}
	shardsGauge   metric.Int64ObservableGauge
	ownersGauge   metric.Int64ObservableGauge
}

var _ nfs.Handler = (*Handler)(nil)

func NewHandler(inner nfs.Handler, cacheLimit int) *Handler {
	h := &Handler{
		inner:         inner,
		cacheLimit:    cacheLimit,
		shards:        make(map[uuid.UUID]*shard),
		shardsByOwner: make(map[Owner]map[uuid.UUID]struct{}),
	}

	meter := otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache")
	if gauge, err := meter.Int64ObservableGauge("nfs.mount_cache.shards", metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
		h.mu.RLock()
		defer h.mu.RUnlock()

		observer.Observe(int64(len(h.shards)))

		return nil
	})); err == nil {
		h.shardsGauge = gauge
	} else {
		nfs.Log.Warnf("failed to create NFS mount cache shard gauge: %v", err)
	}

	if gauge, err := meter.Int64ObservableGauge("nfs.mount_cache.owners", metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
		h.mu.RLock()
		defer h.mu.RUnlock()

		observer.Observe(int64(len(h.shardsByOwner)))

		return nil
	})); err == nil {
		h.ownersGauge = gauge
	} else {
		nfs.Log.Warnf("failed to create NFS mount cache owner gauge: %v", err)
	}

	return h
}

func (h *Handler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	status, filesystem, auth := h.inner.Mount(ctx, conn, request)
	if status != nfs.MountStatusOk || filesystem == nil {
		return status, filesystem, auth
	}

	mountID := uuid.New()
	owner, _ := filesystemOwner(filesystem)

	h.mu.Lock()
	h.shards[mountID] = &shard{handler: helpers.NewCachingHandler(h.inner, h.cacheLimit)}
	if owner.SandboxID != "" && owner.LifecycleID != "" {
		if h.shardsByOwner[owner] == nil {
			h.shardsByOwner[owner] = make(map[uuid.UUID]struct{})
		}
		h.shardsByOwner[owner][mountID] = struct{}{}
	}
	h.mu.Unlock()

	return status, &mountedFS{Filesystem: filesystem, mountID: mountID, owner: owner}, auth
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
	shard := h.shards[mountID]
	h.mu.RUnlock()
	if shard == nil {
		return nil
	}

	localHandle := shard.handler.ToHandle(ctx, filesystem, path)
	if len(localHandle) == 0 {
		return nil
	}

	handle := make([]byte, 0, uuidLength+len(localHandle))
	handle = append(handle, mountID[:]...)
	handle = append(handle, localHandle...)

	return handle
}

func (h *Handler) FromHandle(ctx context.Context, handle []byte) (billy.Filesystem, []string, error) {
	shard, localHandle, err := h.resolve(handle)
	if err != nil {
		return nil, []string{}, err
	}

	return shard.handler.FromHandle(ctx, localHandle)
}

func (h *Handler) InvalidateHandle(ctx context.Context, filesystem billy.Filesystem, handle []byte) error {
	shard, localHandle, err := h.resolve(handle)
	if err != nil {
		return err
	}

	return shard.handler.InvalidateHandle(ctx, filesystem, localHandle)
}

func (h *Handler) HandleLimit() int {
	return h.cacheLimit
}

// RemoveOwner drops all mount caches belonging to one sandbox lifecycle. It is
// safe to call more than once.
func (h *Handler) RemoveOwner(owner Owner) {
	if owner.SandboxID == "" || owner.LifecycleID == "" {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for mountID := range h.shardsByOwner[owner] {
		delete(h.shards, mountID)
	}
	delete(h.shardsByOwner, owner)
}

func (h *Handler) resolve(handle []byte) (*shard, []byte, error) {
	if len(handle) <= uuidLength {
		return nil, nil, staleHandle()
	}

	mountID, err := uuid.FromBytes(handle[:uuidLength])
	if err != nil {
		return nil, nil, staleHandle()
	}

	h.mu.RLock()
	shard := h.shards[mountID]
	h.mu.RUnlock()
	if shard == nil {
		return nil, nil, staleHandle()
	}

	return shard, handle[uuidLength:], nil
}

func staleHandle() error {
	return &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
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

func filesystemOwner(filesystem billy.Filesystem) (Owner, bool) {
	for filesystem != nil {
		if owner, ok := filesystem.(mountOwner); ok {
			return owner.NFSCacheOwner(), true
		}

		unwrapper, ok := filesystem.(filesystemUnwrapper)
		if !ok {
			return Owner{}, false
		}
		filesystem = unwrapper.Unwrap()
	}

	return Owner{}, false
}
