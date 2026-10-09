// Package handlecache maps NFS file handles to a path on a mounted volume.
//
// NFSv3 handles must outlive any open file, but the proxy can only remember
// a bounded number, and a forgotten handle is stale to the guest. Each mounted
// volume gets its own LRU so one sandbox touching many paths cannot evict the
// handles another sandbox holds open, and a volume's handles are dropped as
// soon as its sandbox goes away rather than aging out.
//
// Only registered volumes keep handles. A request that finishes after its
// sandbox was released still asks for handles on the released filesystem; it
// gets handles that are stale on their next use, and nothing is retained.
package handlecache

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/willscott/go-nfs"
)

// A handle is the volume's ID followed by the entry's ID. Both are random:
// the proxy does not check which sandbox presents a handle, so a handle must
// not be guessable.
const (
	idSize     = 8
	handleSize = 2 * idSize
)

type Handler struct {
	nfs.Handler

	limit int

	mu        sync.RWMutex
	volumes   map[uint64]*volume
	byMounted map[billy.Filesystem]*volume
}

type volume struct {
	id uint64
	fs billy.Filesystem

	mu      sync.Mutex
	handles *simplelru.LRU[uint64, []string]
	byPath  map[string]uint64
}

var _ nfs.Handler = (*Handler)(nil)

// Wrap remembers up to limit handles for each mounted volume.
func Wrap(h nfs.Handler, limit int) *Handler {
	return &Handler{
		Handler:   h,
		limit:     limit,
		volumes:   make(map[uint64]*volume),
		byMounted: make(map[billy.Filesystem]*volume),
	}
}

func (h *Handler) ToHandle(_ context.Context, fs billy.Filesystem, path []string) []byte {
	v := h.volumeFor(fs)

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.fs == nil {
		v.fs = fs
	}

	key := pathKey(path)
	id, ok := v.byPath[key]
	if ok {
		// Get marks the entry as recently used, so a path the guest keeps
		// asking for is not the next one evicted.
		v.handles.Get(id)
	} else {
		id = randomID()
		for v.handles.Contains(id) {
			id = randomID()
		}
		v.handles.Add(id, append([]string(nil), path...))
		v.byPath[key] = id
	}

	return format(v.id, id)
}

func (h *Handler) FromHandle(_ context.Context, handle []byte) (billy.Filesystem, []string, error) {
	v, id, ok := h.parse(handle)
	if !ok {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	path, ok := v.handles.Get(id)
	if !ok {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Keep the parent directories at least as fresh as the path, so a
	// directory never ages out while a file in it is still in use.
	for i := range path {
		if parent, ok := v.byPath[pathKey(path[:i])]; ok {
			v.handles.Get(parent)
		}
	}
	v.handles.Get(id)

	return v.fs, append([]string(nil), path...), nil
}

func (h *Handler) InvalidateHandle(_ context.Context, _ billy.Filesystem, handle []byte) error {
	v, id, ok := h.parse(handle)
	if !ok {
		return nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.handles.Remove(id)

	return nil
}

// HandleLimit is per volume. go-nfs uses half of it to cap how many entries
// one directory listing page returns.
func (h *Handler) HandleLimit() int {
	return h.limit
}

// Release forgets every handle on the volume mounted as fs. Later requests
// with those handles are stale.
func (h *Handler) Release(mounted billy.Filesystem) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if v, ok := h.byMounted[mounted]; ok {
		delete(h.byMounted, mounted)
		delete(h.volumes, v.id)
	}
}

// Register gives the volume mounted as fs its own handle cache, until
// Release.
func (h *Handler) Register(mounted billy.Filesystem) {
	mounted = unwrap(mounted)

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.byMounted[mounted]; ok {
		return
	}

	v := h.newVolume()
	for {
		v.id = randomID()
		if _, taken := h.volumes[v.id]; !taken {
			break
		}
	}
	h.volumes[v.id] = v
	h.byMounted[mounted] = v
}

// volumeFor finds the registered volume fs belongs to. A filesystem that is
// not registered gets a volume nobody can look up, so its handles are stale.
func (h *Handler) volumeFor(fs billy.Filesystem) *volume {
	h.mu.RLock()
	v, ok := h.byMounted[unwrap(fs)]
	h.mu.RUnlock()
	if ok {
		return v
	}

	v = h.newVolume()
	v.id = randomID()

	return v
}

func (h *Handler) newVolume() *volume {
	v := &volume{byPath: make(map[string]uint64)}
	v.handles, _ = simplelru.NewLRU(h.limit, func(id uint64, path []string) {
		if key := pathKey(path); v.byPath[key] == id {
			delete(v.byPath, key)
		}
	})

	return v
}

// format builds the handle parse reads: the volume's ID, then the entry's.
func format(volumeID, entryID uint64) []byte {
	handle := make([]byte, 0, handleSize)
	handle = binary.BigEndian.AppendUint64(handle, volumeID)

	return binary.BigEndian.AppendUint64(handle, entryID)
}

func (h *Handler) parse(handle []byte) (*volume, uint64, bool) {
	if len(handle) != handleSize {
		return nil, 0, false
	}

	h.mu.RLock()
	v, ok := h.volumes[binary.BigEndian.Uint64(handle[:idSize])]
	h.mu.RUnlock()

	return v, binary.BigEndian.Uint64(handle[idSize:]), ok
}

// unwrap finds the filesystem the volume was mounted as, under the
// middleware that wraps it.
func unwrap(fs billy.Filesystem) billy.Filesystem {
	for {
		wrapper, ok := fs.(interface{ Unwrap() billy.Filesystem })
		if !ok {
			return fs
		}
		fs = wrapper.Unwrap()
	}
}

func pathKey(path []string) string {
	return strings.Join(path, "/")
}

func randomID() uint64 {
	var b [idSize]byte
	_, _ = rand.Read(b[:])

	return binary.BigEndian.Uint64(b[:])
}
