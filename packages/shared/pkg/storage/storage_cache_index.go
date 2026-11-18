package storage

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// CacheIndexRecorder receives the builds used from the NFS cache (a hit or a
// fill), so the cache cleaner can evict by last use. Touch must not block.
type CacheIndexRecorder interface {
	Touch(buildID string)
}

// CacheOption configures WrapInNFSCache.
type CacheOption func(*cache)

// WithCacheIndex reports every build used through the cache to r.
func WithCacheIndex(r CacheIndexRecorder) CacheOption {
	return func(c *cache) { c.index = r }
}

// indexTouchInterval bounds how often one cached object reports its build, so
// a hot object does not reach the recorder on every chunk.
const indexTouchInterval = 10 * time.Second

// indexToucher reports one cached object's build to the recorder. A nil
// *indexToucher is valid and does nothing.
type indexToucher struct {
	rec     CacheIndexRecorder
	buildID string
	last    atomic.Int64 // unix nanos of the last report
}

// newIndexToucher returns nil when there is no recorder or the object path
// does not start with a build ID ("<buildID>/<file>").
func newIndexToucher(rec CacheIndexRecorder, objectPath string) *indexToucher {
	if rec == nil {
		return nil
	}
	buildID, _, ok := strings.Cut(objectPath, "/")
	if !ok {
		return nil
	}
	if _, err := uuid.Parse(buildID); err != nil {
		return nil
	}

	return &indexToucher{rec: rec, buildID: buildID}
}

func (t *indexToucher) touch() {
	if t == nil {
		return
	}
	now := time.Now().UnixNano()
	last := t.last.Load()
	if last != 0 && now-last < int64(indexTouchInterval) {
		return
	}
	if t.last.CompareAndSwap(last, now) {
		t.rec.Touch(t.buildID)
	}
}
