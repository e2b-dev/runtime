//go:build linux

package template

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// supersededMetric counts pause layers the first time a pause abandons them,
// once per layer however many pauses name it: forks of one generation each
// abandon it, and counting every one would make it a poor denominator for
// what later happens to the layer.
var supersededMetric = utils.Must(meter.Int64Counter("orchestrator.templates.cache.snapshot_layers_superseded",
	metric.WithDescription("Pause layers marked superseded, once per layer"),
	metric.WithUnit("{layer}")))

// outcomeMetric counts what became of each superseded layer, once per layer:
// released by the predicate, or expired_held, closed at the end of its TTL
// without ever being released, which is where a layer whose own upload did
// not land ends. A layer whose devices never resolve reaches neither, and
// shows as the gap between the superseded count and the sum of outcomes.
var outcomeMetric = utils.Must(meter.Int64Counter("orchestrator.templates.cache.snapshot_layer_outcome",
	metric.WithDescription("Superseded pause layers by terminal outcome (outcome=released|expired_held), once per layer"),
	metric.WithUnit("{layer}")))

// suppressedMetric counts each superseded layer once, the first time it would
// have been released while the release is off. It sizes the would-release
// population, not a queue: nothing drains it, and a suppressed layer still
// reaches whichever outcome its next edge or its expiry gives it.
var suppressedMetric = utils.Must(meter.Int64Counter("orchestrator.templates.cache.snapshot_layer_release_suppressed",
	metric.WithDescription("Superseded pause layers that would have been released with the release off, once per layer"),
	metric.WithUnit("{layer}")))

var (
	attrOutcomeReleased    = attribute.String("outcome", "released")
	attrOutcomeExpiredHeld = attribute.String("outcome", "expired_held")

	// A miss is an insert when AddSnapshot admits a layer this node just
	// produced, released_layer when a lookup refetches a build released here
	// within one entry TTL, and cold otherwise.
	attrMissInsert        = attribute.String("reason", "insert")
	attrMissCold          = attribute.String("reason", "cold")
	attrMissReleasedLayer = attribute.String("reason", "released_layer")
)

// WasReleased reports whether this node released buildID's layer within the
// time its entry would still have been cached, and extends that time as a
// lookup of the resident entry would have extended its TTL. A descendant's
// upload asks it at each wait that misses the build's entry, so a released
// ancestor heals as the resident one would have, whatever the flags read then. It is false on
// a nil Cache.
func (c *Cache) WasReleased(buildID string) bool {
	if c == nil || c.released == nil || !c.released.Has(buildID) {
		return false
	}
	c.released.Touch(buildID)

	return true
}

// missReason labels the miss that admitted tmpl.
func (c *Cache) missReason(tmpl *storageTemplate) attribute.KeyValue {
	if tmpl.kind != layerKindFetched {
		return attrMissInsert
	}

	if c.released != nil && c.released.Has(tmpl.Files().CacheKey()) {
		return attrMissReleasedLayer
	}

	return attrMissCold
}

// layerKind is how an entry came to be in the cache, recorded on the candidate
// before it is admitted. A lookup that hits keeps the resident's kind and drops
// the candidate, so the first writer's kind is the entry's for its whole life.
type layerKind string

const (
	// layerKindPause is a layer this node inserted through AddSnapshot for a
	// pause. It is the only kind a later pause can supersede.
	layerKindPause layerKind = "pause_layer"
	// layerKindLocalTemplate is any other AddSnapshot insert: a checkpoint's
	// snapshot template or a template build layer.
	layerKindLocalTemplate layerKind = "local_template"
	// layerKindFetched is an entry admitted by a lookup and fetched from
	// storage, whatever produced it: a base template, a build, or another
	// node's snapshot.
	layerKindFetched layerKind = "fetched"
)

// layerKinds lists every kind, in the order the gauges report them.
var layerKinds = []layerKind{layerKindPause, layerKindLocalTemplate, layerKindFetched}

// generationBand buckets a header's chain generation for the residency
// gauges: a deep chain's header maps several times the ranges a shallow one's
// does, so bytes by band show whether retention falls on deep chains too.
type generationBand string

const (
	generationBandLT20    generationBand = "lt20"
	generationBand20To500 generationBand = "20_500"
	generationBand500To2k generationBand = "500_2000"
	generationBandGTE2000 generationBand = "gte2000"
)

var generationBands = []generationBand{generationBandLT20, generationBand20To500, generationBand500To2k, generationBandGTE2000}

func generationBandOf(generation uint64) generationBand {
	switch {
	case generation < 20:
		return generationBandLT20
	case generation < 500:
		return generationBand20To500
	case generation < 2000:
		return generationBand500To2k
	default:
		return generationBandGTE2000
	}
}

// layerBand keys resident mapping bytes by kind and generation band.
type layerBand struct {
	kind layerKind
	band generationBand
}

// SnapshotLineage describes the snapshot AddSnapshot caches.
type SnapshotLineage struct {
	// Origin is the operation that produced the snapshot.
	Origin storage.ObjectOrigin
	// Predecessor is the build the snapshotted sandbox was running from.
	Predecessor string
	// AbandonsPredecessor is true when the operation leaves Predecessor behind:
	// a pause, or a checkpoint that resumes a fresh sandbox from the new build.
	// An in-place checkpoint keeps its sandbox running on Predecessor, and
	// later checkpoints and the eventual pause still parent on it.
	AbandonsPredecessor bool
}

// supersedes is the build this snapshot's publication marks superseded, or ""
// for none.
func (l SnapshotLineage) supersedes() string {
	if !l.AbandonsPredecessor {
		return ""
	}

	return l.Predecessor
}

func (l SnapshotLineage) kind() layerKind {
	if l.Origin == storage.ObjectOriginPause {
		return layerKindPause
	}

	return layerKindLocalTemplate
}

// layerMark is the supersession state of one cache entry, guarded by
// Cache.extendMu.
type layerMark struct {
	// upload is how the layer's own upload ended, an UploadOutcome, and zero
	// while it runs or when the entry has none. Written under extendMu, by
	// each upload's finish, and never moved off UploadLanded: a second upload
	// of the same build cannot take a layer out of storage. Atomic so the
	// footprint walk can read it without the lock.
	upload atomic.Uint32
	// superseded is set once a pause on this node published a successor that
	// abandons this layer. No consumer can ask for this generation again: every
	// one of them resumes from the successor.
	superseded bool
	// gone is set once the entry is on its way to Close: released here, closed
	// by the eviction callback, or closed as a retired instance. Nothing
	// evaluates it again after that.
	gone bool
	// suppressed is set the first time the entry would have been released
	// while the release was off.
	suppressed bool
	// closedDirectly is set when the release closes the instance itself
	// rather than through the eviction callback, which then leaves it alone.
	closedDirectly bool
}

// markable is a cache entry that can carry a supersession mark.
type markable interface {
	layerKind() layerKind
	mark() *layerMark
	// devicesResolved reports, without blocking, whether every device Close
	// waits on has resolved, to a value or to an error.
	devicesResolved() bool
	// devicesDone returns a channel per device Close waits on, each closed
	// once that device resolves.
	devicesDone() []<-chan struct{}
}

// landed reports whether the layer's own upload put it in storage.
func (lm *layerMark) landed() bool {
	return UploadOutcome(lm.upload.Load()) == UploadLanded
}

func (t *storageTemplate) layerKind() layerKind { return t.kind }

func (t *storageTemplate) mark() *layerMark { return &t.layerMark }

func (t *storageTemplate) devicesDone() []<-chan struct{} {
	return []<-chan struct{}{t.memfile.Done, t.rootfs.Done, t.snapfile.Done}
}

func (t *storageTemplate) devicesResolved() bool {
	for _, done := range t.devicesDone() {
		select {
		case <-done:
		default:
			return false
		}
	}

	return true
}

// releaseEnabled is the release conjunction: the release flag and the
// ancestor storage fallback both on. A released ancestor's upload future
// outlives its entry, and without the fallback that fails a descendant's
// upload, so the release never acts without it.
func releaseEnabled(ctx context.Context, flags *featureflags.Client) bool {
	boolFlag := func(f featureflags.BoolFlag) bool {
		if flags == nil {
			return f.Fallback()
		}

		return flags.BoolFlag(ctx, f)
	}

	return boolFlag(featureflags.SnapshotCacheReleaseSupersededFlag) &&
		boolFlag(featureflags.SnapshotCacheAncestorStorageFallbackFlag)
}

// releaseSupersededLocked deletes t from the cache if it is releasable: marked
// superseded, held by no pin, with every device resolved, so its Close cannot
// block, and landed in storage by its own upload. It reports whether t was
// released, and is called at every edge that can make the last of those true —
// a pin's return, the mark, and the completion of t's fetch — so whichever
// comes last frees it. The upload records landed just before it returns its
// pin, so that return is the edge for it. The caller must
// hold extendMu, so no acquisition can pin t between the check and the delete.
//
// Freeing goes by instance, never by key alone. If the cache slot for t's key
// holds t, the slot is deleted and the eviction callback closes t. Otherwise
// t is closed directly, off the lock, and marked so the eviction callback
// leaves it alone: a live successor holding the slot is left in place, and an
// expired item the lookup no longer returns is deleted so it goes through the
// eviction callback.
func (c *Cache) releaseSupersededLocked(ctx context.Context, t Template) bool {
	m, ok := t.(markable)
	if !ok {
		return false
	}

	lm := m.mark()
	if !lm.superseded || lm.gone || !lm.landed() || c.pinHolding(t) || !m.devicesResolved() {
		return false
	}

	if !releaseEnabled(ctx, c.flags) {
		if !lm.suppressed {
			lm.suppressed = true
			suppressedMetric.Add(ctx, 1)
		}

		return false
	}

	lm.gone = true
	outcomeMetric.Add(ctx, 1, metric.WithAttributes(attrOutcomeReleased))

	key := t.Files().CacheKey()
	item := c.cache.Get(key, ttlcacheNoTouch)
	inSlot := item != nil && item.Value() == t
	if c.released != nil {
		// For the entry's full TTL, so at least as long as it would have
		// lived; one that already left the TTL cache gets the default entry
		// TTL.
		ttl := ttlcache.DefaultTTL
		if inSlot {
			ttl = item.TTL()
		}
		c.released.Set(key, struct{}{}, ttl)
	}
	if inSlot {
		c.cache.Delete(key)

		return true
	}

	// Get hides an entry that expired and has not been swept, and that entry
	// may be t itself. Delete it now, as getOrAdmitLocked does, so it goes
	// through onEvicted like any other removal; closedDirectly tells onEvicted
	// that t is closed here.
	lm.closedDirectly = true
	if item == nil {
		c.cache.Delete(key)
	}

	// Build-scoped, as in onEvicted: purge only if no other instance serves it.
	if c.peers != nil && !c.buildIsPinned(key) && c.cache.Get(key, ttlcacheNoTouch) == nil {
		c.peers.Purge(key)
	}

	closeCtx := context.WithoutCancel(ctx)
	go func() {
		if err := t.Close(closeCtx); err != nil {
			logger.L().Warn(closeCtx, "failed to cleanup released template data",
				zap.String("item_key", key), zap.Error(err))
		}
	}()

	return true
}

// uploadFinisher returns the call that ends t's own upload: it records outcome
// on t's mark and then returns the upload's pin, in that order, so the
// refs == 0 edge the return produces already reads the outcome. The other
// order would evaluate the release with the upload still running and leave a
// landed layer to its TTL. Only the first call acts.
func (c *Cache) uploadFinisher(t Template, release func()) func(UploadOutcome) {
	var once sync.Once

	return func(outcome UploadOutcome) {
		once.Do(func() {
			if m, ok := t.(markable); ok {
				c.extendMu.Lock()
				if !m.mark().landed() {
					m.mark().upload.Store(uint32(outcome))
				}
				c.extendMu.Unlock()
			}
			release()
		})
	}
}

// watchFetch re-evaluates t once every device it will close has resolved,
// since an entry superseded and unpinned while still fetching has no other
// edge left to release it.
func (c *Cache) watchFetch(ctx context.Context, t markable) {
	for _, done := range t.devicesDone() {
		<-done
	}

	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	if tmpl, ok := t.(Template); ok {
		c.releaseSupersededLocked(ctx, tmpl)
	}
}

// noteClosedLocked records that the eviction callback is closing item's
// template, counting a superseded layer that is closed at the end of its TTL
// without having been released. Expiry is read from the item: an expired
// entry reaches the callback as Expired from the sweeper or as Deleted from
// the next admission of its key. The caller must hold extendMu.
func noteClosedLocked(ctx context.Context, item *ttlcache.Item[string, Template]) {
	m, ok := item.Value().(markable)
	if !ok {
		return
	}

	lm := m.mark()
	if lm.gone {
		return
	}
	lm.gone = true

	if lm.superseded && item.IsExpired() {
		outcomeMetric.Add(ctx, 1, metric.WithAttributes(attrOutcomeExpiredHeld))
	}
}

// noteGoneLocked records that t is being closed outside the eviction
// callback, as a retired instance at its last release. The caller must hold
// extendMu.
func noteGoneLocked(t Template) {
	if m, ok := t.(markable); ok {
		m.mark().gone = true
	}
}

// markSupersededLocked records that a pause published a successor abandoning
// key's live entry. Only a layer this node inserted for a pause is marked; any
// other entry under key, and a key with no live entry, is left as it is. The
// caller must hold extendMu and must have published the successor already, in
// the same hold, so no reader can see a mark whose successor does not exist.
func (c *Cache) markSupersededLocked(ctx context.Context, key string) {
	t, ok := c.pinnedTemplate(key)
	if !ok {
		item := c.cache.Get(key, ttlcacheNoTouch)
		if item == nil {
			return
		}
		t = item.Value()
	}

	m, ok := t.(markable)
	if !ok || m.layerKind() != layerKindPause {
		return
	}

	lm := m.mark()
	if lm.superseded {
		return
	}
	lm.superseded = true

	supersededMetric.Add(ctx, 1)

	c.releaseSupersededLocked(ctx, t)
}
