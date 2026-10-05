//go:build linux

package template

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// How long to keep the template in the cache since the last access.
// Should be longer than the maximum possible sandbox lifetime.
const (
	templateExpiration       = time.Hour * 25
	templateExpirationBuffer = time.Hour

	buildCacheTTL           = time.Hour * 25
	buildCacheDelayEviction = time.Second * 60

	// How long a template lingers after its last pin is released, when it had
	// already expired out of the cache while pinned. Short on purpose — see
	// (*Cache).release.
	unpinnedGraceTTL = time.Minute
)

// ttlcacheNoTouch reads an entry without extending its TTL.
var ttlcacheNoTouch = ttlcache.WithDisableTouchOnHit[string, Template]()

var (
	tracer     = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template")
	meter      = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template")
	hitsMetric = utils.Must(meter.Int64Counter("orchestrator.templates.cache.hits",
		metric.WithDescription("Requests for templates that were already cached")))
	missesMetric = utils.Must(meter.Int64Counter("orchestrator.templates.cache.misses",
		metric.WithDescription("Requests for templates that were not cached (reason=insert|cold|released_layer)")))
	memfileDedupDuration = utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxMemfileDedupDurationName))

	deadStructureOutcomeMetric = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureOutcomeCounterName))
	deadStructureBytesMetric   = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorDeadStructureBytesCounterName))

	// Every trip through the eviction callback is counted, with the outcome as
	// an attribute, so the guards have both a numerator and a denominator. A
	// guard that never fires because no pin was ever taken and a guard that
	// fires constantly because retention regressed are indistinguishable from
	// the close count alone.
	evictionsMetric = utils.Must(meter.Int64Counter("orchestrator.templates.cache.evictions",
		metric.WithDescription("Template cache eviction callbacks, by what the callback did")))

	// Gauges for the template cache's resident footprint. The mapping gauges
	// are the ones that matter: a Header's compact Mapping is the single
	// largest long-lived allocation the orchestrator holds, and with an
	// unbounded, TTL-only cache its total was invisible until it dominated the
	// heap. Entry count alone does not show that — headers differ in size by
	// orders of magnitude — so the byte gauge is what makes growth legible.
	cacheEntriesGauge      = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheEntriesGaugeName))
	cachePinnedGauge       = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCachePinnedGaugeName))
	cacheMappingEntryGauge = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheMappingEntriesGaugeName))
	cacheMappingBytesGauge = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheMappingBytesGaugeName))
	cachePinnedRefsGauge   = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCachePinnedRefsGaugeName))
	cacheOldestPinGauge    = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheOldestPinAgeGaugeName))

	// The same walk cut by how each entry was inserted and by its chain
	// generation, on names of their own so the unlabelled series above keep
	// one value per node.
	cacheLayerEntriesGauge      = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheLayerEntriesGaugeName))
	cacheLayerMappingBytesGauge = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheLayerMappingBytesGaugeName))

	// Resident pause layers whose own upload failed for good. Unlabelled and
	// apart from layer_entries, whose kinds must keep summing to entries.
	cacheLayersUnlandedGauge = utils.Must(telemetry.GetGaugeInt(meter, telemetry.OrchestratorTemplateCacheLayersUnlandedGaugeName))
)

// Outcomes of the eviction callback, as the `reason` attribute on
// evictionsMetric.
var (
	attrEvictionClosed           = attribute.String("reason", "closed")
	attrEvictionClosedSuperseded = attribute.String("reason", "closed_superseded")
	attrEvictionSkippedPinned    = attribute.String("reason", "skipped_pinned")
	attrEvictionSkippedReadmit   = attribute.String("reason", "skipped_readmitted")
)

var (
	attrTemplateProvisionalHeaderDropped = deadStructureOutcomeAttr("template_provisional_header", "dropped")
	attrTemplateProvisionalHeaderFlagOff = deadStructureOutcomeAttr("template_provisional_header", "flag_off")
	attrTemplateProvisionalHeaderNone    = deadStructureOutcomeAttr("template_provisional_header", "none")
	attrUploadProvisionalHeaderMisorder  = deadStructureOutcomeAttr("upload_provisional_header", "misordered")
)

func deadStructureOutcomeAttr(structure, outcome string) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(
		attribute.String("structure", structure),
		attribute.String("outcome", outcome),
	))
}

type Cache struct {
	config        cfg.Config
	flags         *featureflags.Client
	cache         *ttlcache.Cache[string, Template]
	persistence   storage.StorageProvider
	buildStore    *build.DiffStore
	blockMetrics  blockmetrics.Metrics
	rootCachePath string
	peers         peerclient.Resolver
	extendMu      sync.Mutex

	// pinned holds templates a live sandbox depends on. It is the authoritative
	// strong reference: lookups consult it before the TTL cache, so a pinned
	// template survives eviction from that cache without a second instance ever
	// being constructed for the same key.
	//
	// Pinning exists because eviction is destructive. OnEviction below calls
	// Template.Close, which for snapfile/metafile is os.RemoveAll — evicting a
	// template a running sandbox is using deletes its Firecracker snapshot file
	// out from under it. The 25h TTL was standing in for this guarantee ("should
	// be longer than the maximum possible sandbox lifetime"); an explicit pin
	// states it directly, and is what makes bounding the cache safe.
	//
	// Refcounted, unlike build.DiffStore's boolean pin: one template is shared by
	// every sandbox started from the same build, so a boolean would let the first
	// sandbox to exit expose templates its siblings are still running on.
	//
	// retired holds entries whose build was Invalidated while pinned. They are
	// no longer discoverable (a lookup builds a fresh instance instead, which is
	// what Invalidate promises) but their holders are still running on them, so
	// eviction must keep its hands off them until the last release.
	pinMu   sync.Mutex
	pinned  map[string]*pinnedEntry
	retired map[*pinnedEntry]struct{}

	// released remembers the builds released here for as long as their entry
	// would have lived: set with the entry's TTL and refreshed by WasReleased
	// where a lookup would have refreshed the entry. A later miss on one is
	// labelled as the cost of the release rather than as a cold load, and a
	// descendant's upload heals one as it would have healed the resident
	// entry.
	released *ttlcache.Cache[string, struct{}]
}

// pinnedEntry is a pinned template plus its outstanding acquisitions. Releases
// carry the entry itself rather than its key: after an Invalidate the key can
// belong to a different instance, and a release must always drop the pin it
// took.
type pinnedEntry struct {
	tmpl Template
	key  string
	// holders carries one timestamp per outstanding acquisition, keyed by a
	// token unique to that acquisition. A refcount beside a single entry-level
	// timestamp cannot answer what the age gauge asks — how long the oldest pin
	// still held has been held — because the first holder's stamp outlives that
	// holder. A build with continuously overlapping sandboxes, which is the case
	// refcounting exists for, would then report an age that only grows, which is
	// exactly what a leaked pin looks like. Restamping the entry on any release
	// is wrong in the other direction: it would hide an older holder still
	// running.
	//
	// A pin never ages out on its own, so the oldest one's age is what separates
	// a busy node from one that is leaking releases.
	holders map[uint64]time.Time
	nextTok uint64
}

// refs is the number of outstanding pins on this entry.
func (e *pinnedEntry) refs() int { return len(e.holders) }

// oldestHold is when the longest-held outstanding pin was taken, or the zero
// time if nothing holds this entry.
func (e *pinnedEntry) oldestHold() time.Time {
	var oldest time.Time

	for _, at := range e.holders {
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}

	return oldest
}

// NewCache initializes a template new cache.
// It also deletes the old build cache directory content
// as it may contain stale data that are not managed by anyone.
func NewCache(
	config cfg.Config,
	flags *featureflags.Client,
	persistence storage.StorageProvider,
	metrics blockmetrics.Metrics,
	peers peerclient.Resolver,
) (*Cache, error) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, Template](templateExpiration),
	)

	c := &Cache{
		pinned:   make(map[string]*pinnedEntry),
		retired:  make(map[*pinnedEntry]struct{}),
		released: ttlcache.New(ttlcache.WithTTL[string, struct{}](templateExpiration), ttlcache.WithDisableTouchOnHit[string, struct{}]()),
	}

	cache.OnEviction(func(ctx context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[string, Template]) {
		c.onEvicted(ctx, item, peers.Purge)
	})

	// Delete the old build cache directory content.
	err := cleanDir(config.DefaultCacheDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove old build cache directory: %w", err)
	}

	buildStore, err := build.NewDiffStore(
		config,
		flags,
		config.DefaultCacheDir,
		buildCacheTTL,
		buildCacheDelayEviction,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create build store: %w", err)
	}

	c.blockMetrics = metrics
	c.config = config
	c.persistence = persistence
	c.buildStore = buildStore
	c.cache = cache
	c.flags = flags
	c.rootCachePath = config.BuilderConfig.SharedChunkCacheDir
	c.peers = peers

	if err := c.registerGauges(); err != nil {
		return nil, fmt.Errorf("failed to register template cache gauges: %w", err)
	}

	return c, nil
}

// onEvicted is the eviction contract for cached templates, shared by NewCache
// and by tests so the guards below are exercised rather than reimplemented.
// purge drops peer chunk routing for the build.
func (c *Cache) onEvicted(ctx context.Context, item *ttlcache.Item[string, Template], purge func(string)) {
	key := item.Key()
	evicted := item.Value()

	// Close is destructive — for the snapfile it is os.RemoveAll — so the
	// question the guard has to answer is whether anything still depends on THIS
	// instance. That is a question about the instance, not about the key:
	// storage.Paths.Cache mints a fresh CacheIdentifier per call, so every
	// storageTemplate for a build owns a private cache directory and closing one
	// instance cannot reach another's files. Guarding by key would get it wrong
	// in both directions — it would skip closing superseded instances, orphaning
	// directories nothing else reclaims, and with one instance pinned and
	// another evicted it would protect the wrong one.
	//
	// ttlcache removes the key from its map before dispatching this callback, so
	// by the time we run the evicted instance is reachable only through a pin or
	// through a release that has put it back. Reading both under extendMu — the
	// lock every admission, every pin and every release is taken under — settles
	// it: either that happened before us and we observe it, or the other side is
	// behind us and will admit and pin its own fresh instance, never this one.
	// Once we have decided to close, nothing can resurrect this instance: it is
	// in neither pin map, and callers only ever admit templates they built
	// themselves.
	c.extendMu.Lock()

	// The release is closing this instance itself and has counted its
	// outcome; the callback records nothing for it.
	if m, ok := evicted.(markable); ok && m.mark().closedDirectly {
		c.extendMu.Unlock()

		return
	}

	cached := c.cache.Get(key, ttlcache.WithDisableTouchOnHit[string, Template]())

	// Reachable again as this same instance, by either route: a pin still holds
	// it, or a release re-admitted it to the cache while this callback was
	// queued. Closing it then would leave the cache serving a template whose
	// snapfile is already gone, and every create off that build would fail to
	// load its snapshot.
	if c.pinHolding(evicted) {
		c.extendMu.Unlock()

		evictionsMetric.Add(ctx, 1, metric.WithAttributes(attrEvictionSkippedPinned))
		logger.L().Info(ctx, "skipping eviction of pinned template",
			zap.String("item_key", key))

		return
	}

	if cached != nil && cached.Value() == evicted {
		c.extendMu.Unlock()

		evictionsMetric.Add(ctx, 1, metric.WithAttributes(attrEvictionSkippedReadmit))
		logger.L().Info(ctx, "skipping eviction of re-admitted template",
			zap.String("item_key", key))

		return
	}

	// Peer chunk routing is keyed by build rather than by instance, so unlike the
	// Close it must survive for as long as any instance for this build does:
	// purging it while a newer or pinned instance is serving reads would degrade
	// them. Decided under the lock so an admission cannot slip in between.
	superseded := c.buildIsPinned(key) || cached != nil
	if !superseded {
		purge(key)
	}

	noteClosedLocked(ctx, item)

	c.extendMu.Unlock()

	// Closed outside the lock on purpose. closeTemplate waits on the template's
	// memfile, rootfs and snapfile futures, each a bare SetOnce.Wait with no
	// context and no deadline, and extendMu is the lock every sandbox create and
	// resume takes. Holding it across that wait would turn an eviction that lands
	// while a fetch or a dedup swap is still in flight into a node-wide
	// create/resume stall, clearable only by restarting the process.
	reason := attrEvictionClosed
	if superseded {
		reason = attrEvictionClosedSuperseded
	}
	evictionsMetric.Add(ctx, 1, metric.WithAttributes(reason))

	if err := evicted.Close(ctx); err != nil {
		logger.L().Warn(ctx, "failed to cleanup template data",
			zap.String("item_key", key), zap.Error(err))
	}
}

func (c *Cache) Start(ctx context.Context) {
	c.buildStore.Start(ctx)

	go c.cache.Start()
	go c.released.Start()
}

func (c *Cache) Stop() {
	c.buildStore.Close()
	c.cache.Stop()
	c.released.Stop()
	c.peers.Close()
}

func (c *Cache) Items() map[string]*ttlcache.Item[string, Template] {
	return c.cache.Items()
}

// pinLocked records a pin on t. The caller must hold extendMu: a pin is only
// valid for the template that is the live cache entry at that moment, and
// extendMu is what serializes that against the eviction callback. Pinning
// outside it can pin an already-Closed template, whose snapfile is gone.
//
// Pins are counted, not boolean — a template is shared by every sandbox started
// from the same build, so a boolean pin would let the first sandbox to exit
// expose templates its siblings are still running on.
//
// The returned release is idempotent, so it is safe to register on several
// cleanup paths (an error rollback and the sandbox lifecycle goroutine) without
// double-decrementing. Every pin must be released: a leaked pin keeps the
// template resident forever, which is a worse leak than the one pinning fixes.
func (c *Cache) pinLocked(ctx context.Context, t Template) func() {
	if t == nil {
		return func() {}
	}

	// Detached from the caller's cancellation but keeping its trace and log
	// context: the release outlives the request that took the pin, and on the
	// retired path it does the Close.
	releaseCtx := context.WithoutCancel(ctx)

	key := t.Files().CacheKey()

	c.pinMu.Lock()
	if c.pinned == nil {
		c.pinned = make(map[string]*pinnedEntry)
	}
	if c.retired == nil {
		c.retired = make(map[*pinnedEntry]struct{})
	}

	e, ok := c.pinned[key]
	if !ok || e.tmpl != t {
		// Normally there is simply nothing pinned for this build: an Invalidate
		// retires the old entry, and the caller arrives holding a freshly built
		// replacement. Finding a *different* instance under the key should not
		// happen, since every mutator of c.pinned holds extendMu and so does this
		// caller — but if it ever did, retire the displaced entry rather than
		// orphan it: its holders' releases still reference it, and the eviction
		// guard consults retired entries.
		if ok {
			c.retired[e] = struct{}{}
		}

		e = &pinnedEntry{tmpl: t, key: key}
		c.pinned[key] = e
	}

	if e.holders == nil {
		e.holders = make(map[uint64]time.Time, 1)
	}

	tok := e.nextTok
	e.nextTok++
	e.holders[tok] = time.Now()
	c.pinMu.Unlock()

	return sync.OnceFunc(func() { c.release(releaseCtx, e, tok) })
}

// release drops one pin from e. On the last release the template rejoins the
// TTL cache if it left while pinned, so it resumes normal expiry and is
// eventually Closed rather than leaking its files — unless it was retired by an
// Invalidate, in which case it is Closed straight away: re-admitting it would
// hand the stale template back to the next lookup.
func (c *Cache) release(ctx context.Context, e *pinnedEntry, tok uint64) {
	// Same lock order as acquisition (extendMu -> pinMu -> cache lock) so the
	// re-admit below cannot interleave with a concurrent acquisition of this key.
	c.extendMu.Lock()

	c.pinMu.Lock()
	if _, held := e.holders[tok]; !held {
		c.pinMu.Unlock()
		c.extendMu.Unlock()

		return
	}

	delete(e.holders, tok)
	if len(e.holders) > 0 {
		c.pinMu.Unlock()
		c.extendMu.Unlock()

		return
	}

	_, retired := c.retired[e]
	delete(c.retired, e)
	if cur, ok := c.pinned[e.key]; ok && cur == e {
		delete(c.pinned, e.key)
	}
	c.pinMu.Unlock()

	if !retired {
		// The last pin on a superseded layer is the edge that usually frees it:
		// no resume can ask for it again, so neither a grace re-admit nor the
		// rest of its TTL would serve anyone.
		if c.releaseSupersededLocked(ctx, e.tmpl) {
			c.extendMu.Unlock()

			return
		}

		// Re-admit only if it is genuinely absent: a live entry under this key
		// must not be replaced, or the template it holds would be dropped without
		// Close. Checked without touching, so probing here cannot extend a live
		// entry's TTL.
		//
		// The grace TTL is deliberately short, not templateExpiration. Reaching
		// here means the entry already aged out of the cache once and was only
		// held back because a sandbox was using it; that sandbox is now gone.
		// Granting it another full 25h would let every pinned template launder
		// its way into a fresh lifetime on release, which is the retention bug
		// this cache already has. The grace exists only so a resume arriving
		// moments later still hits, and so the eventual eviction runs Close and
		// releases the files.
		c.getOrAdmitLocked(e.key, e.tmpl, unpinnedGraceTTL, ttlcache.WithDisableTouchOnHit[string, Template]())

		c.extendMu.Unlock()

		return
	}

	noteGoneLocked(e.tmpl)
	c.extendMu.Unlock()

	// Off this goroutine: closeTemplate waits on the template's futures with no
	// deadline, and the last release usually runs on a sandbox's lifecycle path.
	//
	// An eviction callback for this instance may be queued behind us — Invalidate
	// deletes the cache entry before retiring the pin — and will find nothing
	// holding it and close it too, possibly while this Close is still running.
	// storageTemplate.Close runs its teardown once and makes every other caller
	// wait for that result, so the second close does nothing.
	go func() {
		if err := e.tmpl.Close(ctx); err != nil {
			logger.L().Warn(ctx, "failed to cleanup invalidated template data",
				zap.String("item_key", e.key), zap.Error(err))
		}
	}()
}

// getOrAdmitLocked returns key's live entry, or admits t. ttlcache's GetOrSet
// would overwrite an expired, unswept entry in place without firing
// OnEviction, leaving its template unclosed; deleting it first routes it
// through onEvicted. The caller must hold extendMu: every admission does, and
// writers outside it only sweep expired entries or touch live ones, so the miss
// cannot go stale before the Set.
func (c *Cache) getOrAdmitLocked(key string, t Template, ttl time.Duration, opts ...ttlcache.Option[string, Template]) (*ttlcache.Item[string, Template], bool) {
	if item := c.cache.Get(key, opts...); item != nil {
		return item, true
	}

	c.cache.Delete(key)

	return c.cache.Set(key, t, ttl), false
}

// pinnedTemplate returns the pinned template for key, if any. Retired entries
// are deliberately invisible here: an invalidated build must resolve to a fresh
// instance, not to the one Invalidate was called to discard.
func (c *Cache) pinnedTemplate(key string) (Template, bool) {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	e, ok := c.pinned[key]
	if !ok {
		return nil, false
	}

	return e.tmpl, true
}

// pinHolding reports whether t itself is held by an outstanding pin, retired
// pins included. This is the eviction guard's question: a retired instance is
// unreachable through the cache but a sandbox is still running on it.
func (c *Cache) pinHolding(t Template) bool {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	if e, ok := c.pinned[t.Files().CacheKey()]; ok && e.tmpl == t {
		return true
	}

	for e := range c.retired {
		if e.tmpl == t {
			return true
		}
	}

	return false
}

func (c *Cache) isPinned(key string) bool {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	_, ok := c.pinned[key]

	return ok
}

// buildIsPinned reports whether any instance of this build is pinned, retired
// ones included. It is the question the build-scoped decisions ask — peer chunk
// routing belongs to the build, and a sandbox running on a retired instance
// still reads through it.
func (c *Cache) buildIsPinned(key string) bool {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	if _, ok := c.pinned[key]; ok {
		return true
	}

	for e := range c.retired {
		if e.key == key {
			return true
		}
	}

	return false
}

// retirePinned hides the pin on key from lookups, if there is one. The instance
// stays protected from eviction and is Closed by its last release.
//
// The caller must hold extendMu: retiring an entry a concurrent lookup has
// already read would let that lookup re-pin the same instance under a second
// entry, and the retired entry's last release would then Close a template the
// second one is still holding.
func (c *Cache) retirePinned(key string) {
	c.pinMu.Lock()
	defer c.pinMu.Unlock()

	e, ok := c.pinned[key]
	if !ok {
		return
	}

	delete(c.pinned, key)
	if c.retired == nil {
		c.retired = make(map[*pinnedEntry]struct{})
	}
	c.retired[e] = struct{}{}
}

// registerGauges wires the async observers for cache residency. They are
// observable (pull-based) so the walk over cached templates happens on the
// metrics collection goroutine, never on a sandbox create/resume path.
func (c *Cache) registerGauges() error {
	_, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			f := c.footprint()

			o.ObserveInt64(cacheEntriesGauge, f.entries)
			o.ObserveInt64(cachePinnedGauge, f.pinned)
			o.ObserveInt64(cachePinnedRefsGauge, f.pinnedRefs)
			o.ObserveInt64(cacheOldestPinGauge, f.oldestPinAgeSeconds)
			o.ObserveInt64(cacheMappingEntryGauge, f.mappingEntries)
			o.ObserveInt64(cacheMappingBytesGauge, f.mappingBytes)
			o.ObserveInt64(cacheLayersUnlandedGauge, f.unlandedLayers)

			for _, kind := range layerKinds {
				o.ObserveInt64(cacheLayerEntriesGauge, f.layerEntries[kind], metric.WithAttributes(attribute.String("kind", string(kind))))

				for _, band := range generationBands {
					o.ObserveInt64(cacheLayerMappingBytesGauge, f.layerMappingBytes[layerBand{kind, band}], metric.WithAttributes(
						attribute.String("kind", string(kind)),
						attribute.String("generation_band", string(band)),
					))
				}
			}

			return nil
		},
		cacheEntriesGauge, cachePinnedGauge, cachePinnedRefsGauge, cacheOldestPinGauge,
		cacheMappingEntryGauge, cacheMappingBytesGauge,
		cacheLayerEntriesGauge, cacheLayerMappingBytesGauge, cacheLayersUnlandedGauge,
	)

	return err
}

// cacheFootprint is what the residency gauges report.
//
// pinned and pinnedRefs answer different questions and both are needed. pinned
// counts builds with an outstanding pin, so it sits below the live sandbox
// count whenever several sandboxes share a build and cannot show the size of a
// leak — one stuck ref and a thousand on the same build both read as 1.
// pinnedRefs sums the refs, so it tracks live holders on a busy node, and
// oldestPinAgeSeconds bounds how long any one of them has been held: a pin,
// unlike a cache entry, never ages out by itself.
type cacheFootprint struct {
	entries             int64
	pinned              int64
	pinnedRefs          int64
	oldestPinAgeSeconds int64
	mappingEntries      int64
	mappingBytes        int64

	// layerEntries and layerMappingBytes cut entries and mappingBytes; summed
	// over their keys they equal those two.
	layerEntries      map[layerKind]int64
	layerMappingBytes map[layerBand]int64

	// unlandedLayers counts pause layers whose upload finished with an error.
	// An abandoned upload is not counted: nothing will read that snapshot.
	unlandedLayers int64
}

// footprint walks the resident templates and sums what their headers hold.
// Templates whose devices have not resolved yet contribute nothing rather than
// blocking the collection goroutine on an in-flight fetch.
func (c *Cache) footprint() cacheFootprint {
	f := cacheFootprint{
		layerEntries:      make(map[layerKind]int64, len(layerKinds)),
		layerMappingBytes: make(map[layerBand]int64, len(layerKinds)*len(generationBands)),
	}

	seen := make(map[Template]struct{})

	account := func(t Template) {
		if t == nil {
			return
		}
		if _, dup := seen[t]; dup {
			return
		}
		seen[t] = struct{}{}
		f.entries++

		st, ok := t.(*storageTemplate)
		if !ok {
			f.layerEntries[layerKindFetched]++

			return
		}
		f.layerEntries[st.kind]++
		if st.kind == layerKindPause && UploadOutcome(st.layerMark.upload.Load()) == UploadFailed {
			f.unlandedLayers++
		}

		e, b := st.headerFootprint()
		f.mappingEntries += int64(e)
		f.mappingBytes += int64(b)
		f.layerMappingBytes[layerBand{st.kind, generationBandOf(st.generation())}] += int64(b)
	}

	for _, item := range c.cache.Items() {
		if item == nil {
			continue
		}

		account(item.Value())
	}

	// Pinned templates that left the TTL cache still hold their mappings, and so
	// do retired ones — an Invalidate hides an instance from lookups but the
	// sandbox running on it keeps every byte of it resident.
	var (
		stragglers []Template
		oldest     time.Time
	)

	c.pinMu.Lock()
	f.pinned = int64(len(c.pinned) + len(c.retired))
	accountPin := func(e *pinnedEntry) {
		f.pinnedRefs += int64(e.refs())
		stragglers = append(stragglers, e.tmpl)

		if at := e.oldestHold(); !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
			oldest = at
		}
	}

	for _, e := range c.pinned {
		accountPin(e)
	}

	for e := range c.retired {
		accountPin(e)
	}
	c.pinMu.Unlock()

	for _, t := range stragglers {
		account(t)
	}

	if !oldest.IsZero() {
		f.oldestPinAgeSeconds = int64(time.Since(oldest).Seconds())
	}

	return f
}

// LookupDiff returns the locally-cached diff for the given build and file name.
// Returns (nil, false) if the diff is not cached locally.
func (c *Cache) LookupDiff(buildID string, diffType build.DiffType) (build.Diff, bool) {
	key := build.GetDiffStoreKey(buildID, diffType)

	return c.buildStore.Lookup(key)
}

// Invalidate removes a template from the cache, forcing a refetch on next access.
func (c *Cache) Invalidate(buildID string) {
	// Under extendMu so the delete and the retire are atomic against a lookup.
	// Without it, a lookup that had already read the pin could re-admit and
	// re-pin the same instance behind us: the retired entry and the new pin
	// would then both hold that template, and the retired entry's last release
	// would Close it while the new holder is still running on it.
	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	c.cache.Delete(buildID)

	// A pinned instance survives that Delete — the eviction callback leaves it
	// alone — and lookups consult the pins ahead of the cache, so without this
	// the next lookup would hand back exactly the template Invalidate was
	// called to discard, on a fresh TTL. Retire it instead: its holders keep
	// running on the instance they started on, the next lookup builds and fetches
	// a new one, and the last release Closes the stale instance.
	c.retirePinned(buildID)
}

// InvalidateAll clears all cached templates and build diffs.
// Used for cold start benchmarks to ensure no cached data is reused.
func (c *Cache) InvalidateAll() {
	// One hold for the whole sweep, for the reason Invalidate gives.
	c.extendMu.Lock()

	c.cache.DeleteAll()

	c.pinMu.Lock()
	for key, e := range c.pinned {
		delete(c.pinned, key)
		c.retired[e] = struct{}{}
	}
	c.pinMu.Unlock()

	c.extendMu.Unlock()

	c.buildStore.RemoveCache()
}

// GetTemplateOpts configures optional behavior for GetTemplatePinned.
type GetTemplateOpts struct {
	MaxSandboxLengthHours int64
}

// getTemplateForBuild builds the (uncached) storageTemplate for buildID,
// resolving the persistence stack the flags call for.
func (c *Cache) getTemplateForBuild(
	ctx context.Context,
	buildID string,
	isSnapshot bool,
	isBuilding bool,
) (*storageTemplate, error) {
	ctx, span := tracer.Start(ctx, "get template", trace.WithAttributes(
		attribute.Bool("is_snapshot", isSnapshot),
		attribute.Bool("is_building", isBuilding),
	))
	defer span.End()

	persistence := c.persistence
	// Because of the template caching, if we enable the NFS cache feature flag,
	// it will start working only for new orchestrators or new builds.
	if path, enabled := c.useNFSCache(ctx, isBuilding, isSnapshot); enabled {
		logger.L().Info(ctx, "using local template cache", zap.String("path", c.rootCachePath))
		persistence = storage.WrapInNFSCache(ctx, path, persistence, c.flags)
		span.SetAttributes(attribute.Bool("use_cache", true))
	} else {
		span.SetAttributes(attribute.Bool("use_cache", false))
	}

	// Wrap persistence with per-buildID peer routing.
	// Each layer's buildID is checked against Redis to find the source orchestrator.
	// This allows pulling data directly from the peer before GCS upload completes.
	if c.flags.BoolFlag(ctx, featureflags.PeerToPeerChunkTransferFlag) {
		persistence = peerclient.NewRoutingProvider(persistence, c.peers)
	}

	tmpl, err := newTemplateFromStorage(
		c.config.BuilderConfig,
		buildID,
		resolvedHeader(nil),
		resolvedHeader(nil),
		persistence,
		c.blockMetrics,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create template cache from storage: %w", err)
	}

	return tmpl, nil
}

// GetTemplatePinned returns the template for buildID, fetching it if absent,
// with a pin held for the caller, taken atomically with the cache lookup. It is
// the cache's only acquisition: eviction Closes a template, which deletes its
// snapfile from disk, so every caller that reads a template holds a pin for as
// long as it does, and a pin taken after the lookup could land after that
// decision has been made.
//
// The returned release is idempotent and must be called on every exit path,
// including when an error is returned.
func (c *Cache) GetTemplatePinned(
	ctx context.Context,
	buildID string,
	isSnapshot bool,
	isBuilding bool,
	opts ...GetTemplateOpts,
) (Template, func(), error) {
	tmpl, err := c.getTemplateForBuild(ctx, buildID, isSnapshot, isBuilding)
	if err != nil {
		return nil, func() {}, err
	}

	var maxLen int64
	if len(opts) > 0 {
		maxLen = opts[0].MaxSandboxLengthHours
	}

	t, releases := c.getTemplateWithFetch(ctx, tmpl, maxLen, 1, "")

	return t, releases[0], nil
}

func resolvedHeader(h *header.Header) *utils.SetOnce[*header.Header] {
	s := utils.NewSetOnce[*header.Header]()
	_ = s.SetValue(h)

	return s
}

// UploadOutcome is how a snapshot's own upload ended, reported with the return
// of the template cache pin AddSnapshot took for it.
type UploadOutcome uint32

const (
	// UploadLanded: the snapshot is in storage.
	UploadLanded UploadOutcome = iota + 1
	// UploadFailed: an upload the API already relies on failed for good, so
	// the local entry is the only copy of the snapshot.
	UploadFailed
	// UploadAbandoned: the upload never ran, or failed where nothing will read
	// the snapshot.
	UploadAbandoned
)

func (c *Cache) AddSnapshot(
	ctx context.Context,
	buildId string,
	lineage SnapshotLineage,
	memfileHeader *utils.SetOnce[*header.Header],
	rootfsHeader *utils.SetOnce[*header.Header],
	localSnapfile File,
	localMetafile File,
	memfileDiff build.Diff,
	rootfsDiff build.Diff,
	// provisionalMemfileHeader/Diff: when non-nil the local memfile
	// template is built from the provisional header (which attributes dirty pages
	// to provisionalMemfileDiff's build id at identity offsets) so a concurrent
	// resume serves immediately from the memfd; once memfileHeader (the deduped
	// header) resolves it is swapped in. When nil, the template is built directly
	// from memfileHeader as before. The upload always uses memfileHeader.
	provisionalMemfileHeader *header.Header,
	provisionalMemfileDiff build.Diff,
	// provisionalMemfileCreatedAt, when non-zero, is the provisional header's
	// birth at pause; the swap goroutine records the dedup-duration metric
	// from it.
	provisionalMemfileCreatedAt time.Time,
	// provisionalSwapDone, when non-nil, is called once the deduped header has
	// been swapped in; it lets the dedup goroutine release the memfd the
	// provisional source was serving from.
	provisionalSwapDone func(),
) (finishUpload func(UploadOutcome), err error) {
	switch memfileDiff.(type) {
	case *build.NoDiff:
	default:
		c.buildStore.Add(memfileDiff)
	}
	if provisionalMemfileDiff != nil {
		if _, ok := provisionalMemfileDiff.(*build.NoDiff); !ok {
			// This provisional entry must stay resident until the SwapHeader below
			// (the provisional window). It is keyed by a synthetic build id with no
			// GCS object, so if it were evicted mid-window a resume read routed to
			// it would miss and couldn't be rebuilt (createDiff has nothing to
			// fetch), failing the read. It is pinned below (with the main memfile
			// diff) so disk-pressure eviction skips it for the window; TTL eviction
			// (hours) can't fire within the window (seconds).
			c.buildStore.Add(provisionalMemfileDiff)
		}
	}

	switch rootfsDiff.(type) {
	case *build.NoDiff:
	default:
		c.buildStore.Add(rootfsDiff)
	}

	// A pause produces the provisional header and diff together, so a diff
	// without its header means the caller created the upload first, and
	// NewUpload cleared the header. The deduped header serves correctly, only
	// later; nothing will serve from the memfd, so let dedup release it as
	// soon as the drain finishes rather than after the swap grace.
	if provisionalMemfileHeader == nil && provisionalMemfileDiff != nil {
		logger.L().Error(ctx, "provisional memfile diff without its header: the upload was created before the snapshot was cached",
			logger.WithBuildID(buildId))
		deadStructureOutcomeMetric.Add(ctx, 1, attrUploadProvisionalHeaderMisorder)
		if provisionalSwapDone != nil {
			provisionalSwapDone()
			provisionalSwapDone = nil
		}
	}

	// Build the local template from the provisional header (resolved now) so
	// Memfile() doesn't block on dedup; fall back to the deduped header future.
	// When serving a provisional header, pass the deduped header future as the
	// memfile's durable header so a pause parents off it, never the provisional
	// header (whose synthetic build id has no storage object). It is applied at
	// construction — before the memfile device is published — so no reader can
	// observe the device with the durable header unset.
	localMemfileHeader := memfileHeader
	var durableMemfileHeader *utils.SetOnce[*header.Header]
	if provisionalMemfileHeader != nil {
		localMemfileHeader = resolvedHeader(provisionalMemfileHeader)
		durableMemfileHeader = memfileHeader
	}

	storageTemplate, err := newTemplateFromStorage(
		c.config.BuilderConfig,
		buildId,
		localMemfileHeader,
		rootfsHeader,
		c.persistence,
		c.blockMetrics,
		localSnapfile,
		localMetafile,
		durableMemfileHeader,
	)
	if err != nil {
		// The swap goroutine below (which signals the release) is never spawned on
		// this early-return path, so signal here — otherwise the dedup goroutine
		// holds the provisional memfd for the full swap grace before releasing.
		if provisionalSwapDone != nil {
			provisionalSwapDone()
		}

		return func(UploadOutcome) {}, fmt.Errorf("failed to create template cache from storage: %w", err)
	}
	if provisionalMemfileHeader != nil {
		// Read once, here: Fetch, which acts on it, starts right after.
		storageTemplate.dropProvisionalHeader = c.flags != nil && c.flags.BoolFlag(ctx, featureflags.SnapshotCacheDropProvisionalHeaderFlag)
	}
	storageTemplate.kind = lineage.kind()

	// Use the template that is actually resident in the cache, not the local
	// storageTemplate: on a cache hit getTemplateWithFetch discards the local one
	// (never fetching it, so its memfile future never resolves) and returns the
	// pre-existing entry. The swap goroutine below must call Memfile on the
	// resident template — calling it on the discarded local instance would block
	// forever under swapCtx (no deadline), leaking the goroutine and its pins.
	//
	// Pinned in the same hold as the lookup, once for the snapshot's upload and,
	// when the swap goroutine below will run, once for that goroutine, which
	// keeps reading the template after AddSnapshot returns. The upload's pin is
	// handed to the caller as finishUpload, which the upload calls once, when
	// it stops reading the entry, with how it ended: the outcome is recorded on
	// the entry before the pin is returned, and a layer that did not land is
	// never released, so the entry that is the only copy stays on its TTL.
	//
	// The predecessor this snapshot abandons is marked superseded in the same
	// hold, after this snapshot is published, so a failure inside AddSnapshot
	// leaves it unmarked and resumable. A failure after AddSnapshot returns,
	// such as NewUpload refusing or a fresh checkpoint failing to resume,
	// leaves it marked; that costs at most a refetch, since a marked entry is
	// released only once nothing, its own upload included, holds a pin on it.
	swaps := provisionalMemfileHeader != nil
	pins := 1
	if swaps {
		pins = 2
	}
	cachedTemplate, releases := c.getTemplateWithFetch(ctx, storageTemplate, 0, pins, lineage.supersedes())
	finishUpload = c.uploadFinisher(cachedTemplate, releases[0])
	var releaseSwap func()
	if swaps {
		releaseSwap = releases[1]
	}

	// Swap the provisional header for the deduped one once dedup finishes, so
	// subsequent reads route dirty pages to the (compacted) deduped diff and the
	// provisional memfd source is no longer referenced. The durable header was
	// wired in at construction above. On a cache hit the resident template was
	// built from its own header (not our provisional one), so SwapHeaderIfCurrent
	// below is a safe no-op there.
	if swaps {
		// Pin both the main memfile diff and the provisional diff for the window.
		// They share a DedupedMemfdCache/memfd, but resume reads refresh only the
		// provisional entry, so disk-pressure eviction of either would break the
		// in-flight provisional serve: evicting the main entry Closes it, which
		// cancels dedup and tears down the shared memfd; evicting the provisional
		// entry makes a still-provisional-header read miss the store and fall
		// through to a storage fetch for the synthetic build id (never uploaded).
		// Pinning skips both in disk-pressure eviction (TTL still applies);
		// unpinned after the swap.
		var pinnedKeys []build.DiffStoreKey
		for _, d := range []build.Diff{memfileDiff, provisionalMemfileDiff} {
			if d == nil {
				continue
			}
			if _, isNoDiff := d.(*build.NoDiff); isNoDiff {
				continue
			}
			key := d.CacheKey()
			c.buildStore.Pin(key)
			pinnedKeys = append(pinnedKeys, key)
		}

		swapCtx := context.WithoutCancel(ctx)
		go func() {
			defer releaseSwap()
			// Signal the dedup goroutine on every exit (success or the error
			// returns below) so it releases the memfd promptly. On an error the
			// swap can't happen and the resume is already broken, so nothing needs
			// the memfd kept mapped; without this the dedup goroutine would wait out
			// the full grace before releasing. Unpin the main diff on every exit too.
			if provisionalSwapDone != nil {
				defer provisionalSwapDone()
			}
			defer func() {
				for _, key := range pinnedKeys {
					c.buildStore.Unpin(key)
				}
			}()

			deduped, err := memfileHeader.Wait()
			if err != nil {
				logger.L().Warn(swapCtx, "provisional memfile header swap: deduped header failed", zap.Error(err))

				return
			}
			mem, err := cachedTemplate.Memfile(swapCtx)
			if err != nil {
				logger.L().Warn(swapCtx, "provisional memfile header swap: get memfile", zap.Error(err))

				return
			}
			if mem == nil {
				logger.L().Warn(swapCtx, "provisional memfile header swap: memfile is nil")

				return
			}
			// Swap only if the header is still the provisional one. Upload.publish
			// (and the P2P poll path) install a finalized header unconditionally;
			// if this goroutine runs late we must not clobber that newer header
			// with the older, still-incomplete deduped one. Either way the
			// provisional header is no longer needed, so release + drop below.
			if cas, ok := mem.(interface {
				SwapHeaderIfCurrent(old, next *header.Header) bool
			}); ok {
				if !cas.SwapHeaderIfCurrent(provisionalMemfileHeader, deduped) {
					logger.L().Info(swapCtx, "provisional memfile header swap: header already advanced; skipping")
				}
			} else {
				mem.SwapHeader(deduped)
			}

			if !provisionalMemfileCreatedAt.IsZero() {
				memfileDedupDuration.Record(swapCtx, time.Since(provisionalMemfileCreatedAt).Milliseconds())
			}

			// Reads now route off the provisional build id; the deferred signal
			// above lets the dedup goroutine release the memfd. The provisional
			// store entry is intentionally NOT deleted here: a reader that planned
			// on the provisional header but has not yet hit the store would miss and
			// fall through to a storage fetch for the synthetic build id (never
			// uploaded). It is harmless to leave — no reads route to it post-swap,
			// it reports FileSize 0 so it doesn't skew disk eviction, and the store
			// TTL reclaims it (its Close is a no-op).
		}()
	}

	return finishUpload, nil
}

// LookupPinned returns the template for buildID if it is currently cached,
// with a pin held for the caller, taken in the same extendMu hold as the
// lookup, exactly as GetTemplatePinned takes it. It never admits or fetches:
// ok is false, and release a no-op, when nothing is cached for buildID.
//
// The returned release is idempotent and must be called once the caller stops
// reading the template, including a template handed on to work that outlives
// the call.
func (c *Cache) LookupPinned(ctx context.Context, buildID string) (t Template, release func(), ok bool) {
	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	// Pinned first: a pinned template that was evicted from the TTL cache is
	// still the one live instance for this key.
	t, ok = c.pinnedTemplate(buildID)
	if !ok {
		item := c.cache.Get(buildID)
		if item == nil {
			return nil, func() {}, false
		}
		t = item.Value()
	}

	return t, c.pinLocked(ctx, t), true
}

// UpdateMetadata overwrites the local metadata file for a cached template so that
// subsequent calls to Template.Metadata() on this node return the updated data
// (e.g. with freshly computed prefetch mappings) without requiring a cache
// invalidation or GCS round-trip. The template is pinned for the write.
func (c *Cache) UpdateMetadata(ctx context.Context, buildID string, meta metadata.Template) error {
	t, release, ok := c.LookupPinned(ctx, buildID)
	defer release()
	if !ok {
		return fmt.Errorf("template %q not in cache", buildID)
	}

	return t.UpdateMetadata(meta)
}

func (c *Cache) useNFSCache(ctx context.Context, isBuilding bool, isSnapshot bool) (string, bool) {
	if isBuilding {
		// caching this layer doesn't speed up the next sandbox launch,
		// as the previous template isn't used to load the one that's being built.
		return "", false
	}

	var flagName featureflags.BoolFlag
	if isSnapshot {
		flagName = featureflags.SnapshotFeatureFlag
	} else {
		flagName = featureflags.TemplateFeatureFlag
	}

	useNFSCache := c.flags.BoolFlag(ctx, flagName)
	if useNFSCache {
		if c.rootCachePath == "" {
			logger.L().Warn(ctx, "NFSCache feature flag is enabled but cache path is not set")

			return "", false
		}
	}

	return c.rootCachePath, useNFSCache
}

func cleanDir(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("reading directory contents: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		if err := os.RemoveAll(entryPath); err != nil {
			return fmt.Errorf("removing %q: %w", entryPath, err)
		}
	}

	return nil
}

// getTemplateWithFetch resolves tmpl's build to its live template, admitting and
// fetching tmpl on a miss, and takes pins pins on the result in the same hold.
// It returns exactly pins releases. A non-empty supersedes is marked superseded
// in that hold, after the result is published.
func (c *Cache) getTemplateWithFetch(ctx context.Context, tmpl *storageTemplate, maxSandboxLengthHours int64, pins int, supersedes string) (Template, []func()) {
	ttl := templateExpiration
	if maxSandboxLengthHours > 0 {
		ttl = max(ttl, time.Duration(maxSandboxLengthHours)*time.Hour+templateExpirationBuffer)
	}

	t, found, releases := c.lookupOrAdmitPins(ctx, tmpl.Files().CacheKey(), tmpl, ttl, pins, supersedes)

	if !found {
		missesMetric.Add(ctx, 1, metric.WithAttributes(c.missReason(tmpl)))
		// We don't want to cancel the request if the request was canceled, because it can be used by other templates
		// It's a little bit problematic, because shutdown won't cancel the fetch
		go tmpl.Fetch(context.WithoutCancel(ctx), c.buildStore)

		// Only a pause layer can be superseded, so only its fetch completion is
		// an edge worth watching.
		if tmpl.kind == layerKindPause {
			go c.watchFetch(context.WithoutCancel(ctx), tmpl)
		}

		return t, releases
	}

	hitsMetric.Add(ctx, 1)

	// The candidate lost: a template for this build was already resident, so this
	// instance is dropped without ever being fetched and without ever reaching
	// Close. Its cache directory exists all the same — storage.Paths.Cache
	// creates it eagerly, at construction, before the cache is consulted — and
	// nothing else would reclaim it: the startup sweep covers DefaultCacheDir,
	// while these live under TemplateCacheDir. Left alone it would orphan one
	// empty directory per cache hit, a leak that scales with lookups rather than
	// with templates, on the hottest path there is. Removing it here is safe
	// precisely because the instance was never fetched: the directory is its own,
	// keyed by an identifier no other instance shares, and nothing has written to
	// it.
	if Template(tmpl) != t {
		if err := tmpl.Files().Close(); err != nil {
			logger.L().Warn(ctx, "failed to remove discarded template cache dir",
				zap.String("item_key", tmpl.Files().CacheKey()), zap.Error(err))
		}
	}

	return t, releases
}

// lookupOrAdmit resolves key to the one live template for that build — the
// pinned instance if there is one, otherwise the cached entry, otherwise
// candidate, which it admits — and takes the caller's pin, all under a single
// hold of extendMu. found reports whether an existing template was returned; a
// false means candidate was admitted and still needs fetching.
//
// The single hold is the point of this function, and the reason it exists apart
// from getTemplateWithFetch: pinning what the cache returned, under the lock the
// eviction callback also takes, is what makes the pin safe. Looking up and then
// pinning cannot be fixed by ordering — by the time the caller holds the
// template the eviction decision may already have been taken, and it pins a
// corpse whose snapfile is gone.
func (c *Cache) lookupOrAdmit(ctx context.Context, key string, candidate Template, ttl time.Duration, pin bool) (Template, bool, func()) {
	pins := 0
	if pin {
		pins = 1
	}

	t, found, releases := c.lookupOrAdmitPins(ctx, key, candidate, ttl, pins, "")
	if !pin {
		return t, found, func() {}
	}

	return t, found, releases[0]
}

// lookupOrAdmitPins is lookupOrAdmit taking pins pins, each with its own
// release, all in the one hold. It returns exactly pins releases. A non-empty
// supersedes is marked superseded in the same hold, once key resolves to a live
// template.
func (c *Cache) lookupOrAdmitPins(ctx context.Context, key string, candidate Template, ttl time.Duration, pins int, supersedes string) (Template, bool, []func()) {
	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	// Deferred so it runs after the admission below, still under the lock.
	if supersedes != "" && supersedes != key {
		defer c.markSupersededLocked(ctx, supersedes)
	}

	// A pinned template is authoritative even if it has left the TTL cache.
	// Without this, an eviction while pinned would let the admission below insert
	// the caller's freshly built candidate and Fetch it — producing a second live
	// Template for the same build, with two sets of devices over the same files.
	if pinnedTmpl, ok := c.pinnedTemplate(key); ok {
		// Re-admit it so it is visible to plain cache lookups again. Guarded:
		// never displace a different live entry that took this key meanwhile.
		c.getOrAdmitLocked(key, pinnedTmpl, ttl)

		return pinnedTmpl, true, c.pinNLocked(ctx, pinnedTmpl, pins)
	}

	item, found := c.getOrAdmitLocked(key, candidate, ttl)
	if found && item.TTL() < ttl {
		// Another team with a shorter max length cached this entry; extend it.
		// A plain Set is safe: it writes back the same template.
		c.cache.Set(key, item.Value(), ttl)
	}

	return item.Value(), found, c.pinNLocked(ctx, item.Value(), pins)
}

// pinNLocked takes n pins on t. The caller must hold extendMu.
func (c *Cache) pinNLocked(ctx context.Context, t Template, n int) []func() {
	releases := make([]func(), n)
	for i := range releases {
		releases[i] = c.pinLocked(ctx, t)
	}

	return releases
}
