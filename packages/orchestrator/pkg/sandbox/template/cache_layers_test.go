//go:build linux

package template

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// layerTestTemplate is a markable cache entry whose devices resolve only when
// a test says so, so every term of the release predicate can be driven by
// hand. Close is counted and, like the real one, would park while a device is
// unresolved: a release that reaches Close on an unresolved entry shows up as
// a close that never finishes.
type layerTestTemplate struct {
	key  string
	kind layerKind
	lm   layerMark

	memfile  *utils.SetOnce[block.ReadonlyDevice]
	rootfs   *utils.SetOnce[block.ReadonlyDevice]
	snapfile *utils.SetOnce[File]

	closes atomic.Int32
	closed chan struct{}
	once   sync.Once
}

// newLayerTestTemplate returns a template whose own upload has landed, so
// only the other predicate terms decide its release.
func newLayerTestTemplate(key string, kind layerKind) *layerTestTemplate {
	f := &layerTestTemplate{
		key:      key,
		kind:     kind,
		memfile:  utils.NewSetOnce[block.ReadonlyDevice](),
		rootfs:   utils.NewSetOnce[block.ReadonlyDevice](),
		snapfile: utils.NewSetOnce[File](),
		closed:   make(chan struct{}),
	}
	f.lm.upload.Store(uint32(UploadLanded))

	return f
}

// resolve resolves every device Close waits on.
func (f *layerTestTemplate) resolve() {
	_ = f.memfile.SetValue(nil)
	_ = f.rootfs.SetValue(nil)
	_ = f.snapfile.SetValue(nil)
}

func (f *layerTestTemplate) Files() storage.CachePaths {
	return storage.CachePaths{Paths: storage.Paths{BuildID: f.key}}
}

func (f *layerTestTemplate) Close(context.Context) error {
	_, _ = f.memfile.Wait()
	_, _ = f.rootfs.Wait()
	_, _ = f.snapfile.Wait()

	f.closes.Add(1)
	f.once.Do(func() { close(f.closed) })

	return nil
}

func (f *layerTestTemplate) Memfile(context.Context) (block.ReadonlyDevice, error) {
	return f.memfile.Wait()
}
func (f *layerTestTemplate) Rootfs() (block.ReadonlyDevice, error)  { return f.rootfs.Wait() }
func (f *layerTestTemplate) Snapfile() (File, error)                { return f.snapfile.Wait() }
func (f *layerTestTemplate) Metadata() (metadata.Template, error)   { return metadata.Template{}, nil }
func (f *layerTestTemplate) UpdateMetadata(metadata.Template) error { return nil }

func (f *layerTestTemplate) layerKind() layerKind { return f.kind }
func (f *layerTestTemplate) mark() *layerMark     { return &f.lm }

func (f *layerTestTemplate) devicesDone() []<-chan struct{} {
	return []<-chan struct{}{f.memfile.Done, f.rootfs.Done, f.snapfile.Done}
}

func (f *layerTestTemplate) devicesResolved() bool {
	for _, done := range f.devicesDone() {
		select {
		case <-done:
		default:
			return false
		}
	}

	return true
}

// waitClosed fails the test unless f is closed within a few seconds. Closes
// run on detached goroutines, from the release or from the eviction callback.
func (f *layerTestTemplate) waitClosed(t *testing.T) {
	t.Helper()

	select {
	case <-f.closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("template %q was not closed", f.key)
	}
}

// assertNotClosed gives a stray close time to land before asserting none did.
func (f *layerTestTemplate) assertNotClosed(t *testing.T) {
	t.Helper()

	select {
	case <-f.closed:
		t.Fatalf("template %q was closed", f.key)
	case <-time.After(50 * time.Millisecond):
	}
}

// releaseFlags returns a flags client with the release flag and the ancestor
// storage fallback set as given.
func releaseFlags(t *testing.T, release, fallback bool) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SnapshotCacheReleaseSupersededFlag.Key()).VariationForAll(release))
	td.Update(td.Flag(featureflags.SnapshotCacheAncestorStorageFallbackFlag.Key()).VariationForAll(fallback))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// newReleaseTestCache is newPinTestCache with flags, started so expiry runs.
func newReleaseTestCache(t *testing.T, ttl time.Duration, release, fallback bool) (*Cache, <-chan string) {
	t.Helper()

	c, evicted := newPinTestCache(ttl)
	c.flags = releaseFlags(t, release, fallback)

	go c.cache.Start()
	t.Cleanup(c.cache.Stop)

	return c, evicted
}

// admit makes tmpl the live entry for its key and returns a pin on it.
func admit(t *testing.T, c *Cache, tmpl Template) func() {
	t.Helper()

	return admitWithTTL(t, c, tmpl, time.Hour)
}

func admitWithTTL(t *testing.T, c *Cache, tmpl Template, ttl time.Duration) func() {
	t.Helper()

	got, _, release := c.lookupOrAdmit(t.Context(), tmpl.Files().CacheKey(), tmpl, ttl, true)
	require.Same(t, tmpl, got)

	return release
}

// supersede takes a pause's mark on key, as AddSnapshot does after publishing
// the successor.
func supersede(t *testing.T, c *Cache, key string) {
	t.Helper()

	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	c.markSupersededLocked(t.Context(), key)
}

func TestRelease_PredicateTable(t *testing.T) {
	t.Parallel()

	for _, superseded := range []bool{false, true} {
		for _, held := range []bool{false, true} {
			for _, resolved := range []bool{false, true} {
				name := fmt.Sprintf("superseded=%t/held=%t/resolved=%t", superseded, held, resolved)
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					c, _ := newReleaseTestCache(t, time.Hour, true, true)
					tmpl := newLayerTestTemplate("layer", layerKindPause)

					// The admission's pin stands in for the sandbox that ran on the
					// layer; held keeps a second one outstanding to the end.
					releaseSandbox := admit(t, c, tmpl)
					releaseHolder := func() {}
					if held {
						releaseHolder = pinForTest(t, c, tmpl)
					}
					defer releaseHolder()

					if resolved {
						tmpl.resolve()
					}
					if superseded {
						supersede(t, c, tmpl.key)
					}
					releaseSandbox()

					if superseded && !held && resolved {
						tmpl.waitClosed(t)
						_, _, ok := c.LookupPinned(t.Context(), tmpl.key)
						assert.False(t, ok, "a released layer must not be served again")

						return
					}

					tmpl.assertNotClosed(t)
					got, releaseGot, ok := c.LookupPinned(t.Context(), tmpl.key)
					defer releaseGot()
					require.True(t, ok, "a layer the predicate refuses stays resident")
					assert.Same(t, tmpl, got)
				})
			}
		}
	}
}

func TestRelease_FiresAtWhicheverEdgeComesLast(t *testing.T) {
	t.Parallel()

	edges := map[string]func(c *Cache, tmpl *layerTestTemplate, releaseSandbox func()){
		"mark last": func(c *Cache, tmpl *layerTestTemplate, releaseSandbox func()) {
			tmpl.resolve()
			releaseSandbox()
			supersede(t, c, tmpl.key)
		},
		"pin return last": func(c *Cache, tmpl *layerTestTemplate, releaseSandbox func()) {
			tmpl.resolve()
			supersede(t, c, tmpl.key)
			releaseSandbox()
		},
		// Nothing but the watcher started with the fetch can see this edge.
		"fetch completion last": func(c *Cache, tmpl *layerTestTemplate, releaseSandbox func()) {
			supersede(t, c, tmpl.key)
			releaseSandbox()
			go c.watchFetch(context.WithoutCancel(t.Context()), tmpl)
			tmpl.resolve()
		},
	}

	for name, drive := range edges {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c, _ := newReleaseTestCache(t, time.Hour, true, true)
			tmpl := newLayerTestTemplate("layer", layerKindPause)

			drive(c, tmpl, admit(t, c, tmpl))

			tmpl.waitClosed(t)
			time.Sleep(50 * time.Millisecond)
			assert.Equal(t, int32(1), tmpl.closes.Load(), "released exactly once")
		})
	}
}

func TestRelease_OnlyWithTheConjunction(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		release, fallback bool
	}{
		{release: false, fallback: false},
		{release: true, fallback: false},
		{release: false, fallback: true},
	} {
		t.Run(fmt.Sprintf("release=%t/fallback=%t", tc.release, tc.fallback), func(t *testing.T) {
			t.Parallel()

			c, _ := newReleaseTestCache(t, time.Hour, tc.release, tc.fallback)
			tmpl := newLayerTestTemplate("layer", layerKindPause)
			tmpl.resolve()

			releaseSandbox := admit(t, c, tmpl)
			supersede(t, c, tmpl.key)
			releaseSandbox()

			tmpl.assertNotClosed(t)

			// The entry keeps main's residency: still served, on its TTL.
			item := c.cache.Get(tmpl.key, ttlcacheNoTouch)
			require.NotNil(t, item, "a superseded layer stays in the TTL cache with the release off")
			assert.Same(t, Template(tmpl), item.Value())

			c.extendMu.Lock()
			suppressed, gone := tmpl.lm.suppressed, tmpl.lm.gone
			c.extendMu.Unlock()
			assert.True(t, suppressed, "the would-release is recorded")
			assert.False(t, gone)
		})
	}
}

func TestRelease_AfterTheFlipAtTheNextLookup(t *testing.T) {
	t.Parallel()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SnapshotCacheReleaseSupersededFlag.Key()).VariationForAll(false))
	td.Update(td.Flag(featureflags.SnapshotCacheAncestorStorageFallbackFlag.Key()).VariationForAll(true))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	c, _ := newPinTestCache(time.Hour)
	c.flags = ff

	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()
	releaseSandbox := admit(t, c, tmpl)
	supersede(t, c, tmpl.key)
	releaseSandbox()
	tmpl.assertNotClosed(t)

	td.Update(td.Flag(featureflags.SnapshotCacheReleaseSupersededFlag.Key()).VariationForAll(true))

	// A descendant pause's ancestor wait looks the layer up and returns its pin.
	_, releaseWait, ok := c.LookupPinned(t.Context(), tmpl.key)
	require.True(t, ok)
	releaseWait()

	tmpl.waitClosed(t)
}

func TestRelease_UnmarkableKindsAreNeverReleased(t *testing.T) {
	t.Parallel()

	for _, kind := range []layerKind{layerKindLocalTemplate, layerKindFetched} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()

			c, _ := newReleaseTestCache(t, time.Hour, true, true)
			tmpl := newLayerTestTemplate("layer", kind)
			tmpl.resolve()

			releaseSandbox := admit(t, c, tmpl)
			supersede(t, c, tmpl.key)
			releaseSandbox()

			tmpl.assertNotClosed(t)
			c.extendMu.Lock()
			assert.False(t, tmpl.lm.superseded, "only a pause layer takes the mark")
			c.extendMu.Unlock()
		})
	}
}

// A release never closes by key: a successor under the same key survives
// whichever way its predecessor left the TTL cache.
func TestRelease_FreesByInstance(t *testing.T) {
	t.Parallel()

	t.Run("in the cache slot", func(t *testing.T) {
		t.Parallel()

		c, evicted := newReleaseTestCache(t, time.Hour, true, true)
		tmpl := newLayerTestTemplate("layer", layerKindPause)
		tmpl.resolve()

		releaseSandbox := admit(t, c, tmpl)
		supersede(t, c, tmpl.key)
		releaseSandbox()

		waitEvicted(t, evicted, tmpl.key)
		tmpl.waitClosed(t)
	})

	t.Run("retired, successor under the key", func(t *testing.T) {
		t.Parallel()

		c, _ := newReleaseTestCache(t, time.Hour, true, true)
		old := newLayerTestTemplate("layer", layerKindPause)
		old.resolve()

		releaseOld := admit(t, c, old)
		supersede(t, c, old.key)

		// A pinned instance keeps its key, so only an Invalidate lets a
		// successor take it while the predecessor is still held.
		c.Invalidate(old.key)
		successor := newLayerTestTemplate("layer", layerKindPause)
		successor.resolve()
		releaseSuccessor := admit(t, c, successor)
		defer releaseSuccessor()

		releaseOld()

		old.waitClosed(t)
		successor.assertNotClosed(t)
		got, releaseGot, ok := c.LookupPinned(t.Context(), successor.key)
		defer releaseGot()
		require.True(t, ok)
		assert.Same(t, Template(successor), got)
	})

	t.Run("left the cache while pinned, key empty", func(t *testing.T) {
		t.Parallel()

		c, evicted := newReleaseTestCache(t, 30*time.Millisecond, true, true)
		tmpl := newLayerTestTemplate("layer", layerKindPause)
		tmpl.resolve()

		releaseSandbox := admitWithTTL(t, c, tmpl, 30*time.Millisecond)
		waitEvicted(t, evicted, tmpl.key) // skipped: pinned
		supersede(t, c, tmpl.key)
		releaseSandbox()

		// Released rather than grace re-admitted.
		tmpl.waitClosed(t)
		assert.Nil(t, c.cache.Get(tmpl.key, ttlcacheNoTouch))
	})
}

func TestRelease_UnresolvedEntryWaitsForItsFetch(t *testing.T) {
	t.Parallel()

	c, _ := newReleaseTestCache(t, time.Hour, true, true)
	tmpl := newLayerTestTemplate("layer", layerKindPause)

	releaseSandbox := admit(t, c, tmpl)
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		c.watchFetch(context.WithoutCancel(t.Context()), tmpl)
	}()
	supersede(t, c, tmpl.key)
	releaseSandbox()

	// Superseded and unheld, but the release must not reach a Close that
	// would park on the unresolved devices.
	tmpl.assertNotClosed(t)

	tmpl.resolve()
	<-watched
	tmpl.waitClosed(t)
}

// The last release racing an acquisition of the same build: the acquirer
// either misses, or holds a pin on an instance nothing closes until that pin
// is back.
func TestRelease_RacesAcquisition(t *testing.T) {
	t.Parallel()

	for i := range 200 {
		c, _ := newPinTestCache(time.Hour)
		c.flags = releaseFlags(t, true, true)

		key := fmt.Sprintf("layer-%d", i)
		tmpl := newLayerTestTemplate(key, layerKindPause)
		tmpl.resolve()

		releaseSandbox := admit(t, c, tmpl)
		supersede(t, c, key)

		var (
			wg      sync.WaitGroup
			got     Template
			release func()
			ok      bool
		)
		wg.Go(func() { got, release, ok = c.LookupPinned(t.Context(), key) })
		wg.Go(releaseSandbox)
		wg.Wait()

		if !ok {
			tmpl.waitClosed(t)

			continue
		}

		require.Same(t, Template(tmpl), got)
		assert.Zero(t, tmpl.closes.Load(), "iteration %d: closed under a pin", i)
		release()
		tmpl.waitClosed(t)
	}
}

func TestInvalidate_SupersededHeldEntry(t *testing.T) {
	t.Parallel()

	c, _ := newReleaseTestCache(t, time.Hour, true, true)
	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()

	releaseSandbox := admit(t, c, tmpl)
	supersede(t, c, tmpl.key)
	c.Invalidate(tmpl.key)

	// The next acquisition misses, and its fresh instance carries no mark.
	fresh := newLayerTestTemplate("layer", layerKindPause)
	got, found, releaseFresh := c.lookupOrAdmit(t.Context(), fresh.key, fresh, time.Hour, true)
	defer releaseFresh()
	require.False(t, found, "an invalidated build must miss")
	require.Same(t, Template(fresh), got)

	tmpl.assertNotClosed(t)
	releaseSandbox()
	tmpl.waitClosed(t)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), tmpl.closes.Load(), "closed once, through the retired path")

	c.extendMu.Lock()
	assert.False(t, fresh.lm.superseded)
	c.extendMu.Unlock()
	fresh.assertNotClosed(t)
}

func TestMarkSuperseded_ResolvesThePinnedInstance(t *testing.T) {
	t.Parallel()

	c, evicted := newReleaseTestCache(t, 30*time.Millisecond, false, false)
	tmpl := newLayerTestTemplate("layer", layerKindPause)
	releaseSandbox := admitWithTTL(t, c, tmpl, 30*time.Millisecond)
	defer releaseSandbox()
	waitEvicted(t, evicted, tmpl.key) // left the TTL cache, still pinned

	supersede(t, c, tmpl.key)

	c.extendMu.Lock()
	defer c.extendMu.Unlock()
	assert.True(t, tmpl.lm.superseded)
}

func TestReleaseEnabled_NilClientIsOff(t *testing.T) {
	t.Parallel()

	assert.False(t, releaseEnabled(t.Context(), nil))
}

func TestSnapshotLineage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		lineage    SnapshotLineage
		kind       layerKind
		supersedes string
	}{
		{"pause", SnapshotLineage{Origin: storage.ObjectOriginPause, Predecessor: "p", AbandonsPredecessor: true}, layerKindPause, "p"},
		{"resume-fresh checkpoint", SnapshotLineage{Origin: storage.ObjectOriginSnapshotTemplate, Predecessor: "p", AbandonsPredecessor: true}, layerKindLocalTemplate, "p"},
		{"in-place checkpoint", SnapshotLineage{Origin: storage.ObjectOriginSnapshotTemplate, Predecessor: "p"}, layerKindLocalTemplate, ""},
		{"template build layer", SnapshotLineage{Origin: storage.ObjectOriginTemplateBuild}, layerKindLocalTemplate, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.kind, tc.lineage.kind())
			assert.Equal(t, tc.supersedes, tc.lineage.supersedes())
		})
	}
}

// The mark goes where the pause says, and only to a layer a pause inserted:
// a sandbox's first pause names the base build it was created from, which was
// fetched, and must not make it releasable.
func TestLookupOrAdmit_MarksOnlyAPausePredecessor(t *testing.T) {
	t.Parallel()

	for _, kind := range layerKinds {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()

			c, _ := newReleaseTestCache(t, time.Hour, false, false)
			pred := newLayerTestTemplate("pred", kind)
			releasePred := admit(t, c, pred)
			defer releasePred()

			succ := newLayerTestTemplate("succ", layerKindPause)
			_, _, releases := c.lookupOrAdmitPins(t.Context(), succ.key, succ, time.Hour, 1, pred.key)
			defer releases[0]()

			c.extendMu.Lock()
			defer c.extendMu.Unlock()
			assert.Equal(t, kind == layerKindPause, pred.lm.superseded)
			assert.False(t, succ.lm.superseded, "the successor itself is never marked")
		})
	}
}

// The storage template's own accessors, which the predicate reads.
func TestStorageTemplate_DevicesResolved(t *testing.T) {
	t.Parallel()

	tmpl, err := newTemplateFromStorage(newDedupTestCache(t).config.BuilderConfig, "b", nil, nil, nil, blockmetrics.Metrics{}, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, layerKindFetched, tmpl.layerKind())
	assert.False(t, tmpl.devicesResolved())

	require.NoError(t, tmpl.memfile.SetValue(nil))
	require.NoError(t, tmpl.rootfs.SetError(errors.New("fetch failed")))
	assert.False(t, tmpl.devicesResolved(), "the snapfile is still pending")

	require.NoError(t, tmpl.snapfile.SetValue(nil))
	assert.False(t, tmpl.devicesResolved(), "the metafile is still pending")

	require.NoError(t, tmpl.metafile.SetError(errors.New("metadata fetch failed")))
	assert.True(t, tmpl.devicesResolved(), "a fetch child resolved to an error counts as resolved")
}

// swapLayerMetrics points the release counters and the miss counter at a
// manual reader for the duration of the test. NOT parallel-safe.
func swapLayerMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	m := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template")

	prev := []metric.Int64Counter{supersededMetric, outcomeMetric, suppressedMetric, missesMetric}
	supersededMetric = utils.Must(m.Int64Counter("orchestrator.templates.cache.snapshot_layers_superseded"))
	outcomeMetric = utils.Must(m.Int64Counter("orchestrator.templates.cache.snapshot_layer_outcome"))
	suppressedMetric = utils.Must(m.Int64Counter("orchestrator.templates.cache.snapshot_layer_release_suppressed"))
	missesMetric = utils.Must(m.Int64Counter("orchestrator.templates.cache.misses"))
	t.Cleanup(func() {
		supersededMetric, outcomeMetric, suppressedMetric, missesMetric = prev[0], prev[1], prev[2], prev[3]
	})

	return reader
}

// counterSum sums name's data points whose attributes include every one of
// attrs.
func counterSum(t *testing.T, reader *sdkmetric.ManualReader, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != name {
				continue
			}
			sum, ok := mt.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				matches := true
				for _, want := range attrs {
					if got, ok := dp.Attributes.Value(want.Key); !ok || got != want.Value {
						matches = false
					}
				}
				if matches {
					total += dp.Value
				}
			}
		}
	}

	return total
}

const (
	supersededName = "orchestrator.templates.cache.snapshot_layers_superseded"
	outcomeName    = "orchestrator.templates.cache.snapshot_layer_outcome"
	suppressedName = "orchestrator.templates.cache.snapshot_layer_release_suppressed"
)

// N forks marking one generation count it once, and a released layer reaches
// one outcome, so released/superseded is a true ratio.
//
//nolint:paralleltest // swaps the package-level release counters
func TestReleaseMetrics_OncePerLayer(t *testing.T) {
	reader := swapLayerMetrics(t)

	c, _ := newReleaseTestCache(t, time.Hour, true, true)
	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()

	releaseSandbox := admit(t, c, tmpl)
	releaseFork := pinForTest(t, c, tmpl)
	for range 3 {
		supersede(t, c, tmpl.key)
	}
	releaseFork()
	releaseSandbox()
	tmpl.waitClosed(t)

	assert.Equal(t, int64(1), counterSum(t, reader, supersededName))
	assert.Equal(t, int64(1), counterSum(t, reader, outcomeName, attribute.String("outcome", "released")))
	assert.Equal(t, int64(0), counterSum(t, reader, outcomeName, attribute.String("outcome", "expired_held")))
	assert.Equal(t, int64(0), counterSum(t, reader, suppressedName))
}

// With the release off, a layer that would release is counted once however
// many edges re-evaluate it, and reaches expired_held when its TTL ends —
// whether the sweeper or the next admission of its key removes it.
//
//nolint:paralleltest // swaps the package-level release counters
func TestReleaseMetrics_SuppressedThenExpiredHeld(t *testing.T) {
	for _, removedBy := range []string{"sweeper", "admission"} {
		t.Run(removedBy, func(t *testing.T) {
			reader := swapLayerMetrics(t)

			c, evicted := newPinTestCache(time.Hour)
			c.flags = releaseFlags(t, false, true)
			if removedBy == "sweeper" {
				go c.cache.Start()
				t.Cleanup(c.cache.Stop)
			}

			tmpl := newLayerTestTemplate("layer", layerKindPause)
			tmpl.resolve()
			// Long enough that the edges below all land before it expires.
			releaseSandbox := admitWithTTL(t, c, tmpl, time.Second)
			supersede(t, c, tmpl.key)
			releaseSandbox()

			// More edges: lookups of the layer, each returning its pin.
			for range 3 {
				if _, release, ok := c.LookupPinned(t.Context(), tmpl.key); ok {
					release()
				}
			}
			assert.Equal(t, int64(1), counterSum(t, reader, suppressedName))

			if removedBy == "admission" {
				time.Sleep(1500 * time.Millisecond)
				fresh := newLayerTestTemplate("layer", layerKindFetched)
				_, found, releaseFresh := c.lookupOrAdmit(t.Context(), fresh.key, fresh, time.Hour, true)
				defer releaseFresh()
				require.False(t, found)
			}

			waitEvicted(t, evicted, tmpl.key)
			tmpl.waitClosed(t)
			assert.Equal(t, int64(1), counterSum(t, reader, outcomeName, attribute.String("outcome", "expired_held")))
			assert.Equal(t, int64(0), counterSum(t, reader, outcomeName, attribute.String("outcome", "released")))
		})
	}
}

// An Invalidate of a live superseded layer frees it, so it reaches no
// outcome at all.
//
//nolint:paralleltest // swaps the package-level release counters
func TestReleaseMetrics_InvalidateIsNotAnOutcome(t *testing.T) {
	reader := swapLayerMetrics(t)

	c, evicted := newReleaseTestCache(t, time.Hour, false, false)
	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()
	releaseSandbox := admit(t, c, tmpl)
	supersede(t, c, tmpl.key)
	releaseSandbox()

	c.Invalidate(tmpl.key)
	waitEvicted(t, evicted, tmpl.key)
	tmpl.waitClosed(t)

	assert.Equal(t, int64(0), counterSum(t, reader, outcomeName))
}

//nolint:paralleltest // swaps the package-level release counters
func TestMissReason(t *testing.T) {
	_ = swapLayerMetrics(t)

	c := newDedupTestCache(t)
	c.released = ttlcache.New(ttlcache.WithTTL[string, struct{}](time.Hour))

	newTmpl := func(buildID string, kind layerKind) *storageTemplate {
		tmpl, err := newTemplateFromStorage(c.config.BuilderConfig, buildID, nil, nil, nil, blockmetrics.Metrics{}, nil, nil, nil)
		require.NoError(t, err)
		tmpl.kind = kind

		return tmpl
	}

	c.released.Set("released", struct{}{}, ttlcache.DefaultTTL)

	assert.Equal(t, attrMissInsert, c.missReason(newTmpl("released", layerKindPause)), "an AddSnapshot insert is an insert even for a released build")
	assert.Equal(t, attrMissInsert, c.missReason(newTmpl("other", layerKindLocalTemplate)))
	assert.Equal(t, attrMissReleasedLayer, c.missReason(newTmpl("released", layerKindFetched)))
	assert.Equal(t, attrMissCold, c.missReason(newTmpl("other", layerKindFetched)))

	c.released = nil
	assert.Equal(t, attrMissCold, c.missReason(newTmpl("released", layerKindFetched)))
}

// The release records the build it freed, which is what labels its refetch.
func TestRelease_RecordsTheReleasedBuild(t *testing.T) {
	t.Parallel()

	c, _ := newReleaseTestCache(t, time.Hour, true, true)
	c.released = ttlcache.New(ttlcache.WithTTL[string, struct{}](time.Hour))

	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()
	releaseSandbox := admit(t, c, tmpl)
	supersede(t, c, tmpl.key)
	releaseSandbox()
	tmpl.waitClosed(t)

	assert.True(t, c.released.Has(tmpl.key))
}

// Summed over their labels, the cut gauges equal the unlabelled ones, and each
// entry lands under its own kind and band.
func TestFootprint_LayerCutsSumToTotals(t *testing.T) {
	t.Parallel()

	c := newDedupTestCache(t)
	c.pinned = make(map[string]*pinnedEntry)
	c.retired = make(map[*pinnedEntry]struct{})

	add := func(buildID string, kind layerKind, generation uint64) *storageTemplate {
		h := mustHeader(t, uuid.New())
		h.Metadata.Generation = generation
		tmpl, err := newTemplateFromStorage(c.config.BuilderConfig, buildID, resolvedHeader(h), nil, nil, blockmetrics.Metrics{}, nil, nil, nil)
		require.NoError(t, err)
		tmpl.kind = kind

		return tmpl
	}

	resident := []*storageTemplate{
		add("pause-shallow", layerKindPause, 3),
		add("pause-deep", layerKindPause, 2500),
		add("template", layerKindLocalTemplate, 600),
		add("fetched", layerKindFetched, 40),
	}
	for _, tmpl := range resident {
		c.cache.Set(tmpl.Files().CacheKey(), tmpl, ttlcache.DefaultTTL)
	}

	pinnedOnly := add("pinned", layerKindPause, 700)
	releasePinned := pinForTest(t, c, pinnedOnly)
	defer releasePinned()
	retired := add("retired", layerKindPause, 10)
	releaseRetired := pinForTest(t, c, retired)
	defer releaseRetired()
	c.retirePinned(retired.Files().CacheKey())

	// Still fetching: counted under its kind, with no bytes.
	fetching, err := newTemplateFromStorage(c.config.BuilderConfig, "fetching", nil, nil, nil, blockmetrics.Metrics{}, nil, nil, nil)
	require.NoError(t, err)
	c.cache.Set("fetching", fetching, ttlcache.DefaultTTL)

	// Only a pause layer whose upload finished with an error is unlanded: an
	// abandoned upload is not counted, and neither is another kind.
	resident[0].layerMark.upload.Store(uint32(UploadFailed))
	resident[1].layerMark.upload.Store(uint32(UploadAbandoned))
	resident[2].layerMark.upload.Store(uint32(UploadFailed))
	pinnedOnly.layerMark.upload.Store(uint32(UploadFailed))

	f := c.footprint()
	assert.Equal(t, int64(2), f.unlandedLayers)

	var entries, bytes int64
	for _, n := range f.layerEntries {
		entries += n
	}
	for _, n := range f.layerMappingBytes {
		bytes += n
	}
	assert.Equal(t, f.entries, entries)
	assert.Equal(t, f.mappingBytes, bytes)
	assert.Equal(t, int64(7), f.entries)

	assert.Equal(t, int64(4), f.layerEntries[layerKindPause])
	assert.Equal(t, int64(1), f.layerEntries[layerKindLocalTemplate])
	assert.Equal(t, int64(2), f.layerEntries[layerKindFetched])

	for _, tc := range []struct {
		tmpl *storageTemplate
		band generationBand
	}{
		{resident[0], generationBandLT20},
		{resident[1], generationBandGTE2000},
		{resident[2], generationBand500To2k},
		{resident[3], generationBand20To500},
	} {
		_, b := tc.tmpl.headerFootprint()
		require.Positive(t, b)
		assert.GreaterOrEqual(t, f.layerMappingBytes[layerBand{tc.tmpl.kind, tc.band}], int64(b), "%s", tc.tmpl.Files().CacheKey())
	}
}

func TestGenerationBandOf(t *testing.T) {
	t.Parallel()

	for gen, want := range map[uint64]generationBand{
		0: generationBandLT20, 19: generationBandLT20,
		20: generationBand20To500, 499: generationBand20To500,
		500: generationBand500To2k, 1999: generationBand500To2k,
		2000: generationBandGTE2000, 7537: generationBandGTE2000,
	} {
		assert.Equal(t, want, generationBandOf(gen), "generation %d", gen)
	}
}

// A released layer is gone: a later edge that re-evaluates it, such as its
// fetch watcher waking after a pin's return already freed it, finds nothing
// to do.
func TestRelease_IsFinal(t *testing.T) {
	t.Parallel()

	c, _ := newReleaseTestCache(t, time.Hour, true, true)
	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()

	releaseSandbox := admit(t, c, tmpl)
	supersede(t, c, tmpl.key)
	releaseSandbox()
	tmpl.waitClosed(t)

	c.extendMu.Lock()
	again := c.releaseSupersededLocked(t.Context(), tmpl)
	c.extendMu.Unlock()
	assert.False(t, again)
}

// A layer whose own upload has not landed is never released, whatever else
// holds: the entry is then the only copy of the snapshot. It stays on its TTL
// as before.
func TestRelease_RequiresTheUploadToLand(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		outcome      UploadOutcome
		wantReleased bool
	}{
		{name: "upload still running", outcome: 0},
		{name: "upload failed", outcome: UploadFailed},
		{name: "upload abandoned", outcome: UploadAbandoned},
		{name: "upload landed", outcome: UploadLanded, wantReleased: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newReleaseTestCache(t, time.Hour, true, true)
			tmpl := newLayerTestTemplate("layer", layerKindPause)
			tmpl.lm.upload.Store(uint32(tc.outcome))
			tmpl.resolve()

			releaseSandbox := admit(t, c, tmpl)
			supersede(t, c, tmpl.key)
			releaseSandbox()

			if tc.wantReleased {
				tmpl.waitClosed(t)

				return
			}
			assert.NotNil(t, c.cache.Get(tmpl.key, ttlcacheNoTouch), "an unlanded layer stays resident")
			tmpl.assertNotClosed(t)
		})
	}
}

// A superseded layer whose upload failed reaches expired_held at the end of
// its TTL and is never released.
//
//nolint:paralleltest // swaps the package-level layer metrics
func TestReleaseMetrics_UnlandedLayerExpiresHeld(t *testing.T) {
	reader := swapLayerMetrics(t)

	c, evicted := newPinTestCache(time.Hour) // no sweeper: the test sweeps
	c.flags = releaseFlags(t, true, true)

	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.lm.upload.Store(uint32(UploadFailed))
	tmpl.resolve()
	// Long enough that the pin returns before it expires.
	releaseSandbox := admitWithTTL(t, c, tmpl, time.Second)
	supersede(t, c, tmpl.key)
	releaseSandbox()
	tmpl.assertNotClosed(t)

	time.Sleep(1500 * time.Millisecond)
	c.cache.DeleteExpired()
	waitEvicted(t, evicted, tmpl.key)
	tmpl.waitClosed(t)

	assert.Equal(t, int64(0), counterSum(t, reader, outcomeName, attribute.String("outcome", "released")))
	assert.Equal(t, int64(1), counterSum(t, reader, outcomeName, attribute.String("outcome", "expired_held")))
}

// The release deletes an entry while holding extendMu, and the eviction
// callback takes extendMu, so the release relies on ttlcache running eviction
// callbacks off the deleting goroutine. A ttlcache that ran them inline would
// deadlock every release that deletes; this fails first.
func TestEvictionCallbacksRunOffTheDeletingGoroutine(t *testing.T) {
	t.Parallel()

	c, evicted := newPinTestCache(time.Hour)
	tmpl := newLayerTestTemplate("layer", layerKindFetched)
	tmpl.resolve()
	c.cache.Set(tmpl.key, tmpl, time.Hour)

	deleted := make(chan struct{})
	go func() {
		c.extendMu.Lock()
		c.cache.Delete(tmpl.key)
		c.extendMu.Unlock()
		close(deleted)
	}()

	select {
	case <-deleted:
	case <-time.After(5 * time.Second):
		t.Fatal("a delete under extendMu blocked on its eviction callback")
	}
	waitEvicted(t, evicted, tmpl.key)
}

// A layer in storage stays in storage: a second upload of the same build,
// finishing later with a worse outcome, does not take the landed mark back.
func TestUploadFinisher_LandedIsNotUndone(t *testing.T) {
	t.Parallel()

	for _, later := range []UploadOutcome{UploadFailed, UploadAbandoned} {
		t.Run(fmt.Sprint(later), func(t *testing.T) {
			t.Parallel()

			c, _ := newReleaseTestCache(t, time.Hour, true, true)
			tmpl := newLayerTestTemplate("layer", layerKindPause)
			tmpl.lm.upload.Store(0)
			releaseSandbox := admit(t, c, tmpl)
			defer releaseSandbox()

			c.uploadFinisher(tmpl, pinForTest(t, c, tmpl))(UploadLanded)
			c.uploadFinisher(tmpl, pinForTest(t, c, tmpl))(later)
			assert.True(t, tmpl.lm.landed())
		})
	}
}

// A layer released after its TTL ran out while it was pinned is closed by the
// release, which also deletes the expired item from its slot; the eviction
// callback that follows neither counts nor closes it. The outcome is
// released, once, and the template is closed once.
//
//nolint:paralleltest // swaps the package-level layer metrics
func TestReleaseMetrics_ReleasedAfterExpiryIsNotAlsoExpiredHeld(t *testing.T) {
	reader := swapLayerMetrics(t)

	c, evicted := newPinTestCache(time.Hour) // no sweeper
	c.flags = releaseFlags(t, true, true)

	tmpl := newLayerTestTemplate("layer", layerKindPause)
	tmpl.resolve()
	releaseSandbox := admitWithTTL(t, c, tmpl, 30*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	supersede(t, c, tmpl.key)
	releaseSandbox()
	tmpl.waitClosed(t)

	waitEvicted(t, evicted, tmpl.key)
	c.cache.DeleteExpired()

	assert.Equal(t, int64(1), counterSum(t, reader, outcomeName, attribute.String("outcome", "released")))
	assert.Equal(t, int64(0), counterSum(t, reader, outcomeName, attribute.String("outcome", "expired_held")))
	assert.Equal(t, int32(1), tmpl.closes.Load(), "the release closed it, and the callback did not close it again")
}

// pathFile is a local snapshot file that needs no storage.
type pathFile string

func (f pathFile) Path() string { return string(f) }
func (pathFile) Close() error   { return nil }

// A pause layer admitted through AddSnapshot, superseded and unpinned before
// its devices resolve, has no pin or mark left to free it: the watcher the
// admission starts is the edge that does, once the fetch completes.
func TestAddSnapshot_FetchCompletionReleasesASupersededLayer(t *testing.T) {
	t.Parallel()

	c := newDedupTestCache(t)
	c.pinned = make(map[string]*pinnedEntry)
	c.retired = make(map[*pinnedEntry]struct{})
	c.flags = releaseFlags(t, true, true)
	buildID := uuid.NewString()

	// Header futures the test resolves: until then the memfile and rootfs
	// devices are unresolved. Resolving them with an error completes the fetch
	// without touching storage.
	memfileHeader := utils.NewSetOnce[*header.Header]()
	rootfsHeader := utils.NewSetOnce[*header.Header]()

	finishUpload, err := c.AddSnapshot(t.Context(), buildID, SnapshotLineage{Origin: storage.ObjectOriginPause},
		memfileHeader, rootfsHeader,
		pathFile(filepath.Join(t.TempDir(), "snapfile")), pathFile(filepath.Join(t.TempDir(), "metadata.json")),
		&build.NoDiff{}, &build.NoDiff{},
		nil, nil,
		time.Time{},
		nil,
	)
	require.NoError(t, err)

	finishUpload(UploadLanded)
	supersede(t, c, buildID)
	require.NotNil(t, c.cache.Get(buildID, ttlcacheNoTouch), "unresolved devices hold the release back")

	require.NoError(t, memfileHeader.SetError(errors.New("no memfile")))
	require.NoError(t, rootfsHeader.SetError(errors.New("no rootfs")))

	require.Eventually(t, func() bool { return c.cache.Get(buildID, ttlcacheNoTouch) == nil }, 5*time.Second, time.Millisecond,
		"the fetch's completion releases the layer")
}

// The release tombstone lives as long as the released entry would have: it
// takes the entry's TTL, and each WasReleased refreshes it as a lookup of the
// resident entry would have refreshed that TTL.
func TestWasReleased(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var c *Cache
		assert.False(t, c.WasReleased("layer"))
	})

	// Asserted on the record's TTL and expiry rather than by outwaiting them,
	// so a slow runner cannot fail it.
	t.Run("takes the entry's TTL and is refreshed at each ask", func(t *testing.T) {
		t.Parallel()

		const entryTTL = time.Hour
		c, _ := newReleaseTestCache(t, time.Minute, true, true)
		c.released = ttlcache.New(ttlcache.WithTTL[string, struct{}](time.Minute), ttlcache.WithDisableTouchOnHit[string, struct{}]())

		tmpl := newLayerTestTemplate("layer", layerKindPause)
		tmpl.resolve()
		releaseSandbox := admitWithTTL(t, c, tmpl, entryTTL)
		assert.False(t, c.WasReleased(tmpl.key), "not released yet")

		supersede(t, c, tmpl.key)
		releaseSandbox()
		tmpl.waitClosed(t)

		record := c.released.Get(tmpl.key, ttlcache.WithDisableTouchOnHit[string, struct{}]())
		require.NotNil(t, record)
		assert.Equal(t, entryTTL, record.TTL(), "the entry's TTL, not the record cache's default")
		before := record.ExpiresAt()

		time.Sleep(5 * time.Millisecond)
		require.True(t, c.WasReleased(tmpl.key))
		after := c.released.Get(tmpl.key, ttlcache.WithDisableTouchOnHit[string, struct{}]()).ExpiresAt()
		assert.True(t, after.After(before), "an ask extends it as a lookup of the entry would have")
	})

	t.Run("ends once the entry would have expired", func(t *testing.T) {
		t.Parallel()

		const entryTTL = 20 * time.Millisecond
		c, _ := newReleaseTestCache(t, time.Hour, true, true)
		c.released = ttlcache.New(ttlcache.WithTTL[string, struct{}](time.Hour), ttlcache.WithDisableTouchOnHit[string, struct{}]())

		tmpl := newLayerTestTemplate("layer", layerKindPause)
		tmpl.resolve()
		releaseSandbox := admitWithTTL(t, c, tmpl, time.Hour)
		supersede(t, c, tmpl.key)
		releaseSandbox()
		tmpl.waitClosed(t)
		c.released.Set(tmpl.key, struct{}{}, entryTTL)

		time.Sleep(10 * entryTTL)
		assert.False(t, c.WasReleased(tmpl.key))
	})
}
