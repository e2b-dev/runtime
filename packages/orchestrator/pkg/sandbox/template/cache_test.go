//go:build linux

package template

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func newTestCache(defaultTTL time.Duration) *Cache {
	return &Cache{
		cache: ttlcache.New(ttlcache.WithTTL[string, Template](defaultTTL)),
	}
}

// simulateGetTemplate runs getTemplateWithFetch's TTL computation and admission
// without needing a full storageTemplate (which requires disk paths).
func simulateGetTemplate(c *Cache, key string, maxSandboxLengthHours int64) {
	ttl := templateExpiration
	if maxSandboxLengthHours > 0 {
		ttl = max(ttl, time.Duration(maxSandboxLengthHours)*time.Hour+templateExpirationBuffer)
	}

	_, _, release := c.lookupOrAdmit(context.Background(), key, nil, ttl, false)
	release()
}

func TestGetTemplate_ExtendsTTL(t *testing.T) {
	t.Parallel()

	defaultTTL := 50 * time.Millisecond
	c := newTestCache(defaultTTL)
	go c.cache.Start()
	defer c.cache.Stop()

	key := "build-long-running"
	c.cache.Set(key, nil, defaultTTL)

	simulateGetTemplate(c, key, 168)

	item := c.cache.Get(key)
	require.NotNil(t, item)
	assert.Equal(t, 168*time.Hour+templateExpirationBuffer, item.TTL())

	time.Sleep(defaultTTL + 20*time.Millisecond)

	item = c.cache.Get(key)
	assert.NotNil(t, item, "entry must survive past the original default TTL")
}

func TestGetTemplate_NeverShortens(t *testing.T) {
	t.Parallel()

	c := newTestCache(time.Hour)
	key := "build-shared"

	simulateGetTemplate(c, key, 168)

	item := c.cache.Get(key)
	require.NotNil(t, item)
	longTTL := item.TTL()

	simulateGetTemplate(c, key, 24)

	item = c.cache.Get(key)
	require.NotNil(t, item)
	assert.Equal(t, longTTL, item.TTL(), "TTL must not decrease when a shorter team accesses the template")
}

func TestGetTemplate_HitRestartsExpiry(t *testing.T) {
	t.Parallel()

	c := newTestCache(time.Hour)
	key := "build-touch"
	before := c.cache.Set(key, nil, time.Hour).ExpiresAt()

	time.Sleep(5 * time.Millisecond)

	_, found, release := c.lookupOrAdmit(t.Context(), key, nil, time.Hour, false)
	release()
	require.True(t, found)

	item := c.cache.Get(key, ttlcache.WithDisableTouchOnHit[string, Template]())
	require.NotNil(t, item)
	assert.True(t, item.ExpiresAt().After(before), "a hit must restart the entry's expiry")
}

func TestGetTemplate_DefaultTTLForZero(t *testing.T) {
	t.Parallel()

	c := newTestCache(time.Hour)
	key := "build-default"

	simulateGetTemplate(c, key, 0)

	item := c.cache.Get(key)
	require.NotNil(t, item)
	assert.Equal(t, templateExpiration, item.TTL())
}

func TestGetTemplate_SetDoesNotTriggerOnEviction(t *testing.T) {
	t.Parallel()

	inner := ttlcache.New(ttlcache.WithTTL[string, Template](time.Hour))

	evicted := false
	inner.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, _ *ttlcache.Item[string, Template]) {
		evicted = true
	})

	c := &Cache{cache: inner}

	key := "build-1"
	c.cache.Set(key, nil, ttlcache.DefaultTTL)
	simulateGetTemplate(c, key, 168)

	assert.False(t, evicted, "Set() on existing key must NOT trigger OnEviction")

	item := c.cache.Get(key)
	require.NotNil(t, item)
	assert.Equal(t, 168*time.Hour+templateExpirationBuffer, item.TTL())
}

func TestWithoutExtend_EntryEvictedEarly(t *testing.T) {
	t.Parallel()

	defaultTTL := 50 * time.Millisecond
	c := newTestCache(defaultTTL)
	go c.cache.Start()
	defer c.cache.Stop()

	key := "build-will-expire"
	c.cache.Set(key, nil, defaultTTL)

	time.Sleep(defaultTTL + 30*time.Millisecond)

	item := c.cache.Get(key)
	assert.Nil(t, item, "without TTL extension, the entry should be evicted after the default TTL")
}

func newDropProvisionalFlags(t *testing.T, on bool) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SnapshotCacheDropProvisionalHeaderFlag.Key()).VariationForAll(on))
	flags, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

	return flags, td
}

// addTestSnapshot caches one pause under buildID. With provisionalDiff set it
// passes a provisional diff and a swap callback, and with provisionalHeader
// also the provisional header, as a pause does; a diff without its header is
// what a caller that created the upload first would pass. It returns the
// deduped header and a channel closed once the swap callback has run.
func addTestSnapshot(t *testing.T, c *Cache, buildID string, provisionalDiff, provisionalHeader bool) (*header.Header, <-chan struct{}) {
	t.Helper()

	deduped := mustHeader(t, uuid.New())
	var provisionalHdr *header.Header
	var diff build.Diff
	swapDone := make(chan struct{})
	var onSwap func()
	if provisionalDiff {
		diff = &build.NoDiff{}
		onSwap = func() { close(swapDone) }
	}
	if provisionalHeader {
		provisionalHdr = mustHeader(t, uuid.New())
		provisionalHdr.IncompletePendingUpload = true
	}

	dir := t.TempDir()
	finishUpload, err := c.AddSnapshot(t.Context(), buildID, SnapshotLineage{Origin: storage.ObjectOriginPause},
		resolvedHeader(deduped), resolvedHeader(mustHeader(t, uuid.New())),
		&countingFile{path: filepath.Join(dir, "snapfile")}, &countingFile{path: filepath.Join(dir, "metadata.json")},
		&build.NoDiff{}, &build.NoDiff{},
		provisionalHdr, diff,
		time.Time{},
		onSwap,
	)
	require.NoError(t, err)
	finishUpload(UploadLanded)

	return deduped, swapDone
}

// onlyTemplate returns the cache's only template once its memfile device
// is published, which Fetch's drop precedes.
func onlyTemplate(t *testing.T, c *Cache) *storageTemplate {
	t.Helper()

	items := c.cache.Items()
	require.Len(t, items, 1)
	var tmpl *storageTemplate
	for _, item := range items {
		tmpl = item.Value().(*storageTemplate)
	}
	_, err := tmpl.Memfile(t.Context())
	require.NoError(t, err)

	return tmpl
}

func waitSwap(t *testing.T, swapDone <-chan struct{}) {
	t.Helper()

	select {
	case <-swapDone:
	case <-time.After(15 * time.Second):
		t.Fatal("swap goroutine did not complete")
	}
}

// AddSnapshot gives the drop only to a template it builds from a provisional
// header; on every other template the holder resolves to nil or to the header
// its device is built from. The flag is read per template, so a flip reaches
// the next pause without a redeploy. With the flag on the device still ends up
// on the deduped header once the swap lands.
func TestAddSnapshot_DropsProvisionalHolderPerFlag(t *testing.T) {
	t.Parallel()

	flags, td := newDropProvisionalFlags(t, true)

	addSnapshot := func(t *testing.T, provisional bool) (*storageTemplate, *header.Header) {
		t.Helper()

		c := newDedupTestCache(t)
		c.flags = flags
		deduped, swapDone := addTestSnapshot(t, c, uuid.NewString(), provisional, provisional)
		if provisional {
			waitSwap(t, swapDone)
		}

		return onlyTemplate(t, c), deduped
	}

	tmpl, deduped := addSnapshot(t, true)
	assert.Nil(t, tmpl.memfileHeader.Load(), "a provisional template drops its holder with the flag on")
	mem, err := tmpl.Memfile(t.Context())
	require.NoError(t, err)
	assert.Same(t, deduped, mem.Header(), "the swap still moves the device to the deduped header")

	tmpl, _ = addSnapshot(t, false)
	assert.NotNil(t, tmpl.memfileHeader.Load(), "a template not built from a provisional header keeps its holder")

	td.Update(td.Flag(featureflags.SnapshotCacheDropProvisionalHeaderFlag.Key()).VariationForAll(false))
	tmpl, _ = addSnapshot(t, true)
	assert.NotNil(t, tmpl.memfileHeader.Load(), "the next pause after a flip to off keeps its holder")
}

// A provisional snapshot for a build that is already resident is discarded
// without a Fetch, so its template records nothing.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestAddSnapshot_ResidentEntryDiscardsProvisionalCandidate(t *testing.T) {
	reader := swapDeadStructureMetrics(t)
	flags, _ := newDropProvisionalFlags(t, true)
	c := newDedupTestCache(t)
	c.flags = flags

	buildID := uuid.NewString()
	addTestSnapshot(t, c, buildID, false, false)
	resident := onlyTemplate(t, c)

	_, swapDone := addTestSnapshot(t, c, buildID, true, true)
	waitSwap(t, swapDone)

	assert.Same(t, resident, onlyTemplate(t, c))
	outcomes, bytes := deadStructureTotals(t, reader)
	assert.Equal(t, map[string]int64{"template_provisional_header/none": 1}, outcomes,
		"only the resident template's own Fetch records an outcome")
	assert.Empty(t, bytes)
}

// A provisional diff without its header means the upload was created before
// the snapshot was cached, and NewUpload cleared the header. AddSnapshot
// builds from the deduped header, counts the misordered caller, and signals
// the swap at once so dedup does not hold the memfd for the swap grace.
//
//nolint:paralleltest // swaps the package-level dead-structure counters
func TestAddSnapshot_CountsMisorderedUpload(t *testing.T) {
	reader := swapDeadStructureMetrics(t)
	flags, _ := newDropProvisionalFlags(t, true)
	c := newDedupTestCache(t)
	c.flags = flags

	deduped, swapDone := addTestSnapshot(t, c, uuid.NewString(), true, false)
	select {
	case <-swapDone:
	default:
		t.Fatal("AddSnapshot returned without signalling the swap")
	}

	tmpl := onlyTemplate(t, c)
	mem, err := tmpl.Memfile(t.Context())
	require.NoError(t, err)
	assert.Same(t, deduped, mem.Header(), "the template is built from the deduped header")
	outcomes, _ := deadStructureTotals(t, reader)
	assert.Equal(t, map[string]int64{
		"upload_provisional_header/misordered": 1,
		"template_provisional_header/none":     1,
	}, outcomes)
}
