//go:build linux

package template

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// newSeamCache is a Cache that admits real storage templates through
// AddSnapshot and GetTemplatePinned, with no storage behind it.
func newSeamCache(t *testing.T, release, fallback bool) *Cache {
	t.Helper()

	c := newDedupTestCache(t)
	c.pinned = make(map[string]*pinnedEntry)
	c.retired = make(map[*pinnedEntry]struct{})
	c.flags = releaseFlags(t, release, fallback)

	return c
}

// pauseInto publishes a pause's layer the way the pause path does, with
// headers that resolve at once without a storage read, and returns the
// upload's finish.
func pauseInto(t *testing.T, c *Cache, buildID string, lineage SnapshotLineage) func(UploadOutcome) {
	t.Helper()

	failed := func() *utils.SetOnce[*header.Header] {
		s := utils.NewSetOnce[*header.Header]()
		require.NoError(t, s.SetError(errors.New("no header in this test")))

		return s
	}

	finishUpload, err := c.AddSnapshot(t.Context(), buildID, lineage,
		failed(), failed(),
		pathFile(filepath.Join(t.TempDir(), "snapfile")), pathFile(filepath.Join(t.TempDir(), "metadata.json")),
		&build.NoDiff{}, &build.NoDiff{},
		nil, nil,
		time.Time{},
		nil,
	)
	require.NoError(t, err)

	return finishUpload
}

func resident(c *Cache, key string) bool {
	c.extendMu.Lock()
	defer c.extendMu.Unlock()

	if _, ok := c.pinnedTemplate(key); ok {
		return true
	}

	return c.cache.Get(key, ttlcacheNoTouch) != nil
}

// One generation's life across the calls the orchestrator makes: a pause
// publishes gen1, a resume pins it for its sandbox, the next pause publishes
// gen2 naming gen1, and the sandbox's lifecycle ends by returning its pin.
// That return is the edge that frees gen1, and only when the successor
// abandons it and both flags are on. gen2 stays resident either way.
func TestPauseResumePause_ReleasesTheAbandonedGeneration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		origin       storage.ObjectOrigin
		release      bool
		abandons     bool
		wantReleased bool
	}{
		{name: "pause with the release on", origin: storage.ObjectOriginPause, release: true, abandons: true, wantReleased: true},
		{name: "pause with the release off", origin: storage.ObjectOriginPause, release: false, abandons: true},
		{name: "in-place checkpoint", origin: storage.ObjectOriginSnapshotTemplate, release: true, abandons: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newSeamCache(t, tc.release, true)
			gen1, gen2 := uuid.NewString(), uuid.NewString()

			// The first pause, and its upload landing.
			pauseInto(t, c, gen1, SnapshotLineage{Origin: storage.ObjectOriginPause, Predecessor: uuid.NewString(), AbandonsPredecessor: true})(UploadLanded)

			// The resume: the sandbox pins gen1 for as long as it runs.
			tmpl, releaseSandbox, err := c.GetTemplatePinned(t.Context(), gen1, true, false)
			require.NoError(t, err)
			require.Equal(t, gen1, tmpl.Files().CacheKey())
			require.Eventually(t, func() bool {
				st, ok := tmpl.(*storageTemplate)

				return ok && st.devicesResolved()
			}, 5*time.Second, time.Millisecond)

			// The next pause names gen1; its upload is still in flight.
			finishUpload2 := pauseInto(t, c, gen2, SnapshotLineage{Origin: tc.origin, Predecessor: gen1, AbandonsPredecessor: tc.abandons})
			assert.True(t, resident(c, gen1), "the running sandbox still holds gen1")

			// The sandbox's lifecycle ends.
			releaseSandbox()
			assert.Equal(t, !tc.wantReleased, resident(c, gen1))

			finishUpload2(UploadLanded)
			assert.True(t, resident(c, gen2), "the new generation is never superseded by its own upload")
		})
	}
}

// The upload's finish records its outcome before it returns the pin, so the
// return of the last pin on a superseded, resolved layer releases it at that
// return and not later. A failed upload's return leaves it resident.
func TestAddSnapshot_UploadFinishIsTheReleaseEdge(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		outcome      UploadOutcome
		wantResident bool
	}{
		{name: "landed", outcome: UploadLanded, wantResident: false},
		{name: "failed", outcome: UploadFailed, wantResident: true},
		{name: "abandoned", outcome: UploadAbandoned, wantResident: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newSeamCache(t, true, true)
			buildID := uuid.NewString()
			finishUpload := pauseInto(t, c, buildID, SnapshotLineage{Origin: storage.ObjectOriginPause})
			supersede(t, c, buildID)
			require.Eventually(t, func() bool {
				c.extendMu.Lock()
				defer c.extendMu.Unlock()
				item := c.cache.Get(buildID, ttlcacheNoTouch)

				return item != nil && item.Value().(markable).devicesResolved()
			}, 5*time.Second, time.Millisecond)
			require.True(t, resident(c, buildID), "the running upload holds the layer")

			finishUpload(tc.outcome)
			assert.Equal(t, tc.wantResident, resident(c, buildID))
		})
	}
}
