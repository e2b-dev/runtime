//go:build linux

package sandbox

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	headers "github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// A build this node released has no entry by design, so its fired future
// hands back the future_no_entry verdict whatever the fallback flag reads at
// the wait: the release was decided under the flags as they read then.
func TestUploads_Wait_ReleasedBuildHealsWhateverTheFallback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		ff   *featureflags.Client
	}{
		{name: "fallback off", ff: newFallbackFF(t, false)},
		{name: "nil client"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, cache := newUploads(t)
			c.ff = tc.ff
			id := uuid.New()
			cache.release(id.String())
			firedFuture(t, c, id, nil)

			h, verdict, err := c.Wait(t.Context(), id, build.Memfile)
			require.NoError(t, err)
			require.Nil(t, h)
			require.Equal(t, verdictFutureNoEntry, verdict)
		})
	}
}

// The release record lives as long as the entry it replaced only if every
// lookup that would have touched that entry touches the record instead. So
// each wait that misses the build asks it once, on every branch — including
// the no-future branch and the V3 walk, which fills no Builds map — and a
// wait that finds the entry does not.
func TestUploads_Wait_AsksTheReleaseRecordOnEveryMiss(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		resident bool
		future   bool
		v3Walk   bool
		wantAsks int
	}{
		{name: "fired future, entry gone", future: true, wantAsks: 1},
		{name: "no future", wantAsks: 1},
		{name: "no future, V3 walk", v3Walk: true, wantAsks: 1},
		{name: "resident entry", resident: true, future: true, wantAsks: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, cache := newUploads(t)
			c.p2p = peerclient.NopResolver()
			id := uuid.New()
			if tc.resident {
				putResidentHeader(t, cache, id, ancestorHeader(t, id, headers.MetadataVersionV4, map[uuid.UUID]headers.BuildData{id: framedBuildData()}))
			} else {
				cache.release(id.String())
			}
			if tc.future {
				firedFuture(t, c, id, nil)
			}

			if tc.v3Walk {
				// The walk records a resolution; hold the counter as every
				// test reading it does.
				delta := watchAncestorResolutions(t)
				u := &Upload{buildID: uuid.New(), uploads: c, store: storage.NewMockStorageProvider(t)}
				require.NoError(t, u.appendAncestorBuilds(t.Context(), nil, mappingTo(t, id), build.Memfile))
				assert.Equal(t, map[string]int64{"no_future/none": 1}, delta())
			} else {
				_, _, err := c.Wait(t.Context(), id, build.Memfile)
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantAsks, cache.askCount(id.String()))
		})
	}
}
