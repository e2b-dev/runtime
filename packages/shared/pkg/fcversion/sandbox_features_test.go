package fcversion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2BSnapshotFeatures pins the per-feature release floors: the in-place
// checkpoint needs 0.2.0 (the balloon reporting-pause API), filesystem-only
// snapshots ship with every e2b release from 0.1.0 — production 0.1.x fleets
// run them live, so the floors must not be coupled. Legacy _hash builds and
// bare upstream versions never qualify for either — the version string is
// the support contract, not a capability probe, so a dev build that happens
// to carry the endpoints must still be refused.
func TestE2BSnapshotFeatures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		version string
		inPlace bool
		fsOnly  bool
	}{
		{"v1.14-0.2.0", true, true},
		{"v1.14-0.2.1", true, true},
		{"v1.14-0.3.0", true, true},
		{"v1.14-1.0.0", true, true},
		{"v1.15-0.2.0", true, true},
		{"v1.14-0.1.0", false, true},
		{"v1.14-0.1.1", false, true},
		{"v1.14.1_431f1fc", false, false}, // legacy: never qualifies
		{"v1.14.1_6ecb627", false, false}, // legacy, even though this binary carries the endpoints
		{"v1.10.1", false, false},         // bare upstream dev build
	}

	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()

			info, err := New(tc.version)
			require.NoError(t, err)
			assert.Equal(t, tc.inPlace, info.HasInPlaceCheckpoint(), "HasInPlaceCheckpoint(%s)", tc.version)
			assert.Equal(t, tc.fsOnly, info.HasFilesystemSnapshots(), "HasFilesystemSnapshots(%s)", tc.version)
		})
	}
}

// v1.14-0.3.0 is the first release whose PUT /cpu-config accepts x86_tsc_khz.
func TestHasTscKhzTemplate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		version string
		want    bool
	}{
		{"v1.14-0.3.0", true},
		{"v1.14-0.3.1", true},
		{"v1.14-1.0.0", true},
		{"v1.14-0.2.0", false},
		{"v1.14.1_431f1fc", false}, // legacy: never qualifies
		{"v1.10.1", false},         // bare upstream dev build
	}

	for _, tc := range cases {
		info, err := New(tc.version)
		require.NoError(t, err)
		assert.Equal(t, tc.want, info.HasTscKhzTemplate(), "HasTscKhzTemplate(%s)", tc.version)
	}
}
