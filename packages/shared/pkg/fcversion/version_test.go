package fcversion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ParsesWithCommit(t *testing.T) {
	t.Parallel()

	info, err := New("v1.12.1_210cbac")
	require.NoError(t, err)

	ver := info.Version()
	assert.Equal(t, uint64(1), ver.Major())
	assert.Equal(t, uint64(12), ver.Minor())
	assert.Equal(t, uint64(1), ver.Patch())
}

func TestNew_ParsesWithoutCommit(t *testing.T) {
	t.Parallel()

	info, err := New("v1.10.1")
	require.NoError(t, err)

	ver := info.Version()
	assert.Equal(t, uint64(1), ver.Major())
	assert.Equal(t, uint64(10), ver.Minor())
}

func TestNew_ParsesWithoutVPrefix(t *testing.T) {
	t.Parallel()

	info, err := New("1.12.0_deadbee")
	require.NoError(t, err)

	assert.Equal(t, uint64(12), info.Version().Minor())
}

func TestNew_RejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := New("not-a-version")
	assert.Error(t, err)
}

func TestNew_LegacyFormatGoldens(t *testing.T) {
	t.Parallel()

	// Pins parse and gate results for the legacy versions stored in production.
	cases := []struct {
		version             string
		major, minor, patch uint64
		memfd               bool
		freePageReporting   bool
		freePageHinting     bool
	}{
		{"v1.10.1_30cbb07", 1, 10, 1, false, false, false},
		{"v1.12.1_210cbac", 1, 12, 1, false, false, false},
		{"v1.14.1_431f1fc", 1, 14, 1, true, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()

			info, err := New(tc.version)
			require.NoError(t, err)

			ver := info.Version()
			assert.Equal(t, tc.major, ver.Major())
			assert.Equal(t, tc.minor, ver.Minor())
			assert.Equal(t, tc.patch, ver.Patch())
			assert.Equal(t, tc.memfd, info.HasMemfd())
			assert.Equal(t, tc.freePageReporting, info.HasFreePageReporting())
			assert.Equal(t, tc.freePageHinting, info.HasFreePageHinting())
		})
	}
}

func TestNew_ParsesE2BFormat(t *testing.T) {
	t.Parallel()

	info, err := New("v1.14-0.1.0")
	require.NoError(t, err)

	ver := info.Version()
	assert.Equal(t, uint64(1), ver.Major())
	assert.Equal(t, uint64(14), ver.Minor())

	e2bVer, ok := info.E2BVersion()
	require.True(t, ok)
	assert.Equal(t, "0.1.0", e2bVer.String())
}

func TestNew_LegacyFormatHasNoE2BVersion(t *testing.T) {
	t.Parallel()

	info, err := New("v1.14.1_431f1fc")
	require.NoError(t, err)

	_, ok := info.E2BVersion()
	assert.False(t, ok)
}

func TestNew_RejectsMalformedVersions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		version string
	}{
		{"bare line without patch or e2b suffix", "v1.14"},
		{"e2b format without v prefix", "1.14-0.1.0"},
		{"e2b suffix with two components", "v1.14-0.1"},
		{"commit hash mixed with e2b suffix", "v1.14_abc1234-0.1.0"},
		{"e2b tag with commit hash appended", "v1.14-0.1.0_431f1fc"},
		{"leading zero in release line", "v01.14-0.1.0"},
		{"empty string", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(tc.version)
			assert.ErrorIs(t, err, ErrInvalidVersion)
		})
	}
}

func TestLDKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		version string
		key     string
		ok      bool
	}{
		{"v1.10.1_30cbb07", "v1.10", true},
		{"v1.14.1_431f1fc", "v1.14", true},
		{"v1.14-0.1.0", "v1.14-0", true},
		{"v2.3-1.0.2", "v2.3-1", true},
		{"v1.10.1", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()

			info, err := New(tc.version)
			require.NoError(t, err)

			key, ok := info.LDKey()
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.key, key)
		})
	}
}

func TestLDKey_ZeroValueInfo(t *testing.T) {
	t.Parallel()

	var info Info

	_, ok := info.LDKey()
	assert.False(t, ok)
}

func TestCapabilityGates_AgreeAcrossFormats(t *testing.T) {
	t.Parallel()

	legacy, err := New("v1.14.1_431f1fc")
	require.NoError(t, err)
	e2b, err := New("v1.14-0.1.0")
	require.NoError(t, err)

	assert.Equal(t, legacy.HasMemfd(), e2b.HasMemfd())
	assert.Equal(t, legacy.HasFreePageReporting(), e2b.HasFreePageReporting())
	assert.Equal(t, legacy.HasFreePageHinting(), e2b.HasFreePageHinting())
}

func TestCapabilityGates_E2BFormatOldLine(t *testing.T) {
	t.Parallel()

	info, err := New("v1.7-0.1.0")
	require.NoError(t, err)

	assert.False(t, info.HasMemfd())
	assert.False(t, info.HasFreePageReporting())
	assert.False(t, info.HasFreePageHinting())
}
