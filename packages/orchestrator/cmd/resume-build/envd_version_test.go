//go:build linux

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestResolveEnvdVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		flag      string
		want      string
		wantGiven bool
		valid     bool
	}{
		{name: "unset falls back to the placeholder", flag: "", want: placeholderEnvdVersion, valid: true},
		{name: "plain version", flag: "0.7.0", want: "0.7.0", wantGiven: true, valid: true},
		{name: "v-prefixed version", flag: "v0.7.0", want: "v0.7.0", wantGiven: true, valid: true},
		{name: "prerelease", flag: "0.7.0-rc.1", want: "0.7.0-rc.1", wantGiven: true, valid: true},
		{name: "placeholder passed explicitly is still given", flag: placeholderEnvdVersion, want: placeholderEnvdVersion, wantGiven: true, valid: true},
		{name: "not a version", flag: "latest", valid: false},
		{name: "truncated", flag: "0.", valid: false},
		{name: "empty-ish", flag: " ", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveEnvdVersion(tt.flag)
			if !tt.valid {
				require.Error(t, err)
				assert.Equal(t, envdVersion{}, got, "a rejected version must not reach the sandbox config")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
			assert.Equal(t, tt.wantGiven, got.given)
		})
	}
}

// TestEnvdVersionReportsOnlyGiven covers the values resolveEnvdVersion never produced: a
// zero value, and a literal carrying an unvalidated version.
func TestEnvdVersionReportsOnlyGiven(t *testing.T) {
	t.Parallel()

	assert.Equal(t, placeholderEnvdVersion, envdVersion{}.String())
	assert.False(t, envdVersion{}.given)
	assert.Equal(t, placeholderEnvdVersion, envdVersion{value: "not a version"}.String())
}

func TestEnvdVersionDescribe(t *testing.T) {
	t.Parallel()

	given, err := resolveEnvdVersion("0.7.0")
	require.NoError(t, err)
	assert.Equal(t, "0.7.0 (-envd-version)", given.describe())

	assert.Contains(t, envdVersion{}.describe(), placeholderEnvdVersion)
	assert.Contains(t, envdVersion{}.describe(), "placeholder")
}

// TestResolveEnvdVersionKeepsGatesReadable guards both the placeholder and an operator's
// value: one the gates cannot parse disables /freeze, /fsfreeze and /collapse silently.
func TestResolveEnvdVersionKeepsGatesReadable(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{"", "0.6.3", placeholderEnvdVersion} {
		version, err := resolveEnvdVersion(flag)
		require.NoError(t, err)

		_, err = utils.IsGTEVersion(version.String(), utils.MinEnvdVersionForCgroupFreeze)
		require.NoErrorf(t, err, "resolved version %q does not parse for the version gates", version)
	}
}

// TestPlaceholderClearsEveryGate pins the placeholder at or above every envd version gate,
// which is what makes an unspecified version exercise the modern paths.
func TestPlaceholderClearsEveryGate(t *testing.T) {
	t.Parallel()

	gates := map[string]string{
		"snapshot":      utils.MinEnvdVersionForSnapshot,
		"cgroup freeze": utils.MinEnvdVersionForCgroupFreeze,
		"heap collapse": utils.MinEnvdVersionForHeapCollapse,
		"fsfreeze":      utils.MinEnvdVersionForFsFreeze,
		"upgrade":       utils.MinEnvdVersionForUpgrade,
	}

	for name, min := range gates {
		ok, err := utils.IsGTEVersion(placeholderEnvdVersion, min)
		require.NoError(t, err)
		assert.Truef(t, ok, "placeholder %s is below the %s gate (%s)", placeholderEnvdVersion, name, min)
	}
}
