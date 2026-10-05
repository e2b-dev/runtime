package featureflags

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolveEnvdUpgradePath exercises the resume-time upgrade decision without a
// LaunchDarkly client: the flag value is passed directly, binaries are real temp
// files (for the os.Stat check), and version resolution is injected.
func TestResolveEnvdUpgradePath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")         // HOST_ENVD_PATH
	shaBin := filepath.Join(dir, "envd.1f95888")   // legacy SHA-named binary, flat layout
	verBin := filepath.Join(dir, "envd.v0.7.1")    // version-named binary, flat layout
	nestedBin := filepath.Join(dir, "v0.8.0/envd") // versioned binary, release-bucket layout
	require.NoError(t, os.WriteFile(promoted, []byte("x"), 0o755))
	require.NoError(t, os.WriteFile(shaBin, []byte("x"), 0o755))
	require.NoError(t, os.WriteFile(verBin, []byte("x"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(nestedBin), 0o755))
	require.NoError(t, os.WriteFile(nestedBin, []byte("x"), 0o755))

	// Version map: promoted binary is 0.6.12, the SHA binary is 0.7.0, the
	// flat version-named binary is 0.7.1, the nested release binary is 0.8.0.
	// An unknown path returns an error (unreadable binary).
	versions := map[string]string{promoted: "0.6.12", shaBin: "0.7.0", verBin: "0.7.1", nestedBin: "0.8.0"}
	getVersion := func(_ context.Context, path string) (string, error) {
		v, ok := versions[path]
		if !ok {
			return "", fmt.Errorf("unknown binary %s", path)
		}

		return v, nil
	}

	tests := []struct {
		name        string
		target      string // flag value
		builtWith   string
		wantPath    string
		wantVersion string
		wantReason  string
	}{
		{"off returns empty", "off", "0.6.11", "", "", "off"},
		{"empty (unset) returns empty", "", "0.6.11", "", "", "off"},
		{"promoted, newer -> promoted path", "promoted", "0.6.11", promoted, "0.6.12", ""},
		{"promoted, same version -> empty (idempotent)", "promoted", "0.6.12", "", "", "same_version"},
		{"sha, exists and newer -> sha path", "1f95888", "0.6.11", shaBin, "0.7.0", ""},
		{"sha, same version -> empty", "1f95888", "0.7.0", "", "", "same_version"},
		{"sha, missing binary -> not_staged", "deadbee", "0.6.11", "", "", "not_staged"},
		{"version, exists and newer -> version path", "v0.7.1", "0.7.0", verBin, "0.7.1", ""},
		{"version, missing binary -> not_staged", "v0.9.9", "0.6.11", "", "", "not_staged"},
		// Upgrade-only: an older staged target must not trigger a downgrade.
		{"sha, older target -> downgrade refused", "1f95888", "0.8.0", "", "", "downgrade"},
		{"promoted, older target -> downgrade refused", "promoted", "0.7.0", "", "", "downgrade"},
		// A target that isn't a bare identifier must be rejected before it is
		// joined into a path (no traversal out of the staging dir / arbitrary exec).
		{"target with .. -> invalid_target", "../../etc/passwd", "0.6.11", "", "", "invalid_target"},
		{"target with slash -> invalid_target", "sub/envd", "0.6.11", "", "", "invalid_target"},
		{"target with leading dot -> invalid_target", ".hidden", "0.6.11", "", "", "invalid_target"},
		{"target starting with dot -> invalid_target", ".v0.7.0", "0.6.11", "", "", "invalid_target"},
		// Dotted release names are valid targets; an unstaged one is not_staged.
		{"release target, nested layout -> nested path", "v0.8.0", "0.6.11", nestedBin, "0.8.0", ""},
		{"release target, not staged", "v9.9.9", "0.6.11", "", "", "not_staged"},
		// Hyphens are admitted (prerelease names like v0.7.0-rc1), so a dashed
		// target that isn't staged is not_staged, not invalid_target.
		{"dashed release target, not staged", "v0.7.0-rc1", "0.6.11", "", "", "not_staged"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotPath, gotVersion, gotReason := resolveEnvdUpgradePath(t.Context(), tt.target, tt.builtWith, promoted, getVersion, nil)
			assert.Equal(t, tt.wantPath, gotPath)
			assert.Equal(t, tt.wantVersion, gotVersion)
			assert.Equal(t, tt.wantReason, gotReason)
		})
	}
}

// TestResolveEnvdUpgradePath_VersionError verifies an unreadable target is
// treated as "no upgrade" rather than propagating an error into the resume path.
func TestResolveEnvdUpgradePath_VersionError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	require.NoError(t, os.WriteFile(promoted, []byte("x"), 0o755))

	getVersion := func(_ context.Context, _ string) (string, error) {
		return "", assert.AnError
	}

	gotPath, gotVersion, gotReason := resolveEnvdUpgradePath(t.Context(), "promoted", "0.6.11", promoted, getVersion, nil)
	assert.Empty(t, gotPath)
	assert.Empty(t, gotVersion)
	assert.Equal(t, "getversion_failed", gotReason)
}

// The cache's delivery refusal — an engaged resolver holding no entry for the
// path — is unreachable only while this resolution asks for the version exactly
// once, on the string it goes on to return: a lookup that hit here cannot miss at
// delivery time. Nothing else pins that. A second call, or a normalisation
// applied to the returned path but not to the probed one, would put a mount read
// back on the resume path and deliver bytes whose version was never verified,
// with no other test failing.
func TestTheUpgradeResolutionProbesExactlyOnceOnThePathItReturns(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	host := filepath.Join(dir, "envd")
	require.NoError(t, os.WriteFile(host, []byte("binary"), 0o755))

	var asked []string
	getVersion := func(_ context.Context, path string) (string, error) {
		asked = append(asked, path)

		return "0.7.0", nil
	}

	path, version, reason := resolveEnvdUpgradePath(t.Context(), "promoted", "0.6.0", host, getVersion, nil)

	require.Empty(t, reason)
	require.Equal(t, "0.7.0", version)
	require.Len(t, asked, 1, "exactly one probe, or the delivery refusal becomes reachable")
	require.Equal(t, path, asked[0],
		"the version must be probed on the very string the caller later delivers")
}
