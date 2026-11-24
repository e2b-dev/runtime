//go:build linux

package sandboxtools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The pack command ships verbatim to the guest shell; a parse error fails every step.
func TestPackStepCertBundleCmdIsValidSh(t *testing.T) {
	t.Parallel()

	shellPath, err := exec.LookPath("sh")
	require.NoError(t, err, "sh is required to parse-check the guest script")

	script := filepath.Join(t.TempDir(), "pack-step-cert-bundle.sh")
	require.NoError(t, os.WriteFile(script, []byte(packStepCertBundleCmd), 0o600))

	out, err := exec.CommandContext(t.Context(), shellPath, "-n", script).CombinedOutput()
	require.NoErrorf(t, err, "packStepCertBundleCmd is not valid sh:\n%s", out)
}

// A guest without the cert directory is a no-op, not a failed step.
func TestPackStepCertBundleCmdSkipsGuestWithoutCertDir(t *testing.T) {
	t.Parallel()

	shellPath, err := exec.LookPath("sh")
	require.NoError(t, err, "sh is required to run the guest script")

	root := t.TempDir()
	script := certBundleScriptForRoot(root)

	out, err := exec.CommandContext(t.Context(), shellPath, "-c", script).CombinedOutput()
	require.NoErrorf(t, err, "pack failed on a guest without /etc/ssl/certs:\n%s", out)
	_, err = os.Stat(filepath.Join(root, CertBundlePath))
	require.ErrorIs(t, err, os.ErrNotExist, "a tar was packed with no cert directory")

	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc", "ssl", "certs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "etc", "ssl", "certs", "marker"), []byte("x"), 0o644))
	out, err = exec.CommandContext(t.Context(), shellPath, "-c", script).CombinedOutput()
	require.NoErrorf(t, err, "pack failed:\n%s", out)
	listing, err := exec.CommandContext(t.Context(), "tar", "-tf", filepath.Join(root, CertBundlePath)).CombinedOutput()
	require.NoError(t, err)
	require.Contains(t, string(listing), "./marker")
}

// Rewrite only guest absolute paths, leaving the production script unchanged.
func certBundleScriptForRoot(root string) string {
	return strings.NewReplacer("/etc/ssl/certs", root+"/etc/ssl/certs",
		"/usr/local/share/e2b", root+"/usr/local/share/e2b").Replace(packStepCertBundleCmd)
}

func TestPackCertBundlePreservesLinks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	certs := filepath.Join(root, "etc/ssl/certs")
	require.NoError(t, os.MkdirAll(filepath.Join(certs, "java"), 0o755))
	vendor := filepath.Join(root, "etc/ssl/vendor-certs")
	require.NoError(t, os.MkdirAll(vendor, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(vendor, "good.pem"), []byte("vendor"), 0o644))
	require.NoError(t, os.Symlink("missing.pem", filepath.Join(vendor, "stale.pem")))
	require.NoError(t, os.Symlink(vendor, filepath.Join(certs, "vendor")))
	source := filepath.Join(root, "source.crt")
	require.NoError(t, os.WriteFile(source, []byte("certificate"), 0o644))
	require.NoError(t, os.Symlink("../../../source.crt", filepath.Join(certs, "valid.pem")))
	require.NoError(t, os.Symlink("missing.crt", filepath.Join(certs, "dangling.pem")))
	require.NoError(t, os.Symlink("dangling.pem", filepath.Join(certs, "hash.0")))
	require.NoError(t, os.WriteFile(filepath.Join(certs, "java/cacerts"), []byte("keystore"), 0o600))
	out, err := exec.CommandContext(t.Context(), "sh", "-c", certBundleScriptForRoot(root)).CombinedOutput()
	require.NoErrorf(t, err, "pack failed: %s", out)
	// The pack must not remove the broken links from the running guest.
	_, err = os.Lstat(filepath.Join(certs, "dangling.pem"))
	require.NoError(t, err)
	listing, err := exec.CommandContext(t.Context(), "tar", "-tf", filepath.Join(root, CertBundlePath)).CombinedOutput()
	require.NoError(t, err)
	require.Contains(t, string(listing), "dangling.pem")
	require.Contains(t, string(listing), "hash.0")
	require.Contains(t, string(listing), "./java/cacerts")
	// Extract into a fresh directory at the same filesystem depth, like a
	// cold boot reseeding the cert tmpfs while the rootfs stays in place.
	restored := filepath.Join(root, "restored/ssl/certs")
	require.NoError(t, os.MkdirAll(restored, 0o755))
	out, err = exec.CommandContext(t.Context(), "tar", "-xf", filepath.Join(root, CertBundlePath), "-C", restored).CombinedOutput()
	require.NoErrorf(t, err, "extract failed: %s", out)
	target, err := os.Readlink(filepath.Join(restored, "valid.pem"))
	require.NoError(t, err)
	require.Equal(t, "../../../source.crt", target)
	content, err := os.ReadFile(filepath.Join(restored, "valid.pem"))
	require.NoError(t, err)
	require.Equal(t, "certificate", string(content))
	target, err = os.Readlink(filepath.Join(restored, "hash.0"))
	require.NoError(t, err)
	require.Equal(t, "dangling.pem", target)
	content, err = os.ReadFile(filepath.Join(restored, "vendor/good.pem"))
	require.NoError(t, err)
	require.Equal(t, "vendor", string(content))
}

func TestPackCertBundleReportsArchiveFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "etc/ssl/certs"), 0o755))
	// A directory in place of the output file causes a real tar write failure.
	require.NoError(t, os.MkdirAll(filepath.Join(root, CertBundlePath), 0o755))
	out, err := exec.CommandContext(t.Context(), "sh", "-c", certBundleScriptForRoot(root)).CombinedOutput()
	require.Error(t, err, "archive errors must not be suppressed: %s", out)
}

func TestFinalCertBundleDereferencesLinks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	certs := filepath.Join(root, "etc/ssl/certs")
	require.NoError(t, os.MkdirAll(certs, 0o755))
	source := filepath.Join(root, "source.crt")
	require.NoError(t, os.WriteFile(source, []byte("certificate"), 0o644))
	require.NoError(t, os.Symlink(source, filepath.Join(certs, "valid.pem")))
	script := strings.NewReplacer("/etc/ssl/certs", certs,
		"/usr/local/share/e2b", root+"/usr/local/share/e2b").Replace("set -e\n" + PackCertBundleTar)
	out, err := exec.CommandContext(t.Context(), "sh", "-c", script).CombinedOutput()
	require.NoErrorf(t, err, "final pack failed: %s", out)
	require.NoError(t, os.Remove(source))
	content, err := exec.CommandContext(t.Context(), "tar", "-xOf", filepath.Join(root, CertBundlePath), "./valid.pem").CombinedOutput()
	require.NoError(t, err)
	require.Equal(t, "certificate", string(content))
}
