//go:build unix

package host

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/envd/internal/cabundle"
)

func TestInstallCACerts_SlowCleanupDoesNotBlockOrOverwriteInit(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b := newTestCA(t, "A"), newTestCA(t, "B")
	tr.update(t, c, a)
	legacy := filepath.Join(tr.certDir, legacyCACertFile)
	require.NoError(t, unix.Mkfifo(legacy, 0o600))
	cleanup := make(chan error, 1)
	go func() { cleanup <- c.persist(t.Context(), tr.bundlePath, tr.certDir) }()
	// Opening the writer synchronizes with cleanup's read. Keep it open to stall rootfs work.
	writer, err := os.OpenFile(legacy, os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	parsed, err := cabundle.Parse(b)
	require.NoError(t, err)
	installed := make(chan error, 1)
	go func() { installed <- c.updateBundle(t.Context(), parsed, tr.bundlePath) }()
	select {
	case err := <-installed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("init blocked behind source-file cleanup")
	}
	tr.requireBundle(t, a, b)
	// The delayed read sees B while a newer init adopts B. Its legacy hash link must survive.
	require.NoError(t, os.Remove(legacy))
	require.NoError(t, os.WriteFile(legacy, []byte(b), 0o644))
	hashLink := filepath.Join(filepath.Dir(tr.bundlePath), "subject-hash.0")
	require.NoError(t, os.Symlink(legacy, hashLink))
	_, err = writer.WriteString(b)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, <-cleanup)
	tr.requireBundle(t, a, b) // The superseded job must not publish its old snapshot.
	legacyData, err := os.ReadFile(hashLink)
	require.NoError(t, err)
	require.Equal(t, b, string(legacyData))
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireBundle(t, b)
	tr.install(t, c, a)
	require.NoFileExists(t, legacy)
	tr.requireTrust(t, a)
}
