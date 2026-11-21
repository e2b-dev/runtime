//go:build linux

package volumes

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeMounts(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "mounts")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	return p
}

// a realistic /proc/mounts: root local, a JuiceFS FUSE mount, and a
// prefix-colliding sibling that must NOT match /mnt/jfs.
const sampleMounts = `/dev/sda1 / ext4 rw,relatime 0 0
JuiceFS:vol /mnt/jfs fuse.juicefs rw,nosuid,nodev 0 0
/dev/sdb1 /mnt/jfs-backup ext4 rw,relatime 0 0
`

func TestMountFSTypeFrom(t *testing.T) {
	t.Parallel()
	mp := writeMounts(t, sampleMounts)

	for name, tc := range map[string]struct {
		target string
		want   string
	}{
		"inside juicefs mount":       {"/mnt/jfs/team/vol", "fuse.juicefs"},
		"juicefs mount point itself": {"/mnt/jfs", "fuse.juicefs"},
		"prefix-colliding sibling":   {"/mnt/jfs-backup/x", "ext4"},
		"falls back to root fs":      {"/var/lib/whatever", "ext4"},
		"root itself":                {"/", "ext4"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := mountFSTypeFrom(mp, tc.target)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestVerifyVolumeMountFrom(t *testing.T) {
	t.Parallel()
	mp := writeMounts(t, sampleMounts)

	t.Run("empty expected type is a no-op (default)", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, verifyVolumeMountFrom(mp, "/anywhere/at/all", ""))
	})

	t.Run("path on expected fs passes", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, verifyVolumeMountFrom(mp, "/mnt/jfs/team/vol", "fuse.juicefs"))
	})

	t.Run("distributed FS not mounted -> path on local root rejected", func(t *testing.T) {
		t.Parallel()
		// JuiceFS expected but the volume path resolves to the ext4 root mount:
		// exactly the silent-to-local-disk footgun the guard prevents.
		err := verifyVolumeMountFrom(mp, "/mnt/jfs-not-mounted/team/vol", "fuse.juicefs")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected \"fuse.juicefs\"")
	})

	t.Run("wrong fs type rejected", func(t *testing.T) {
		t.Parallel()
		err := verifyVolumeMountFrom(mp, "/mnt/jfs-backup/vol", "fuse.juicefs")
		require.Error(t, err)
	})

	t.Run("missing mounts file surfaces an error when enabled", func(t *testing.T) {
		t.Parallel()
		err := verifyVolumeMountFrom(filepath.Join(t.TempDir(), "nope"), "/mnt/jfs/v", "fuse.juicefs")
		require.Error(t, err)
	})
}
