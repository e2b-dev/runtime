//go:build linux

package host

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const zoneinfoFixture = `Node 0, zone      DMA
  pages free     3840
        boost    0
        min      12
  spanned  4095
  present  3998
  managed  3840
Node 0, zone    DMA32
  pages free     100000
  spanned  1044480
  present  126976
  managed  120000
Node 0, zone   Normal
  pages free     1000
  spanned  0
  present  0
  managed  0
Node 0, zone  Movable
  pages free     393216
  spanned  393216
  present  393216
  managed  393216
`

func TestReadMemoryLayout(t *testing.T) {
	t.Parallel()

	t.Run("reports the policy and the movable and kernel split", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		write := func(name, body string) string {
			p := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(p, []byte(body), 0o600))

			return p
		}

		l := readMemoryLayout(write("policy", "auto-movable\n"), write("auto", "online\n"), write("zoneinfo", zoneinfoFixture), 4096)

		assert.Equal(t, MemoryLayout{
			OnlinePolicy: "auto-movable",
			AutoOnline:   "online",
			Movable:      393216 * 4096,
			Kernel:       (3840 + 120000) * 4096,
		}, l)
	})

	t.Run("a guest without hotplug parameters reports only the split", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		zi := filepath.Join(dir, "zoneinfo")
		require.NoError(t, os.WriteFile(zi, []byte(zoneinfoFixture), 0o600))

		l := readMemoryLayout(filepath.Join(dir, "missing"), filepath.Join(dir, "missing2"), zi, 4096)

		assert.Empty(t, l.OnlinePolicy)
		assert.Empty(t, l.AutoOnline)
		assert.Equal(t, uint64(393216*4096), l.Movable)
	})

	t.Run("nothing readable reports zeros", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		assert.Equal(t, MemoryLayout{}, readMemoryLayout(filepath.Join(dir, "a"), filepath.Join(dir, "b"), filepath.Join(dir, "c"), 4096))
	})
}
