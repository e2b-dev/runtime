package host

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mib = uint64(1) << 20
	gib = 1024 * mib
)

// iomemBase is the start of a 1 GiB guest's /proc/iomem, without the hotplug region.
const iomemBase = `00000000-00000fff : Reserved
00001000-0009fbff : System RAM
000f0000-000fffff : Reserved
00100000-3fffffff : System RAM
  01000000-01ffffff : Kernel code
  02000000-0259ffff : Kernel rodata
d0000000-d0000fff : LNRO0005:00
  d0000000-d0000fff : LNRO0005:00
`

func TestParseHotplugAdded(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		iomem   string
		want    uint64
		wantErr string
	}{
		"no virtio-mem": {iomem: iomemBase, want: 0},
		"one full 2 GiB range": {iomem: iomemBase + `8000000000-80bfffffff : virtio4
  8000000000-807fffffff : System RAM (virtio_mem)
`, want: 2 * gib},
		// 2114 MiB plugged onlines 17 whole 128 MiB blocks.
		"a partial last block counts whole": {iomem: iomemBase + `8000000000-80bfffffff : virtio4
  8000000000-8087ffffff : System RAM (virtio_mem)
`, want: 2176 * mib},
		"several ranges with a gap": {iomem: iomemBase + `8000000000-80bfffffff : virtio4
  8000000000-803fffffff : System RAM (virtio_mem)
  8048000000-807fffffff : System RAM (virtio_mem)
  8090000000-8097ffffff : System RAM (virtio_mem)
`, want: 1*gib + 896*mib + 128*mib},
		"malformed lines elsewhere are skipped": {iomem: "garbage\n zz-yy : Reserved\n" + iomemBase + `8000000000-80bfffffff : virtio4
  8000000000-807fffffff : System RAM (virtio_mem)
`, want: 2 * gib},
		"a malformed virtio-mem range is unusable": {iomem: iomemBase + `  80000000zz-807fffffff : System RAM (virtio_mem)
`, wantErr: "malformed"},
		"zeroed addresses (a non-root read)": {iomem: `00000000-00000000 : Reserved
00000000-00000000 : System RAM
  00000000-00000000 : System RAM (virtio_mem)
`, wantErr: "zeroed"},
		"empty": {iomem: "", wantErr: "no ranges"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := parseHotplugAdded([]byte(tc.iomem))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
