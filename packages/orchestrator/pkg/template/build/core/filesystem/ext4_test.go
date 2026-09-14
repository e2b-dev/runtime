//go:build linux

package filesystem

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Make requires a block size >= inodesRatio; 4 KiB is what the rootfs uses.
const makeBlockSize = 4096

// TestMakeKeepsDirIndex guards the htree directory index against a "^dir_index"
// finding its way back into the feature list, which mkfs.ext4 would accept
// silently.
func TestMakeKeepsDirIndex(t *testing.T) {
	t.Parallel()

	requireTools(t, "mkfs.ext4", "dumpe2fs")

	rootfsPath := filepath.Join(t.TempDir(), "rootfs.ext4")
	require.NoError(t, Make(t.Context(), rootfsPath, 64, makeBlockSize))

	features := filesystemFeatures(t, rootfsPath)
	assert.Contains(t, features, "dir_index", "mkfs.ext4 feature set %v", features)
}

func requireTools(t *testing.T, tools ...string) {
	t.Helper()

	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("requires %s to inspect an ext4 filesystem: %v", tool, err)
		}
	}
}

// filesystemFeatures returns the feature names dumpe2fs reports for the image.
func filesystemFeatures(t *testing.T, rootfsPath string) []string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "dumpe2fs", "-h", rootfsPath)
	// The parser below relies on dumpe2fs's English field names.
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	output, err := cmd.Output()
	require.NoError(t, err, "dumpe2fs -h %s", rootfsPath)

	const prefix = "Filesystem features:"
	for line := range strings.Lines(string(output)) {
		if after, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			return strings.Fields(after)
		}
	}

	t.Fatalf("no %q line in dumpe2fs output:\n%s", prefix, output)

	return nil
}

func TestParseFreeBlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected int64
		wantErr  bool
	}{
		{
			name: "sums group counters instead of stale superblock",
			input: `Free blocks:              50000
 Group  0: block bitmap at 1, inode bitmap at 2, inode table at 3
           28629 free blocks, 100 free inodes, 1 used directory
 Group  1: block bitmap at 4, inode bitmap at 5, inode table at 6
           28639 free blocks, 100 free inodes, 1 used directory
`,
			expected: 57268,
		},
		{
			name: "supports singular block counter",
			input: ` Group  0: block bitmap at 1, inode bitmap at 2, inode table at 3
           1 free block, 1 free inode, 1 used directory
`,
			expected: 1,
		},
		{
			name: "rejects incomplete group output",
			input: ` Group  0: block bitmap at 1, inode bitmap at 2, inode table at 3
           10 free blocks, 100 free inodes, 1 used directory
 Group  1: block bitmap at 4, inode bitmap at 5, inode table at 6
`,
			wantErr: true,
		},
		{
			name:    "rejects global counter without block groups",
			input:   "Free blocks:              50000\n",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseFreeBlocks(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestParseReservedBlocks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected int64
		wantErr  bool
	}{
		{
			name:     "standard debugfs output",
			input:    "Block count:              131072\nReserved block count:     6553\nFree blocks:              120000\n",
			expected: 6553,
		},
		{
			name:     "zero reserved blocks",
			input:    "Reserved block count:     0\n",
			expected: 0,
		},
		{
			name:    "missing reserved blocks",
			input:   "Block count:              131072\nFree blocks:              120000\n",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseReservedBlocks(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
