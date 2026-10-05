package cgroups

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mib = 1 << 20

// memoryTree builds a cgroup tree under a temp root with the given memory.min and
// memory.low contents per relative path (a nil map for a directory with no interface
// files at all), and a /proc/self/cgroup fixture holding selfLine verbatim -- a whole
// cgroup line, so a test can hand it one that names no cgroup2 hierarchy. Intermediate
// directories are created but get no files unless named.
func memoryTree(t *testing.T, selfLine string, files map[string]map[string]string) (procFile, root string) {
	t.Helper()

	root = t.TempDir()
	for rel, content := range files {
		full := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(full, 0o755))
		for name, v := range content {
			require.NoError(t, os.WriteFile(filepath.Join(full, name), []byte(v+"\n"), 0o644))
		}
	}

	procFile = filepath.Join(t.TempDir(), "cgroup")
	require.NoError(t, os.WriteFile(procFile, []byte(selfLine+"\n"), 0o644))

	return procFile, root
}

// TestReadMemoryProtection pins the walk: the floor is a property of the whole chain
// below the root, envd's own cgroup included, and never of envd.service alone.
func TestReadMemoryProtection(t *testing.T) {
	t.Parallel()

	const self = "0::/system.slice/envd.service"

	for name, tc := range map[string]struct {
		selfLine string
		files    map[string]map[string]string
		want     MemoryProtection
	}{
		"every level carries a request: floor is the smallest": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              {"memory.min": "67108864"},
				"system.slice/envd.service": {"memory.min": "134217728", "memory.low": "268435456"},
			},
			want: MemoryProtection{Request: 128 * mib, Low: 256 * mib, Floor: 64 * mib},
		},
		"a deeper chain takes the minimum over all of it": {
			selfLine: "0::/a/b/c",
			files: map[string]map[string]string{
				"a":     {"memory.min": "100"},
				"a/b":   {"memory.min": "50"},
				"a/b/c": {"memory.min": "200", "memory.low": "0"},
			},
			want: MemoryProtection{Request: 200, Low: 0, Floor: 50},
		},
		// The artifact this exists to see: envd.service asks, system.slice does not, and the
		// request reads back as set while envd is granted nothing.
		"an ancestor at 0 floors the request at 0": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              {"memory.min": "0"},
				"system.slice/envd.service": {"memory.min": "52428800", "memory.low": "0"},
			},
			want: MemoryProtection{Request: 50 * mib, Low: 0, Floor: 0},
		},
		// max is an unbounded request (systemd's MemoryMin=infinity), so it is the
		// identity of the floor's minimum: envd gets everything it asked for, and
		// reading it as 0 would report the most protected chain there is as unprotected.
		"an ancestor at max leaves the floor to the rest of the chain": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              {"memory.min": "max"},
				"system.slice/envd.service": {"memory.min": "52428800", "memory.low": "0"},
			},
			want: MemoryProtection{Request: 50 * mib, Low: 0, Floor: 50 * mib},
		},
		"a chain that is unbounded throughout floors at max, not at 0": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              {"memory.min": "max"},
				"system.slice/envd.service": {"memory.min": "max", "memory.low": "max"},
			},
			want: MemoryProtection{Request: math.MaxInt64, Low: math.MaxInt64, Floor: math.MaxInt64},
		},
		"an ancestor with no memory.min is a partial read": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              nil,
				"system.slice/envd.service": {"memory.min": "52428800", "memory.low": "104857600"},
			},
			want: MemoryProtection{Request: 50 * mib, Low: 100 * mib, Floor: 0, Partial: true},
		},
		"only memory.low unreadable: the request stands, the report is partial": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              {"memory.min": "67108864"},
				"system.slice/envd.service": {"memory.min": "134217728"},
			},
			want: MemoryProtection{Request: 128 * mib, Floor: 64 * mib, Partial: true},
		},
		"a value that does not parse is a partial read": {
			selfLine: self,
			files: map[string]map[string]string{
				"system.slice":              {"memory.min": "67108864"},
				"system.slice/envd.service": {"memory.min": "lots", "memory.low": "0"},
			},
			want: MemoryProtection{Request: 0, Low: 0, Floor: 0, Partial: true},
		},
		"no cgroup2 line: nothing can be located": {
			selfLine: "1:name=systemd:/system.slice/envd.service",
			files:    map[string]map[string]string{"system.slice/envd.service": {"memory.min": "1", "memory.low": "1"}},
			want:     MemoryProtection{Partial: true},
		},
		"self outside the root: nothing can be located": {
			selfLine: "0::/../elsewhere",
			files:    map[string]map[string]string{},
			want:     MemoryProtection{Partial: true},
		},
		// The root has no memory.min by construction, so this is a determinate
		// "unprotected", not a failure to read one.
		"envd in the root cgroup: zeros, not partial": {
			selfLine: "0::/",
			files:    map[string]map[string]string{},
			want:     MemoryProtection{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			procFile, root := memoryTree(t, tc.selfLine, tc.files)
			assert.Equal(t, tc.want, ReadMemoryProtection(procFile, root))
		})
	}
}

// TestWorkloadFreezer_MemoryProtection pins the adapter the API constructs through: it
// reads the tree the freezer's manager addresses, so two envds constructed over two
// trees report two values (a re-exec is a fresh construction), and a manager with no
// tree to read reports a partial zero rather than a value.
func TestWorkloadFreezer_MemoryProtection(t *testing.T) {
	t.Parallel()

	withMin := func(t *testing.T, sliceMin string) MemoryProtection {
		t.Helper()

		f, root := newTreeFixture(t, "/system.slice/envd.service", "system.slice/envd.service")
		require.NoError(t, os.WriteFile(filepath.Join(root, "system.slice", "memory.min"), []byte(sliceMin+"\n"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(root, "system.slice", "envd.service", "memory.min"), []byte("134217728\n"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(root, "system.slice", "envd.service", "memory.low"), []byte("268435456\n"), 0o644))

		return f.MemoryProtection()
	}

	protected := withMin(t, "134217728")
	assert.Equal(t, MemoryProtection{Request: 128 * mib, Low: 256 * mib, Floor: 128 * mib}, protected)

	unprotected := withMin(t, "0")
	assert.Equal(t, MemoryProtection{Request: 128 * mib, Low: 256 * mib, Floor: 0}, unprotected)
	assert.NotEqual(t, protected, unprotected, "two trees must yield two reports")

	assert.Equal(t, MemoryProtection{Partial: true}, NewWorkloadFreezer(NewNoopManager()).MemoryProtection())
}
