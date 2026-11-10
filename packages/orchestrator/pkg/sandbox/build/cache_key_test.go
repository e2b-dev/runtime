//go:build linux

package build

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGetDiffStoreKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		id   string
		kind DiffType
		want DiffStoreKey
	}{
		{"build", Memfile, "build/memfile"},
		{"build", Rootfs, "build/rootfs.ext4"},
		{"", Rootfs, "/rootfs.ext4"},
		{"a/b", DiffType("other/%s"), "a/b/other/%s"},
	} {
		t.Run(tc.id+"/"+string(tc.kind), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, GetDiffStoreKey(tc.id, tc.kind))
		})
	}
}

//nolint:paralleltest // AllocsPerRun changes GOMAXPROCS and compares serial calls.
func TestGetDiffStoreKeyAvoidsFormattingAllocations(t *testing.T) {
	id := uuid.NewString()
	var key DiffStoreKey
	current := testing.AllocsPerRun(2048, func() { key = GetDiffStoreKey(id, Memfile) })
	formatted := testing.AllocsPerRun(2048, func() {
		key = DiffStoreKey(fmt.Sprintf("%s/%s", id, Memfile))
	})
	require.Equal(t, DiffStoreKey(id+"/memfile"), key)
	require.Less(t, current, formatted, "cache hits must not allocate a formatting argument or buffer")
}

func BenchmarkGetDiffStoreKey(b *testing.B) {
	id := uuid.NewString()
	var key DiffStoreKey
	b.ReportAllocs()
	for b.Loop() {
		key = GetDiffStoreKey(id, Memfile)
	}
	require.Equal(b, DiffStoreKey(id+"/memfile"), key)
}
