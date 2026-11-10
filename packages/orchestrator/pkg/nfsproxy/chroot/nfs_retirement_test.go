//go:build linux

package chroot

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
)

func retireIdleRoots(tb testing.TB, h *NFSHandler, path string, count int) {
	tb.Helper()
	owner := mountcache.Owner{SandboxID: "retirement-cost", LifecycleID: "one"}
	roots := make(map[uuid.UUID]*chrooted.Chrooted, count)
	for i := range count {
		fs, err := chrooted.Chroot(path)
		if err != nil {
			tb.Fatal(err)
		}
		var id uuid.UUID
		id[0] = byte(i)
		roots[id] = fs
	}
	h.chrootsByOwner[owner] = roots
	if err := h.RemoveOwner(tb.Context(), owner); err != nil {
		tb.Fatal(err)
	}
	if h.Count() != 0 {
		tb.Fatal("retired roots remain registered")
	}
	for _, fs := range roots {
		if err := fs.WaitClosed(tb.Context()); err != nil {
			tb.Fatal(err)
		}
	}
}

func benchmarkRemoveOwnerIdle(b *testing.B, path string, count int, cold bool) {
	b.Helper()
	h, err := NewNFSHandler(nil, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if cold {
			h, err = NewNFSHandler(nil, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
		retireIdleRoots(b, h, path, count)
	}
}

func BenchmarkRemoveOwnerIdle(b *testing.B) {
	path := b.TempDir()
	if err := os.WriteFile(filepath.Join(path, "unread-payload"), make([]byte, 64<<10), 0o600); err != nil {
		b.Fatal(err)
	}
	for _, count := range []int{1, 8} {
		for _, cold := range []bool{false, true} {
			b.Run(fmt.Sprintf("roots=%d/cold=%t", count, cold), func(b *testing.B) {
				benchmarkRemoveOwnerIdle(b, path, count, cold)
			})
		}
	}
}

//nolint:paralleltest // Process-wide heap accounting must exclude parallel tests.
func TestRemoveOwnerIdleCost(t *testing.T) {
	path := t.TempDir()
	for _, tc := range []struct {
		roots  int
		cold   bool
		allocs int64
		bytes  int64
	}{
		{1, false, 5, 560},
		{1, true, 11, 1080},
		{8, false, 26, 2688},
		{8, true, 32, 3208},
	} {
		t.Run(fmt.Sprintf("roots=%d/cold=%t", tc.roots, tc.cold), func(t *testing.T) {
			result := testing.Benchmark(func(b *testing.B) {
				b.Helper()
				benchmarkRemoveOwnerIdle(b, path, tc.roots, tc.cold)
			})
			if result.AllocsPerOp() > tc.allocs || result.AllocedBytesPerOp() > tc.bytes {
				t.Fatalf("idle retirement exceeds pre-change cost: %d allocs/%d bytes; maximum %d/%d",
					result.AllocsPerOp(), result.AllocedBytesPerOp(), tc.allocs, tc.bytes)
			}
		})
	}
}
