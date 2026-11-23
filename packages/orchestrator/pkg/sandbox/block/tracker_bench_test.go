//go:build linux

package block

import (
	"fmt"
	"testing"
)

func BenchmarkTrackerFaultCycle(b *testing.B) {
	for _, n := range []uint32{1, 262144} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			t := NewMemoryTracker(4096)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				p := uint32(i) % n
				t.MarkInstalled(p, p+1, Clean)
				t.MarkWritten(p)
				t.SetRange(p, p+1, Removed)
			}
		})
	}
}
