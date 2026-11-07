//go:build linux

package metrics

import "testing"

// shareableBaseBytes is the per-template lower bound driving the density
// go/no-go metric; it must never over-report, so these lock the floor and the
// (N-1)*min shape.
func TestShareableBaseBytes(t *testing.T) {
	t.Parallel()

	const mib = int64(1) << 20
	tests := []struct {
		name string
		src  []int64
		want int64
	}{
		{"no sandboxes shares nothing", nil, 0},
		{"one sandbox shares nothing", []int64{512 * mib}, 0},
		{"two equal: one copy freeable", []int64{512 * mib, 512 * mib}, 512 * mib},
		{"two unequal: uses the smaller (lower bound)", []int64{512 * mib, 300 * mib}, 300 * mib},
		{"four sandboxes: (4-1)*min", []int64{400 * mib, 500 * mib, 420 * mib, 410 * mib}, 3 * 400 * mib},
		{"a zero-footprint member drags the lower bound to 0", []int64{512 * mib, 0, 512 * mib}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shareableBaseBytes(tt.src); got != tt.want {
				t.Errorf("shareableBaseBytes(%v) = %d, want %d", tt.src, got, tt.want)
			}
		})
	}
}
