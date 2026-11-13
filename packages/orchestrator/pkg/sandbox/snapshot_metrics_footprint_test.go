//go:build linux

package sandbox

import "testing"

func TestSnapshotDiffClean(t *testing.T) {
	cases := []struct {
		name  string
		dirty int64
		want  bool
	}{
		{"zero dirty is clean", 0, true},
		{"one block is clean", snapshotCleanThresholdBytes, true},
		{"just over one block is dirty", snapshotCleanThresholdBytes + 1, false},
		{"large dirty is dirty", 119537664, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := snapshotDiffClean(c.dirty); got != c.want {
				t.Fatalf("snapshotDiffClean(%d) = %v, want %v", c.dirty, got, c.want)
			}
		})
	}
}
