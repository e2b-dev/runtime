//go:build linux

package metrics

import "testing"

func TestOOMVictimClass(t *testing.T) {
	cases := map[string]string{
		"envd":    oomVictimControlPlane,
		"python3": oomVictimWorkload,
		"node":    oomVictimWorkload,
		"":        oomVictimWorkload,
		"Unknown": oomVictimWorkload, // envd's placeholder for an unreadable victim name
	}
	for process, want := range cases {
		if got := oomVictimClass(process); got != want {
			t.Errorf("oomVictimClass(%q) = %q, want %q", process, got, want)
		}
	}
}
