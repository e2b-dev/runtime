//go:build linux

package chrooted

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadDirScalingWorkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scaling ReadDirScaling
		entries int
		want    int
	}{
		// The default scaling: serial below 1000 entries, 8 workers from
		// there, one more per 5000 additional entries, capped at 16.
		{"default: empty directory", defaultReadDirScaling, 0, 1},
		{"default: below the threshold", defaultReadDirScaling, 999, 1},
		{"default: at the threshold", defaultReadDirScaling, 1000, 8},
		{"default: one below the first step", defaultReadDirScaling, 5999, 8},
		{"default: at the first step", defaultReadDirScaling, 6000, 9},
		{"default: at the second step", defaultReadDirScaling, 11000, 10},
		{"default: one below the cap", defaultReadDirScaling, 40999, 15},
		{"default: at the cap", defaultReadDirScaling, 41000, 16},
		{"default: far above the cap", defaultReadDirScaling, 1000000, 16},

		// The zero value is serial rather than explosive.
		{"zero value", ReadDirScaling{}, 1000000, 1},

		// A step of 0 adds no workers rather than dividing by zero.
		{"zero step", ReadDirScaling{Threshold: 1, Base: 4, Step: 0, Max: 16}, 1000000, 4},

		// A max below the base caps at the max.
		{"max below the base", ReadDirScaling{Threshold: 1, Base: 8, Step: 1, Max: 2}, 1000000, 2},

		// A base of 1 stays serial until a step adds workers.
		{"base 1 grows with steps", ReadDirScaling{Threshold: 1, Base: 1, Step: 1, Max: 16}, 5, 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, test.scaling.Workers(test.entries))
		})
	}
}

// TestReadDirScalingListing lists the same small directory once through the
// default scaling (serial) and once through a scaling tiny enough to put the
// per-entry stats on the worker pool, and expects the same sorted listing.
func TestReadDirScalingListing(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	const files = 40

	want := make([]string, 0, files+1)
	for i := range files {
		name := fmt.Sprintf("file-%03d", i)
		require.NoError(t, os.WriteFile(filepath.Join(source, name), []byte(name), 0o644))
		want = append(want, name)
	}
	require.NoError(t, os.Mkdir(filepath.Join(source, "zz-dir"), 0o755))
	want = append(want, "zz-dir")

	serial, err := Chroot(source)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, serial.Close())
	})

	concurrent, err := Chroot(source,
		WithReadDirScaling(ReadDirScaling{Threshold: 2, Base: 4, Step: 1, Max: 8}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, concurrent.Close())
	})

	serialInfos, err := serial.ReadDir("/")
	require.NoError(t, err)

	concurrentInfos, err := concurrent.ReadDir("/")
	require.NoError(t, err)

	assert.Equal(t, want, entryNames(serialInfos))
	assert.Equal(t, want, entryNames(concurrentInfos))
}

func entryNames(infos []os.FileInfo) []string {
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name()
	}

	return names
}
