package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readFile is a small helper to read a file's full contents in a test.
func readFileT(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)

	return b
}

// TestReplaceFile_DurableWritesExactContent verifies the durable path (fsync
// temp + fsync dir) writes exactly the payload across the replacement matrix and
// leaves no temp residue. Calls replaceFile directly with durable=true so it
// touches no package-level state and stays parallel-safe.
func TestReplaceFile_DurableWritesExactContent(t *testing.T) {
	t.Parallel()

	cases := []struct{ name, initial, replace string }{
		{"shorter", "a longer initial payload", "short"},
		{"longer", "short", "a longer replacement payload"},
		{"same_length", "first", "other"},
		{"empty", "a non-empty payload", ""},
		{"into_empty", "", "filled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "obj.bin")

			n, err := replaceFile(path, bytes.NewReader([]byte(tc.initial)), true)
			require.NoError(t, err)
			require.EqualValues(t, len(tc.initial), n)

			n, err = replaceFile(path, bytes.NewReader([]byte(tc.replace)), true)
			require.NoError(t, err)
			require.EqualValues(t, len(tc.replace), n)

			assert.Equal(t, tc.replace, string(readFileT(t, path)), "no stale tail")

			info, err := os.Stat(path)
			require.NoError(t, err)
			assert.EqualValues(t, len(tc.replace), info.Size())

			// no temp residue
			entries, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			for _, e := range entries {
				assert.NotContains(t, e.Name(), tmpFileMarker, "temp residue left: %s", e.Name())
			}
		})
	}
}

// TestReplaceFile_DurableEqualsNonDurableContent asserts durable and
// non-durable modes produce byte-identical results (durability only affects
// crash semantics, never observable content).
func TestReplaceFile_DurableEqualsNonDurableContent(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("payload-"), 512)

	dDir := t.TempDir()
	dPath := filepath.Join(dDir, "d.bin")
	_, err := replaceFile(dPath, bytes.NewReader(payload), true)
	require.NoError(t, err)

	nDir := t.TempDir()
	nPath := filepath.Join(nDir, "n.bin")
	_, err = replaceFile(nPath, bytes.NewReader(payload), false)
	require.NoError(t, err)

	assert.Equal(t, readFileT(t, dPath), readFileT(t, nPath))
	assert.Equal(t, payload, readFileT(t, dPath))
}

// TestPutDurableToggle exercises the public Put path with the package-level
// fsyncDurable toggle flipped on. It mutates a package var, so it is
// deliberately NOT parallel and restores the previous value.
//
//nolint:paralleltest // mutates package-level fsyncDurable; must run serially
func TestPutDurableToggle(t *testing.T) {
	prev := fsyncDurable
	fsyncDurable = true
	t.Cleanup(func() { fsyncDurable = prev })

	ctx := t.Context()
	p := newTempProvider(t)
	obj, err := p.OpenBlob(ctx, filepath.Join("durable", "obj.bin"))
	require.NoError(t, err)

	require.NoError(t, obj.Put(ctx, []byte("a longer initial payload")))
	require.NoError(t, obj.Put(ctx, []byte("short")))

	seekable, ok := obj.(Seekable)
	require.True(t, ok)
	size, err := seekable.Size(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5, size)

	data, err := GetBlob(ctx, obj)
	require.NoError(t, err)
	require.Equal(t, []byte("short"), data)
}

// TestSweepStaleTempFiles_RemovesOnlyOldOrphans asserts the sweep reclaims aged
// orphan temp files while leaving fresh temp files (in-flight writes), the real
// object, and non-temp dotfiles (sidecars) untouched.
func TestSweepStaleTempFiles_RemovesOnlyOldOrphans(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	realObj := filepath.Join(dir, "memfile.bin")
	require.NoError(t, os.WriteFile(realObj, []byte("real data"), 0o644))

	oldOrphan := filepath.Join(dir, ".memfile.bin"+tmpFileMarker+"aaaa")
	require.NoError(t, os.WriteFile(oldOrphan, []byte("half-written"), 0o644))
	old := time.Now().Add(-30 * time.Minute)
	require.NoError(t, os.Chtimes(oldOrphan, old, old))

	freshTmp := filepath.Join(dir, ".other.bin"+tmpFileMarker+"bbbb")
	require.NoError(t, os.WriteFile(freshTmp, []byte("in flight"), 0o644))

	sidecar := filepath.Join(dir, "memfile.bin.uncompressed-size")
	require.NoError(t, os.WriteFile(sidecar, []byte("123"), 0o644))

	removed, err := SweepStaleTempFiles(dir, 5*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "only the aged orphan should be reclaimed")

	_, err = os.Stat(oldOrphan)
	assert.True(t, os.IsNotExist(err), "aged orphan must be removed")
	assert.FileExists(t, realObj, "real object must survive")
	assert.FileExists(t, freshTmp, "fresh temp (in-flight) must survive")
	assert.FileExists(t, sidecar, "sidecar must not be treated as a temp file")
}

// TestSweepStaleTempFiles_MissingDir is a no-op, not an error.
func TestSweepStaleTempFiles_MissingDir(t *testing.T) {
	t.Parallel()
	removed, err := SweepStaleTempFiles(filepath.Join(t.TempDir(), "nope"), time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 0, removed)
}

// TestSweepStaleTempFiles_MatchesReplaceFileNaming guarantees the sweep pattern
// stays in sync with the names replaceFile actually creates.
func TestSweepStaleTempFiles_MatchesReplaceFileNaming(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "obj.bin")

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(target)+tmpFileMarker+"*")
	require.NoError(t, err)
	name := tmp.Name()
	require.NoError(t, tmp.Close())
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(name, old, old))

	removed, err := SweepStaleTempFiles(dir, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
	_, err = os.Stat(name)
	assert.True(t, os.IsNotExist(err))
}
