package servicestatus

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileStore_RoundTripAtomicAndClear(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	store := NewFileStore(path)

	_, found, err := store.Load()
	require.NoError(t, err)
	require.False(t, found)

	changedAt := time.Date(2026, 11, 4, 12, 0, 0, 123456789, time.UTC)
	require.NoError(t, store.Save(Snapshot{Status: standby, Owner: "x", ChangedAt: changedAt}))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, `{"status":"Standby","owner":"x","changed_at":"2026-11-04T12:00:00.123456789Z"}`, string(raw))

	// A rewrite replaces the record in place.
	saved := Snapshot{Status: standby, Owner: "y", ChangedAt: changedAt}
	require.NoError(t, store.Save(saved))
	loaded, found, err := store.Load()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, saved, loaded)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temp file left beside the record")
	require.Equal(t, FileName, entries[0].Name())

	require.NoError(t, store.Clear())
	_, found, err = store.Load()
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, store.Clear(), "a missing record is fine")

	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestFileStore_CorruptFileIsAnError(t *testing.T) {
	t.Parallel()

	for name, content := range map[string]string{
		"not json":       `{"status":`,
		"unknown status": `{"status":"Parked","owner":"x","changed_at":"2026-11-04T12:00:00Z"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), FileName)
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

			_, found, err := NewFileStore(path).Load()
			require.Error(t, err)
			require.False(t, found)
		})
	}
}
