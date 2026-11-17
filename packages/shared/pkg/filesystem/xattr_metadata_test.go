package filesystem

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/xattr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadMetadata(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

	want := map[string]string{"author": "mish", "purpose": "upload"}
	err := writeMetadataForTest(path, want)
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)

	got, err := ReadMetadata(path)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for k, v := range want {
		assert.Equal(t, v, got[k], "key %q", k)
	}
}

func TestReadMetadataEmptyValue(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))

	err := xattr.Set(path, MetadataXattrPrefix+"empty", []byte{})
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)

	got, err := ReadMetadata(path)
	require.NoError(t, err)
	require.Contains(t, got, "empty")
	assert.Empty(t, got["empty"])
}

// TestReadMetadataSkipsInvalidUTF8 verifies that metadata set directly inside
// the sandbox (e.g. via setfattr) with non-UTF-8 bytes is skipped on read, so
// it can't break proto marshaling of EntryInfo.metadata. Valid entries on the
// same file are still returned.
func TestReadMetadataSkipsInvalidUTF8(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))

	err := xattr.Set(path, MetadataXattrPrefix+"bad", []byte{0xff, 0xfe})
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)
	require.NoError(t, xattr.Set(path, MetadataXattrPrefix+"good", []byte("ok")))

	got, err := ReadMetadata(path)
	require.NoError(t, err)
	assert.NotContains(t, got, "bad", "non-UTF-8 value should be skipped")
	assert.Equal(t, "ok", got["good"])
}

func TestReadMetadataIgnoresNonUserPrefix(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))

	err := xattr.Set(path, MetadataXattrPrefix+"keep", []byte("yes"))
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)

	got, err := ReadMetadata(path)
	require.NoError(t, err)
	assert.Contains(t, got, "keep")
	assert.NotContains(t, got, MetadataXattrPrefix+"keep", "prefix leaked into keys")
}

// TestReadMetadataIgnoresForeignUserXattrs verifies that user.* xattrs
// written by other tooling (outside the user.e2b.* namespace) are not
// surfaced as envd metadata.
func TestReadMetadataIgnoresForeignUserXattrs(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))

	err := xattr.Set(path, "user.foreign", []byte("ignored"))
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)

	got, err := ReadMetadata(path)
	require.NoError(t, err)
	assert.Empty(t, got, "expected no envd metadata")
}

// TestWriteMetadataReplacesFullSet verifies that WriteMetadata replaces the
// full set of user.e2b.* xattrs: a non-empty call drops stale keys and sets
// the new ones, and an empty/nil call clears every key. Xattrs outside the
// e2b namespace must survive in both cases.
func TestWriteMetadataReplacesFullSet(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))

	err := writeMetadataForTest(path, map[string]string{"author": "mish", "purpose": "upload"})
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)

	// Foreign user.* xattr must survive every WriteMetadata call.
	require.NoError(t, xattr.Set(path, "user.foreign", []byte("keep")))

	// Non-empty call replaces the full set: "purpose" is dropped because
	// it's not in the new map.
	require.NoError(t, writeMetadataForTest(path, map[string]string{"author": "alice"}))
	got, err := ReadMetadata(path)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"author": "alice"}, got)

	// Empty/nil call clears all e2b metadata.
	require.NoError(t, writeMetadataForTest(path, nil))
	got, err = ReadMetadata(path)
	require.NoError(t, err)
	assert.Empty(t, got, "nil write should clear all metadata")

	// Foreign xattr untouched throughout.
	_, err = xattr.Get(path, "user.foreign")
	assert.NoError(t, err, "foreign xattr was removed")
}

func TestWriteMetadataSurvivesRename(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	renamed := filepath.Join(dir, "renamed")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, os.Rename(path, renamed))

	want := map[string]string{"author": "mish"}
	err = WriteMetadata(file, want)
	if IsXattrUnsupported(err) {
		t.Skipf("filesystem does not support xattrs: %v", err)
	}
	require.NoError(t, err)

	got, err := ReadMetadata(renamed)
	require.NoError(t, err)
	require.Equal(t, want["author"], got["author"])
}

func writeMetadataForTest(path string, metadata map[string]string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	return WriteMetadata(file, metadata)
}
