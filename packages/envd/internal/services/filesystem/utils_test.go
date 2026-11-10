package filesystem

import (
	"context"
	"os/exec"
	osuser "os/user"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fsmodel "github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
)

func TestIsPathOnNetworkMount(t *testing.T) {
	t.Parallel()

	// Test with a regular directory (should not be on network mount)
	tempDir := t.TempDir()
	isNetwork, err := IsPathOnNetworkMount(tempDir)
	require.NoError(t, err)
	assert.False(t, isNetwork, "temp directory should not be on a network mount")
}

func TestIsPathOnNetworkMount_FuseMount(t *testing.T) {
	t.Parallel()

	// FUSE mounts via bindfs are exercised on Linux only.
	if runtime.GOOS != "linux" {
		t.Skip("FUSE bindfs mount test runs only on Linux")
	}

	// Require bindfs to be available
	_, err := exec.LookPath("bindfs")
	require.NoError(t, err, "bindfs must be installed for this test")

	// Require fusermount to be available (needed for unmounting)
	_, err = exec.LookPath("fusermount")
	require.NoError(t, err, "fusermount must be installed for this test")

	// Create source and mount directories
	sourceDir := t.TempDir()
	mountDir := t.TempDir()

	// Mount sourceDir onto mountDir using bindfs (FUSE)
	ctx := t.Context()
	cmd := exec.CommandContext(ctx, "bindfs", sourceDir, mountDir)
	require.NoError(t, cmd.Run(), "failed to mount bindfs")

	// Ensure we unmount on cleanup
	t.Cleanup(func() {
		_ = exec.CommandContext(context.WithoutCancel(t.Context()), "fusermount", "-u", mountDir).Run()
	})

	// Test that the FUSE mount is detected
	isNetwork, err := IsPathOnNetworkMount(mountDir)
	require.NoError(t, err)
	assert.True(t, isNetwork, "FUSE mount should be detected as network filesystem")

	// Test that the source directory is NOT detected as network mount
	isNetworkSource, err := IsPathOnNetworkMount(sourceDir)
	require.NoError(t, err)
	assert.False(t, isNetworkSource, "source directory should not be detected as network filesystem")
}

func TestGetFileOwnership_CurrentUser(t *testing.T) {
	t.Parallel()

	t.Run("current user", func(t *testing.T) {
		t.Parallel()
		skipWithoutNumericOwner(t)

		// Get current user running the tests
		cur, err := osuser.Current()
		if err != nil {
			t.Skipf("unable to determine current user: %v", err)
		}

		// Determine expected owner/group using the same lookup logic
		expectedOwner := cur.Uid
		if u, err := osuser.LookupId(cur.Uid); err == nil {
			expectedOwner = u.Username
		}

		expectedGroup := cur.Gid
		if g, err := osuser.LookupGroupId(cur.Gid); err == nil {
			expectedGroup = g.Name
		}

		// Parse UID/GID strings to uint32 for EntryInfo
		uid64, err := strconv.ParseUint(cur.Uid, 10, 32)
		require.NoError(t, err)
		gid64, err := strconv.ParseUint(cur.Gid, 10, 32)
		require.NoError(t, err)

		// Build a minimal EntryInfo with current UID/GID
		info := fsmodel.EntryInfo{ // from shared pkg
			UID: uint32(uid64),
			GID: uint32(gid64),
		}

		owner, group := getFileOwnership(info)
		assert.Equal(t, expectedOwner, owner)
		assert.Equal(t, expectedGroup, group)
	})

	t.Run("no user", func(t *testing.T) {
		t.Parallel()

		// Find a UID that does not exist on this system
		var unknownUIDStr string
		for i := 60001; i < 70000; i++ { // search a high range typically unused
			idStr := strconv.Itoa(i)
			if _, err := osuser.LookupId(idStr); err != nil {
				unknownUIDStr = idStr

				break
			}
		}
		if unknownUIDStr == "" {
			t.Skip("could not find a non-existent UID in the probed range")
		}

		// Find a GID that does not exist on this system
		var unknownGIDStr string
		for i := 60001; i < 70000; i++ { // search a high range typically unused
			idStr := strconv.Itoa(i)
			if _, err := osuser.LookupGroupId(idStr); err != nil {
				unknownGIDStr = idStr

				break
			}
		}
		if unknownGIDStr == "" {
			t.Skip("could not find a non-existent GID in the probed range")
		}

		// Parse to uint32 for EntryInfo construction
		uid64, err := strconv.ParseUint(unknownUIDStr, 10, 32)
		require.NoError(t, err)
		gid64, err := strconv.ParseUint(unknownGIDStr, 10, 32)
		require.NoError(t, err)

		info := fsmodel.EntryInfo{
			UID: uint32(uid64),
			GID: uint32(gid64),
		}

		owner, group := getFileOwnership(info)
		// Expect numeric fallbacks because lookups should fail for unknown IDs
		assert.Equal(t, unknownUIDStr, owner)
		assert.Equal(t, unknownGIDStr, group)
	})
}
