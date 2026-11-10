//go:build linux

package factories

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestAcquireOrchestratorLockPersistsPIDAndRejectsConflict(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchestrator.lock")
	// The metadata writer must truncate old content after verifying identity.
	require.NoError(t, os.WriteFile(path, []byte("123456789012345678901234567890\n"), 0o600))
	holder, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	pid, err := readLockHolderPID(path)
	require.NoError(t, err)
	require.Equal(t, os.Getpid(), pid)
	conflict, err := acquireOrchestratorLock(path)
	if conflict != nil {
		t.Cleanup(func() { require.NoError(t, conflict.Close()) })
	}
	require.ErrorContains(t, err, "another instance is running")
	require.Nil(t, conflict)
	pid, err = readLockHolderPID(path)
	require.NoError(t, err)
	require.Equal(t, os.Getpid(), pid, "a refused contender must preserve the current owner's PID")
}

func TestLockMetadataRejectsRetiredOpenedInode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchestrator.lock")
	holder, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	// A contender can open the old inode before retirement but be scheduled
	// to take its nonblocking flock only after that inode has been unlinked.
	stale, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stale.Close()) })
	require.NoError(t, releaseOrchestratorLock(path, holder.Close))
	successor, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, successor.Close()) })
	require.NoError(t, unix.Flock(int(stale.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	staleInfo, err := stale.Stat()
	require.NoError(t, err)
	// Distinct metadata makes an accidental truncate/write visible even though
	// these independent file-lock owners execute within one test process.
	expected := []byte("424242\n")
	require.NoError(t, os.WriteFile(path, expected, 0o644))
	err = writeLockHolderPID(path, staleInfo)
	require.ErrorContains(t, err, "lock file path changed during acquisition")
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, expected, contents, "a stale opener must not overwrite the current owner's PID")
}

func TestReleaseOrchestratorLockPreservesSuccessor(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchestrator.lock")
	holder, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	successorAcquired := false
	err = releaseOrchestratorLock(path, func() error {
		if err := holder.Close(); err != nil {
			return err
		}
		// Acquire a real successor in the exact interval after the old lock
		// closes but before its release operation returns.
		successor, err := acquireOrchestratorLock(path)
		if successor != nil {
			t.Cleanup(func() { require.NoError(t, successor.Close()) })
			successorAcquired = true
		}

		return err
	})
	require.NoError(t, err)
	require.True(t, successorAcquired)
	third, err := acquireOrchestratorLock(path)
	if third != nil {
		t.Cleanup(func() { require.NoError(t, third.Close()) })
	}
	require.Error(t, err, "retiring runtime must not unlink the successor lock and admit a third owner")
	require.Nil(t, third)
}

func TestReleaseOrchestratorLockRemovesLegacyMarkerBeforeClose(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchestrator.lock")
	holder, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	closed := false
	err = releaseOrchestratorLock(path, func() error {
		_, statErr := os.Stat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist, "rollback's stat-based guard must not see a stale marker")
		closed = true

		return holder.Close()
	})
	require.NoError(t, err)
	require.True(t, closed)
	require.NoFileExists(t, path)
}

func TestReleaseOrchestratorLockMissingMarkerStillCloses(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchestrator.lock")
	holder, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	require.NoError(t, os.Remove(path))
	closed := false
	err = releaseOrchestratorLock(path, func() error {
		closed = true

		return holder.Close()
	})
	require.NoError(t, err)
	require.True(t, closed, "an already absent marker must not skip the actual lock close")
}

func TestReleaseOrchestratorLockPreservesRemovalAndCloseErrors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "orchestrator.lock")
	moved := filepath.Join(root, "held.lock")
	holder, err := acquireOrchestratorLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	// Replace only our private fixture pathname with a nonempty directory so
	// Remove fails without changing host permissions or mounting a filesystem.
	require.NoError(t, os.Rename(path, moved))
	require.NoError(t, os.Mkdir(path, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "retained"), nil, 0o600))
	closeDiagnostic := errors.New("lock close diagnostic")
	err = releaseOrchestratorLock(path, func() error {
		return errors.Join(holder.Close(), closeDiagnostic)
	})
	var removeErr *os.PathError
	require.ErrorAs(t, err, &removeErr)
	require.Equal(t, "remove", removeErr.Op)
	require.Equal(t, path, removeErr.Path)
	require.ErrorIs(t, err, closeDiagnostic)
	// Closing must still release the actual held inode when removal fails.
	next, err := acquireOrchestratorLock(moved)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, next.Close()) })
}
