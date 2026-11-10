//go:build linux

package factories

import (
	"errors"
	"os"
)

// releaseOrchestratorLock is called by Run's deferred lock cleanup.
// Remove the marker while its lock is still held: removing it after unlocking
// could unlink a new owner's lock and allow another instance to start.
// Keep removing the marker for rollback to releases with a stat-based guard.
func releaseOrchestratorLock(path string, closeLock func() error) error {
	removeErr := os.Remove(path)
	if os.IsNotExist(removeErr) {
		removeErr = nil
	}

	return errors.Join(removeErr, closeLock())
}
