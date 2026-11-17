//go:build linux

package userfaultfd

import (
	"context"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// TestPrefaultConcurrentWithClose is a regression test for the race between
// Prefault() and Close() that produced ENOTTY/EBADF in production:
//
//	Prefault                           Close
//	 (about to acquire RLock)
//	                                    acquires Lock
//	                                    closed = true
//	                                    fd.close()          ← fd freed/recycled
//	                                    releases Lock
//	 acquires RLock
//	 sees closed == true → return ErrClosed  ✓
//
// Without the fix, Prefault had no closed check and would call UFFDIO_COPY
// on the closed (and potentially recycled) fd, returning EBADF or ENOTTY.
//
// The test uses faultPhaseBeforePrefaultRLock to park Prefault before the
// RLock acquisition, lets Close() run to completion, then releases the park
// and asserts Prefault returns ErrClosed without touching the fd.
func TestPrefaultConcurrentWithClose(t *testing.T) {
	t.Parallel()

	withRaceContext(t, func(ctx context.Context) {
		// Create a real userfaultfd so Close() can close a valid fd.  We do
		// NOT call configureApi/register because the test short-circuits via
		// the closed flag before any UFFDIO_COPY is attempted.
		uffdFd, err := newFd(syscall.O_CLOEXEC | syscall.O_NONBLOCK)
		require.NoError(t, err)

		// Minimal page-sized backing store (content doesn't matter; Prefault
		// won't reach ReadAt in the short-circuit path).
		pageData := make([]byte, header.PageSize)
		src := NewMemorySlicer(pageData, int64(header.PageSize))

		// A single-region mapping anchored at a real heap address.
		// GetHostVirtAddr is called after the closed check, so the address
		// only needs to satisfy NewUserfaultfdFromFd's region validation.
		regionBuf := make([]byte, header.PageSize)
		mapping := memory.NewMapping([]memory.Region{{
			BaseHostVirtAddr: uintptr(unsafe.Pointer(&regionBuf[0])),
			Size:             uintptr(header.PageSize),
			Offset:           0,
			PageSize:         uintptr(header.PageSize),
		}})

		log, err := logger.NewDevelopmentLogger()
		require.NoError(t, err)

		u, err := NewUserfaultfdFromFd(uintptr(uffdFd), src, mapping, 0, log)
		require.NoError(t, err)

		// Barrier channels: park Prefault at faultPhaseBeforePrefaultRLock so
		// Close() can run to completion before the RLock is acquired.
		arrived := make(chan struct{})
		release := make(chan struct{})

		u.SetTestFaultHook(func(_ uintptr, phase faultPhase) {
			if phase != faultPhaseBeforePrefaultRLock {
				return
			}
			// Signal arrival exactly once (guard against re-entry).
			select {
			case <-arrived:
			default:
				close(arrived)
			}
			// Wait for the test to release us.
			select {
			case <-release:
			case <-ctx.Done():
			}
		})

		prefaultErrs := make(chan error, 1)
		go func() {
			_, err := u.Prefault(ctx, 0, pageData)
			prefaultErrs <- err
		}()

		// Wait for Prefault to park at the pre-RLock hook.
		select {
		case <-arrived:
		case <-ctx.Done():
			require.Fail(t, "Prefault goroutine did not reach pre-RLock hook within budget")
		}

		// Close the uffd while Prefault is parked.
		// Pre-fix: fd gets closed and may be recycled; Prefault later calls
		//   UFFDIO_COPY on it and returns EBADF or ENOTTY.
		// Post-fix: Close() holds Lock(); Prefault acquires RLock, sees
		//   closed==true, returns ErrClosed without touching the fd.
		require.NoError(t, u.Close())

		// Release the parked Prefault goroutine.
		close(release)

		select {
		case err := <-prefaultErrs:
			require.ErrorIs(t, err, ErrClosed,
				"Prefault must return ErrClosed after concurrent Close() — "+
					"any other error means UFFDIO_COPY was attempted on the closed fd (EBADF/ENOTTY regression)")
		case <-ctx.Done():
			require.Fail(t, "Prefault goroutine did not complete after hook release")
		}
	})
}
