//go:build linux

package userfaultfd

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// A failed install carries ErrCopyFailed only when UFFDIO_COPY failed: a
// write fault on a 4 KiB hole is served by UFFDIO_ZEROPAGE, which allocates
// no hugepage, so its ENOMEM must not count as hugepage exhaustion. The fd
// is invalid, so either ioctl fails with EBADF.
func TestFaultPageMarksOnlyCopyFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		accessType block.AccessType
		wantCopy   bool
	}{
		{"read fault zero-fills with UFFDIO_COPY", block.Read, true},
		{"write fault zero-fills with UFFDIO_ZEROPAGE", block.Write, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			u := &Userfaultfd{
				fd:       Fd(^uintptr(0)),
				pageSize: header.PageSize,
				logger:   logger.NewNopLogger(),
			}

			outcome, err := u.faultPage(t.Context(), header.PageSize, 0, tc.accessType, nil, nil)
			require.Error(t, err)
			assert.Equal(t, faultDiscarded, outcome)
			require.ErrorIs(t, err, syscall.EBADF, "the kernel's errno stays in the chain")
			assert.Equal(t, tc.wantCopy, errors.Is(err, ErrCopyFailed), "marked as a UFFDIO_COPY failure")
		})
	}
}

type panickingReader struct{}

func (panickingReader) ReadAt(context.Context, []byte, int64) (int, error) {
	panic("boom")
}

// A panic while serving a fault is fatal, like a failed read: faults deduped
// onto this install are blocked on the page and only the handler's exit
// wakes them, so the recover must signal it, not just return an error.
func TestFaultPagePanicSignalsExit(t *testing.T) {
	t.Parallel()

	u := &Userfaultfd{
		fd:       Fd(^uintptr(0)),
		pageSize: header.PageSize,
		logger:   logger.NewNopLogger(),
	}
	signalErr := errors.New("signal failed")
	signalled := 0
	onFailure := func() error {
		signalled++

		return signalErr
	}

	outcome, err := u.faultPage(t.Context(), header.PageSize, 0, block.Read, panickingReader{}, onFailure)
	assert.Equal(t, faultDiscarded, outcome)
	require.ErrorContains(t, err, "uffd serve panic: boom")
	require.ErrorIs(t, err, signalErr, "the exit signal's own error stays in the chain")
	assert.Equal(t, 1, signalled, "the recover must signal the handler exit exactly once")

	_, err = u.faultPage(t.Context(), header.PageSize, 0, block.Read, panickingReader{}, nil)
	require.ErrorContains(t, err, "uffd serve panic: boom", "a nil onFailure is tolerated")
}
