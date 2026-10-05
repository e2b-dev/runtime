//go:build linux

package uffd

import (
	"context"
	"errors"
	"io"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// ErrCoWExportUnsupported reports that the memory backend cannot run a
// deferred (CoW) memory export; the caller falls back to the synchronous
// copy.
var ErrCoWExportUnsupported = errors.New("memory backend does not support CoW export")

// CoWExporter is the optional capability of a MemoryBackend to run a
// deferred (CoW) memory export. Callers type-assert:
//
//	if ce, ok := backend.(CoWExporter); ok { ... } else { sync copy }
//
// Only the UFFD backend implements it — the capture relies on the sync-WP
// serve loop intercepting guest writes.
type CoWExporter interface {
	// BeginCoWExport arms the dirty set for write-protection and installs a
	// CoW window capturing those pages' pause-time content into sink. MUST be
	// called while the VM is paused (the arm cannot race guest writes). The
	// caller drives the returned window: Sweep after the guest resumes,
	// Cancel on abort, and EndCoWExport when done. A backend without a live
	// UFFD handler + memfd returns ErrCoWExportUnsupported and the caller
	// falls back to the synchronous copy.
	BeginCoWExport(ctx context.Context, dirty *roaring.Bitmap, sink io.WriterAt) (*userfaultfd.CoWWindow, error)
	// EndCoWExport uninstalls the window if it is still the active one.
	EndCoWExport(w *userfaultfd.CoWWindow)
}

type MemoryBackend interface {
	// DiffMetadata returns the pause-time dirty/empty page sets. With
	// useTrackerDirty set, the UFFD backend derives them from its own page
	// tracker (installs + synchronous WP-fault promotions) without calling
	// Firecracker; callers must only set it for sandboxes resumed with
	// use_sync_wp — under WP_ASYNC the tracker never sees guest writes.
	DiffMetadata(ctx context.Context, f *fc.Process, useTrackerDirty bool) (*header.DiffMetadata, error)
	PrefetchData(ctx context.Context) (block.PrefetchData, error)
	// Prefault returns whether this call installed the page (false on
	// skipped/present/deferred nil-error paths); see Userfaultfd.Prefault.
	Prefault(ctx context.Context, offset int64, data []byte) (installed bool, e error)
	Start(ctx context.Context) error
	Stop() error
	Ready() chan struct{}
	Exit() *utils.ErrorOnce
	Memfd(ctx context.Context) *block.Memfd
	// PeekMemfd returns the memfd without transferring ownership, so an in-place
	// snapshot can copy the guest's dirty pages while the running VM keeps using
	// it. Unlike Memfd it does not consume the fd.
	PeekMemfd(ctx context.Context) *block.Memfd
	// ServeStats returns a cumulative snapshot of demand faults served so far.
	// Sampled at the envd-init boundary it yields the pages/bytes a guest
	// needed to start.
	ServeStats() userfaultfd.ServeSnapshot
}

// BalloonModeLabeler is implemented by backends whose serve metrics carry the
// balloon's free-page mechanism once it has been read from Firecracker.
type BalloonModeLabeler interface {
	SetBalloonMode(mode userfaultfd.BalloonMode)
}
