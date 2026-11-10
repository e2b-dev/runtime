//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/scheduling"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// ErrPoweredOffKeepsNothingRunning refuses a capture that must keep the
// sandbox running after its guest powered off; the held disk waits for the
// sandbox's pause or kill.
var ErrPoweredOffKeepsNothingRunning = errors.New("the guest powered off: a capture cannot keep the sandbox running")

// ErrPoweredOffHasNoMemory refuses a capture whose caller needs guest memory
// after the guest powered off: only the disk is left to capture.
var ErrPoweredOffHasNoMemory = errors.New("the guest powered off: there is no memory to capture")

// poweredOffHold keeps a sandbox whose guest powered off, disk included,
// until something asks it to stop.
type poweredOffHold struct {
	once     sync.Once
	released chan struct{}
}

func (h *poweredOffHold) release() {
	h.once.Do(func() { close(h.released) })
}

// HoldPoweredOff marks the sandbox's guest as powered off and returns a
// channel that closes once the sandbox is asked to stop, by a kill or by a
// pause. Until then the sandbox keeps its disk, stays listed, and can be
// paused filesystem-only.
func (s *Sandbox) HoldPoweredOff() <-chan struct{} {
	h := &poweredOffHold{released: make(chan struct{})}
	if !s.poweredOff.CompareAndSwap(nil, h) {
		h = s.poweredOff.Load()
	}

	// endPoweredOff marks the end before it releases the hold, so a kill or
	// a pause that came before the hold was installed is seen here. The stop
	// that the Firecracker exit runs is not one: it always comes first.
	if s.poweredOffEnded.Load() {
		h.release()
	}

	return h.released
}

// PowerOffDeviceDeclared reports whether this lifecycle's Firecracker took
// the ACPI power-off device, whatever guest-poweroff-end says now.
func (s *Sandbox) PowerOffDeviceDeclared() bool {
	return s.Resources != nil && s.process.ACPIPowerOffDeclared()
}

// GuestPoweredOff reports whether the sandbox is held after its guest powered
// off.
func (s *Sandbox) GuestPoweredOff() bool {
	return s.poweredOff.Load() != nil
}

// endPoweredOff lets a powered-off sandbox be torn down, now if it is held,
// or as soon as it is.
func (s *Sandbox) endPoweredOff() {
	s.poweredOffEnded.Store(true)
	if h := s.poweredOff.Load(); h != nil {
		h.release()
	}
}

// pausePoweredOff snapshots the disk of a sandbox whose guest powered off.
// There is no Firecracker process: no envd sync, no VM pause, no snapfile and
// no memory. The guest shut its filesystems down itself, but the host cannot
// see that, so the snapshot is not marked quiesced.
func (s *Sandbox) pausePoweredOff(ctx context.Context, m metadata.Template) (st *Snapshot, e error) {
	ctx, span := tracer.Start(ctx, "sandbox-snapshot-powered-off", trace.WithAttributes(
		attribute.Bool("fs-only-snapshot", true),
	))
	defer span.End()

	cleanup := NewCleanup()
	defer func() {
		if e != nil {
			e = errors.Join(e, cleanup.Run(ctx))
		}
	}()

	cachePaths, err := storage.Paths{BuildID: m.Template.BuildID}.Cache(s.config.StorageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache paths: %w", err)
	}
	cleanup.AddNoContext(ctx, cachePaths.Close)

	buildID, err := uuid.Parse(cachePaths.BuildID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse build id: %w", err)
	}

	if err := s.waitForRootfsSeal(ctx); err != nil {
		return nil, fmt.Errorf("previous rootfs seal did not complete: %w", err)
	}

	m = m.MarkFilesystemOnly(true)
	m = m.MarkFsQuiesced(false)

	originalRootfs, err := s.Template.Rootfs()
	if err != nil {
		return nil, fmt.Errorf("failed to get original rootfs: %w", err)
	}

	// The synchronous export: it ejects the overlay's cache and closes the
	// sandbox, which there is no guest left to notice.
	pauseOpts := pauseOptions{filesystemSnapshot: true}
	rootfsDiff, rootfsHeader, _, err := s.processRootfsSnapshot(ctx, buildID, originalRootfs.Header(), &pauseOpts, cleanup)
	if err != nil {
		return nil, fmt.Errorf("error while post processing: %w", err)
	}

	metadataFileLink := template.NewLocalFileLink(cachePaths.CacheMetadata())
	if err := m.ToFile(metadataFileLink.Path()); err != nil {
		return nil, err
	}

	return &Snapshot{
		MemorySnapshot: MemorySnapshot{
			Diff:       build.Diff(&build.NoDiff{}),
			DiffHeader: NewResolvedDiffHeader(nil),
		},
		Snapfile:           &template.NoopFile{},
		Metafile:           metadataFileLink,
		Metadata:           m,
		RootfsDiff:         rootfsDiff,
		RootfsDiffHeader:   NewResolvedDiffHeader(rootfsHeader),
		SchedulingMetadata: scheduling.FromHeaders(buildID, nil, rootfsHeader, 0),
		FilesystemSnapshot: true,
		RootfsBlockSize:    originalRootfs.Header().Metadata.BlockSize,
		BuildID:            buildID,
		cleanup:            cleanup,
	}, nil
}
