//go:build linux

package sandbox

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// exitedSandbox is a sandbox whose Firecracker exited: the exit has run its
// stop, which a unit test cannot drive, so a stand-in takes its place in the
// once. Later Stop calls run only Stop's own checks, as they do in production.
func exitedSandbox(t *testing.T) *Sandbox {
	t.Helper()

	s := &Sandbox{Metadata: &Metadata{}}
	s.stop.GetOrInit(func() error {
		s.stopping.Store(true)

		return nil
	})
	require.NoError(t, s.Stop(t.Context()), "the exit's stop")

	return s
}

func requireHeld(t *testing.T, released <-chan struct{}) {
	t.Helper()

	select {
	case <-released:
		t.Fatal("released before a kill or a pause")
	default:
	}
}

// The lifecycle reaches the hold only after the Firecracker exit has stopped
// the sandbox; that stop must not release it, a kill or a pause must.
func TestHoldPoweredOff(t *testing.T) {
	t.Parallel()

	for _, reason := range []StopReason{StopReasonKilled, StopReasonPaused} {
		t.Run(string(reason)+" releases the hold the exit's stop kept", func(t *testing.T) {
			t.Parallel()

			s := exitedSandbox(t)
			assert.False(t, s.GuestPoweredOff())

			released := s.HoldPoweredOff()
			assert.True(t, s.GuestPoweredOff())
			requireHeld(t, released)

			require.NoError(t, s.Stop(t.Context()), "a stop with no reason is not a kill or a pause")
			requireHeld(t, released)

			s.SetStopReason(reason)
			require.NoError(t, s.Stop(t.Context()))
			<-released
		})
	}

	// A kill can reach the sandbox between its exit and the hold.
	t.Run("a kill before the hold releases it at once", func(t *testing.T) {
		t.Parallel()

		s := exitedSandbox(t)
		s.SetStopReason(StopReasonKilled)
		require.NoError(t, s.Stop(t.Context()))

		<-s.HoldPoweredOff()
	})

	t.Run("a second hold shares the first", func(t *testing.T) {
		t.Parallel()

		s := exitedSandbox(t)
		first := s.HoldPoweredOff()
		second := s.HoldPoweredOff()
		requireHeld(t, second)

		s.SetStopReason(StopReasonKilled)
		require.NoError(t, s.Stop(t.Context()))
		<-first
		<-second
	})
}

// exportingRootfs stands in for the overlay of a sandbox whose Firecracker
// exited: its export closes the sandbox, then writes one dirty block.
type exportingRootfs struct {
	rootfs.Provider

	cache *block.Cache
}

func (r exportingRootfs) ExportDiff(ctx context.Context, out *os.File, closeSandbox func(context.Context) error) (*header.DiffMetadata, error) {
	if err := closeSandbox(ctx); err != nil {
		return nil, err
	}

	m, err := r.cache.ExportToDiff(ctx, out)
	if err != nil {
		return nil, err
	}

	return m, r.cache.Close()
}

type headerDevice struct {
	block.ReadonlyDevice

	header *header.Header
}

func (d headerDevice) Header() *header.Header { return d.header }

type rootfsTemplate struct {
	template.Template

	rootfs block.ReadonlyDevice
}

func (t rootfsTemplate) Rootfs() (block.ReadonlyDevice, error) { return t.rootfs, nil }

// A pause of a powered-off sandbox touches neither envd nor Firecracker: it
// exports the disk, closing the sandbox, and produces a filesystem-only
// snapshot with no snapfile and no memory, not marked quiesced.
func TestPausePoweredOff(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	size := 4 * blockSize

	cache, err := block.NewCache(size, blockSize, t.TempDir()+"/cow", false)
	require.NoError(t, err)
	dirty := make([]byte, blockSize)
	for i := range dirty {
		dirty[i] = 0x7E
	}
	_, err = cache.WriteAt(dirty, 2*blockSize)
	require.NoError(t, err)

	originalHeader, err := header.NewHeader(header.NewTemplateMetadata(uuid.New(), uint64(blockSize), uint64(size)), nil)
	require.NoError(t, err)

	closed := false
	sandboxCleanup := NewCleanup()
	sandboxCleanup.AddNoContext(t.Context(), func() error {
		closed = true

		return nil
	})

	s := &Sandbox{
		Metadata:  &Metadata{},
		Resources: &Resources{rootfs: exportingRootfs{cache: cache}},
		Template:  rootfsTemplate{rootfs: headerDevice{header: originalHeader}},
		config: cfg.BuilderConfig{
			DefaultCacheDir: t.TempDir(),
			StorageConfig:   storage.Config{TemplateCacheDir: t.TempDir()},
		},
		cleanup: sandboxCleanup,
	}
	s.HoldPoweredOff()

	buildID := uuid.New()
	m := metadata.Template{Template: metadata.TemplateMetadata{BuildID: buildID.String()}}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// A memory pause is asked for; the sandbox has no memory left to keep.
	snapshot, err := s.Pause(ctx, m, SnapshotUseCasePause)
	require.NoError(t, err)
	t.Cleanup(func() { _ = snapshot.Close(context.WithoutCancel(t.Context())) })

	assert.True(t, closed, "the export closes the sandbox")
	assert.True(t, snapshot.FilesystemSnapshot)
	assert.IsType(t, &template.NoopFile{}, snapshot.Snapfile)
	assert.Equal(t, buildID, snapshot.BuildID)
	assert.Equal(t, buildID.String(), snapshot.Metadata.Template.BuildID, "the node caches and advertises it under its build")
	assert.True(t, snapshot.Metadata.IsFilesystemOnly())

	memHeader, err := snapshot.MemorySnapshot.DiffHeader.Wait()
	require.NoError(t, err)
	assert.Nil(t, memHeader, "no memory diff")

	rootfsHeader, err := snapshot.RootfsDiffHeader.Wait()
	require.NoError(t, err)
	require.NotNil(t, rootfsHeader)
	assert.Equal(t, buildID, rootfsHeader.Metadata.BuildId)

	diffPath, err := snapshot.RootfsDiff.CachePath(ctx)
	require.NoError(t, err)
	exported, err := os.ReadFile(diffPath)
	require.NoError(t, err)
	assert.Equal(t, dirty, exported, "the diff holds the dirty block")

	written, err := metadata.FromFile(snapshot.Metafile.Path())
	require.NoError(t, err)
	assert.True(t, written.IsFilesystemOnly())
	assert.False(t, written.IsFsQuiesced(), "only the guest knows its filesystems were shut down")
}

// A capture that keeps the sandbox running is refused once the guest powered
// off: the held sandbox and its disk stay for the pause or kill that ends it.
func TestPausePoweredOffRefusesAMaintainedCapture(t *testing.T) {
	t.Parallel()

	closed := false
	sandboxCleanup := NewCleanup()
	sandboxCleanup.AddNoContext(t.Context(), func() error {
		closed = true

		return nil
	})
	s := &Sandbox{Metadata: &Metadata{}, Resources: &Resources{}, cleanup: sandboxCleanup}
	released := s.HoldPoweredOff()

	_, err := s.Pause(t.Context(), metadata.Template{}, SnapshotUseCasePause, WithMaintainSandbox())
	require.ErrorIs(t, err, ErrPoweredOffKeepsNothingRunning)
	assert.False(t, closed, "the sandbox is not closed")
	assert.True(t, s.GuestPoweredOff(), "the hold stays")
	select {
	case <-released:
		t.Fatal("the refusal released the hold")
	default:
	}
}

// A capture whose caller resumes it from memory is refused once the guest
// powered off, rather than downgraded to the disk-only capture.
func TestPausePoweredOffRefusesAMemoryRequiredCapture(t *testing.T) {
	t.Parallel()

	closed := false
	sandboxCleanup := NewCleanup()
	sandboxCleanup.AddNoContext(t.Context(), func() error {
		closed = true

		return nil
	})
	s := &Sandbox{Metadata: &Metadata{}, Resources: &Resources{}, cleanup: sandboxCleanup}
	s.HoldPoweredOff()

	_, err := s.Pause(t.Context(), metadata.Template{}, SnapshotUseCasePause, WithMemoryRequired())
	require.ErrorIs(t, err, ErrPoweredOffHasNoMemory)
	assert.False(t, closed, "no disk-only capture closed the sandbox")
	assert.True(t, s.GuestPoweredOff())
}
