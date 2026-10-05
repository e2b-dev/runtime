//go:build linux

package uffd

import (
	"context"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type NoopMemory struct {
	size      int64
	blockSize int64

	exit *utils.ErrorOnce
}

var _ MemoryBackend = (*NoopMemory)(nil)

func NewNoopMemory(size, blockSize int64) *NoopMemory {
	return &NoopMemory{
		size:      size,
		blockSize: blockSize,
		exit:      utils.NewErrorOnce(),
	}
}

func (m *NoopMemory) Prefault(_ context.Context, _ int64, _ []byte) (bool, error) {
	return false, nil
}

// DiffMetadata ignores useTrackerDirty: without a UFFD serve loop there is no
// page tracker, so the memory-info RPC is the only source.
func (m *NoopMemory) DiffMetadata(ctx context.Context, f *fc.Process, _ bool) (*header.DiffMetadata, error) {
	diffInfo, err := f.MemoryInfo(ctx, m.blockSize)
	if err != nil {
		return nil, err
	}

	diffInfo.Dirty.AndNot(diffInfo.Empty)

	numberOfPages := header.TotalBlocks(m.size, m.blockSize)

	empty := roaring.Flip(diffInfo.Dirty, 0, uint64(numberOfPages))
	empty.RemoveRange(uint64(numberOfPages), uint64(1)<<32)

	return &header.DiffMetadata{
		Dirty:     diffInfo.Dirty,
		Empty:     empty,
		BlockSize: m.blockSize,
	}, nil
}

func (m *NoopMemory) PrefetchData(_ context.Context) (block.PrefetchData, error) {
	// NoopMemory doesn't track block accesses, so return empty data
	return block.PrefetchData{
		BlockEntries: nil,
		BlockSize:    m.blockSize,
	}, nil
}

func (m *NoopMemory) Start(context.Context) error {
	return nil
}

func (m *NoopMemory) Stop() error {
	m.exit.SetSuccess()

	// This should be idempotent, so no need to return an error if it's already set.
	return nil
}

func (m *NoopMemory) Ready() chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}

func (m *NoopMemory) Exit() *utils.ErrorOnce {
	return m.exit
}

func (m *NoopMemory) Memfd(context.Context) *block.Memfd {
	return nil
}

func (m *NoopMemory) PeekMemfd(context.Context) *block.Memfd {
	return nil
}

// ServeStats returns a zero snapshot: NoopMemory has no UFFD serve loop, so no
// pages are demand-faulted through it.
func (m *NoopMemory) ServeStats() userfaultfd.ServeSnapshot {
	return userfaultfd.ServeSnapshot{}
}

var _ BalloonModeLabeler = (*NoopMemory)(nil)

// SetBalloonMode is a no-op: NoopMemory emits no serve metrics.
func (m *NoopMemory) SetBalloonMode(userfaultfd.BalloonMode) {}
