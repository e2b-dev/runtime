//go:build linux

package sandbox

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// zeroOriginalMemfile is an all-zero base memfile, so dedup keeps every dirty
// page of a random memfd.
type zeroOriginalMemfile struct{ size int64 }

func (z zeroOriginalMemfile) ReadAt(_ context.Context, p []byte, off int64) (int, error) {
	if off >= z.size {
		return 0, io.EOF
	}
	clear(p)

	return len(p), nil
}

func (z zeroOriginalMemfile) Slice(_ context.Context, _, length int64) ([]byte, error) {
	return make([]byte, length), nil
}

func (z zeroOriginalMemfile) Size(context.Context) (int64, error) { return z.size, nil }
func (zeroOriginalMemfile) Close() error                          { return nil }
func (zeroOriginalMemfile) BlockSize() int64                      { return int64(header.PageSize) }
func (zeroOriginalMemfile) Header() *header.Header                { return nil }
func (zeroOriginalMemfile) SwapHeader(*header.Header)             {}

// deadStructureOutcomeTotal reads the cumulative dead-structure outcome count
// for one structure and outcome from the package's process-wide reader.
func deadStructureOutcomeTotal(t *testing.T, structure, outcome string) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorDeadStructureOutcomeCounterName) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				if hasAttr(dp.Attributes, "structure", structure) && hasAttr(dp.Attributes, "outcome", outcome) {
					total += dp.Value
				}
			}
		}
	}

	return total
}

// processMemorySamples counts the process_memory.duration samples recorded
// so far with the given balloon_mode, across every other attribute.
func processMemorySamples(t *testing.T, balloonMode string) uint64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	var total uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.SnapshotProcessMemoryDurationName) {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, dp := range hist.DataPoints {
				if hasAttr(dp.Attributes, "balloon_mode", balloonMode) {
					total += dp.Count
				}
			}
		}
	}

	return total
}

// The index-free flag is read in processMemorySnapshot and acted on deep
// inside the block package, three calls away. Nothing but this test notices
// if a hop drops it: the index would simply never be freed. Drive the real
// memfd dedup path from pauseProcessMemory with the flag's value set and
// require the release to free the index. No other test in this package runs
// a dedup, so the process-wide counter moves only for this one.
func TestPauseProcessMemory_CarriesFreeIndexToRelease(t *testing.T) {
	t.Parallel()

	ps := int64(header.PageSize)
	const numPages = 16
	size := ps * numPages

	fd, err := unix.MemfdCreate("process-memory-test", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, size))
	data := make([]byte, size)
	_, err = rand.Read(data)
	require.NoError(t, err)
	_, err = unix.Pwrite(fd, data, 0)
	require.NoError(t, err)
	memfd, err := block.NewFromFd(fd)
	require.NoError(t, err)

	originalHeader, err := header.NewHeader(&header.Metadata{
		Version:     3,
		BlockSize:   uint64(ps),
		Size:        uint64(size),
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}, nil)
	require.NoError(t, err)

	dirty := roaring.New()
	dirty.AddRange(0, numPages)

	before := deadStructureOutcomeTotal(t, "dedup_index", "dropped")
	samplesBefore := processMemorySamples(t, "none")
	diff, diffHeader, _, _, swapDone, err := pauseProcessMemory(
		t.Context(),
		uuid.New(),
		originalHeader,
		&header.DiffMetadata{Dirty: dirty, Empty: roaring.New(), BlockSize: ps},
		t.TempDir(),
		(*fc.Process)(nil),
		memfd,
		false,
		zeroOriginalMemfile{size: size},
		block.DedupOptions{},
		true,
		true,
		false,
		"none",
		"",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = diff.Close() })
	assert.Equal(t, samplesBefore+1, processMemorySamples(t, "none"), "the process_memory sample carries the balloon_mode it was handed")

	_, err = diffHeader.WaitWithContext(t.Context())
	require.NoError(t, err)
	require.NotNil(t, swapDone, "inflight serving must build a provisional source here")
	swapDone()

	require.Eventually(t, func() bool {
		return deadStructureOutcomeTotal(t, "dedup_index", "dropped") == before+1
	}, 15*time.Second, time.Millisecond, "the memfd release never freed the index")
}

// failingOriginalMemfile is a base memfile whose reads fail, as a parent fetch that times out.
type failingOriginalMemfile struct{ zeroOriginalMemfile }

func (failingOriginalMemfile) ReadAt(context.Context, []byte, int64) (int, error) {
	return 0, errors.New("parent fetch timed out")
}

func (failingOriginalMemfile) Slice(context.Context, int64, int64) ([]byte, error) {
	return nil, errors.New("parent fetch timed out")
}

// A dedup that keeps the raw diff on failure does so on a memfd-backed guest
// too, as it does after a Firecracker export. Not parallel: the dedup index
// counter above counts only its own test's dedup.
func TestPauseProcessMemory_MemfdDedupKeepsTheRawDiff(t *testing.T) { //nolint:paralleltest // shares the dedup index counter with the test above
	ps := int64(header.PageSize)
	const numPages = 16
	size := ps * numPages

	fd, err := unix.MemfdCreate("process-memory-forced-dedup", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, size))
	data := make([]byte, size)
	_, err = rand.Read(data)
	require.NoError(t, err)
	_, err = unix.Pwrite(fd, data, 0)
	require.NoError(t, err)
	memfd, err := block.NewFromFd(fd)
	require.NoError(t, err)

	originalHeader, err := header.NewHeader(&header.Metadata{
		Version:     3,
		BlockSize:   uint64(ps),
		Size:        uint64(size),
		BuildId:     uuid.New(),
		BaseBuildId: uuid.New(),
	}, nil)
	require.NoError(t, err)

	dirty := roaring.New()
	dirty.AddRange(0, numPages)

	diff, diffHeader, _, _, swapDone, err := pauseProcessMemory(
		t.Context(),
		uuid.New(),
		originalHeader,
		&header.DiffMetadata{Dirty: dirty, Empty: roaring.New(), BlockSize: ps},
		t.TempDir(),
		(*fc.Process)(nil),
		memfd,
		false,
		failingOriginalMemfile{zeroOriginalMemfile{size: size}},
		block.DedupOptions{KeepRawOnError: true},
		false,
		false,
		false,
		"none",
		"",
	)
	require.NoError(t, err, "a failed dedup keeps the raw diff instead of failing the pause")
	t.Cleanup(func() { _ = diff.Close() })
	assert.Nil(t, swapDone, "no provisional source without in-flight serving")
	_, err = diffHeader.WaitWithContext(t.Context())
	require.NoError(t, err)

	got := make([]byte, size)
	_, err = diff.ReadAt(t.Context(), got, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, data, got, "the raw diff holds every dirty page")
}
