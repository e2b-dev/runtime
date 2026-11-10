//go:build linux

package build

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// headerSwapUnavailableSource models a released provisional source at File's
// DiffSource boundary. It swaps the file header once, then returns the same
// BytesNotAvailableError that a released MemfdIdentitySource exposes.
type headerSwapUnavailableSource struct {
	size int64
	swap func()
	once sync.Once
}

func (s *headerSwapUnavailableSource) ReadAt(_ []byte, _ int64) (int, error) {
	if s.swap != nil {
		s.once.Do(s.swap)
	}

	return 0, block.BytesNotAvailableError{}
}

func (s *headerSwapUnavailableSource) Slice(_ int64, _ int64) ([]byte, error) {
	if s.swap != nil {
		s.once.Do(s.swap)
	}

	return nil, block.BytesNotAvailableError{}
}

func (s *headerSwapUnavailableSource) Close() error { return nil }

func (s *headerSwapUnavailableSource) Size() (int64, error) { return s.size, nil }

func (s *headerSwapUnavailableSource) FileSize(context.Context) (int64, error) { return 0, nil }

func (s *headerSwapUnavailableSource) BlockSize() int64 { return testBlockSize }

func (s *headerSwapUnavailableSource) Path(context.Context) (string, error) { return "", nil }

type headerSwapBytesSource struct{ data []byte }

func (s *headerSwapBytesSource) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(s.data)) {
		return 0, io.EOF
	}

	n := copy(p, s.data[off:])
	if n != len(p) {
		return n, io.EOF
	}

	return n, nil
}

func (s *headerSwapBytesSource) Slice(off, length int64) ([]byte, error) {
	if off < 0 || off+length > int64(len(s.data)) {
		return nil, io.EOF
	}

	return bytes.Clone(s.data[off : off+length]), nil
}

func (s *headerSwapBytesSource) Close() error { return nil }

func (s *headerSwapBytesSource) Size() (int64, error) { return int64(len(s.data)), nil }

func (s *headerSwapBytesSource) FileSize(context.Context) (int64, error) { return 0, nil }

func (s *headerSwapBytesSource) BlockSize() int64 { return testBlockSize }

func (s *headerSwapBytesSource) Path(context.Context) (string, error) { return "", nil }

func newHeaderSwapReadFile(t *testing.T, swap bool) (*File, []byte) {
	t.Helper()

	fileID := uuid.New()
	provisionalID := uuid.New()
	durableID := uuid.New()
	provisional := buildHeader(t, fileID, testBlockSize, provisionalID)
	durable := buildHeader(t, fileID, testBlockSize, durableID)

	store, err := NewDiffStore(cfg.Config{}, &featureflags.Client{}, t.TempDir(), time.Hour, time.Minute)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	metrics, err := blockmetrics.NewMetrics(noop.NewMeterProvider())
	require.NoError(t, err)
	file := NewFile(provisional, store, Memfile, nil, metrics)

	var headerSwap func()
	if swap {
		headerSwap = func() {
			require.True(t, file.SwapHeaderIfCurrent(provisional, durable))
		}
	}
	oldDiff, err := NewLocalDiffFromCache(
		GetDiffStoreKey(provisionalID.String(), Memfile),
		&headerSwapUnavailableSource{size: testBlockSize, swap: headerSwap},
	)
	require.NoError(t, err)
	want := bytes.Repeat([]byte{0x5a}, int(testBlockSize))
	newDiff, err := NewLocalDiffFromCache(
		GetDiffStoreKey(durableID.String(), Memfile),
		&headerSwapBytesSource{data: want},
	)
	require.NoError(t, err)
	store.Add(oldDiff)
	store.Add(newDiff)

	return file, want
}

// ReadAt is the dedup caller. A planned provisional source that becomes
// unavailable only because the header moved to the durable source must be
// replanned against the durable logical snapshot.
func TestFileReadAtReplansAfterHeaderSwapOnBytesNotAvailable(t *testing.T) {
	t.Parallel()

	file, want := newHeaderSwapReadFile(t, true)
	got := make([]byte, testBlockSize)
	n, err := file.ReadAt(t.Context(), got, 0)
	require.NoError(t, err)
	require.Equal(t, len(want), n)
	require.Equal(t, want, got)
}

// An unavailable source without a header transition remains a real error; this
// prevents a blanket or unbounded retry from hiding missing bytes.
func TestFileReadAtDoesNotRetryUnchangedBytesNotAvailable(t *testing.T) {
	t.Parallel()

	file, _ := newHeaderSwapReadFile(t, false)
	got := make([]byte, testBlockSize)
	_, err := file.ReadAt(t.Context(), got, 0)
	require.ErrorIs(t, err, block.BytesNotAvailableError{})
}
