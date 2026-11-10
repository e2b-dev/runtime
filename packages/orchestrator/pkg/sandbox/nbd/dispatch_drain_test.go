//go:build linux

package nbd

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type nopProv struct{}

func (nopProv) ReadAt(_ context.Context, b []byte, _ int64) (int, error) { return len(b), nil }

func (nopProv) Size(context.Context) (int64, error) { return 0, nil }

func (nopProv) WriteAt(b []byte, _ int64) (int, error) { return len(b), nil }

func (nopProv) WriteZeroesAt(_, length int64) (int, error) { return int(length), nil }

// countingConn counts replies instead of decoding them, so a test can ask how
// many commands had answered by a given moment.
type countingConn struct{ writes atomic.Int64 }

func (c *countingConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *countingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)

	return len(p), nil
}

func TestDispatch_RefusesCommandsAfterDrain(t *testing.T) {
	t.Parallel()

	d := NewDispatch(&countingConn{}, nopProv{}, true, logger.L())
	d.Drain()

	require.ErrorIs(t, d.cmdRead(t.Context(), command{op: opRead, handle: 1}), ErrShuttingDown)
}

// A command admitted while Drain runs must have answered by the time Drain
// returns, or the mount tears the socket down under a live reply.
func TestDispatch_DrainCoversCommandsAdmittedConcurrently(t *testing.T) {
	t.Parallel()

	for range 200 {
		conn := &countingConn{}
		d := NewDispatch(conn, nopProv{}, true, logger.L())

		var (
			wg             sync.WaitGroup
			admitted       bool
			repliesAtDrain int64
		)

		start := make(chan struct{})

		wg.Go(func() {
			<-start
			admitted = d.cmdRead(t.Context(), command{op: opRead, handle: 1}) == nil
		})

		wg.Go(func() {
			<-start
			d.Drain()
			repliesAtDrain = conn.writes.Load()
		})

		close(start)
		wg.Wait()

		if admitted {
			require.Equal(t, int64(1), repliesAtDrain,
				"Drain returned before an admitted command had replied")
		}
	}
}
