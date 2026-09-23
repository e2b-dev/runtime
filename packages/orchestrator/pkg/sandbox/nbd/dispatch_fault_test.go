//go:build linux

package nbd

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// replyConn is a fake NBD socket: Read serves queued request bytes, Write
// parses reply headers onto replies. Assumes header-only replies, which holds
// because every request in this test fails.
type replyConn struct {
	reqCh   chan []byte
	rbuf    []byte
	wbuf    []byte
	replies chan Response
}

func (c *replyConn) Read(p []byte) (int, error) {
	for len(c.rbuf) == 0 {
		b, ok := <-c.reqCh
		if !ok {
			return 0, io.EOF
		}
		c.rbuf = b
	}
	n := copy(p, c.rbuf)
	c.rbuf = c.rbuf[n:]

	return n, nil
}

func (c *replyConn) Write(p []byte) (int, error) {
	c.wbuf = append(c.wbuf, p...)
	for len(c.wbuf) >= 16 {
		c.replies <- Response{
			Magic:  binary.BigEndian.Uint32(c.wbuf[0:4]),
			Error:  binary.BigEndian.Uint32(c.wbuf[4:8]),
			Handle: binary.BigEndian.Uint64(c.wbuf[8:16]),
		}
		c.wbuf = c.wbuf[16:]
	}

	return len(p), nil
}

// faultProv serves requests straight from a block.Cache mmap, like
// block.Overlay does in production.
type faultProv struct {
	cache *block.Cache
}

func (p *faultProv) ReadAt(_ context.Context, b []byte, off int64) (int, error) {
	return p.cache.ReadAt(b, off)
}

func (p *faultProv) Size(_ context.Context) (int64, error) { return p.cache.Size() }

func (p *faultProv) WriteAt(b []byte, off int64) (int, error) { return p.cache.WriteAt(b, off) }

func (p *faultProv) WriteZeroesAt(off, length int64) (int, error) {
	return p.cache.WriteZeroesAt(off, length)
}

// A backend memory fault must become a per-request NBD error reply while the
// dispatch loop keeps serving. If the fault guard regresses, this test
// crashes the whole test binary with "unexpected fault address" — that crash
// is the failure signal.
func TestDispatch_MmapFault(t *testing.T) {
	t.Parallel()

	const (
		blockSize = int64(4096)
		size      = 2 * blockSize
	)

	path := filepath.Join(t.TempDir(), "cache")
	cache, err := block.NewCache(size, blockSize, path, true)
	require.NoError(t, err)
	defer cache.Close()

	// Mapped pages beyond EOF raise SIGBUS on access, like a bad sector.
	require.NoError(t, os.Truncate(path, 0))

	conn := &replyConn{reqCh: make(chan []byte, 8), replies: make(chan Response, 8)}
	d := NewDispatch(conn, &faultProv{cache: cache}, true, logger.L())

	done := make(chan error, 1)
	go func() { done <- d.Handle(t.Context()) }()

	// Non-zero payload: an all-zero write takes the punch-hole path, which
	// does not touch the mapping. A store into the truncated mmap faults
	// like a read.
	payload := make([]byte, blockSize)
	for i := range payload {
		payload[i] = 0xAB
	}

	requests := [][]byte{
		nbdRequest(NBDCmdRead, 1, 0, uint32(blockSize)),
		nbdRequest(NBDCmdRead, 2, 0, uint32(blockSize)),
		append(nbdRequest(NBDCmdWrite, 3, 0, uint32(blockSize)), payload...),
	}
	for handle, req := range requests {
		conn.reqCh <- req
		select {
		case resp := <-conn.replies:
			require.Equal(t, uint32(NBDResponseMagic), resp.Magic)
			require.Equal(t, uint64(handle+1), resp.Handle)
			require.NotZerof(t, resp.Error, "request %d: backend fault must be an NBD error reply", handle+1)
		case <-time.After(5 * time.Second):
			t.Fatalf("no reply for request %d: dispatch loop is stuck or dead", handle+1)
		}
	}

	close(conn.reqCh) // Handle sees io.EOF and exits
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch loop did not exit")
	}
}
