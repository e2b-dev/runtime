//go:build linux

package nbd

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// ctrlConn is a fake NBD socket driving the real Dispatch.Handle loop.
//
//   - Read serves queued request bytes (one queued slice per Read), blocking
//     until a request is queued (or reqCh is closed -> io.EOF).
//   - Write fires firstWrite once, then blocks until gate is closed, simulating
//     a full socket send buffer (the kernel not draining replies).
type ctrlConn struct {
	reqCh      chan []byte
	rbuf       []byte
	gate       chan struct{}
	firstWrite chan struct{}
	fwOnce     sync.Once
}

func (c *ctrlConn) Read(p []byte) (int, error) {
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

func (c *ctrlConn) Write(p []byte) (int, error) {
	c.fwOnce.Do(func() { close(c.firstWrite) })
	<-c.gate

	return len(p), nil
}

// stallProv is a minimal Provider. It records the offsets passed to ReadAt so a
// test can tell whether the read loop served a particular request, and signals
// the first WriteZeroesAt call.
type stallProv struct {
	mu     sync.Mutex
	seen   map[int64]bool
	wz     chan struct{}
	wzOnce sync.Once
}

func (m *stallProv) ReadAt(_ context.Context, p []byte, off int64) (int, error) {
	m.mu.Lock()
	m.seen[off] = true
	m.mu.Unlock()

	return len(p), nil
}

func (m *stallProv) sawRead(off int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.seen[off]
}

func (m *stallProv) Size(_ context.Context) (int64, error)  { return 1 << 40, nil }
func (m *stallProv) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

func (m *stallProv) WriteZeroesAt(_, length int64) (int, error) {
	m.wzOnce.Do(func() { close(m.wz) })

	return int(length), nil
}

func nbdRequest(typ uint16, handle, from uint64, length uint32) []byte {
	b := make([]byte, 28)
	binary.BigEndian.PutUint32(b[0:], NBDRequestMagic)
	binary.BigEndian.PutUint16(b[4:], 0) // flags
	binary.BigEndian.PutUint16(b[6:], typ)
	binary.BigEndian.PutUint64(b[8:], handle)
	binary.BigEndian.PutUint64(b[16:], from)
	binary.BigEndian.PutUint32(b[24:], length)

	return b
}

func waitForRead(p *stallProv, off int64, attempts int) bool {
	for range attempts {
		if p.sawRead(off) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return p.sawRead(off)
}

// TestDispatchWriteZeroesReadLoopStall pins the head-of-line stall the
// nbd-async-write-zeroes flag fixes.
//
// A READ reply writer is made to block inside writeResponse while holding
// writeLock (simulating a full socket send buffer). A WRITE_ZEROES is then
// dispatched:
//   - inline (asyncWriteZeroes=false): cmdWriteZeroes runs on the read loop and
//     blocks acquiring writeLock, so the loop stops serving new requests.
//   - async (asyncWriteZeroes=true): cmdWriteZeroes runs in a goroutine, so the
//     read loop keeps serving.
//
// In both modes, once the blocked reply drains the loop must make progress
// again (matches the transient, self-clearing incidents this fix targets).
func TestDispatchWriteZeroesReadLoopStall(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		asyncWriteZeroes bool
		wantStall        bool
	}{
		{name: "inline_stalls_read_loop", asyncWriteZeroes: false, wantStall: true},
		{name: "async_keeps_read_loop_alive", asyncWriteZeroes: true, wantStall: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			conn := &ctrlConn{
				reqCh:      make(chan []byte, 8),
				gate:       make(chan struct{}),
				firstWrite: make(chan struct{}),
			}
			prov := &stallProv{seen: map[int64]bool{}, wz: make(chan struct{})}
			d := NewDispatch(conn, prov, tc.asyncWriteZeroes, logger.L())

			done := make(chan struct{})
			go func() {
				_ = d.Handle(t.Context())
				close(done)
			}()
			// Wind the loop down deterministically at the end: unblock any
			// blocked reply Write, then close the request stream so Handle's
			// Read returns io.EOF and the goroutine exits.
			t.Cleanup(func() {
				select {
				case <-conn.gate:
				default:
					close(conn.gate)
				}
				close(conn.reqCh)
				<-done
			})

			const offProbe = int64(0xABCDEF000)

			// 1) READ -> its async reply writer blocks in Write holding writeLock.
			conn.reqCh <- nbdRequest(NBDCmdRead, 1, 0, 512)
			select {
			case <-conn.firstWrite:
			case <-time.After(2 * time.Second):
				t.Fatal("read reply writer never reached the blocked socket Write")
			}

			// 2) WRITE_ZEROES -> inline blocks the loop on writeLock; async does not.
			conn.reqCh <- nbdRequest(NBDCmdWriteZeroes, 2, 4096, 4096)
			select {
			case <-prov.wz:
			case <-time.After(2 * time.Second):
				t.Fatal("WriteZeroesAt was never called")
			}

			// 3) Probe with a fresh READ; a live loop serves it (records offProbe).
			conn.reqCh <- nbdRequest(NBDCmdRead, 3, uint64(offProbe), 512)

			served := waitForRead(prov, offProbe, 50) // ~500ms
			switch {
			case tc.wantStall && served:
				t.Fatal("expected the read loop to STALL while a reply write is blocked, but it served a new request")
			case !tc.wantStall && !served:
				t.Fatal("expected the read loop to keep serving with async WRITE_ZEROES, but it stalled")
			}

			// Unblock replies; in both modes the loop must then serve the probe.
			close(conn.gate)
			if !waitForRead(prov, offProbe, 300) { // ~3s
				t.Fatal("read loop did not serve the probe after replies drained")
			}
		})
	}
}
