//go:build linux

package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	nbdReadDuration = utils.Must(meter.Int64Histogram("orchestrator.nbd.dispatch.read.duration",
		metric.WithDescription("Duration of NBD dispatch handler ReadAt calls to the backend."),
		metric.WithUnit("ms"),
	))
	nbdReadConncurent = utils.Must(meter.Int64UpDownCounter("orchestrator.nbd.dispatch.read.concurrent",
		metric.WithDescription("Number of NBD read requests currently waiting for a response. A sustained high value indicates reads stuck in kernel I/O."),
		metric.WithUnit("{read}"),
	))
	nbdReadSuccess   = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "success")))
	nbdReadFailure   = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "failure")))
	nbdReadCancelled = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "cancelled")))
)

var ErrShuttingDown = errors.New("shutting down. Cannot serve any new requests")

var dispatchBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, dispatchBufferSize)

		return &b
	},
}

type Provider interface {
	ReadAt(ctx context.Context, p []byte, off int64) (int, error)
	Size(ctx context.Context) (int64, error)
	io.WriterAt
	WriteZeroesAt(off, length int64) (int, error)
}

const (
	// TODO: Look into optimizing the buffer reads by increasing the buffer size by 28 bytes,
	// to account for a request that is 28 bytes of header + 4MB of data (this seems to be preferred kernel buffer size).
	dispatchBufferSize = 4 * 1024 * 1024
	// https://sourceforge.net/p/nbd/mailman/message/35081223/
	// 32MB is the maximum buffer size for a single request that should be universally supported.
	dispatchMaxWriteBufferSize = 32 * 1024 * 1024
)

// NBD Commands
const (
	NBDCmdRead        = 0
	NBDCmdWrite       = 1
	NBDCmdDisconnect  = 2
	NBDCmdFlush       = 3
	NBDCmdTrim        = 4
	NBDCmdWriteZeroes = 6
)

const (
	NBDRequestMagic  = 0x25609513
	NBDResponseMagic = 0x67446698
)

// NBD Request packet. Wire layout (big-endian, 28 bytes total):
//
//	magic(4) | flags(2) | type(2) | handle(8) | from(8) | length(4)
//
// Spec: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#request-message
//
// Flags carries the NBD_CMD_FLAG_* bits and is intentionally ignored — split
// from Type so a non-zero flag bit doesn't corrupt the command opcode.
type Request struct {
	Magic  uint32
	Flags  uint16
	Type   uint16
	Handle uint64
	From   uint64
	Length uint32
}

// NBD Response packet
type Response struct {
	Magic  uint32
	Error  uint32
	Handle uint64
}

const (
	opRead        = "read"
	opWrite       = "write"
	opWriteZeroes = "write-zeroes"
	opTrim        = "trim"
)

// punchOp names the command a punch came from: a failed WRITE_ZEROES leaves
// data the guest was told was zeroed, a failed TRIM only unreclaimed space.
func punchOp(cmdType uint16) string {
	if cmdType == NBDCmdTrim {
		return opTrim
	}

	return opWriteZeroes
}

// command is one NBD request, distilled from the wire header to what serving
// and reporting it need.
type command struct {
	op     string
	handle uint64
	offset uint64
	length int64
}

// cmdOf distils r under the given op. It takes r by value on purpose: the read
// loop reuses one Request for the next command, while the handlers keep
// serving this one in their own goroutines.
func cmdOf(op string, r Request) command {
	return command{
		op:     op,
		handle: r.Handle,
		offset: r.From,
		length: int64(r.Length),
	}
}

func (c command) fields() []zap.Field {
	return []zap.Field{
		zap.String("nbd_op", c.op),
		zap.Uint64("nbd_handle", c.handle),
		zap.Uint64("nbd_offset", c.offset),
		zap.Int64("nbd_length", c.length),
	}
}

type Dispatch struct {
	fp             io.ReadWriter
	responseHeader []byte
	writeLock      sync.Mutex
	prov           Provider
	// logger is tagged with the backend type name at construction, so the
	// reflection stays out of the per-request path.
	logger           logger.Logger
	pendingResponses sync.WaitGroup
	shuttingDown     bool
	shuttingDownLock sync.Mutex
	fatal            chan error
	// asyncWriteZeroes controls whether WRITE_ZEROES/TRIM commands are handled
	// in a goroutine (true) instead of inline on the read loop (false). Handling
	// them inline lets a blocked reply writer holding writeLock stall the whole
	// read loop; see cmdWriteZeroes.
	asyncWriteZeroes bool
}

func NewDispatch(fp io.ReadWriter, prov Provider, asyncWriteZeroes bool, lg logger.Logger) *Dispatch {
	d := &Dispatch{
		responseHeader:   make([]byte, 16),
		fp:               fp,
		prov:             prov,
		logger:           lg.With(zap.String("nbd_provider", fmt.Sprintf("%T", prov))),
		fatal:            make(chan error, 1),
		asyncWriteZeroes: asyncWriteZeroes,
	}

	binary.BigEndian.PutUint32(d.responseHeader, NBDResponseMagic)

	return d
}

func (d *Dispatch) Drain() {
	d.shuttingDownLock.Lock()
	d.shuttingDown = true
	defer d.shuttingDownLock.Unlock()

	// Wait for any pending responses
	d.pendingResponses.Wait()
}

/**
 * Write a response...
 *
 */
func (d *Dispatch) writeResponse(respError uint32, respHandle uint64, chunk []byte) error {
	d.writeLock.Lock()
	defer d.writeLock.Unlock()

	binary.BigEndian.PutUint32(d.responseHeader[4:], respError)
	binary.BigEndian.PutUint64(d.responseHeader[8:], respHandle)

	_, err := d.fp.Write(d.responseHeader)
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		_, err = d.fp.Write(chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

/**
 * This dispatches incoming NBD requests sequentially to the provider.
 *
 */
func (d *Dispatch) Handle(ctx context.Context) error {
	poolBuf := dispatchBufPool.Get().(*[]byte)
	defer dispatchBufPool.Put(poolBuf)
	buffer := *poolBuf
	wp := 0

	request := Request{}

	for {
		n, err := d.fp.Read(buffer[wp:])
		if err != nil {
			return err
		}
		wp += n

		// Now go through processing complete packets
		rp := 0
		for {
			// Check if there is a fatal error from an async read/write to return
			select {
			case err := <-d.fatal:
				return err
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Make sure we have a complete header
			if wp-rp < 28 {
				break // Try again when we have more data...
			}

			// We can read the neader...

			header := buffer[rp : rp+28]
			request.Magic = binary.BigEndian.Uint32(header)
			request.Flags = binary.BigEndian.Uint16(header[4:6])
			request.Type = binary.BigEndian.Uint16(header[6:8])
			request.Handle = binary.BigEndian.Uint64(header[8:16])
			request.From = binary.BigEndian.Uint64(header[16:24])
			request.Length = binary.BigEndian.Uint32(header[24:28])

			if request.Magic != NBDRequestMagic {
				return errors.New("received invalid MAGIC")
			}

			switch request.Type {
			case NBDCmdDisconnect:
				return nil // All done
			case NBDCmdFlush:
				return errors.New("not supported: Flush")
			case NBDCmdRead:
				rp += 28
				err := d.cmdRead(ctx, cmdOf(opRead, request))
				if err != nil {
					return err
				}
			case NBDCmdWrite:
				rp += 28

				if request.Length > dispatchMaxWriteBufferSize {
					return fmt.Errorf("nbd write request length %d exceeds maximum %d", request.Length, dispatchMaxWriteBufferSize)
				}

				data := make([]byte, request.Length)

				dataCopied := copy(data, buffer[rp:wp])

				rp += dataCopied

				// We need to wait for more data here, otherwise we will deadlock if the buffer is Xmb and the length is Xmb because of the header's extra 28 bytes needed.
				// At the same time we don't want to increase the default buffer size as the max would be 32mb which is too large for hundreds of sandbox connections.

				for dataCopied < int(request.Length) {
					n, err := d.fp.Read(data[dataCopied:])
					if err != nil {
						return fmt.Errorf("nbd write read error: %w", err)
					}

					dataCopied += n

					select {
					case err := <-d.fatal:
						return err
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
				}

				err := d.cmdWrite(ctx, cmdOf(opWrite, request), data)
				if err != nil {
					return err
				}
			case NBDCmdWriteZeroes, NBDCmdTrim:
				// TRIM and WRITE_ZEROES both punch the cache; NO_HOLE is intentionally ignored.
				rp += 28
				err := d.cmdWriteZeroes(ctx, cmdOf(punchOp(request.Type), request))
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("nbd not implemented %d", request.Type)
			}
		}
		// Now we need to move any partial to the start
		if rp != 0 && rp != wp {
			copy(buffer, buffer[rp:wp])
		}
		wp -= rp
	}
}

func (d *Dispatch) cmdRead(ctx context.Context, cmd command) error {
	d.shuttingDownLock.Lock()
	if d.shuttingDown {
		d.shuttingDownLock.Unlock()

		return ErrShuttingDown
	}

	d.pendingResponses.Add(1)
	d.shuttingDownLock.Unlock()

	performRead := func() error {
		// buffered to avoid goroutine leak
		errchan := make(chan error, 1)
		data := make([]byte, cmd.length)

		nbdReadConncurent.Add(ctx, 1)

		go func() {
			start := time.Now()
			err := block.RunFaultSafe(ctx, func() error {
				_, readErr := d.prov.ReadAt(ctx, data, int64(cmd.offset))

				return readErr
			})

			attrs := nbdReadSuccess
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					attrs = nbdReadCancelled
				} else {
					attrs = nbdReadFailure
				}
			}

			nbdReadDuration.Record(ctx, time.Since(start).Milliseconds(), attrs)
			errchan <- err
		}()

		// Wait until either the ReadAt completed, or our context is cancelled...
		var readErr error
		select {
		case <-ctx.Done():
			readErr = ctx.Err()
		case readErr = <-errchan:
		}

		nbdReadConncurent.Add(ctx, -1)

		if readErr != nil {
			// Per-request backend failure: signal it to the NBD client via the
			// response error byte and keep the dispatch loop alive. Only
			// writeResponse errors (dead NBD socket) escalate through d.fatal.
			d.logger.Error(ctx, "nbd backend read failed", append(cmd.fields(), zap.Error(readErr))...)

			return d.writeResponse(1, cmd.handle, []byte{})
		}

		// read was successful
		return d.writeResponse(0, cmd.handle, data)
	}

	go func() {
		err := performRead()
		if err != nil {
			select {
			case d.fatal <- err:
			default:
				d.logger.Error(ctx, "nbd error cmd read", append(cmd.fields(), zap.Error(err))...)
			}
		}
		d.pendingResponses.Done()
	}()

	return nil
}

func (d *Dispatch) cmdWrite(ctx context.Context, cmd command, cmdData []byte) error {
	d.shuttingDownLock.Lock()
	if d.shuttingDown {
		d.shuttingDownLock.Unlock()

		return ErrShuttingDown
	}

	d.pendingResponses.Add(1)
	d.shuttingDownLock.Unlock()

	performWrite := func() error {
		// buffered to avoid goroutine leak
		errchan := make(chan error, 1)
		go func() {
			// Even a write can fault: a store to a non-resident page pages
			// it in first.
			errchan <- block.RunFaultSafe(ctx, func() error {
				_, writeErr := d.prov.WriteAt(cmdData, int64(cmd.offset))

				return writeErr
			})
		}()

		// Wait until either the WriteAt completed, or our context is cancelled...
		var writeErr error
		select {
		case <-ctx.Done():
			writeErr = ctx.Err()
		case err := <-errchan:
			writeErr = err
		}

		if writeErr != nil {
			d.logger.Error(ctx, "nbd backend write failed", append(cmd.fields(), zap.Error(writeErr))...)

			return d.writeResponse(1, cmd.handle, []byte{})
		}

		// write was successful
		return d.writeResponse(0, cmd.handle, []byte{})
	}

	go func() {
		err := performWrite()
		if err != nil {
			select {
			case d.fatal <- err:
			default:
				d.logger.Error(ctx, "nbd error cmd write", append(cmd.fields(), zap.Error(err))...)
			}
		}
		d.pendingResponses.Done()
	}()

	return nil
}

// cmdWriteZeroes handles NBD WRITE_ZEROES and TRIM. The backend WriteZeroesAt
// call is cheap (mmap + bitmap), but the following writeResponse takes the
// shared writeLock and writes to the socket. Running this inline on the read
// loop (asyncWriteZeroes == false) means that if a concurrent reply writer is
// blocked inside writeResponse while holding writeLock (e.g. its socket write
// is blocked on a full send buffer), this command blocks the read loop on
// writeLock, the loop stops draining the socket, and the kernel eventually
// times out the connection (EIO). When asyncWriteZeroes is true the work runs
// in a goroutine like cmdRead/cmdWrite, so the read loop is never blocked.
func (d *Dispatch) cmdWriteZeroes(ctx context.Context, cmd command) error {
	performWriteZeroes := func() error {
		// Run the backend call in a goroutine and select on ctx, mirroring
		// cmdRead/cmdWrite, so a WriteZeroesAt that blocks cannot hang this
		// goroutine (and the pendingResponses drain) during shutdown. The
		// channel is buffered so the goroutine never leaks on the ctx.Done path.
		errchan := make(chan error, 1)
		go func() {
			// The punch-hole fallback clears the mmap in-process, so this
			// can fault like a write.
			errchan <- block.RunFaultSafe(ctx, func() error {
				_, zeroErr := d.prov.WriteZeroesAt(int64(cmd.offset), cmd.length)

				return zeroErr
			})
		}()

		var zeroErr error
		select {
		case <-ctx.Done():
			zeroErr = ctx.Err()
		case zeroErr = <-errchan:
		}

		var respErr uint32
		if zeroErr != nil {
			respErr = 1
			d.logger.Error(ctx, "nbd backend punch failed", append(cmd.fields(), zap.Error(zeroErr))...)
		}

		return d.writeResponse(respErr, cmd.handle, nil)
	}

	if !d.asyncWriteZeroes {
		return performWriteZeroes()
	}

	d.shuttingDownLock.Lock()
	if d.shuttingDown {
		d.shuttingDownLock.Unlock()

		return ErrShuttingDown
	}

	d.pendingResponses.Add(1)
	d.shuttingDownLock.Unlock()

	go func() {
		if err := performWriteZeroes(); err != nil {
			select {
			case d.fatal <- err:
			default:
				d.logger.Error(ctx, "nbd error cmd punch", append(cmd.fields(), zap.Error(err))...)
			}
		}

		d.pendingResponses.Done()
	}()

	return nil
}
