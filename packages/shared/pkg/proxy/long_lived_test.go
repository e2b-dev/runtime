package proxy

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// Minimal RFC 6455 framing, enough to drive a WebSocket through the proxy
// with every control frame under the test's control.
const (
	wsOpText   byte = 0x1
	wsOpBinary byte = 0x2
	wsOpClose  byte = 0x8
	wsOpPing   byte = 0x9
	wsOpPong   byte = 0xA

	wsHandshakeGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
)

func wsAcceptKey(key string) string {
	sum := sha1.Sum([]byte(key + wsHandshakeGUID))

	return base64.StdEncoding.EncodeToString(sum[:])
}

func wsWriteFrame(w io.Writer, op byte, payload []byte, masked bool) error {
	header := []byte{0x80 | op}
	switch size := len(payload); {
	case size < 126:
		header = append(header, byte(size))
	case size < 1<<16:
		header = append(header, 126, byte(size>>8), byte(size))
	default:
		header = append(header, 127)
		header = binary.BigEndian.AppendUint64(header, uint64(size))
	}
	if masked {
		header[1] |= 0x80
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		header = append(header, key[:]...)
		maskedPayload := make([]byte, len(payload))
		for i, b := range payload {
			maskedPayload[i] = b ^ key[i%4]
		}
		payload = maskedPayload
	}
	_, err := w.Write(append(header, payload...))

	return err
}

func wsReadFrame(r *bufio.Reader) (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	op := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	size := uint64(header[1] & 0x7f)
	switch size {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		size = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		size = binary.BigEndian.Uint64(ext[:])
	}
	var key [4]byte
	if masked {
		if _, err := io.ReadFull(r, key[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}

	return op, payload, nil
}

// wsEchoSandbox upgrades every connection and echoes data frames, answers
// pings with pongs and mirrors a close frame before closing the connection.
func wsEchoSandbox(t *testing.T) *url.URL {
	t.Helper()

	return rawSandbox(t, func(req *http.Request, conn net.Conn, reader *bufio.Reader) bool {
		if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
			return false
		}
		_, err := fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", wsAcceptKey(req.Header.Get("Sec-WebSocket-Key")))
		if err != nil {
			return false
		}
		for {
			op, payload, err := wsReadFrame(reader)
			if err != nil {
				return false
			}
			switch op {
			case wsOpPing:
				op = wsOpPong
			case wsOpClose:
				_ = wsWriteFrame(conn, wsOpClose, payload, false)

				return false
			}
			if err := wsWriteFrame(conn, op, payload, false); err != nil {
				return false
			}
		}
	})
}

// wsDialProxy completes a WebSocket handshake through the proxy and returns the raw connection.
func wsDialProxy(t *testing.T, port int) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, reader := dialProxy(t, port)
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	require.NoError(t, err)
	key := base64.StdEncoding.EncodeToString(nonce[:])
	_, err = fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", key)
	require.NoError(t, err)

	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.Equal(t, wsAcceptKey(key), resp.Header.Get("Sec-WebSocket-Accept"))

	return conn, reader
}

// wsEcho sends a frame and expects it back with the same payload; a ping comes back as a pong.
func wsEcho(t *testing.T, conn net.Conn, reader *bufio.Reader, op byte, payload []byte) {
	t.Helper()

	require.NoError(t, wsWriteFrame(conn, op, payload, true))
	gotOp, got, err := wsReadFrame(reader)
	require.NoError(t, err)
	want := op
	if op == wsOpPing {
		want = wsOpPong
	}
	require.Equal(t, want, gotOp)
	require.True(t, bytes.Equal(payload, got), "payload of %d bytes should come back unchanged", len(payload))
}

// countedProxy is a proxy whose connections and limiter slot can be watched
// until it lets go of all of them.
type countedProxy struct {
	proxy   *Proxy
	port    int
	limiter *connlimit.ConnectionLimiter
}

const countedConnectionKey = "counted-sandbox"

func startCountedProxy(t *testing.T, target *url.URL, idleTimeout time.Duration) countedProxy {
	t.Helper()

	proxy, port, _, limiter := startProxyWithIdleTimeout(t, destinationFor(target, countedConnectionKey), idleTimeout)

	return countedProxy{proxy: proxy, port: port, limiter: limiter}
}

// idleGap is a silence longer than every idle timer the proxy configures.
func (p countedProxy) idleGap() {
	time.Sleep(3 * p.proxy.IdleTimeout)
}

// released waits for the proxy to let go of every connection and slot. The
// upstream connection of a finished request leaves the pool once the short
// idle timeout fires.
func (p countedProxy) released(t *testing.T, message string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return p.proxy.CurrentServerConnections() == 0 && p.proxy.CurrentPoolConnections() == 0 && p.limiter.Count(countedConnectionKey) == 0
	}, 5*time.Second, 10*time.Millisecond, "%s: server=%d pool=%d limiter=%d", message, p.proxy.CurrentServerConnections(), p.proxy.CurrentPoolConnections(), p.limiter.Count(countedConnectionKey))
}

// streamIdleTimeout makes the server's idle timeout 800 ms, so a silence of
// three times it also outlasts any deadline of up to 2.4 s that a change might
// put on a request.
const streamIdleTimeout = 400 * time.Millisecond

// A WebSocket through the proxy must carry text, binary and control frames
// both ways, survive a silence longer than every idle timer the proxy
// configures, and end with a clean close handshake that releases the
// connection and its limiter slot.
func TestProxyRelaysWebSocketAcrossIdleTimeouts(t *testing.T) {
	t.Parallel()

	p := startCountedProxy(t, wsEchoSandbox(t), streamIdleTimeout)
	conn, reader := wsDialProxy(t, p.port)

	wsEcho(t, conn, reader, wsOpText, []byte("hello"))
	wsEcho(t, conn, reader, wsOpBinary, bytes.Repeat([]byte{0, 1, 2, 255}, 20<<10))
	wsEcho(t, conn, reader, wsOpPing, []byte("ping"))
	require.Equal(t, int64(1), p.limiter.Count(countedConnectionKey), "the upgraded connection should hold its slot")

	p.idleGap()
	wsEcho(t, conn, reader, wsOpText, []byte("still here"))

	require.NoError(t, wsWriteFrame(conn, wsOpClose, []byte{0x03, 0xe8}, true))
	op, payload, err := wsReadFrame(reader)
	require.NoError(t, err)
	assert.Equal(t, wsOpClose, op)
	assert.Equal(t, []byte{0x03, 0xe8}, payload)
	_, err = reader.ReadByte()
	require.ErrorIs(t, err, io.EOF, "the connection should end after the close handshake")

	p.released(t, "after the close handshake")
}

// Server-sent events must reach the client as the sandbox emits them, across
// silences longer than every timer the proxy configures: each event after the
// first is released only once the previous one has been read through the
// proxy and an idle gap has passed, so a proxy that buffered or timed out
// would never deliver it.
func TestProxyStreamsServerSentEvents(t *testing.T) {
	t.Parallel()

	const events = 3
	// Buffered so a release never waits on a sandbox that has already left; the next read reports that.
	next := make(chan struct{}, events)
	sandbox := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := range events {
			if i > 0 {
				select {
				case <-next:
				case <-r.Context().Done():
					return
				}
			}
			_, _ = fmt.Fprintf(w, "id: %d\ndata: event %d\n\n", i, i)
			_ = http.NewResponseController(w).Flush()
		}
	})

	p := startCountedProxy(t, sandbox, streamIdleTimeout)
	conn, reader := dialProxy(t, p.port)
	_, err := io.WriteString(conn, "GET /events HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nAccept: text/event-stream\r\n\r\n")
	require.NoError(t, err)
	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	body := bufio.NewReader(resp.Body)
	for i := range events {
		// An event runs up to its blank line.
		var lines []string
		for {
			line, err := body.ReadString('\n')
			require.NoError(t, err, "event %d should arrive before the next one is released", i)
			if line == "\n" {
				break
			}
			lines = append(lines, strings.TrimSpace(line))
		}
		assert.Equal(t, []string{fmt.Sprintf("id: %d", i), fmt.Sprintf("data: event %d", i)}, lines)
		if i < events-1 {
			p.idleGap()
			next <- struct{}{}
		}
	}

	rest, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Empty(t, rest, "the stream should end cleanly")
	assert.False(t, resp.Close, "a stream whose request had no body should keep the connection")
	assert.Zero(t, p.limiter.Count(countedConnectionKey), "the slot should be free once the stream has ended")
	require.NoError(t, conn.Close())
	p.released(t, "after the stream")
}

// A bidirectional HTTP/1 stream: the client keeps sending request chunks
// while the sandbox streams each one back, with a silence longer than every
// timer the proxy configures in the middle. The response announces that the
// connection closes, since its body was still open when the sandbox answered,
// and the stream ends once the client finishes its body.
func TestProxyRelaysBidirectionalHTTP1Stream(t *testing.T) {
	t.Parallel()

	sandbox := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		scanner := bufio.NewScanner(r.Body)
		for scanner.Scan() {
			_, _ = fmt.Fprintf(w, "echo:%s\n", scanner.Text())
			_ = http.NewResponseController(w).Flush()
		}
		_, _ = io.WriteString(w, "end\n")
	})

	p := startCountedProxy(t, sandbox, streamIdleTimeout)
	conn, reader := dialProxy(t, p.port)
	_, err := io.WriteString(conn, "POST /stream HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nTransfer-Encoding: chunked\r\n\r\n")
	require.NoError(t, err)
	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err, "the response should start before the request body has ended")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, resp.Close, "a response to a body still being sent should announce the close")

	body := bufio.NewReader(resp.Body)
	for i := range 5 {
		if i == 2 {
			p.idleGap()
		}
		line := fmt.Sprintf("message %d\n", i)
		_, err = fmt.Fprintf(conn, "%x\r\n%s\r\n", len(line), line)
		require.NoError(t, err)
		echoed, err := body.ReadString('\n')
		require.NoError(t, err, "message %d should be echoed", i)
		assert.Equal(t, "echo:"+line, echoed)
	}

	_, err = io.WriteString(conn, "0\r\n\r\n")
	require.NoError(t, err)
	end, err := body.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "end\n", end)
	rest, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Empty(t, rest)
	_, err = reader.ReadByte()
	require.ErrorIs(t, err, io.EOF, "the connection should close as announced")

	p.released(t, "after the stream")
}

func TestOwnerRetirementIsolatesUpgradesAndSuccessors(t *testing.T) {
	t.Parallel()
	backend := wsEchoSandbox(t)
	var destination atomic.Pointer[pool.Destination]
	proxy, port, _, limiter := startProxyWithIdleTimeout(t, func(*http.Request) (*pool.Destination, error) { return destination.Load(), nil }, time.Minute)
	admit := func(key string) *pool.Owner {
		d, err := destinationFor(backend, key)(nil)
		require.NoError(t, err)
		d.Owner = proxy.NewOwner()
		destination.Store(d)

		return d.Owner
	}
	old := admit("old")
	first, firstReader := wsDialProxy(t, port)
	defer first.Close()
	unrelated := admit("unrelated")
	second, secondReader := wsDialProxy(t, port)
	defer second.Close()
	require.NoError(t, old.Close(t.Context()))
	require.NoError(t, first.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, _, err := wsReadFrame(firstReader)
	require.Error(t, err)
	assert.Zero(t, limiter.Count("old"))
	successor := admit("old")
	third, thirdReader := wsDialProxy(t, port)
	defer third.Close()
	require.NoError(t, old.Close(t.Context()))
	for i, connection := range []net.Conn{second, third} {
		reader := []*bufio.Reader{secondReader, thirdReader}[i]
		require.NoError(t, connection.SetDeadline(time.Now().Add(5*time.Second)))
		require.NoError(t, wsWriteFrame(connection, wsOpText, []byte("still live"), true))
		op, payload, err := wsReadFrame(reader)
		require.NoError(t, err)
		assert.Equal(t, wsOpText, op)
		assert.Equal(t, "still live", string(payload))
	}
	require.NoError(t, unrelated.Close(t.Context()))
	require.NoError(t, successor.Close(t.Context()))
	assert.Zero(t, proxy.CurrentPoolConnections())
	assert.Zero(t, limiter.Count("old"))
	assert.Zero(t, limiter.Count("unrelated"))
}
