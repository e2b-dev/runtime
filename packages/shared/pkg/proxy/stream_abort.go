package proxy

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	connectStreamContentType = "application/connect+"

	connectFlagEndStream = 0b00000010

	envelopePrefixLen = 5

	// The peer went away rather than refused the call: the one status clients retry on.
	connectCodeUnavailable = "unavailable"
)

// serveStream runs proxy, converting an aborted RPC stream into the protocol's
// own terminal error frame. ReverseProxy answers a mid-body read error with
// panic(http.ErrAbortHandler), so the body stops with no status and the client
// can only report a framing error. A framing that cannot carry an error is
// re-panicked: ending a plain body cleanly would hide the truncation.
func serveStream(w http.ResponseWriter, r *http.Request, proxy http.Handler, message string) {
	sw := &streamWriter{ResponseWriter: w}
	if r.ProtoMajor == 1 && r.ContentLength != 0 {
		sw.body = trackBody(r)
	}

	defer func() {
		recovered := recover()
		sw.settleBody()
		if recovered == nil {
			return
		}

		err, ok := recovered.(error)
		if !ok || !errors.Is(err, http.ErrAbortHandler) || !sw.endStream(message) {
			panic(recovered)
		}
	}()

	proxy.ServeHTTP(sw, r)
}

// streamWriter follows the envelope framing so an aborted response can be closed
// off at a frame boundary.
type streamWriter struct {
	http.ResponseWriter

	// nil when the response framing cannot report an error mid-body.
	endFrame  func(message string) []byte
	envelopes envelopeTracker
	// nil when the request has no body or came over HTTP/2.
	body     *requestBody
	hijacked bool
}

// Unwrap lets http.ResponseController reach Flush and the deadlines. Without
// it a protocol upgrade through the proxy fails.
func (w *streamWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Hijack records that the connection is no longer the server's, so the request
// body is not ours to settle either.
func (w *streamWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	w.hijacked = true

	return hijackedConn{Conn: conn}, rw, nil
}

// hijackedConn hides the server connection's CloseWrite from the reverse proxy:
// an upgraded connection ends with a close on both sides, as before, instead of
// a half-close that waits for the other side.
type hijackedConn struct {
	net.Conn
}

func (w *streamWriter) WriteHeader(status int) {
	if status == http.StatusOK {
		w.endFrame = endFrameFor(w.Header().Get("Content-Type"))
	}

	// With full duplex the server no longer closes the connection itself over a body the handler left unread.
	if status >= http.StatusOK && w.body != nil && !w.body.finished() {
		w.Header().Set("Connection", "close")
	}

	w.ResponseWriter.WriteHeader(status)
}

func (w *streamWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	if w.endFrame != nil {
		w.envelopes.advance(b[:n])
	}

	return n, err
}

// settleBody finishes with the request body before the handler returns, without
// waiting on the client for any of it. The server cannot take over an
// unfinished body: one that reaches EOF after the handler panics the
// connection's next request with "invalid concurrent Body.Read call", and a
// transport read it finds in flight is aborted in a way that cancels the
// connection's context. The rest of the body is left on the wire the way the
// server leaves a body too large to drain: the connection was announced
// closed, and the server half-closes it after the response and gives the
// client its usual grace to read the response before the close resets it.
func (w *streamWriter) settleBody() {
	if w.body == nil || w.hijacked {
		return
	}

	if !w.body.finished() {
		closeUnreadBody(w.ResponseWriter)
	}
	_ = w.body.ReadCloser.Close()
}

// closeUnreadBody keeps an unread HTTP/1 request body from holding the
// server: it fails the read in flight and the close's own drain at once, and
// has the server close the frontend after the response, the documented way,
// since the body left on it would desync the next request.
func closeUnreadBody(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Unix(1, 0))
	_, _ = http.MaxBytesReader(w, io.NopCloser(strings.NewReader("+")), 0).Read(make([]byte, 1))
}

// trackBody has r's HTTP/1 body count what the transport takes of it, once.
func trackBody(r *http.Request) *requestBody {
	if b, ok := r.Body.(*requestBody); ok {
		return b
	}
	b := &requestBody{ReadCloser: r.Body}
	b.remaining.Store(r.ContentLength)
	r.Body = b

	return b
}

// requestBody tracks how much of the request body the transport has taken.
type requestBody struct {
	io.ReadCloser

	remaining atomic.Int64 // negative when the length is unknown
	done      atomic.Bool
}

func (b *requestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.remaining.Load() >= 0 {
		b.remaining.Add(-int64(n))
	}
	if errors.Is(err, io.EOF) {
		b.done.Store(true)
	}

	return n, err
}

// Close does nothing: ReverseProxy closes its outbound body as it returns, and
// closing an HTTP/1 server body waits for a read in flight (golang/go#78939),
// which the transport may still be doing on the client's upload. settleBody
// closes the body once it has cut that read.
func (*requestBody) Close() error { return nil }

// finished reports whether the transport has taken the whole body, so nothing
// of it is left for the server to read after the handler.
func (b *requestBody) finished() bool {
	return b.done.Load() || b.remaining.Load() == 0
}

// endStream reports whether it could close the response off. A half-written
// envelope cannot be completed without inventing message bytes.
func (w *streamWriter) endStream(message string) bool {
	if w.endFrame == nil || !w.envelopes.atBoundary() {
		return false
	}

	frame := w.endFrame(message)
	if frame == nil {
		return false
	}

	_, err := w.ResponseWriter.Write(frame)

	return err == nil
}

// endFrameFor returns nil when the framing cannot report an error once the body
// has started.
func endFrameFor(contentType string) func(message string) []byte {
	if strings.HasPrefix(contentType, connectStreamContentType) {
		return connectEndStreamFrame
	}

	return nil
}

func connectEndStreamFrame(message string) []byte {
	body, err := json.Marshal(connectEndStream{
		Error: &connectEndStreamError{Code: connectCodeUnavailable, Message: message},
	})
	if err != nil {
		return nil
	}

	return envelopeFrame(connectFlagEndStream, body)
}

func envelopeFrame(flag byte, body []byte) []byte {
	frame := make([]byte, envelopePrefixLen, envelopePrefixLen+len(body))
	frame[0] = flag
	binary.BigEndian.PutUint32(frame[1:], uint32(len(body)))

	return append(frame, body...)
}

type connectEndStream struct {
	Error *connectEndStreamError `json:"error,omitempty"`
}

type connectEndStreamError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// envelopeTracker holds the position within the current 5-byte-prefixed frame.
type envelopeTracker struct {
	prefix    [envelopePrefixLen]byte
	prefixLen int
	remaining int64
}

func (t *envelopeTracker) advance(b []byte) {
	for {
		// A frame ends the moment its payload is consumed, which for a
		// zero-length payload is as soon as the prefix is read.
		if t.prefixLen == envelopePrefixLen && t.remaining == 0 {
			t.prefixLen = 0
		}

		if len(b) == 0 {
			return
		}

		if t.prefixLen < envelopePrefixLen {
			read := copy(t.prefix[t.prefixLen:], b)
			t.prefixLen += read
			b = b[read:]

			if t.prefixLen == envelopePrefixLen {
				t.remaining = int64(binary.BigEndian.Uint32(t.prefix[1:]))
			}

			continue
		}

		read := min(int64(len(b)), t.remaining)
		t.remaining -= read
		b = b[read:]
	}
}

// atBoundary reports whether a further frame can be appended.
func (t *envelopeTracker) atBoundary() bool {
	return t.prefixLen == 0 && t.remaining == 0
}
