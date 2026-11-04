package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

type envelope struct {
	flag    byte
	payload string
}

// cleanEndFrame ends a Connect stream without an error.
func cleanEndFrame() []byte {
	return envelopeFrame(connectFlagEndStream, []byte("{}"))
}

func splitEnvelopes(t *testing.T, raw []byte) []envelope {
	t.Helper()

	var frames []envelope
	for len(raw) > 0 {
		require.GreaterOrEqual(t, len(raw), envelopePrefixLen, "truncated envelope prefix")
		size := int(binary.BigEndian.Uint32(raw[1:envelopePrefixLen]))
		require.GreaterOrEqual(t, len(raw), envelopePrefixLen+size, "truncated envelope payload")
		frames = append(frames, envelope{flag: raw[0], payload: string(raw[envelopePrefixLen : envelopePrefixLen+size])})
		raw = raw[envelopePrefixLen+size:]
	}

	return frames
}

// A sandbox can answer before the proxy has relayed the whole request body; the
// proxy must keep relaying it after the response has started.
func TestProxyRelaysRequestBodyAfterSandboxAnswers(t *testing.T) { //nolint:paralleltest // GOMAXPROCS is process-wide
	// One P fixes the order in which the two readers of the request body run.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	backendURL := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", connectStreamContentType+"proto")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(dataEnvelope("start"))
		_ = http.NewResponseController(w).Flush()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			_, _ = w.Write(dataEnvelope("request body: " + err.Error()))
		}
		_, _ = w.Write(dataEnvelope(fmt.Sprintf("received %d bytes", len(body))))
		_, _ = w.Write(cleanEndFrame())
	})

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "sbx-1",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "full-duplex-backend",
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer conn.Close()

	message := bytes.Repeat([]byte("m"), streamRequestSize)
	_, err = fmt.Fprintf(conn, "POST /process.Process/Start HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nContent-Type: %sproto\r\nContent-Length: %d\r\n\r\n", connectStreamContentType, len(message))
	require.NoError(t, err)

	// Longer than the mutex starvation threshold, which fixes the order of the body's two readers.
	for _, half := range [][]byte{message[:len(message)/2], message[len(message)/2:]} {
		time.Sleep(20 * time.Millisecond)
		_, err = conn.Write(half)
		require.NoError(t, err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	frames := splitEnvelopes(t, raw)
	require.NotEmpty(t, frames)
	assert.Contains(t, frames, envelope{payload: fmt.Sprintf("received %d bytes", len(message))}, "the sandbox should get the whole request")

	last := frames[len(frames)-1]
	assert.Equal(t, byte(connectFlagEndStream), last.flag)
	assert.JSONEq(t, "{}", last.payload, "the stream should end without an error")
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// startProxy serves a proxy that dials each sandbox once and keeps its server
// log. Keep-alives stay on: with them off the sandbox closes right after an
// early answer, and the transport may report the reset of the body it was
// still writing instead of the answer.
func startProxy(t *testing.T, getDestination func(r *http.Request) (*pool.Destination, error)) (int, *lockedBuffer) {
	t.Helper()

	_, port, serverLog, _ := startLimitedProxy(t, getDestination)

	return port, serverLog
}

// startLimitedProxy also counts connections per sandbox, without a limit.
func startLimitedProxy(t *testing.T, getDestination func(r *http.Request) (*pool.Destination, error)) (*Proxy, int, *lockedBuffer, *connlimit.ConnectionLimiter) {
	t.Helper()

	return startProxyWithIdleTimeout(t, getDestination, 20*time.Second)
}

// startProxyWithIdleTimeout serves a proxy whose upstream idle timeout is
// idleTimeout and whose server idle timeout is twice that, so a test can let
// both fire.
func startProxyWithIdleTimeout(t *testing.T, getDestination func(r *http.Request) (*pool.Destination, error), idleTimeout time.Duration) (*Proxy, int, *lockedBuffer, *connlimit.ConnectionLimiter) {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	limiter := connlimit.NewConnectionLimiter()
	limits := &ConnectionLimitConfig{Limiter: limiter, GetMaxLimit: func(context.Context) int { return -1 }}
	serverLog := &lockedBuffer{}
	port := listener.Addr().(*net.TCPAddr).Port
	proxy := New(uint16(port), 1, idleTimeout, getDestination, limits, false, nil)
	proxy.IdleTimeout = 2 * idleTimeout
	proxy.ErrorLog = log.New(serverLog, "", 0)
	go func() {
		_ = proxy.Serve(listener)
	}()
	t.Cleanup(func() { _ = proxy.Close() })

	return proxy, port, serverLog, limiter
}

func destinationFor(target *url.URL, connectionKey string) func(*http.Request) (*pool.Destination, error) {
	return func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           target,
			SandboxId:     "sbx-1",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: connectionKey,
		}, nil
	}
}

// fullDuplexSandbox serves handle with full duplex enabled, so it can answer
// before it has read the request body. Its log is dropped: a fake that leaves
// a body unread makes its own net/http panic the connection's next request,
// which is the fake's doing and not the proxy's.
func fullDuplexSandbox(t *testing.T, handle func(w http.ResponseWriter, r *http.Request)) *url.URL {
	t.Helper()

	sandbox := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = http.NewResponseController(w).EnableFullDuplex()
		handle(w, r)
	}))
	sandbox.Config.ErrorLog = log.New(io.Discard, "", 0)
	sandbox.Start()
	t.Cleanup(sandbox.Close)

	target, err := url.Parse(sandbox.URL)
	require.NoError(t, err)

	return target
}

// rawSandbox hands every request on a connection to serve once its head has
// been read; serve returns whether the connection can take another.
func rawSandbox(t *testing.T, serve func(req *http.Request, conn net.Conn, reader *bufio.Reader) bool) *url.URL {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go func() {
				defer conn.Close()

				reader := bufio.NewReader(conn)
				for {
					req, err := http.ReadRequest(reader)
					if err != nil || !serve(req, conn, reader) {
						return
					}
				}
			}()
		}
	}()

	target, err := url.Parse("http://" + listener.Addr().String())
	require.NoError(t, err)

	return target
}

// rejectingSandbox answers 413 once bodyBeforeAnswer bytes of the body have
// arrived, then reads the rest of the body for the next request on the connection.
func rejectingSandbox(t *testing.T, bodyBeforeAnswer func(contentLength int64) int64) *url.URL {
	t.Helper()

	return rawSandbox(t, func(req *http.Request, conn net.Conn, reader *bufio.Reader) bool {
		before := bodyBeforeAnswer(req.ContentLength)
		if _, err := io.CopyN(io.Discard, reader, before); err != nil {
			return false
		}

		_, _ = io.WriteString(conn, "HTTP/1.1 413 Request Entity Too Large\r\nContent-Length: 0\r\n\r\n")
		_, err := io.CopyN(io.Discard, reader, req.ContentLength-before)

		return err == nil
	})
}

// dialProxy opens a client connection whose deadline bounds every wait in the test.
func dialProxy(t *testing.T, port int) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	return conn, bufio.NewReader(conn)
}

// readFinalResponse skips the interim 100 Continue a proxy may relay.
func readFinalResponse(t *testing.T, reader *bufio.Reader) *http.Response {
	t.Helper()

	for {
		resp, err := http.ReadResponse(reader, nil)
		require.NoError(t, err, "the final response should arrive without the request body")
		if resp.StatusCode != http.StatusContinue {
			return resp
		}
	}
}

// assertConnectionClosed accepts a reset as well as EOF: a server that closes
// over request bytes it has not read resets the connection.
func assertConnectionClosed(t *testing.T, err error, message string) {
	t.Helper()

	require.Error(t, err, message)
	assert.NotErrorIs(t, err, os.ErrDeadlineExceeded, "%s, but the read timed out instead", message)
}

// A sandbox that rejects an upload before it has fully arrived must get its
// answer to the client at once, and the client must be told that the
// connection closes: however little of the upload is left, the proxy does not
// wait for it.
func TestProxyAnswersBeforeARejectedUploadHasEnded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		upload int
		// The part of the upload the client sends before it looks for an answer.
		sent int
		// Whether the client finishes the upload once it has the answer.
		finishUpload bool
	}{
		{name: "a byte short of the upload", upload: 300 << 10, sent: 300<<10 - 1, finishUpload: true},
		{name: "most of the upload outstanding", upload: 1 << 20, sent: 64 << 10, finishUpload: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sandbox := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "0")
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				_ = http.NewResponseController(w).Flush()
				_, _ = io.Copy(io.Discard, r.Body)
			})
			port, serverLog := startProxy(t, destinationFor(sandbox, "rejecting-backend"))
			conn, reader := dialProxy(t, port)

			upload := bytes.Repeat([]byte("u"), tt.upload)
			_, err := fmt.Fprintf(conn, "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nContent-Length: %d\r\n\r\n", len(upload))
			require.NoError(t, err)
			_, err = conn.Write(upload[:tt.sent])
			require.NoError(t, err)

			resp, err := http.ReadResponse(reader, nil)
			require.NoError(t, err, "the answer should arrive before the upload has ended")
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
			assert.True(t, resp.Close, "the response should carry Connection: close")

			// The server may have reset the connection by now, which fails the write.
			if tt.finishUpload {
				_, _ = conn.Write(upload[tt.sent:])
			}

			_, err = reader.ReadByte()
			assertConnectionClosed(t, err, "the server should close the connection it announced closing")
			assert.NotContains(t, serverLog.String(), "panic serving")
		})
	}
}

// A sandbox that answers once it has the whole upload must not cost the client
// its connection, whether or not the answer is a refusal.
func TestProxyKeepsConnectionWhenSandboxReadsTheUpload(t *testing.T) {
	t.Parallel()

	sandbox := rejectingSandbox(t, func(contentLength int64) int64 { return contentLength })
	port, serverLog := startProxy(t, destinationFor(sandbox, "reading-sandbox"))
	conn, reader := dialProxy(t, port)

	upload := bytes.Repeat([]byte("u"), 300<<10)
	for range 2 {
		_, err := fmt.Fprintf(conn, "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nContent-Length: %d\r\n\r\n", len(upload))
		require.NoError(t, err)
		_, err = conn.Write(upload)
		require.NoError(t, err)

		resp, err := http.ReadResponse(reader, nil)
		require.NoError(t, err, "the connection should serve both uploads")
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
		assert.False(t, resp.Close, "the connection should be kept for an upload the sandbox read")
	}

	assert.NotContains(t, serverLog.String(), "panic serving")
}

// A client that stops uploading when it sees an error streamed back early, as
// curl does, must still get the end of that response.
func TestProxyEndsAStreamedErrorWhenClientStopsUploading(t *testing.T) {
	t.Parallel()

	sandbox := fullDuplexSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "denied")
		_ = http.NewResponseController(w).Flush()
	})
	port, serverLog := startProxy(t, destinationFor(sandbox, "streaming-error-backend"))
	conn, reader := dialProxy(t, port)

	upload := bytes.Repeat([]byte("u"), 200<<10)
	_, err := fmt.Fprintf(conn, "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nContent-Length: %d\r\n\r\n", len(upload))
	require.NoError(t, err)
	_, err = conn.Write(upload[:len(upload)/2])
	require.NoError(t, err)

	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.True(t, resp.Close, "the response should carry Connection: close")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "the response should end without the rest of the upload")
	assert.Equal(t, "denied", string(body))
	assert.NotContains(t, serverLog.String(), "panic serving")
}

// A client that sends its body only once it has the server's verdict must get
// the final response when the sandbox rejects the request without reading it,
// and the proxy must close the connection without waiting for that body.
func TestProxyAnswersBeforeAnExpectedBodyIsSent(t *testing.T) {
	t.Parallel()

	sandbox := rejectingSandbox(t, func(int64) int64 { return 0 })
	port, serverLog := startProxy(t, destinationFor(sandbox, "rejecting-sandbox"))
	conn, reader := dialProxy(t, port)

	_, err := io.WriteString(conn, "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nExpect: 100-continue\r\nContent-Length: 10\r\n\r\n")
	require.NoError(t, err)

	resp := readFinalResponse(t, reader)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	assert.True(t, resp.Close, "the response should carry Connection: close")

	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	_, err = reader.ReadByte()
	assertConnectionClosed(t, err, "the server should close the connection without the body")
	assert.NotContains(t, serverLog.String(), "panic serving")
}

// The proxy's own answer when the sandbox cannot be dialled, which is the one
// failure that reads no body at all, closes the connection too: whether the
// client is still waiting for the verdict or has sent its whole body, nothing
// has taken the body, and the proxy does not read it after answering.
func TestProxyAnswersWhenSandboxCannotBeDialled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request string
	}{
		{name: "the client waits for the verdict", request: "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nExpect: 100-continue\r\nContent-Length: 10\r\n\r\n"},
		{name: "the client has sent its body", request: "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nContent-Length: 10\r\n\r\n0123456789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			closed, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			require.NoError(t, closed.Close())
			sandbox, err := url.Parse("http://" + closed.Addr().String())
			require.NoError(t, err)

			port, serverLog := startProxy(t, destinationFor(sandbox, "unreachable-sandbox"))
			conn, reader := dialProxy(t, port)

			_, err = io.WriteString(conn, tt.request)
			require.NoError(t, err)

			resp := readFinalResponse(t, reader)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
			assert.True(t, resp.Close, "the response should carry Connection: close")

			_, err = io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)
			_, err = reader.ReadByte()
			assertConnectionClosed(t, err, "the server should close the connection it announced closing")
			assert.NotContains(t, serverLog.String(), "panic serving")
		})
	}
}

// A client still uploading when a large early answer arrives must get the
// whole answer: the server has to half-close and wait before the close that
// resets the client's upload, or the reset takes the tail of the answer with it.
func TestProxyDeliversWholeAnswerToAClientStillUploading(t *testing.T) {
	t.Parallel()

	answer := bytes.Repeat([]byte("a"), 256<<10)
	sandbox := fullDuplexSandbox(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(answer)))
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(answer)
	})
	_, port, serverLog, limiter := startLimitedProxy(t, destinationFor(sandbox, "large-answer-sandbox"))
	conn, reader := dialProxy(t, port)

	upload := bytes.Repeat([]byte("u"), 200<<10)
	_, err := fmt.Fprintf(conn, "POST /files HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nContent-Length: %d\r\n\r\n", len(upload))
	require.NoError(t, err)
	sent := 16 << 10
	_, err = conn.Write(upload[:sent])
	require.NoError(t, err)

	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.True(t, resp.Close, "the response should carry Connection: close")

	// The slot is released when the handler returns, which is when the server
	// decides how to close. Most of the answer is still on the server's side,
	// and the client sends more of its upload before it reads any of it: a
	// server that has closed already answers the upload with a reset, and
	// the reset takes the rest of the answer with it.
	require.Eventually(t, func() bool { return limiter.Count("large-answer-sandbox") == 0 }, 5*time.Second, time.Millisecond)
	for range 2 {
		// A reset fails the second write, which is the case being tested.
		_, _ = conn.Write(upload[sent : sent+1<<10])
		sent += 1 << 10
		time.Sleep(time.Millisecond)
	}
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "the client should get the whole answer, not a reset after %d of %d bytes", len(body), len(answer))
	assert.Len(t, body, len(answer))
	assert.NotContains(t, serverLog.String(), "panic serving")
}

const upgradeRequest = "GET /ws HTTP/1.1\r\nHost: 49983-sbx-1-00000000.localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"

func upgradeThroughProxy(t *testing.T, port int) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, reader := dialProxy(t, port)
	_, err := io.WriteString(conn, upgradeRequest)
	require.NoError(t, err)

	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

	return conn, reader
}

// An upgraded connection ends on both sides as soon as either side hangs up,
// and its connection-limit slot goes with it. A half-close toward the side
// that stayed would wait for it instead.
func TestProxyEndsAnUpgradedConnectionWhenEitherSideLeaves(t *testing.T) {
	t.Parallel()

	t.Run("the sandbox hangs up", func(t *testing.T) {
		t.Parallel()

		sandbox := rawSandbox(t, func(_ *http.Request, conn net.Conn, _ *bufio.Reader) bool {
			_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")

			return false
		})
		proxy, port, _, limiter := startProxyWithIdleTimeout(t, destinationFor(sandbox, "upgrade-sandbox-leaves"), time.Minute)
		_, reader := upgradeThroughProxy(t, port)

		_, err := reader.ReadByte()
		require.ErrorIs(t, err, io.EOF, "the client should see the end of the upgraded stream")

		// The client stays connected, as one waiting to send would.
		assert.Eventually(t, func() bool { return limiter.Count("upgrade-sandbox-leaves") == 0 }, 3*time.Second, 10*time.Millisecond,
			"the connection-limit slot should be released once the sandbox has hung up")
		assert.Eventually(t, func() bool { return proxy.CurrentServerConnections() == 0 }, 3*time.Second, 10*time.Millisecond,
			"the client connection should be closed once the sandbox has hung up")
		assert.Eventually(t, func() bool { return proxy.CurrentPoolConnections() == 0 }, 3*time.Second, 10*time.Millisecond,
			"the sandbox connection should leave the pool once the sandbox has hung up")
	})

	t.Run("the client hangs up", func(t *testing.T) {
		t.Parallel()

		hold := make(chan struct{})
		t.Cleanup(func() { close(hold) })
		sandbox := rawSandbox(t, func(_ *http.Request, conn net.Conn, _ *bufio.Reader) bool {
			_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
			<-hold

			return false
		})
		proxy, port, _, limiter := startProxyWithIdleTimeout(t, destinationFor(sandbox, "upgrade-client-leaves"), time.Minute)
		conn, _ := upgradeThroughProxy(t, port)
		require.Equal(t, int64(1), limiter.Count("upgrade-client-leaves"))

		require.NoError(t, conn.Close())

		// The sandbox does not react to the end of its input.
		assert.Eventually(t, func() bool { return limiter.Count("upgrade-client-leaves") == 0 }, 3*time.Second, 10*time.Millisecond,
			"the connection-limit slot should be released once the client has hung up")
		assert.Eventually(t, func() bool { return proxy.CurrentPoolConnections() == 0 }, 3*time.Second, 10*time.Millisecond,
			"the sandbox connection should leave the pool once the client has hung up")
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		assert.NoError(t, proxy.Shutdown(ctx), "shutdown should not wait on a connection the client has left")
	})
}

// The size of the one request message a Connect server-stream call sends.
const streamRequestSize = 146

// TestProxyStreamCutStress measures the stream cut through the real server
// stack for STRESS_DURATION, with a sandbox that answers as soon as it has the
// request message. It is for re-measuring the full-duplex dependence by hand,
// so it is skipped unless STRESS_DURATION is set.
func TestProxyStreamCutStress(t *testing.T) { //nolint:paralleltest // a load test
	duration, err := time.ParseDuration(os.Getenv("STRESS_DURATION"))
	if err != nil {
		t.Skip("STRESS_DURATION is not set")
	}

	sandbox := fullDuplexSandbox(t, func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadFull(r.Body, make([]byte, r.ContentLength)); err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		w.Header().Set("Content-Type", connectStreamContentType+"proto")
		w.WriteHeader(http.StatusOK)
		for i := range 3 {
			_, _ = w.Write(dataEnvelope(fmt.Sprintf("event %d", i)))
			_ = http.NewResponseController(w).Flush()
			time.Sleep(2 * time.Millisecond)
		}
		_, _ = w.Write(cleanEndFrame())
	})
	port, serverLog := startProxy(t, destinationFor(sandbox, "stress-sandbox"))

	transport := &http.Transport{MaxIdleConnsPerHost: 64, MaxIdleConns: 256}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}
	message := bytes.Repeat([]byte("m"), streamRequestSize)
	cleanEnd := cleanEndFrame()

	var streams, cuts, failures atomic.Int64
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	for range 48 {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/process.Process/Start", port), bytes.NewReader(message))
				if err != nil {
					failures.Add(1)

					continue
				}
				req.Host = "49983-sbx-1-00000000.localhost"
				req.Header.Set("Content-Type", connectStreamContentType+"proto")

				resp, err := client.Do(req)
				streams.Add(1)
				if err != nil {
					failures.Add(1)

					continue
				}
				raw, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				switch {
				case err != nil:
					failures.Add(1)
				case !bytes.HasSuffix(raw, cleanEnd):
					cuts.Add(1)
				}
			}
		})
	}
	wg.Wait()

	t.Logf("%d streams, %d cut, %d failed, %d server panics", streams.Load(), cuts.Load(), failures.Load(), strings.Count(serverLog.String(), "panic serving"))
	assert.Zero(t, cuts.Load(), "no stream should be cut")
	assert.Zero(t, failures.Load(), "no stream should fail")
}

func TestRequestBodyFinished(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		remaining int64
		done      bool
		want      bool
	}{
		{name: "end of body seen", remaining: 0, done: true, want: true},
		{name: "last byte taken without the end being read", remaining: 0, want: true},
		{name: "a byte outstanding", remaining: 1, want: false},
		{name: "unknown length still open", remaining: -1, want: false},
		{name: "unknown length ended", remaining: -1, done: true, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := &requestBody{}
			body.remaining.Store(tt.remaining)
			body.done.Store(tt.done)
			assert.Equal(t, tt.want, body.finished())
		})
	}
}

// fullDuplexRecorder records whether the handler asked for full duplex.
type fullDuplexRecorder struct {
	*httptest.ResponseRecorder

	enabled bool
}

func (r *fullDuplexRecorder) EnableFullDuplex() error {
	r.enabled = true

	return nil
}

// The connection stays usable to the transport only because the handler asks
// for full duplex; without it the server closes the request body as soon as
// the response starts, and the transport's end-of-body read fails.
func TestHandlerEnablesFullDuplex(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)

	p := pool.New(maxClientConns, 1, time.Second, false, nil)

	recorder := &fullDuplexRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://49983-sbx-1-00000000.localhost/", nil)
	handler(p, destinationFor(backendURL, "full-duplex-pin"), nil).ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusNoContent, recorder.Code)
	assert.True(t, recorder.enabled, "the handler should enable full duplex before proxying")
}
