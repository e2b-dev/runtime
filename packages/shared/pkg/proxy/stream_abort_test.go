package proxy

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// dataEnvelope frames a payload as a Connect data frame, the zero-flag case.
func dataEnvelope(payload string) []byte {
	frame := make([]byte, envelopePrefixLen, envelopePrefixLen+len(payload))
	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)))

	return append(frame, payload...)
}

func TestEnvelopeTrackerFindsFrameBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		writes     [][]byte
		atBoundary bool
	}{
		{name: "nothing written", writes: nil, atBoundary: true},
		{name: "one whole frame", writes: [][]byte{dataEnvelope(`{}`)}, atBoundary: true},
		{name: "two whole frames", writes: [][]byte{dataEnvelope(`{"a":1}`), dataEnvelope(`{}`)}, atBoundary: true},
		{name: "empty payload", writes: [][]byte{dataEnvelope("")}, atBoundary: true},
		{name: "partial prefix", writes: [][]byte{{0, 0, 0}}, atBoundary: false},
		{name: "prefix only", writes: [][]byte{dataEnvelope(`{}`)[:envelopePrefixLen]}, atBoundary: false},
		{name: "partial payload", writes: [][]byte{dataEnvelope(`{"a":1}`)[:envelopePrefixLen+2]}, atBoundary: false},
		{
			name:       "frame split across writes",
			writes:     [][]byte{dataEnvelope(`{"a":1}`)[:3], dataEnvelope(`{"a":1}`)[3:]},
			atBoundary: true,
		},
		{
			name:       "whole frame then partial",
			writes:     [][]byte{dataEnvelope(`{}`), dataEnvelope(`{"a":1}`)[:6]},
			atBoundary: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var tracker envelopeTracker
			for _, write := range test.writes {
				tracker.advance(write)
			}

			assert.Equal(t, test.atBoundary, tracker.atBoundary())
		})
	}
}

func TestConnectStreamWriterEndsStreamAtFrameBoundary(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writer := &streamWriter{ResponseWriter: recorder}

	writer.Header().Set("Content-Type", "application/connect+json")
	writer.WriteHeader(http.StatusOK)

	data := dataEnvelope(`{}`)
	_, err := writer.Write(data)
	require.NoError(t, err)

	require.True(t, writer.endStream("the connection to sandbox sbx-1 ended before the stream completed"))

	written := recorder.Body.Bytes()
	require.Greater(t, len(written), len(data)+envelopePrefixLen)

	end := written[len(data):]
	assert.Equal(t, byte(connectFlagEndStream), end[0])
	assert.Equal(t, uint32(len(end)-envelopePrefixLen), binary.BigEndian.Uint32(end[1:envelopePrefixLen]))

	var message connectEndStream
	require.NoError(t, json.Unmarshal(end[envelopePrefixLen:], &message))
	require.NotNil(t, message.Error)
	assert.Equal(t, connectCodeUnavailable, message.Error.Code)
	assert.Equal(t, "the connection to sandbox sbx-1 ended before the stream completed", message.Error.Message)
}

func TestConnectStreamWriterRefusesToEndStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		status      int
		write       []byte
	}{
		{
			name:        "not a Connect stream",
			contentType: "text/plain",
			status:      http.StatusOK,
			write:       []byte("hello"),
		},
		{
			name:        "Connect unary response",
			contentType: "application/proto",
			status:      http.StatusOK,
			write:       []byte("hello"),
		},
		{
			name:        "error status",
			contentType: "application/connect+json",
			status:      http.StatusBadGateway,
			write:       nil,
		},
		{
			name:        "aborted mid frame",
			contentType: "application/connect+json",
			status:      http.StatusOK,
			write:       dataEnvelope(`{"a":1}`)[:envelopePrefixLen+2],
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			writer := &streamWriter{ResponseWriter: recorder}

			writer.Header().Set("Content-Type", test.contentType)
			writer.WriteHeader(test.status)

			if test.write != nil {
				_, err := writer.Write(test.write)
				require.NoError(t, err)
			}

			before := recorder.Body.Len()
			assert.False(t, writer.endStream("the connection to sandbox sbx-1 ended before the stream completed"))
			assert.Equal(t, before, recorder.Body.Len(), "nothing should be appended when the stream cannot be ended")
		})
	}
}

// connectStreamBackend serves one Connect data frame and then holds the
// response open until the test resets the connection under it.
func connectStreamBackend(t *testing.T, sentFirstFrame chan<- struct{}) *url.URL {
	t.Helper()

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/connect+json")
			w.WriteHeader(http.StatusOK)

			_, writeErr := w.Write(dataEnvelope(`{}`))
			assert.NoError(t, writeErr)
			http.NewResponseController(w).Flush()

			close(sentFirstFrame)

			<-r.Context().Done()
		}),
	}

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	backendURL, err := url.Parse("http://" + listener.Addr().String())
	require.NoError(t, err)

	return backendURL
}

// A killed sandbox reaches a Connect client as a status it can act on, rather
// than as a truncated body it can only report as a framing error.
func TestProxyEndsConnectStreamWhenSandboxStops(t *testing.T) {
	t.Parallel()

	sentFirstFrame := make(chan struct{})
	backendURL := connectStreamBackend(t, sentFirstFrame)

	const connectionKey = "connect-stream-backend"

	var owner atomic.Pointer[pool.Owner]
	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "sbx-1",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: connectionKey,
			Owner:         owner.Load(),
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()
	owner.Store(proxy.NewOwner())

	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		testHTTPClient(t),
		fmt.Sprintf("http://127.0.0.1:%d/test.v1.Service/Stream", port),
		connect.WithProtoJSON(),
	)

	stream, err := client.CallServerStream(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	require.NoError(t, err)
	defer stream.Close()

	require.True(t, stream.Receive(), "the first frame should arrive: %v", stream.Err())

	select {
	case <-sentFirstFrame:
	case <-t.Context().Done():
		t.Fatalf("backend never sent the first frame: %v", t.Context().Err())
	}

	require.NoError(t, owner.Load().Close(t.Context()))

	assert.False(t, stream.Receive(), "the stream should end after the sandbox stops")

	streamErr := stream.Err()
	require.Error(t, streamErr)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(streamErr))
	assert.Contains(t, streamErr.Error(), "the connection to sandbox sbx-1 ended before the stream completed")
	assert.NotContains(t, streamErr.Error(), "incomplete envelope")
}

// The wrapper must stay transparent to http.ResponseController, which is how
// httputil.ReverseProxy reaches Hijack to switch protocols.
func TestProxyStillSwitchesProtocols(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig
	listener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			conn, buffered, hijackErr := http.NewResponseController(w).Hijack()
			if !assert.NoError(t, hijackErr) {
				return
			}
			defer conn.Close()

			_, writeErr := buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: e2b\r\nConnection: Upgrade\r\n\r\n")
			assert.NoError(t, writeErr)
			assert.NoError(t, buffered.Flush())

			line, readErr := buffered.ReadString('\n')
			if !assert.NoError(t, readErr) {
				return
			}

			_, writeErr = buffered.WriteString("echo: " + line)
			assert.NoError(t, writeErr)
			assert.NoError(t, buffered.Flush())
		}),
	}

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	backendURL, err := url.Parse("http://" + listener.Addr().String())
	require.NoError(t, err)

	getDestination := func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "sbx-1",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "upgrade-backend",
		}, nil
	}

	proxy, port, err := newTestProxy(t, getDestination)
	require.NoError(t, err)
	defer proxy.Close()

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("GET /ws HTTP/1.1\r\nHost: sandbox\r\nUpgrade: e2b\r\nConnection: Upgrade\r\n\r\n"))
	require.NoError(t, err)

	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	buffered := bufio.NewReader(conn)
	status, err := buffered.ReadString('\n')
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(status, "HTTP/1.1 101"), "expected a protocol switch, got %q", status)

	for {
		line, headerErr := buffered.ReadString('\n')
		require.NoError(t, headerErr)

		if strings.TrimSpace(line) == "" {
			break
		}
	}

	_, err = conn.Write([]byte("ping\n"))
	require.NoError(t, err)

	echoed, err := buffered.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "echo: ping\n", echoed)
}

func TestEndFrameForRecognisesFramings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		contentType string
		terminable  bool
	}{
		{contentType: "application/connect+proto", terminable: true},
		{contentType: "application/connect+json", terminable: true},
		// Nothing E2B ships speaks gRPC-Web; envd only accepts it because
		// connect handlers are permissive by default.
		{contentType: "application/grpc-web", terminable: false},
		{contentType: "application/grpc-web+proto", terminable: false},
		{contentType: "application/grpc-web-text", terminable: false},
		{contentType: "application/grpc", terminable: false},
		// Unary Connect reports errors in the status and body, both already sent.
		{contentType: "application/proto", terminable: false},
		{contentType: "application/json", terminable: false},
		// Ending a plain body cleanly would hide the truncation.
		{contentType: "text/event-stream", terminable: false},
		{contentType: "text/plain", terminable: false},
		{contentType: "", terminable: false},
	}

	for _, test := range tests {
		t.Run(test.contentType, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.terminable, endFrameFor(test.contentType) != nil)
		})
	}
}
