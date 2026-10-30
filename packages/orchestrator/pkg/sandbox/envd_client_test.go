//go:build linux

package sandbox

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process/processconnect"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// redirectPair is a stand-in envd that answers every request with a redirect to
// a second listener, which only counts what reaches it.
type redirectPair struct {
	envd, other *httptest.Server
	envdHits    atomic.Int64
	otherHits   atomic.Int64
}

func newRedirectPair(t *testing.T, code int, relative bool) *redirectPair {
	t.Helper()

	p := &redirectPair{}
	p.other = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		p.otherHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(p.other.Close)

	p.envd = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.envdHits.Add(1)
		target := p.other.URL + "/elsewhere"
		if relative {
			target = "/elsewhere"
		}
		http.Redirect(w, r, target, code)
	}))
	t.Cleanup(p.envd.Close)

	return p
}

func redirectCodes() []int {
	return []int{
		http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect,
	}
}

// envdHTTPClients builds each host-to-envd HTTP client for the envd at url.
func envdHTTPClients() map[string]func(t *testing.T, url string) *http.Client {
	return map[string]func(*testing.T, string) *http.Client{
		"package client": func(*testing.T, string) *http.Client { return &sandboxHttpClient },
		"NewEnvdHTTPClient": func(*testing.T, string) *http.Client {
			return NewEnvdHTTPClient(5 * time.Second)
		},
		"held init client": func(t *testing.T, url string) *http.Client {
			t.Helper()

			h, err := newHeldInitClient(url, time.Second, time.Second)
			require.NoError(t, err)
			t.Cleanup(h.close)
			conn, err := h.dial(t.Context())
			require.NoError(t, err)
			h.handOff(conn)

			return h.client
		},
	}
}

func TestEnvdHTTPClientsDoNotFollowRedirects(t *testing.T) {
	t.Parallel()

	for name, client := range envdHTTPClients() {
		for _, code := range redirectCodes() {
			for _, relative := range []bool{false, true} {
				for _, method := range []string{http.MethodGet, http.MethodPost} {
					t.Run(fmt.Sprintf("%s/%d/relative=%v/%s", name, code, relative, method), func(t *testing.T) {
						t.Parallel()

						p := newRedirectPair(t, code, relative)
						ctx, cancel := context.WithTimeout(sandboxContext(t), 5*time.Second)
						defer cancel()
						req, err := http.NewRequestWithContext(ctx, method, p.envd.URL+"/health", strings.NewReader("{}"))
						require.NoError(t, err)
						req.Header.Set("X-Access-Token", "secret-token")

						resp, err := client(t, p.envd.URL).Do(req)
						require.NoError(t, err)
						resp.Body.Close()

						assert.Equal(t, code, resp.StatusCode, "the redirect is returned to the caller")
						assert.Equal(t, int64(1), p.envdHits.Load(), "no follow-up request to envd")
						assert.Zero(t, p.otherHits.Load(), "the redirect target is never reached")

						redirectHost := p.other.Listener.Addr().String()
						if relative {
							redirectHost = p.envd.Listener.Addr().String()
						}
						warning := envdRedirectWarning(t)
						assert.Equal(t, int64(code), warning["status_code"])
						assert.Equal(t, method, warning["method"])
						assert.Equal(t, "/health", warning["path"])
						assert.Equal(t, p.envd.Listener.Addr().String(), warning["envd_host"])
						assert.Equal(t, redirectHost, warning["redirect_host"])
						assert.NotContains(t, fmt.Sprint(warning), "secret-token")
					})
				}
			}
		}
	}
}

// No envd client asks for or expands a compressed response, so a body cap
// counts the bytes envd sent.
func TestEnvdHTTPClientsDoNotDecompress(t *testing.T) {
	t.Parallel()

	for name, client := range envdHTTPClients() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Accept-Encoding", r.Header.Get("Accept-Encoding"))
				w.Header().Set("Content-Encoding", "gzip")
				zw := gzip.NewWriter(w)
				_, _ = zw.Write([]byte("x"))
				_ = zw.Close()
			}))
			t.Cleanup(srv.Close)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/health", nil)
			require.NoError(t, err)
			resp, err := client(t, srv.URL).Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Empty(t, resp.Header.Get("X-Accept-Encoding"))
			assert.False(t, resp.Uncompressed)
			assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
		})
	}
}

// The control proves the fixture: a client without the rule follows the
// redirect to the other listener.
func TestRedirectFixtureReachesTheOtherListenerWithoutTheRule(t *testing.T) {
	t.Parallel()

	p := newRedirectPair(t, http.StatusFound, false)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, p.envd.URL+"/health", nil)
	require.NoError(t, err)

	resp, err := (&http.Client{Transport: sandboxHttpTransport}).Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, int64(1), p.otherHits.Load())
}

func TestEnvdProcessClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	for _, code := range redirectCodes() {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			t.Parallel()

			p := newRedirectPair(t, code, false)
			err := startAndReceive(sandboxContext(t), NewEnvdProcessClient(p.envd.URL, 5*time.Second))

			require.Error(t, err)
			assert.Equal(t, int64(1), p.envdHits.Load())
			assert.Zero(t, p.otherHits.Load(), "the redirect target is never reached")

			warning := envdRedirectWarning(t)
			assert.Equal(t, int64(code), warning["status_code"])
			assert.Equal(t, p.envd.Listener.Addr().String(), warning["envd_host"])
			assert.Equal(t, p.other.Listener.Addr().String(), warning["redirect_host"])
		})
	}
}

func TestEnvdRefusedRedirectIsCounted(t *testing.T) { //nolint:paralleltest // reads a delta of a counter with no per-test key
	read := func() map[int64]int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, testMetricReader.Collect(t.Context(), &rm))
		out := map[int64]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != string(telemetry.EnvdRedirectsRefused) {
					continue
				}
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					code, _ := dp.Attributes.Value("status_code")
					out[code.AsInt64()] += dp.Value
				}
			}
		}

		return out
	}

	p := newRedirectPair(t, http.StatusTemporaryRedirect, false)
	before := read()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, p.envd.URL+"/init", nil)
	require.NoError(t, err)
	resp, err := NewEnvdHTTPClient(5 * time.Second).Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	after := read()

	assert.Equal(t, before[http.StatusTemporaryRedirect]+1, after[http.StatusTemporaryRedirect])
}

// sandboxContext returns a context naming a sandbox unique to this test, so
// its warnings can be told apart; listener ports repeat across tests.
func sandboxContext(t *testing.T) context.Context {
	t.Helper()

	return WithEnvdSandbox(t.Context(), sbxlogger.SandboxMetadata{
		SandboxID:  t.Name(),
		TeamID:     "team-" + t.Name(),
		TemplateID: "template-" + t.Name(),
	})
}

// envdRedirectWarning returns the fields of the one refused-redirect warning
// logged for this test's sandbox, after checking its identity fields.
func envdRedirectWarning(t *testing.T) map[string]any {
	t.Helper()

	var found []map[string]any
	for _, e := range testLogObserver.FilterMessage("envd answered with a redirect, refused it").All() {
		fields := e.ContextMap()
		if fields["sandbox.id"] == t.Name() {
			assert.Equal(t, zapcore.WarnLevel, e.Level)
			found = append(found, fields)
		}
	}
	require.Len(t, found, 1, "one warning per refused redirect")
	assert.Equal(t, "team-"+t.Name(), found[0]["team.id"])
	assert.Equal(t, "template-"+t.Name(), found[0]["template.id"])

	return found[0]
}

// A redirect host cut to its cap never ends in half a rune.
func TestTruncateRedirectHost(t *testing.T) {
	t.Parallel()

	short := "envd.example"
	assert.Equal(t, short, truncateRedirectHost(short))

	host := strings.Repeat("a", envdRedirectHostMaxLen-1) + "é"
	got := truncateRedirectHost(host)
	assert.True(t, utf8.ValidString(got), "%q", got[len(got)-4:])
	assert.Equal(t, strings.Repeat("a", envdRedirectHostMaxLen-1), got)
}

func startAndReceive(ctx context.Context, c processconnect.ProcessClient) error {
	stream, err := c.Start(ctx, connect.NewRequest(&process.StartRequest{Process: &process.ProcessConfig{Cmd: "true"}}))
	if err != nil {
		return err
	}
	defer stream.Close()

	if !stream.Receive() {
		return stream.Err()
	}

	return nil
}

type stdoutHandler struct {
	processconnect.UnimplementedProcessHandler

	size int
}

func (h stdoutHandler) Start(_ context.Context, _ *connect.Request[process.StartRequest], s *connect.ServerStream[process.StartResponse]) error {
	return s.Send(stdoutResponse(bytes.Repeat([]byte("x"), h.size)))
}

func stdoutResponse(b []byte) *process.StartResponse {
	return &process.StartResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_Data{
		Data: &process.ProcessEvent_DataEvent{Output: &process.ProcessEvent_DataEvent_Stdout{Stdout: b}},
	}}}
}

// compressionRecorder records whether the first envelope it sends is
// compressed; the Connect-Content-Encoding header alone does not say.
type compressionRecorder struct {
	http.ResponseWriter

	seen       *atomic.Bool
	compressed *atomic.Bool
}

func (w compressionRecorder) Write(b []byte) (int, error) {
	if len(b) > 0 && w.seen.CompareAndSwap(false, true) {
		w.compressed.Store(b[0]&1 == 1)
	}

	return w.ResponseWriter.Write(b)
}

func (w compressionRecorder) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

// envdStub serves h and reports whether its first message went out compressed.
func envdStub(t *testing.T, h http.Handler) (string, *atomic.Bool) {
	t.Helper()

	var seen, compressed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(compressionRecorder{ResponseWriter: w, seen: &seen, compressed: &compressed}, r)
	}))
	t.Cleanup(srv.Close)

	return srv.URL, &compressed
}

func TestEnvdProcessClientAcceptsEnvdSizedFrames(t *testing.T) {
	t.Parallel()

	// 32 KiB is envd's stdout chunk; 512 KiB leaves room for a larger one.
	for _, size := range []int{32 << 10, 512 << 10} {
		for compressed, compressMin := range map[bool]int{false: math.MaxInt32, true: 1} {
			t.Run(fmt.Sprintf("%d/compressed=%v", size, compressed), func(t *testing.T) {
				t.Parallel()

				path, h := processconnect.NewProcessHandler(stdoutHandler{size: size}, connect.WithCompressMinBytes(compressMin))
				mux := http.NewServeMux()
				mux.Handle(path, h)
				url, sent := envdStub(t, mux)

				require.NoError(t, startAndReceive(t.Context(), NewEnvdProcessClient(url, 5*time.Second)))
				assert.Equal(t, compressed, sent.Load())
			})
		}
	}
}

func TestEnvdProcessClientRefusesOversizedFrames(t *testing.T) {
	t.Parallel()

	path, h := processconnect.NewProcessHandler(stdoutHandler{size: 2 * envdReadMaxBytes})
	mux := http.NewServeMux()
	mux.Handle(path, h)
	url, _ := envdStub(t, mux)

	err := startAndReceive(t.Context(), NewEnvdProcessClient(url, 5*time.Second))
	require.Error(t, err)
	assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err), err)
}

// gzipStdout returns a gzip stream of a StartResponse carrying n bytes of stdout.
func gzipStdout(t *testing.T, n int) []byte {
	t.Helper()

	msg, err := proto.Marshal(stdoutResponse(bytes.Repeat([]byte("x"), n)))
	require.NoError(t, err)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err = zw.Write(msg)
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

// A valid message whose compressed frame is under the cap but expands past it
// is refused, so the cap applies after decompression.
func TestEnvdProcessClientRefusesAFrameThatExpandsPastTheCap(t *testing.T) {
	t.Parallel()

	payload := gzipStdout(t, 3<<20)
	require.Less(t, len(payload), envdReadMaxBytes, "the frame is under the cap on the wire")

	frame := make([]byte, 5, 5+len(payload))
	frame[0] = 1 // compressed
	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)))
	frame = append(frame, payload...)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/connect+proto")
		w.Header().Set("Connect-Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(frame)
	}))
	t.Cleanup(srv.Close)

	err := startAndReceive(t.Context(), NewEnvdProcessClient(srv.URL, 5*time.Second))
	require.Error(t, err)
	assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err), err)
}

func TestReadEnvdBodyRefusesPastTheCap(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 1, envdBodyMaxBytes - 1, envdBodyMaxBytes} {
		body, err := ReadEnvdBody(bytes.NewReader(bytes.Repeat([]byte("x"), size)))
		require.NoError(t, err, size)
		assert.Len(t, body, size)
	}

	for _, size := range []int{envdBodyMaxBytes + 1, 8 << 20} {
		body, err := ReadEnvdBody(bytes.NewReader(bytes.Repeat([]byte("x"), size)))
		require.ErrorIs(t, err, ErrEnvdBodyTooLarge, size)
		assert.Len(t, body, envdBodyMaxBytes)
	}
}

// connect-go decodes a unary call's error body without the read limit, so the
// client caps it: an oversized one fails on its HTTP status, undecoded.
func TestEnvdProcessClientCapsAUnaryErrorBody(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		size    int
		gzip    bool
		decoded bool
	}{
		"small":          {size: 64, decoded: true},
		"under cap":      {size: envdBodyMaxBytes - 64, decoded: true},
		"over cap":       {size: envdBodyMaxBytes + 1},
		"far over":       {size: 8 << 20},
		"gzip small":     {size: 64, gzip: true, decoded: true},
		"gzip under cap": {size: envdBodyMaxBytes - 64, gzip: true, decoded: true},
		"gzip over cap":  {size: envdBodyMaxBytes + 1, gzip: true},
		"gzip far over":  {size: 8 << 20, gzip: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				body := []byte(`{"code":"not_found","message":"` + strings.Repeat("x", tc.size-len(`{"code":"not_found","message":""}`)) + `"}`)
				w.Header().Set("Content-Type", "application/json")
				if tc.gzip {
					var buf bytes.Buffer
					zw := gzip.NewWriter(&buf)
					_, _ = zw.Write(body)
					_ = zw.Close()
					body = buf.Bytes()
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write(body)
			}))
			t.Cleanup(srv.Close)

			_, err := NewEnvdProcessClient(srv.URL, 5*time.Second).Update(t.Context(), connect.NewRequest(&process.UpdateRequest{}))
			require.Error(t, err)

			if tc.decoded {
				assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err), err)

				return
			}
			assert.NotEqual(t, connect.CodeNotFound, connect.CodeOf(err), "the oversized body is not decoded")
			assert.Less(t, len(err.Error()), 1<<10)
		})
	}
}

// A gzip error body is capped on its compressed bytes as well as its expanded ones.
func TestEnvdProcessClientCapsAGzipErrorBodyOnTheWire(t *testing.T) {
	t.Parallel()

	var member bytes.Buffer
	require.NoError(t, gzip.NewWriter(&member).Close())
	buf := bytes.NewBuffer(bytes.Repeat(member.Bytes(), envdBodyMaxBytes/member.Len()+1))
	zw := gzip.NewWriter(buf)
	_, err := zw.Write([]byte(`{"code":"not_found","message":"x"}`))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(buf.Bytes())
	}))
	t.Cleanup(srv.Close)

	_, err = NewEnvdProcessClient(srv.URL, 5*time.Second).Update(t.Context(), connect.NewRequest(&process.UpdateRequest{}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnknown, connect.CodeOf(err), "the body past the wire cap is not decoded: %v", err)
}

// A unary error body in an encoding the client lacks fails on that encoding,
// as connect-go has it, rather than being read as plain JSON.
func TestEnvdProcessClientRefusesAnUnknownErrorEncoding(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "br")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"not_found","message":"x"}`))
	}))
	t.Cleanup(srv.Close)

	_, err := NewEnvdProcessClient(srv.URL, 5*time.Second).Update(t.Context(), connect.NewRequest(&process.UpdateRequest{}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err), err)
}

// A non-200 Start response whose body never ends must fail on its status, as
// connect-go's streaming client does, not wait on the body.
func TestEnvdProcessClientFailsAStartOnItsStatusWithoutReadingTheBody(t *testing.T) {
	t.Parallel()

	for _, gzipped := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%v", gzipped), func(t *testing.T) {
			t.Parallel()

			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if gzipped {
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("{"))
				w.(http.Flusher).Flush()
				<-release
			}))
			t.Cleanup(srv.Close)
			t.Cleanup(func() { close(release) })

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			err := startAndReceive(ctx, NewEnvdProcessClient(srv.URL, 0))

			require.Error(t, err)
			assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err), err)
		})
	}
}
