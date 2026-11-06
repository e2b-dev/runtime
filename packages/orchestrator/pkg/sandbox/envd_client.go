//go:build linux

package sandbox

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process/processconnect"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

// envdReadMaxBytes caps one envd Connect message, on the wire and after
// decompression; envd emits process output in chunks of at most 32 KiB.
const envdReadMaxBytes = 1 << 20

// envdBodyMaxBytes caps a status or error body read from envd.
const envdBodyMaxBytes = 64 << 10

// ErrEnvdBodyTooLarge is a status or error body from envd over envdBodyMaxBytes.
var ErrEnvdBodyTooLarge = fmt.Errorf("envd response body is over %d KiB", envdBodyMaxBytes>>10)

// ReadEnvdBody reads a status or error body from envd, refusing one over
// envdBodyMaxBytes. The returned bytes are at most envdBodyMaxBytes.
func ReadEnvdBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(&cappedBody{Reader: r, left: envdBodyMaxBytes + 1})
	if errors.Is(err, ErrEnvdBodyTooLarge) {
		return body[:envdBodyMaxBytes], err
	}

	return body, err
}

// cappedBody fails the read that would take it past left-1 bytes.
type cappedBody struct {
	io.Reader

	left int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, ErrEnvdBodyTooLarge
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.Reader.Read(p)
	b.left -= int64(n)
	if b.left <= 0 {
		return n, ErrEnvdBodyTooLarge
	}

	return n, err
}

// errorBodyCap caps every non-200 body as it is read, before and after gzip:
// connect-go decodes a unary call's error body without its read limit. The
// streaming path never reads the body, so nothing here may read it eagerly.
type errorBodyCap struct {
	hc connect.HTTPClient
}

func (c errorBodyCap) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.hc.Do(req)
	if err != nil || resp.StatusCode == http.StatusOK {
		return resp, err
	}

	// connect-go decodes by the first value alone, so the cap must own that one.
	gzipped := resp.Header.Get("Content-Encoding") == "gzip"
	if gzipped {
		resp.Header.Del("Content-Encoding")
	}
	resp.Body = &cappedErrorBody{Closer: resp.Body, wire: &cappedBody{Reader: resp.Body, left: envdBodyMaxBytes + 1}, gzipped: gzipped}

	return resp, nil
}

// cappedErrorBody gunzips, if asked, on first read; wire spans every read.
type cappedErrorBody struct {
	io.Closer

	wire    *cappedBody
	gzipped bool
	r       io.Reader
}

func (b *cappedErrorBody) Read(p []byte) (int, error) {
	if b.r == nil {
		if !b.gzipped {
			b.r = b.wire
		} else {
			zr, err := gzip.NewReader(b.wire)
			if err != nil {
				return 0, err
			}
			b.r = &cappedBody{Reader: zr, left: envdBodyMaxBytes + 1}
		}
	}

	return b.r.Read(p)
}

// NewEnvdHTTPClient returns a client for host-to-envd HTTP requests over
// sandboxHttpTransport. A zero timeout means none.
func NewEnvdHTTPClient(timeout time.Duration) *http.Client {
	c := envdHTTPClient(sandboxHttpTransport, timeout)

	return &c
}

// NewEnvdProcessClient returns a Process client for the envd at baseURL.
func NewEnvdProcessClient(baseURL string, timeout time.Duration) processconnect.ProcessClient {
	return processconnect.NewProcessClient(errorBodyCap{hc: NewEnvdHTTPClient(timeout)}, baseURL, connect.WithReadMaxBytes(envdReadMaxBytes))
}

// envdHTTPClient never follows a redirect: the guest writes envd's responses.
func envdHTTPClient(rt http.RoundTripper, timeout time.Duration) http.Client {
	return http.Client{
		Timeout:       timeout,
		Transport:     rt,
		CheckRedirect: refuseEnvdRedirect,
	}
}

type envdSandboxKey struct{}

// WithEnvdSandbox returns ctx naming the sandbox its envd requests are for, so
// a refused redirect is reported with the sandbox's identity.
func WithEnvdSandbox(ctx context.Context, sbx sbxlogger.LoggerMetadata) context.Context {
	return context.WithValue(ctx, envdSandboxKey{}, sbx.LoggerMetadata())
}

// envdRedirectHostMaxLen caps the guest-written redirect host in a log line.
const envdRedirectHostMaxLen = 256

// refuseEnvdRedirect returns the redirect to the caller and warns: envd never
// redirects, so one means the guest has changed what answers the host.
//
// net/http returns some 3xx responses without calling it: one without a
// Location, a 300, 304 or 305, and a 307 or 308 whose body cannot be re-sent.
// None of them could take the host elsewhere, so they go unreported and fail
// their caller on the status alone.
func refuseEnvdRedirect(req *http.Request, via []*http.Request) error {
	orig := via[0]
	fields := []zap.Field{
		zap.Int("status_code", req.Response.StatusCode),
		zap.String("method", orig.Method),
		zap.String("path", orig.URL.Path),
		zap.String("envd_host", orig.Host),
		zap.String("redirect_host", truncateRedirectHost(req.URL.Host)),
	}
	if sbx, ok := req.Context().Value(envdSandboxKey{}).(sbxlogger.SandboxMetadata); ok {
		fields = append(fields,
			logger.WithSandboxID(sbx.SandboxID),
			logger.WithTeamID(sbx.TeamID),
			logger.WithTemplateID(sbx.TemplateID),
		)
	}

	logger.L().Warn(req.Context(), "envd answered with a redirect, refused it", fields...)
	envdRedirectsRefused.Add(req.Context(), 1, metric.WithAttributes(attribute.Int("status_code", req.Response.StatusCode)))

	return http.ErrUseLastResponse
}

// truncateRedirectHost cuts host to envdRedirectHostMaxLen bytes on a rune
// boundary: url.Parse admits non-ASCII bytes in a host.
func truncateRedirectHost(host string) string {
	if len(host) <= envdRedirectHostMaxLen {
		return host
	}

	cut := envdRedirectHostMaxLen
	for cut > 0 && !utf8.RuneStart(host[cut]) {
		cut--
	}

	return host[:cut]
}
