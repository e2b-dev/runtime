package pool

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/cors"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/tracking"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// Bound the TLS handshake so a stalled HTTPS backend cannot hold a
// connection open indefinitely (response streaming is intentionally unbounded).
const tlsHandshakeTimeout = 10 * time.Second

type ProxyClient struct {
	httputil.ReverseProxy

	transport *http.Transport

	activeConnections *smap.Map[*tracking.Connection]
}

func newProxyClient(
	maxIdleConns,
	maxHostIdleConns int,
	maxConnectionAttempts int,
	idleTimeout time.Duration,
	totalConnsCounter *atomic.Uint64,
	currentConnsCounter *atomic.Int64,
	l *log.Logger,
	disableKeepAlives bool,
	insecureSkipTLSVerify bool,
) *ProxyClient {
	activeConnections := smap.New[*tracking.Connection]()

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// Limit the max connection per host to avoid exhausting the number of available ports to one host.
		MaxIdleConnsPerHost:   maxHostIdleConns,
		MaxIdleConns:          maxIdleConns,
		IdleConnTimeout:       idleTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: 0,
		DisableKeepAlives:     disableKeepAlives,
		ForceAttemptHTTP2:     false,
		// TCP configuration
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var conn net.Conn
			var err error

			// Retry connection attempts to handle port forwarding delays in sandbox envd.
			// When a process binds to localhost inside the sandbox, it can take up to 1s (delay is 1s + socat startup delay)
			// for the port scanner to detect it and start socat forwarding to the host IP.
			maxAttempts := max(maxConnectionAttempts, 1)
			for attempt := range maxAttempts {
				conn, err = (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 20 * time.Second,
				}).DialContext(ctx, network, addr)

				if err == nil {
					totalConnsCounter.Add(1)

					return tracking.NewConnection(conn, currentConnsCounter, activeConnections), nil
				}

				if ctx.Err() != nil {
					return nil, ctx.Err()
				}

				// Don't sleep on the last attempt
				if attempt < maxAttempts-1 {
					// Linear backoff: 100ms, 200ms, 300ms, 400ms
					backoff := time.Duration(100*(attempt+1)) * time.Millisecond
					select {
					case <-time.After(backoff):
						// Continue to next attempt
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			}

			return nil, err
		},
		DisableCompression: true, // No need to request or manipulate compression
	}
	if insecureSkipTLSVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Sandbox services commonly use self-signed certificates.
	}

	pc := &ProxyClient{
		transport:         transport,
		activeConnections: activeConnections,
	}

	pc.ReverseProxy = httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(r *httputil.ProxyRequest) {
			t, ok := pc.getDestination(r.In)
			if !ok {
				r.SetURL(r.In.URL) // make linters happy, shouldn't matter

				return
			}

			r.SetURL(t.Url)

			if t.MaskRequestHost != nil {
				// Mask the request host to bypass source host protections.
				r.Out.Header.Set("X-Forwarded-Host", r.In.Host)
				r.Out.Host = *t.MaskRequestHost
			} else {
				// We are **not** using SetXForwarded() because servers can sometimes modify the content-location header to be http which might break some customer services.
				r.Out.Host = r.In.Host
			}
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// The upstream could not answer, so the proxy is the only party left
			// to answer the preflight, and a browser drops the real request
			// unless the preflight gets a 2xx.
			if cors.HandlePreflight(w, r) {
				return
			}

			ctx := r.Context()
			t, ok := pc.getDestination(r)
			if !ok {
				logger.L().Error(ctx, "proxy request without sandbox received error", zap.Error(err))
				cors.Error(w, "Failed to route request to sandbox", http.StatusInternalServerError)

				return
			}

			if r.Host == "" { // kept around for historical reasons, unsure of usefulness. todo: find out if this is useful.
				t.RequestLogger.Error(ctx, "error handler called from rewrite because of missing DestinationContext", zap.Error(err))
				cors.Error(w, "Failed to route request to sandbox", http.StatusInternalServerError)

				return
			}

			if err != nil {
				// Client-caused disconnects (cancel, connection reset, EOF) are not
				// server faults: an SDK/browser that gives up mid-request, or a weak
				// (CGNAT/mobile) link that drops a large upload, surfaces here. Log
				// them at Warn so they do not drown real upstream faults in ERROR.
				clientGone := errors.Is(err, context.Canceled) ||
					errors.Is(err, syscall.ECONNRESET) ||
					errors.Is(err, io.EOF)
				switch {
				case clientGone:
					t.RequestLogger.Warn(ctx, "sandbox request aborted by client", zap.Error(err))
				case t.SandboxPort == uint64(consts.DefaultEnvdServerPort):
					t.RequestLogger.Error(ctx, "sandbox upstream error", zap.Error(err))
				default:
					t.RequestLogger.Warn(ctx, "sandbox upstream error", zap.Error(err))
				}
			}

			if t.DefaultToPortError {
				err = template.
					NewPortClosedError(t.SandboxId, r.Host, t.SandboxPort).
					HandleError(w, r)
				if err != nil {
					logger.L().Error(ctx, "failed to handle error", zap.Error(err))

					cors.Error(w, "Failed to handle closed port error", http.StatusInternalServerError)

					return
				}

				return
			}

			// Destination resolved; this is an upstream/transport failure reaching
			// the sandbox, not a routing failure. A distinct message and 502 let
			// logs and clients tell it apart from the 500 routing failures above.
			cors.Error(w, "Failed to reach sandbox (upstream connection error)", http.StatusBadGateway)
		},
		ModifyResponse: func(r *http.Response) error {
			ctx := r.Request.Context()
			t, ok := pc.getDestination(r.Request)
			if !ok {
				return nil
			}

			if r.StatusCode >= 500 {
				t.RequestLogger.Warn(
					ctx,
					"Reverse proxy error",
					zap.Int("status_code", r.StatusCode),
				)
			} else {
				t.RequestLogger.Debug(ctx, "Reverse proxy response",
					zap.Int("status_code", r.StatusCode),
				)
			}

			return nil
		},
		// Ideally we would add info about sandbox to each error log, but there is no easy way right now.
		ErrorLog: l,
	}

	return pc
}

func (p *ProxyClient) getDestination(r *http.Request) (*Destination, bool) {
	ctx := r.Context()
	d, ok := getDestination(ctx)
	if !ok {
		logger.L().Error(ctx, "failed to get routing target from context",
			zap.String("request_method", r.Method),
			zap.String("request_url", r.URL.String()))

		return nil, false
	}

	return d, true
}

func (p *ProxyClient) closeIdleConnections() {
	p.transport.CloseIdleConnections()
}

func (p *ProxyClient) resetAllConnections() error {
	var errs []error

	for _, conn := range p.activeConnections.Items() {
		err := conn.Reset()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
