package pool

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/http2"

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

	transport    *http.Transport
	h2cTransport *http2.Transport

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

	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialUpstream(
			ctx,
			network,
			addr,
			maxConnectionAttempts,
			totalConnsCounter,
			currentConnsCounter,
			activeConnections,
		)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// Limit the max connection per host to avoid exhausting the number of available ports to one host.
		MaxIdleConnsPerHost:   maxHostIdleConns,
		MaxIdleConns:          maxIdleConns,
		IdleConnTimeout:       idleTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: 0,
		DisableKeepAlives:     disableKeepAlives,
		// HTTP/1.1 stays the default: browsers already speak HTTP/2 to the edge,
		// and user HTTP/WebSocket servers inside the sandbox are HTTP/1.1.
		ForceAttemptHTTP2:  false,
		DialContext:        dial,
		DisableCompression: true, // No need to request or manipulate compression
	}
	if insecureSkipTLSVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Sandbox services commonly use self-signed certificates.
	}

	h2IdleTimeout := idleTimeout
	if disableKeepAlives {
		// http2.Transport has no DisableKeepAlives; a short idle timeout avoids
		// pinning a multiplexed connection to a sandbox process that just restarted.
		h2IdleTimeout = time.Second
	}

	h2cTransport := &http2.Transport{
		AllowHTTP: true,
		// DialTLSContext is the plaintext dial when AllowHTTP is set.
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return dial(ctx, network, addr)
		},
		IdleConnTimeout:    h2IdleTimeout,
		DisableCompression: true,
		ReadIdleTimeout:    30 * time.Second,
		PingTimeout:        15 * time.Second,
	}

	pc := &ProxyClient{
		transport:         transport,
		h2cTransport:      h2cTransport,
		activeConnections: activeConnections,
	}

	pc.ReverseProxy = httputil.ReverseProxy{
		Transport: &protocolSwitchTransport{http1: transport, h2c: h2cTransport},
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
				if t.SandboxPort == uint64(consts.DefaultEnvdServerPort) {
					if errors.Is(err, context.Canceled) {
						t.RequestLogger.Warn(ctx, "sandbox request canceled by client", zap.Error(err))
					} else {
						t.RequestLogger.Error(ctx, "sandbox error handler called", zap.Error(err))
					}
				} else {
					t.RequestLogger.Warn(ctx, "sandbox error handler called", zap.Error(err))
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

			cors.Error(w, "Failed to route request to sandbox", http.StatusBadGateway)
		},
		ModifyResponse: func(r *http.Response) error {
			if IsGRPCRequest(r.Request) {
				// Native gRPC status lives in trailers. HTTP/2 backends often
				// set Content-Length, and HTTP/1.1 cannot attach trailers to a
				// length-delimited body. Do not replace r.Trailer: the HTTP/2
				// transport fills that map after the body is closed.
				r.ContentLength = -1
				r.Header.Del("Content-Length")
				if r.Header.Get("Trailer") == "" {
					r.Header.Set("Trailer", "Grpc-Status, Grpc-Message, Grpc-Status-Details-Bin")
				}
			}

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
	if p.h2cTransport != nil {
		p.h2cTransport.CloseIdleConnections()
	}
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

// dialUpstream retries the TCP dial so a request that arrives before envd's
// socat has published the localhost port still succeeds. When a process binds
// to localhost inside the sandbox, it can take up to 1s (scanner interval plus
// socat startup) before the host IP accepts connections.
func dialUpstream(
	ctx context.Context,
	network, addr string,
	maxConnectionAttempts int,
	totalConnsCounter *atomic.Uint64,
	currentConnsCounter *atomic.Int64,
	activeConnections *smap.Map[*tracking.Connection],
) (net.Conn, error) {
	var conn net.Conn
	var err error

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

		if attempt < maxAttempts-1 {
			backoff := time.Duration(100*(attempt+1)) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, err
}
