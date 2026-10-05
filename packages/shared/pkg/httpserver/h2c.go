package httpserver

import (
	"bufio"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	h2cUpgradeBodyLimit = 1 << 20 // 1 MiB
)

// ConfigureH2C wraps server's handler with H2C support using server timeouts.
func ConfigureH2C(server *http.Server) {
	handler := server.Handler
	if handler == nil {
		handler = http.DefaultServeMux
	}

	h2Server := newHTTP2Server()
	if err := http2.ConfigureServer(server, h2Server); err != nil {
		panic(err)
	}

	h2cHandler := h2c.NewHandler(handler, h2Server)
	trackedH2CHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := &h2cResponseWriter{ResponseWriter: w}
		defer func() {
			// h2c can return a preface read error without closing the hijacked socket.
			if writer.conn != nil {
				_ = writer.conn.Close()
			}
		}()
		h2cHandler.ServeHTTP(writer, r)
	})
	// MaxBytesHandler needs the original writer's private requestTooLarge hook.
	limitedH2CHandler := http.MaxBytesHandler(trackedH2CHandler, h2cUpgradeBodyLimit)

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isH2CUpgrade(r.Header) {
			limitedH2CHandler.ServeHTTP(w, r)

			return
		}
		if r.Method == "PRI" && len(r.Header) == 0 && r.URL.Path == "*" && r.Proto == "HTTP/2.0" {
			trackedH2CHandler.ServeHTTP(w, r)

			return
		}

		h2cHandler.ServeHTTP(w, r)
	})
}

type h2cResponseWriter struct {
	http.ResponseWriter

	conn net.Conn
}

func (w *h2cResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *h2cResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffered, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.conn = conn
	}

	return conn, buffered, err
}

func newHTTP2Server() *http2.Server {
	return &http2.Server{
		MaxConcurrentStreams:         100,
		ReadIdleTimeout:              30 * time.Second,
		PingTimeout:                  15 * time.Second,
		WriteByteTimeout:             30 * time.Second,
		MaxUploadBufferPerConnection: 1 << 20,
		MaxUploadBufferPerStream:     1 << 20,
	}
}

func isH2CUpgrade(header http.Header) bool {
	return httpguts.HeaderValuesContainsToken(header["Upgrade"], "h2c") &&
		httpguts.HeaderValuesContainsToken(header["Connection"], "HTTP2-Settings")
}
