package idempotency

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/felixge/httpsnoop"
	"github.com/gin-gonic/gin"
)

var cachedResponseHeaders = [...]string{"Content-Type"}

const maxPooledResponseBytes = 64 * 1024

var responseBodyPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

type interceptedWriter interface {
	http.ResponseWriter
	http.Flusher
	http.Hijacker
	http.CloseNotifier
	io.StringWriter
}

type responseCapture struct {
	interceptedWriter

	body    *bytes.Buffer
	status  int
	written bool
}

var _ gin.ResponseWriter = (*responseCapture)(nil)

func newResponseCapture(writer gin.ResponseWriter) *responseCapture {
	w := &responseCapture{body: responseBodyPool.Get().(*bytes.Buffer), status: http.StatusOK}
	w.interceptedWriter = httpsnoop.Wrap(writer, httpsnoop.Hooks{
		WriteHeader: func(_ httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(status int) {
				// Gin defers committing status until a write or WriteHeaderNow.
				if !w.written && status > 0 {
					w.status = status
				}
			}
		},
		Write: func(_ httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(data []byte) (int, error) {
				w.WriteHeaderNow()

				return w.body.Write(data)
			}
		},
		Flush: func(_ httpsnoop.FlushFunc) httpsnoop.FlushFunc {
			return w.WriteHeaderNow
		},
		Hijack: func(_ httpsnoop.HijackFunc) httpsnoop.HijackFunc {
			return func() (net.Conn, *bufio.ReadWriter, error) {
				return nil, nil, errors.New("idempotent responses cannot be hijacked")
			}
		},
	}).(interceptedWriter)

	return w
}

func (w *responseCapture) Status() int   { return w.status }
func (w *responseCapture) Written() bool { return w.written }
func (w *responseCapture) Size() int {
	if !w.written {
		return -1
	}

	return w.body.Len()
}

func (w *responseCapture) WriteHeaderNow()     { w.written = true }
func (w *responseCapture) Pusher() http.Pusher { return nil }

func (w *responseCapture) release() {
	// Cached bodies can contain credentials; erase them before reusing the allocation.
	clear(w.body.Bytes())
	w.body.Reset()
	if w.body.Cap() <= maxPooledResponseBytes {
		responseBodyPool.Put(w.body)
	}
	w.body = nil
}

func (w *responseCapture) response() cachedResponse {
	result := cachedResponse{Status: w.status, Body: w.body.Bytes(), Headers: make(http.Header)}
	for _, name := range cachedResponseHeaders {
		if values, present := w.Header()[name]; present {
			result.Headers[name] = slices.Clone(values)
		}
	}

	return result
}
