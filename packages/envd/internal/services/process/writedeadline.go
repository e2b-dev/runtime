package process

import (
	"context"
	"net/http"
	"time"
)

// streamWriteDeadline is the maximum duration allowed for a single stream.Send
// call. A live client will complete the send well within this window even on a
// slow network; a ghost subscriber (consumer goroutine stuck because the client's
// TCP connection is dead) will have stream.Send time out, causing the consumer
// goroutine to exit and closing s.done so the fan-out loop unblocks immediately.
//
// 15 s is chosen to be safely below the 90 s keepalive interval while giving
// real clients generous headroom on slow networks.
const streamWriteDeadline = 15 * time.Second

type responseWriterKey struct{}

// streamDeadlineMiddleware stores the http.ResponseWriter in the request context
// so that stream handlers can set per-send write deadlines via
// setStreamWriteDeadline / clearStreamWriteDeadline.
func streamDeadlineMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), responseWriterKey{}, w)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// setStreamWriteDeadline arms a write deadline on the HTTP connection
// associated with ctx. It is a no-op when the ResponseWriter is absent from
// ctx or does not support deadlines.
func setStreamWriteDeadline(ctx context.Context) {
	w, ok := ctx.Value(responseWriterKey{}).(http.ResponseWriter)
	if !ok {
		return
	}

	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(streamWriteDeadline))
}

// clearStreamWriteDeadline removes any write deadline previously set by
// setStreamWriteDeadline. Should be called after each stream.Send to avoid
// leaving a stale deadline that would affect subsequent sends.
func clearStreamWriteDeadline(ctx context.Context) {
	w, ok := ctx.Value(responseWriterKey{}).(http.ResponseWriter)
	if !ok {
		return
	}

	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
}
