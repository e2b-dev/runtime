// Package streaming holds connect interceptors that apply to envd's
// streaming RPCs.
package streaming

import (
	"context"

	"connectrpc.com/connect"
)

// AccelBufferingHeader is the response header reverse proxies such as nginx
// read on a per-response basis to decide whether to buffer the body. Setting
// it to "no" makes the proxy forward each chunk as soon as it is written.
const AccelBufferingHeader = "X-Accel-Buffering"

// DisableProxyBuffering returns an interceptor that marks every response of a
// server-streaming (or bidi) RPC with "X-Accel-Buffering: no".
//
// Without it, a proxy with response buffering enabled (the nginx default)
// holds stream messages such as process output or watch events until its
// buffer fills, so an interactive client sees nothing until something forces a
// flush. Unary and client-streaming RPCs return a single message and are left
// untouched, so they keep the proxy's normal buffering behavior.
func DisableProxyBuffering() NoProxyBufferingInterceptor {
	return NoProxyBufferingInterceptor{}
}

type NoProxyBufferingInterceptor struct{}

var _ connect.Interceptor = NoProxyBufferingInterceptor{}

func (NoProxyBufferingInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return next
}

func (NoProxyBufferingInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (NoProxyBufferingInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if conn.Spec().StreamType&connect.StreamTypeServer != 0 {
			conn.ResponseHeader().Set(AccelBufferingHeader, "no")
		}

		return next(ctx, conn)
	}
}
