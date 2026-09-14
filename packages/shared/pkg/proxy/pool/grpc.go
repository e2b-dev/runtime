package pool

import (
	"mime"
	"net/http"
	"strings"
)

// IsGRPCRequest reports whether *r* is native gRPC (Content-Type
// application/grpc), as opposed to HTTP, WebSocket, Connect-RPC, or gRPC-Web.
//
// The public sandbox proxy must keep those other protocols on HTTP/1.1: browsers
// already speak HTTP/2 to the edge, and blindly forwarding HTTP/2 to a Python
// http.server or a VNC upgrade would break them. Native gRPC is identified by
// Content-Type so only those requests use an h2c backend transport.
//
// If an edge LB ever protocol-selects on Content-Type, it must use this same
// cutoff and must not prefix-match "application/grpc" (that would include
// grpc-web). Suggested matchers, kept in tests:
//
//	regexp: (?i)^application/grpc($|[+;].*)
//	AWS-style: application/grpc, application/grpc;*, application/grpc+*
func IsGRPCRequest(r *http.Request) bool {
	if r == nil {
		return false
	}

	return isGRPCContentType(r.Header.Get("Content-Type"))
}

func isGRPCContentType(contentType string) bool {
	if contentType == "" {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType, _, _ = strings.Cut(contentType, ";")
		mediaType = strings.TrimSpace(mediaType)
	}

	mediaType = strings.ToLower(mediaType)
	if mediaType == "application/grpc" {
		return true
	}

	// application/grpc+proto, application/grpc+json, … — not application/grpc-web.
	return strings.HasPrefix(mediaType, "application/grpc+")
}

// protocolSwitchTransport sends native gRPC over unencrypted HTTP/2 (h2c) and
// everything else over the existing HTTP/1.1 transport.
type protocolSwitchTransport struct {
	http1 http.RoundTripper
	h2c   http.RoundTripper
}

func (t *protocolSwitchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if IsGRPCRequest(req) {
		return t.h2c.RoundTrip(req)
	}

	return t.http1.RoundTrip(req)
}

func (t *protocolSwitchTransport) CloseIdleConnections() {
	type idleCloser interface {
		CloseIdleConnections()
	}

	if closer, ok := t.http1.(idleCloser); ok {
		closer.CloseIdleConnections()
	}

	if closer, ok := t.h2c.(idleCloser); ok {
		closer.CloseIdleConnections()
	}
}
