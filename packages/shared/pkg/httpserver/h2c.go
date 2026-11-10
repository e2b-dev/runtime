package httpserver

import (
	"net/http"
	"time"
)

// ConfigureH2C has server accept HTTP/2 in cleartext with prior knowledge,
// beside HTTP/1 and HTTP/2 over TLS. net/http serves those connections
// itself, so Shutdown sends them a GOAWAY and waits for their streams like any
// other connection. An HTTP/1.1 request asking to upgrade to h2c is answered
// over HTTP/1.1.
func ConfigureH2C(server *http.Server) {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	server.Protocols = &protocols
	server.HTTP2 = &http.HTTP2Config{
		MaxConcurrentStreams:          100,
		MaxReceiveBufferPerConnection: 1 << 20,
		MaxReceiveBufferPerStream:     1 << 20,
		SendPingTimeout:               30 * time.Second,
		PingTimeout:                   15 * time.Second,
		WriteByteTimeout:              30 * time.Second,
	}
}
