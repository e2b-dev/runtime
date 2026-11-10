package mtls

import "errors"

var (
	// ErrNoCertificateLoaded is returned to a handshake while no consistent
	// set of files has loaded yet.
	ErrNoCertificateLoaded = errors.New("mtls: no certificate loaded")
	// ErrNoRootsLoaded is returned when a peer must be verified and no trust
	// bundle has loaded yet.
	ErrNoRootsLoaded = errors.New("mtls: no trust bundle loaded")
	// ErrInvalidBundle reports a certificate file or trust bundle in which some
	// PEM block is not a certificate that parses.
	ErrInvalidBundle = errors.New("mtls: bundle must be one or more PEM certificates")
	// ErrInvalidAllowList reports an allow-list entry that is not a SPIFFE ID,
	// or a wildcard where none is allowed.
	ErrInvalidAllowList = errors.New("mtls: invalid allow-list entry")
	// ErrInvalidMode reports a mode value that is not off, permissive or
	// required (or on/off for a client hop).
	ErrInvalidMode = errors.New("mtls: invalid mode")
)

// Reason names why a peer was refused, or would have been refused in
// permissive mode. The values are metric attribute values.
type Reason string

const (
	// ReasonNoCertificate: the peer presented no certificate.
	ReasonNoCertificate Reason = "no_certificate"
	// ReasonUnknownAuthority: the chain does not reach a loaded root.
	ReasonUnknownAuthority Reason = "unknown_authority"
	// ReasonExpired: a certificate in the chain is outside its validity.
	ReasonExpired Reason = "expired"
	// ReasonKeyUsage: the leaf lacks the client (or server) authentication usage.
	ReasonKeyUsage Reason = "key_usage"
	// ReasonChain: any other chain verification failure.
	ReasonChain Reason = "chain"
	// ReasonSAN: not exactly one URI SAN with the spiffe scheme.
	ReasonSAN Reason = "san"
	// ReasonNotAllowed: the name is well formed but not on the list.
	ReasonNotAllowed Reason = "not_allowed"
	// ReasonHostname: the server's DNS SANs do not include the configured name.
	ReasonHostname Reason = "hostname"
	// ReasonNoALPN: the peer did not negotiate h2 on a gRPC connection.
	ReasonNoALPN Reason = "no_alpn"
	// ReasonPlaintext: a plaintext request or RPC on a required listener.
	ReasonPlaintext Reason = "plaintext"
	// ReasonUnverifiedChain: a request or RPC on a required listener over a
	// TLS connection whose chain did not verify at its handshake, admitted
	// under permissive.
	ReasonUnverifiedChain Reason = "unverified_chain"
)

// RejectionError is returned from a handshake, request or RPC check when the
// peer is refused.
type RejectionError struct {
	Reason Reason
	Err    error
}

func (e *RejectionError) Error() string {
	if e.Err == nil {
		return "mtls: refused: " + string(e.Reason)
	}

	return "mtls: refused: " + string(e.Reason) + ": " + e.Err.Error()
}

func (e *RejectionError) Unwrap() error {
	return e.Err
}
