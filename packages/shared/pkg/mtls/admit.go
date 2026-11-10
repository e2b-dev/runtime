package mtls

import (
	"context"
	"crypto/tls"
	"fmt"

	"go.uber.org/zap"
)

// decision is what the policy in force says about one request or RPC: the
// peer the handler sees, or the reason the call is refused.
type decision struct {
	peer    Peer
	refused *RejectionError
}

// unauthenticated reports whether the refusal is for a caller that presented
// no verified identity at all, plaintext, an unverified chain or a
// certificate without a usable SPIFFE ID, as opposed to a name the
// allow-list does not admit; the transports answer the two differently.
func (d decision) unauthenticated() bool {
	if d.refused == nil {
		return false
	}
	switch d.refused.Reason {
	case ReasonPlaintext, ReasonUnverifiedChain, ReasonNoCertificate, ReasonSAN:
		return true
	default:
		return false
	}
}

// admit applies the policy in force to one request or RPC. kind labels the
// metrics (KindGRPC or KindHTTP), health says whether the call is one a
// plaintext probe may make, and fields describe the call for the logs. It is
// the one place the gRPC interceptors and the HTTP guard decide, so the two
// transports cannot drift apart.
func (c ServerConfig) admit(ctx context.Context, kind string, health, isTLS, chainVerified bool, state tls.ConnectionState, fields ...zap.Field) decision {
	p := c.policy(ctx)
	metrics := c.metrics()
	fields = append(fields, zap.String("listener", c.Name))

	// Plaintext, or a TLS chain that did not verify at its handshake and was
	// admitted under permissive: required refuses both, health excepted,
	// until the service closes the connection; permissive counts what
	// required would refuse; off counts nothing.
	if !isTLS || !chainVerified {
		reason := ReasonUnverifiedChain
		if !isTLS {
			reason = ReasonPlaintext
		}
		fields = append(fields, zap.String("reason", string(reason)))
		switch {
		case p.mode == ModeOff, health:
		case p.mode == ModeRequired:
			if !isTLS {
				metrics.plaintextRequest(ctx, c.Name, kind)
			}
			metrics.refused(ctx, c.Name, reason, kind)
			c.log().Warn(ctx, "mtls: refused an unauthenticated call on a required listener", fields...)

			return decision{refused: &RejectionError{Reason: reason}}
		case !isTLS:
			metrics.plaintextRequest(ctx, c.Name, kind)
		default:
			metrics.wouldReject(ctx, c.Name, reason, kind)
			c.log().Info(ctx, "mtls: permissive listener would refuse a call", fields...)
		}
		if !isTLS || p.mode == ModeRequired {
			return decision{peer: Peer{TLS: isTLS}}
		}
		// Off and permissive admit the chain: the name is reported as
		// presented, and the allow-list is not consulted for a chain that
		// already failed, as at the handshake.
		id, _ := p.peerAllowed(state)

		return decision{peer: Peer{ID: id, TLS: true}}
	}

	// Off checks nothing: the name is extracted for the handler, nothing more.
	id, err := p.peerAllowed(state)
	if err != nil && p.mode != ModeOff {
		rejection := asRejection(err)
		fields = append(fields, zap.String("peer", id), zap.String("reason", string(rejection.Reason)))
		if p.mode == ModeRequired {
			metrics.refused(ctx, c.Name, rejection.Reason, kind)
			c.log().Warn(ctx, "mtls: refused a call from a name no longer allowed", fields...)

			return decision{refused: rejection}
		}
		metrics.wouldReject(ctx, c.Name, rejection.Reason, kind)
		c.log().Info(ctx, "mtls: permissive listener would refuse a call", fields...)
	}

	return decision{peer: Peer{ID: id, TLS: true, ChainVerified: chainVerified}}
}

// peerAllowed returns the caller's ID when its leaf carries one spiffe URI
// SAN that is on the list. It is the per-request and per-RPC check; the
// chain was verified at the handshake.
func (p policy) peerAllowed(state tls.ConnectionState) (string, error) {
	if len(state.PeerCertificates) == 0 {
		return "", &RejectionError{Reason: ReasonNoCertificate}
	}

	id, err := PeerID(state.PeerCertificates[0])
	if err != nil {
		return "", err
	}
	if !p.allow.allows(id) {
		return id, &RejectionError{Reason: ReasonNotAllowed, Err: fmt.Errorf("%q is not on the allow-list", id)}
	}

	return id, nil
}
