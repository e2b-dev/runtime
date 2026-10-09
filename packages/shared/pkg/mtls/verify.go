package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	// DefaultHandshakeTimeout bounds the first-byte read and the TLS handshake
	// together, so a silent or stalled connection holds nothing for long.
	DefaultHandshakeTimeout = 5 * time.Second

	// ALPN protocol names: gRPC negotiates h2, the HTTP listeners offer http/1.1 only.
	protoH2     = "h2"
	protoHTTP11 = "http/1.1"
)

// ServerConfig is what one listener needs: its material, its policy and its name.
type ServerConfig struct {
	// Name labels the listener in metrics and logs, for example "grpc-internal".
	Name string
	// Files serves the certificate presented and the roots peers are verified against.
	Files *Files
	// Mode is read on every handshake, request and RPC. nil means ModeOff.
	Mode ModeSource
	// Allow is the set of SPIFFE IDs admitted. nil or empty admits nobody.
	Allow *AllowList
	// HandshakeTimeout bounds classification plus handshake; zero means DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration
	// Logger defaults to logger.L().
	Logger logger.Logger
	// Metrics defaults to DefaultMetrics().
	Metrics *Metrics
}

func (c ServerConfig) log() logger.Logger {
	if c.Logger != nil {
		return c.Logger
	}

	return logger.L()
}

func (c ServerConfig) metrics() *Metrics {
	if c.Metrics != nil {
		return c.Metrics
	}

	return DefaultMetrics()
}

func (c ServerConfig) handshakeTimeout() time.Duration {
	if c.HandshakeTimeout > 0 {
		return c.HandshakeTimeout
	}

	return DefaultHandshakeTimeout
}

// policy is one snapshot of the mode and the allow-list, taken once per
// handshake, request or RPC, so a check never mixes a new mode with an old list.
type policy struct {
	mode  Mode
	allow *allowSet
}

func (c ServerConfig) policy(ctx context.Context) policy {
	mode, source := ModeOff, SourceFallback
	if c.Mode != nil {
		mode, source = modeWithSource(ctx, c.Mode)
	}
	allow := c.Allow.snapshot()
	if mode == ModeRequired && len(allow.entries) == 0 {
		mode = ModePermissive
		c.warnEmptyAllowList(ctx)
	}
	c.metrics().recordMode(ctx, c.Name, mode, source)

	return policy{mode: mode, allow: allow}
}

// emptyAllowListWarned remembers which listeners have reported the
// downgrade, so a required mode with an empty list is logged once per
// listener name, not on every handshake.
var emptyAllowListWarned sync.Map

// warnEmptyAllowList logs that required is being served as permissive.
// FlagModeSource refuses the flip at the flag; this is the same rule applied
// to whatever any ModeSource yields, so a static required mode from the
// environment and a flag fallback of required behave the same with an empty
// list: nobody could be admitted, and refusing everybody is not the intent.
func (c ServerConfig) warnEmptyAllowList(ctx context.Context) {
	if _, logged := emptyAllowListWarned.LoadOrStore(c.Name, struct{}{}); logged {
		return
	}
	c.log().Warn(ctx, "mtls: required mode with an empty allow-list; serving permissive",
		zap.String("listener", c.Name))
}

// PeerID returns the SPIFFE ID of a leaf certificate as go-spiffe reads it:
// exactly one URI SAN holding a well-formed SPIFFE ID. A trust domain with
// uppercase letters is not well formed and is refused here, where an
// allow-list entry typed that way is lowercased on parse.
func PeerID(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", &RejectionError{Reason: ReasonNoCertificate}
	}
	id, err := x509svid.IDFromCert(cert)
	if err != nil {
		return "", &RejectionError{Reason: ReasonSAN, Err: err}
	}

	return id.String(), nil
}

// serverTLSConfig builds the tls.Config for one listener. nextProtos is
// []string{"h2"} for gRPC and []string{"http/1.1"} for HTTP. Every
// per-connection decision happens in GetConfigForClient, so the mode applied
// is the one in force at that handshake. Session tickets stay enabled: Go
// runs VerifyConnection on a resumed session as well, so a name removed or a
// bundle swapped since the ticket was issued is caught there.
func (c ServerConfig) serverTLSConfig(nextProtos []string) *tls.Config {
	base := &tls.Config{
		MinVersion:     tls.VersionTLS13,
		NextProtos:     nextProtos,
		GetCertificate: tlsconfig.GetCertificate(c.Files),
	}
	base.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		ctx := hello.Context()
		p := c.policy(ctx)

		cfg := base.Clone()
		cfg.GetConfigForClient = nil
		cfg.ClientAuth = tls.RequestClientCert
		if p.mode == ModeRequired {
			cfg.ClientAuth = tls.RequireAnyClientCert
		}
		cfg.VerifyConnection = func(state tls.ConnectionState) error {
			chains, err := c.verifyPeer(ctx, p, state)
			// Go does not let VerifyConnection change the connection's
			// state, so the chain verdict rides on the hooked connection:
			// the registry and the per-call checks read it after the
			// handshake, in the goroutine that ran it.
			if hooked, ok := hello.Conn.(*hookedConn); ok {
				hooked.chainVerified = len(chains) > 0
			}

			return err
		}

		return cfg, nil
	}

	return base
}

// verifyPeer runs every check on the peer and returns the verified chains
// whenever the chain reached a loaded root, whatever the later checks said.
// Required refuses on the first failure; permissive counts it and admits the
// connection; off checks nothing, so nothing is verified.
func (c ServerConfig) verifyPeer(ctx context.Context, p policy, state tls.ConnectionState) ([][]*x509.Certificate, error) {
	if p.mode == ModeOff {
		return nil, nil
	}

	id, chains, err := c.checkPeer(p, state)
	if err == nil {
		c.metrics().connection(ctx, c.Name, id)

		return chains, nil
	}

	rejection := asRejection(err)
	fields := []zap.Field{
		zap.String("listener", c.Name),
		zap.String("reason", string(rejection.Reason)),
		zap.String("peer", id),
		zap.Bool("resumed", state.DidResume),
		zap.Error(rejection.Err),
	}
	if p.mode == ModePermissive {
		c.metrics().wouldReject(ctx, c.Name, rejection.Reason, KindHandshake)
		c.log().Info(ctx, "mtls: permissive listener would refuse a peer", fields...)

		return chains, nil
	}

	c.metrics().refused(ctx, c.Name, rejection.Reason, KindHandshake)
	c.log().Warn(ctx, "mtls: refused a peer", fields...)

	return chains, rejection
}

// checkPeer verifies the chain against the roots loaded now with the client
// authentication usage, then the SAN, then the allow-list. The ID is
// returned even when the chain fails, so logs can name the peer, and the
// verified chains are returned even when a later check fails, so the
// connection's chain verdict can be kept.
func (c ServerConfig) checkPeer(p policy, state tls.ConnectionState) (string, [][]*x509.Certificate, error) {
	if len(state.PeerCertificates) == 0 {
		return "", nil, &RejectionError{Reason: ReasonNoCertificate}
	}

	id, idErr := PeerID(state.PeerCertificates[0])
	chains, err := verifyChain(c.Files, state.PeerCertificates, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return id, nil, err
	}
	if idErr != nil {
		return "", chains, idErr
	}
	if !p.allow.allows(id) {
		return id, chains, &RejectionError{Reason: ReasonNotAllowed, Err: fmt.Errorf("%q is not on the allow-list", id)}
	}

	return id, chains, nil
}

// verifyChain verifies certs through go-spiffe against the roots loaded now:
// the leaf's SPIFFE ID, its CA and key-signing bits, and the chain to a root
// for its trust domain. go-spiffe verifies for any extended key usage, so the
// client or server authentication usage is checked here afterwards along
// each chain Go built, as x509.Verify would, and the chains that permit it
// are returned.
func verifyChain(source x509bundle.Source, certs []*x509.Certificate, usage x509.ExtKeyUsage) ([][]*x509.Certificate, error) {
	if len(certs) == 0 {
		return nil, &RejectionError{Reason: ReasonNoCertificate}
	}

	_, chains, err := x509svid.Verify(certs, source)
	if err != nil {
		return nil, &RejectionError{Reason: chainReason(certs[0], err), Err: err}
	}
	usable := chains[:0:0]
	for _, chain := range chains {
		if chainAllowsUsage(chain, usage) {
			usable = append(usable, chain)
		}
	}
	if len(usable) == 0 {
		name := "server authentication"
		if usage == x509.ExtKeyUsageClientAuth {
			name = "client authentication"
		}

		return nil, &RejectionError{Reason: ReasonKeyUsage, Err: fmt.Errorf("certificate chain lacks the %s extended key usage", name)}
	}

	return usable, nil
}

// chainAllowsUsage reports whether every certificate of the chain permits
// the usage, as x509.Verify requires of a chain it returns.
func chainAllowsUsage(chain []*x509.Certificate, usage x509.ExtKeyUsage) bool {
	for _, cert := range chain {
		if !allowsUsage(cert, usage) {
			return false
		}
	}

	return true
}

// allowsUsage reads a certificate's extended key usage the way x509.Verify
// does: one listing none permits every usage, otherwise the usage itself or
// Any must be listed.
func allowsUsage(cert *x509.Certificate, usage x509.ExtKeyUsage) bool {
	if len(cert.ExtKeyUsage) == 0 && len(cert.UnknownExtKeyUsage) == 0 {
		return true
	}
	for _, u := range cert.ExtKeyUsage {
		if u == usage || u == x509.ExtKeyUsageAny {
			return true
		}
	}

	return false
}

// chainReason labels a go-spiffe verification error. Go's chain errors keep
// their types through go-spiffe's wrapping, a missing trust bundle is this
// package's own error, and a leaf without a usable SPIFFE ID is refused by
// go-spiffe before the chain is looked at.
func chainReason(leaf *x509.Certificate, err error) Reason {
	if invalid, ok := errors.AsType[x509.CertificateInvalidError](err); ok {
		switch invalid.Reason {
		case x509.Expired:
			return ReasonExpired
		case x509.IncompatibleUsage:
			return ReasonKeyUsage
		default:
			return ReasonChain
		}
	}
	if _, ok := errors.AsType[x509.UnknownAuthorityError](err); ok || errors.Is(err, ErrNoRootsLoaded) {
		return ReasonUnknownAuthority
	}
	if _, idErr := x509svid.IDFromCert(leaf); idErr != nil {
		return ReasonSAN
	}

	return ReasonChain
}

// asRejection returns err as a RejectionError, wrapping anything else as a
// chain failure.
func asRejection(err error) *RejectionError {
	if rejection, ok := errors.AsType[*RejectionError](err); ok {
		return rejection
	}

	return &RejectionError{Reason: ReasonChain, Err: err}
}

// ClientConfig is what one client hop needs.
type ClientConfig struct {
	// Name labels the hop in metrics and logs, for example "api-to-store".
	Name string
	// Files serves the certificate presented and the roots the server is verified against.
	Files *Files
	// Mode is read on every handshake. nil means ClientOff.
	Mode ClientModeSource
	// ServerName is sent as SNI and checked against the server's DNS SANs
	// when set. Leave it empty for a server dialled by IP; the URI SAN check
	// still runs.
	ServerName string
	// ExpectedServerIDs are the server's acceptable SPIFFE IDs; a listener
	// shared by several workloads has several.
	ExpectedServerIDs *AllowList
	// Logger defaults to logger.L().
	Logger logger.Logger
	// Metrics defaults to DefaultMetrics().
	Metrics *Metrics
}

func (c ClientConfig) log() logger.Logger {
	if c.Logger != nil {
		return c.Logger
	}

	return logger.L()
}

func (c ClientConfig) metrics() *Metrics {
	if c.Metrics != nil {
		return c.Metrics
	}

	return DefaultMetrics()
}

func (c ClientConfig) clientMode(ctx context.Context) (ClientMode, string) {
	if c.Mode == nil {
		return ClientOff, SourceFallback
	}

	return clientModeWithSource(ctx, c.Mode)
}

// clientTLSConfig builds the hop's tls.Config. Go's own verification is
// disabled so that the roots loaded at handshake time are the ones used;
// VerifyConnection does the whole job: the chain with the server
// authentication usage, the DNS name when one is configured, then the URI SAN.
func (c ClientConfig) clientTLSConfig(nextProtos []string) *tls.Config {
	return &tls.Config{
		MinVersion:           tls.VersionTLS13,
		NextProtos:           nextProtos,
		ServerName:           c.ServerName,
		InsecureSkipVerify:   true, // every check runs in VerifyConnection, against the roots loaded now
		GetClientCertificate: tlsconfig.GetClientCertificate(c.Files),
		VerifyConnection:     c.verifyServer,
	}
}

// verifyServer runs on every handshake, resumed or not.
func (c ClientConfig) verifyServer(state tls.ConnectionState) error {
	if _, err := verifyChain(c.Files, state.PeerCertificates, x509.ExtKeyUsageServerAuth); err != nil {
		return err
	}

	leaf := state.PeerCertificates[0]
	if c.ServerName != "" {
		if err := leaf.VerifyHostname(c.ServerName); err != nil {
			return &RejectionError{Reason: ReasonHostname, Err: err}
		}
	}

	id, err := PeerID(leaf)
	if err != nil {
		return err
	}
	if !c.ExpectedServerIDs.Allows(id) {
		return &RejectionError{Reason: ReasonNotAllowed, Err: fmt.Errorf("server %q is not one of the expected names", id)}
	}

	return nil
}
