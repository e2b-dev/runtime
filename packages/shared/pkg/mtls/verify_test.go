package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

func TestPeerID(t *testing.T) {
	t.Parallel()

	root := mtlstest.NewRootCA(t, "root")

	tests := []struct {
		name   string
		spec   mtlstest.LeafSpec
		want   string
		reason Reason
	}{
		{name: "one spiffe URI", spec: mtlstest.LeafSpec{SPIFFEID: clientID}, want: clientID},
		{name: "uppercase trust domain is refused", spec: mtlstest.LeafSpec{SPIFFEID: "spiffe://Cluster-A.Example.Internal/ns/platform/sa/client"}, reason: ReasonSAN},
		{name: "zero URI SANs", spec: mtlstest.LeafSpec{URIs: []string{}}, reason: ReasonSAN},
		{name: "two URI SANs", spec: mtlstest.LeafSpec{URIs: []string{clientID, strangerID}}, reason: ReasonSAN},
		{name: "not a spiffe URI", spec: mtlstest.LeafSpec{URIs: []string{"https://cluster-a.example.internal/ns/platform/sa/client"}}, reason: ReasonSAN},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, err := PeerID(root.Leaf(t, tt.spec).Cert)
			if tt.reason != "" {
				var rejection *RejectionError
				require.ErrorAs(t, err, &rejection)
				assert.Equal(t, tt.reason, rejection.Reason)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, id)
		})
	}

	_, err := PeerID(nil)
	var rejection *RejectionError
	require.ErrorAs(t, err, &rejection)
	assert.Equal(t, ReasonNoCertificate, rejection.Reason)
}

// admissionCase is one row of the proposal's validation matrix, as the
// server sees it: the client presented, and why the server refuses it.
type admissionCase struct {
	name   string
	client *tls.Config
	reason Reason
}

func admissionCases(t *testing.T, fx *fixture) []admissionCase {
	t.Helper()

	otherRoot := mtlstest.NewRootCA(t, "other-root")
	expiredAt := time.Now().Add(-time.Minute)

	return []admissionCase{
		{name: "allowed name", client: rawClientTLS(fx.peerLeaf(t, clientID), protoH2)},
		{name: "no certificate", client: rawClientTLS(nil, protoH2), reason: ReasonNoCertificate},
		{name: "wrong root", client: rawClientTLS(otherRoot.Leaf(t, mtlstest.LeafSpec{SPIFFEID: clientID}), protoH2), reason: ReasonUnknownAuthority},
		{name: "expired leaf", client: rawClientTLS(fx.inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: clientID, NotBefore: expiredAt.Add(-time.Hour), NotAfter: expiredAt}), protoH2), reason: ReasonExpired},
		{name: "name not on the list", client: rawClientTLS(fx.peerLeaf(t, strangerID), protoH2), reason: ReasonNotAllowed},
		{name: "same path under another trust domain", client: rawClientTLS(fx.peerLeaf(t, otherDomain), protoH2), reason: ReasonNotAllowed},
		{name: "serverAuth-only leaf", client: rawClientTLS(fx.inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: clientID, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}), protoH2), reason: ReasonKeyUsage},
		{name: "serverAuth-only intermediate", client: rawClientTLS(fx.root.Intermediate(t, "server-only", x509.ExtKeyUsageServerAuth).Leaf(t, mtlstest.LeafSpec{SPIFFEID: clientID}), protoH2), reason: ReasonKeyUsage},
		{name: "zero URI SANs", client: rawClientTLS(fx.inter.Leaf(t, mtlstest.LeafSpec{URIs: []string{}}), protoH2), reason: ReasonSAN},
		{name: "two URI SANs", client: rawClientTLS(fx.inter.Leaf(t, mtlstest.LeafSpec{URIs: []string{clientID, strangerID}}), protoH2), reason: ReasonSAN},
	}
}

func TestServerRequiredRefusesEverythingButTheIntendedName(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	serverCfg := cfg.serverTLSConfig([]string{protoH2})

	for _, tc := range admissionCases(t, fx) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := handshake(t, serverCfg, tc.client)
			if tc.reason == "" {
				require.NoError(t, result.serverErr)
				require.NoError(t, result.clientErr)
				assert.Equal(t, protoH2, result.client.NegotiatedProtocol)

				return
			}

			require.Error(t, result.serverErr)
			require.Error(t, result.clientErr, "the client cannot use the connection")
			if tc.reason == ReasonNoCertificate {
				// Go refuses a missing certificate under RequireAnyClientCert
				// before VerifyConnection runs.
				assert.Contains(t, result.serverErr.Error(), "certificate")

				return
			}
			var rejection *RejectionError
			require.ErrorAs(t, result.serverErr, &rejection)
			assert.Equal(t, tc.reason, rejection.Reason)
		})
	}
}

func TestServerPermissiveAdmitsEverythingAndCountsWhatItWouldRefuse(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	serverCfg := cfg.serverTLSConfig([]string{protoH2})

	// Sequential on purpose: each case inspects the log entries it added.
	for _, tc := range admissionCases(t, fx) {
		before := fx.logs.FilterMessage("mtls: permissive listener would refuse a peer").Len()
		result := handshake(t, serverCfg, tc.client)
		require.NoError(t, result.serverErr, tc.name)
		require.NoError(t, result.clientErr, tc.name)

		added := fx.logs.FilterMessage("mtls: permissive listener would refuse a peer").All()[before:]
		if tc.reason == "" {
			assert.Empty(t, added, tc.name)

			continue
		}
		require.Len(t, added, 1, tc.name)
		assert.Equal(t, string(tc.reason), added[0].ContextMap()["reason"], tc.name)
	}
}

func TestServerOffChecksNothing(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	otherRoot := mtlstest.NewRootCA(t, "other-root")
	cfg := newServerConfig(t, fx, StaticMode(ModeOff), clientID)

	result := handshake(t, cfg.serverTLSConfig([]string{protoH2}), rawClientTLS(otherRoot.Leaf(t, mtlstest.LeafSpec{SPIFFEID: strangerID}), protoH2))
	require.NoError(t, result.serverErr)
	require.NoError(t, result.clientErr)
	assert.Empty(t, fx.logs.FilterLevelExact(zapcore.WarnLevel).All())
}

func TestServerWithoutFilesFailsTLSHandshakes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	log, _ := testLogger(t)
	files := NewFiles(t.Context(), FileConfig{
		CertFile: filepath.Join(dir, "tls.crt"),
		KeyFile:  filepath.Join(dir, "tls.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}, WithReloadInterval(time.Hour), WithFilesLogger(log))
	cfg := ServerConfig{Name: "empty", Files: files, Mode: StaticMode(ModePermissive), Allow: mustAllow(t, clientID), Logger: log}

	result := handshake(t, cfg.serverTLSConfig([]string{protoH2}), rawClientTLS(nil, protoH2))
	require.ErrorIs(t, result.serverErr, ErrNoCertificateLoaded, "there is no fallback to plaintext inside TLS")
	require.Error(t, result.clientErr)
}

func TestServerRefusesAResumedSessionAfterTheNameWasRemoved(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	// One tls.Config for both handshakes, so the ticket the first one issues
	// resumes the second.
	serverCfg := cfg.serverTLSConfig([]string{protoH2})
	clientCfg := rawClientTLS(fx.peerLeaf(t, clientID), protoH2)
	clientCfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)

	first := handshake(t, serverCfg, clientCfg)
	require.NoError(t, first.serverErr)
	require.NoError(t, first.clientErr)
	require.False(t, first.client.DidResume)

	require.NoError(t, cfg.Allow.Replace([]string{strangerID}))

	second := handshake(t, serverCfg, clientCfg)
	require.True(t, second.client.DidResume, "the client resumed by ticket")
	require.Error(t, second.serverErr)
	var rejection *RejectionError
	require.ErrorAs(t, second.serverErr, &rejection)
	assert.Equal(t, ReasonNotAllowed, rejection.Reason)
}

func TestServerRefusesAResumedSessionAfterTheBundleWasSwapped(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	serverCfg := cfg.serverTLSConfig([]string{protoH2})
	clientCfg := rawClientTLS(fx.peerLeaf(t, clientID), protoH2)
	clientCfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)

	first := handshake(t, serverCfg, clientCfg)
	require.NoError(t, first.serverErr)
	require.NoError(t, first.clientErr)

	replacement := mtlstest.NewRootCA(t, "replacement-root")
	fx.dir.Rewrite(t, fx.leaf, replacement.BundlePEM())
	require.NoError(t, fx.files.Reload(t.Context()))

	second := handshake(t, serverCfg, clientCfg)
	require.True(t, second.client.DidResume)
	require.Error(t, second.serverErr)
	var rejection *RejectionError
	require.ErrorAs(t, second.serverErr, &rejection)
	assert.Equal(t, ReasonUnknownAuthority, rejection.Reason)
}

func TestServerPicksUpARenewedCertificateWithoutRestart(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	serverCfg := cfg.serverTLSConfig([]string{protoH2})
	clientCfg := rawClientTLS(fx.peerLeaf(t, clientID), protoH2)

	first := handshake(t, serverCfg, clientCfg)
	require.NoError(t, first.clientErr)
	assert.Equal(t, fx.leaf.Cert.SerialNumber, first.client.PeerCertificates[0].SerialNumber)

	renewed := fx.peerLeaf(t, serverID, serverDNS)
	fx.dir.Rewrite(t, renewed, fx.root.BundlePEM())
	require.NoError(t, fx.files.Reload(t.Context()))

	second := handshake(t, serverCfg, clientCfg)
	require.NoError(t, second.clientErr)
	assert.Equal(t, renewed.Cert.SerialNumber, second.client.PeerCertificates[0].SerialNumber, "new handshakes present the renewed certificate")
}

func TestClientVerifiesTheServer(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	clientFiles := fx.peerFiles(t, clientID)
	serverCfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID).serverTLSConfig([]string{protoH2})

	otherFx := newFixture(t, serverDNS)
	otherServerCfg := newServerConfig(t, otherFx, StaticMode(ModeRequired), clientID).serverTLSConfig([]string{protoH2})

	clientOnlyLeaf := fx.inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: serverID, DNSNames: []string{serverDNS}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	clientOnlyDir := mtlstest.WriteFiles(t, clientOnlyLeaf, fx.root.BundlePEM())
	clientOnlyFiles := NewFiles(t.Context(), FileConfig{CertFile: clientOnlyDir.CertFile, KeyFile: clientOnlyDir.KeyFile, CAFile: clientOnlyDir.CAFile}, WithReloadInterval(time.Hour), WithFilesLogger(fx.log))
	clientOnlyServerCfg := ServerConfig{Name: "client-only", Files: clientOnlyFiles, Mode: StaticMode(ModeRequired), Allow: mustAllow(t, clientID), Logger: fx.log}.serverTLSConfig([]string{protoH2})

	tests := []struct {
		name       string
		server     *tls.Config
		serverName string
		expected   []string
		reason     Reason
	}{
		{name: "expected name and DNS name", server: serverCfg, serverName: serverDNS, expected: []string{serverID}},
		{name: "dialled by IP, no DNS name configured", server: serverCfg, expected: []string{serverID}},
		{name: "one of several expected names", server: serverCfg, serverName: serverDNS, expected: []string{strangerID, serverID}},
		{name: "unexpected name", server: serverCfg, serverName: serverDNS, expected: []string{strangerID}, reason: ReasonNotAllowed},
		{name: "DNS name mismatch", server: serverCfg, serverName: "other.platform.svc.cluster.local", expected: []string{serverID}, reason: ReasonHostname},
		{name: "server under another root", server: otherServerCfg, serverName: serverDNS, expected: []string{serverID}, reason: ReasonUnknownAuthority},
		{name: "clientAuth-only server leaf", server: clientOnlyServerCfg, serverName: serverDNS, expected: []string{serverID}, reason: ReasonKeyUsage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOn), tt.serverName, tt.expected...)
			result := handshake(t, tt.server, client.clientTLSConfig([]string{protoH2}))
			if tt.reason == "" {
				require.NoError(t, result.clientErr)
				require.NoError(t, result.serverErr)
				assert.Equal(t, protoH2, result.client.NegotiatedProtocol)

				return
			}

			var rejection *RejectionError
			require.ErrorAs(t, result.clientErr, &rejection)
			assert.Equal(t, tt.reason, rejection.Reason)
		})
	}
}

func TestServerRequiredWithAnEmptyAllowListServesPermissive(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	// The downgrade is logged once per listener name for the process's life,
	// so the name is unique to this run: t.TempDir() is <per-test random
	// directory>/001, and the random directory is what differs between runs.
	cfg := ServerConfig{Name: "required-empty-" + filepath.Base(filepath.Dir(fx.dir.Dir)), Files: fx.files, Mode: StaticMode(ModeRequired), Allow: mustAllow(t), HandshakeTimeout: time.Second, Logger: fx.log}
	const downgraded = "mtls: required mode with an empty allow-list; serving permissive"

	for range 3 {
		assert.Equal(t, ModePermissive, cfg.policy(t.Context()).mode, "an empty list names nobody to admit")
	}
	nilList := cfg
	nilList.Allow = nil
	assert.Equal(t, ModePermissive, nilList.policy(t.Context()).mode, "a nil list is an empty list")
	assert.Len(t, fx.logs.FilterMessage(downgraded).All(), 1, "logged once per listener, not per handshake")

	// A flag fallback of required is under the same rule as the flag itself.
	fromFlag := cfg
	fromFlag.Mode = NewFlagModeSource(newFakeFlags(), "listener-mode", ModeRequired, cfg.Allow, WithModeLogger(fx.log))
	assert.Equal(t, ModePermissive, fromFlag.policy(t.Context()).mode)

	// What permissive admits, this listener admits: a name no list holds.
	result := handshake(t, cfg.serverTLSConfig([]string{protoH2}), rawClientTLS(fx.peerLeaf(t, strangerID), protoH2))
	require.NoError(t, result.serverErr)
	require.NoError(t, result.clientErr)
	assert.Len(t, fx.logs.FilterMessage("mtls: permissive listener would refuse a peer").All(), 1)

	require.NoError(t, cfg.Allow.Replace([]string{clientID}))
	assert.Equal(t, ModeRequired, cfg.policy(t.Context()).mode, "once the list has a name, required is in force")
}

func TestClientWithoutRootsCannotVerifyTheServer(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	serverCfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID).serverTLSConfig([]string{protoH2})

	dir := t.TempDir()
	empty := NewFiles(t.Context(), FileConfig{
		CertFile: filepath.Join(dir, "tls.crt"),
		KeyFile:  filepath.Join(dir, "tls.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}, WithReloadInterval(time.Hour), WithFilesLogger(fx.log))
	client := newClientConfig(t, empty, fx.log, StaticClientMode(ClientOn), serverDNS, serverID)

	result := handshake(t, serverCfg, client.clientTLSConfig([]string{protoH2}))
	// Go's TLS 1.3 client verifies the server before it is asked for its own
	// certificate, so an empty set fails at the roots first.
	var rejection *RejectionError
	require.ErrorAs(t, result.clientErr, &rejection)
	assert.Equal(t, ReasonUnknownAuthority, rejection.Reason)
	require.ErrorIs(t, result.clientErr, ErrNoRootsLoaded)
}
