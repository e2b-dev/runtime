package sandbox_network

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubResolver returns a HostResolver that resolves IP literals natively and
// looks every other host up in table. Unknown hosts produce an error so a
// test cannot accidentally hit real DNS.
func stubResolver(table map[string][]net.IP) HostResolver {
	return func(_ context.Context, host string) ([]net.IP, error) {
		if ip := net.ParseIP(host); ip != nil {
			return []net.IP{ip}, nil
		}
		ips, ok := table[host]
		if !ok {
			return nil, errors.New("stub resolver: unknown host " + host)
		}

		return ips, nil
	}
}

// literalOnlyResolver is a resolver that resolves IP literals but errors on
// every hostname. Tests that should never hit DNS use it as a guard.
func literalOnlyResolver() HostResolver {
	return stubResolver(nil)
}

func TestValidateEgressProxy_NilPassthrough(t *testing.T) {
	t.Parallel()
	got, err := ValidateEgressProxy(t.Context(), nil, literalOnlyResolver())
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestValidateEgressProxy_HappyPath(t *testing.T) {
	t.Parallel()
	resolve := stubResolver(map[string][]net.IP{
		"proxy.example.com": {net.ParseIP("203.0.113.5")},
	})
	cfg := &EgressProxyConfig{
		Address:  "Proxy.Example.com:1080",
		Username: "alice",
		Password: "s3cret",
	}
	got, err := ValidateEgressProxy(t.Context(), cfg, resolve)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "proxy.example.com:1080", got.Address, "host should be lower-cased, port preserved")
	assert.Equal(t, "alice", got.Username)
	assert.Equal(t, "s3cret", got.Password)
}

func TestValidateEgressProxy_AcceptsIPLiteralWithoutResolving(t *testing.T) {
	t.Parallel()
	// IP literals must short-circuit before the resolver is invoked.
	got, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{Address: "203.0.113.5:1080"}, literalOnlyResolver())
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.5:1080", got.Address)
}

func TestValidateEgressProxy_RejectsMalformedAddress(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		addr  string
		match string
	}{
		{"empty", "", "must not be empty"},
		{"whitespace_only", "   ", "must not be empty"},
		{"no_port", "proxy.example.com", "host:port"},
		{"zero_port", "proxy.example.com:0", "valid 1-65535"},
		{"negative_port", "proxy.example.com:-1", "valid 1-65535"},
		{"out_of_range_port", "proxy.example.com:70000", "valid 1-65535"},
		{"non_numeric_port", "proxy.example.com:ssh", "valid 1-65535"},
		{"empty_host", ":1080", "host must not be empty"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{Address: c.addr}, literalOnlyResolver())
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.match)
		})
	}
}

func TestValidateEgressProxy_RejectsInternalEndpoint(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		addr string
		ips  []net.IP
	}{
		{"literal_loopback", "127.0.0.1:1080", nil},
		{"literal_ipv6_loopback", "[::1]:1080", nil},
		{"literal_private_10", "10.0.0.5:1080", nil},
		{"literal_private_172", "172.16.5.5:1080", nil},
		{"literal_private_192_168", "192.168.1.1:1080", nil},
		{"literal_link_local", "169.254.169.254:1080", nil},
		{"literal_ipv6_link_local", "[fe80::1]:1080", nil},
		{"resolves_to_private", "evil.example.com:1080", []net.IP{net.ParseIP("10.0.0.5")}},
		{
			"resolves_mixed_one_denied",
			"mixed.example.com:1080",
			[]net.IP{net.ParseIP("203.0.113.5"), net.ParseIP("10.0.0.5")},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			resolve := literalOnlyResolver()
			if c.ips != nil {
				resolve = stubResolver(map[string][]net.IP{
					"evil.example.com":  c.ips,
					"mixed.example.com": c.ips,
				})
			}
			_, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{Address: c.addr}, resolve)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrEgressProxyInternalEndpoint,
				"expected ErrEgressProxyInternalEndpoint, got: %v", err)
		})
	}
}

func TestValidateEgressProxy_RejectsOrphanPassword(t *testing.T) {
	t.Parallel()
	_, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{
		Address:  "203.0.113.5:1080",
		Username: "",
		Password: "somepw",
	}, literalOnlyResolver())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password must be empty when username is empty")
}

func TestValidateEgressProxy_RejectsOverlongUsername(t *testing.T) {
	t.Parallel()
	_, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{
		Address:  "203.0.113.5:1080",
		Username: strings.Repeat("a", 256),
		Password: "pw",
	}, literalOnlyResolver())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username must not exceed 255 bytes")
}

func TestValidateEgressProxy_RejectsOverlongPassword(t *testing.T) {
	t.Parallel()
	_, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{
		Address:  "203.0.113.5:1080",
		Username: "alice",
		Password: strings.Repeat("p", 256),
	}, literalOnlyResolver())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password must not exceed 255 bytes")
}

func TestValidateEgressProxy_AcceptsMaxLengthCreds(t *testing.T) {
	t.Parallel()
	user := strings.Repeat("a", 255)
	pass := strings.Repeat("p", 255)
	got, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{
		Address:  "203.0.113.5:1080",
		Username: user,
		Password: pass,
	}, literalOnlyResolver())
	require.NoError(t, err)
	assert.Equal(t, user, got.Username)
	assert.Equal(t, pass, got.Password)
}

func TestValidateEgressProxy_AcceptsEmptyCreds(t *testing.T) {
	t.Parallel()
	got, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{Address: "203.0.113.5:1080"}, literalOnlyResolver())
	require.NoError(t, err)
	assert.Empty(t, got.Username)
	assert.Empty(t, got.Password)
}

func TestValidateEgressProxy_DoesNotMutateInput(t *testing.T) {
	t.Parallel()
	in := &EgressProxyConfig{
		Address:  "PROXY.EXAMPLE.COM:1080",
		Username: "u",
		Password: "p",
	}
	resolve := stubResolver(map[string][]net.IP{
		"proxy.example.com": {net.ParseIP("203.0.113.5")},
	})
	_, err := ValidateEgressProxy(t.Context(), in, resolve)
	require.NoError(t, err)
	// Input preserved verbatim.
	assert.Equal(t, "PROXY.EXAMPLE.COM:1080", in.Address)
	assert.Equal(t, "u", in.Username)
	assert.Equal(t, "p", in.Password)
}

func TestValidateEgressProxy_NilResolverFallsBackToDefault(t *testing.T) {
	t.Parallel()
	// Passing nil resolve must not panic and must accept IP literals (the
	// default resolver short-circuits literals without touching DNS).
	got, err := ValidateEgressProxy(t.Context(), &EgressProxyConfig{Address: "203.0.113.5:1080"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.5:1080", got.Address)
}

// testCACertPEM builds a self-signed certificate, the shape a private-CA
// bundle takes. Nothing here verifies a chain, only that the bundle parses.
func testCACertPEM(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sandbox-network test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// Every block pem.Decode reads must be a certificate that parses.
func TestParseCACertPool(t *testing.T) {
	t.Parallel()

	good := testCACertPEM(t)
	second := testCACertPEM(t)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	accepted := []struct {
		name   string
		bundle string
	}{
		{name: "one certificate", bundle: good},
		{name: "a chain", bundle: good + second},
		{name: "comments around the blocks", bundle: "# root\n" + good + "subject=CN=intermediate\n" + second + "# end\n"},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pool, err := ParseCACertPool(tt.bundle)
			require.NoError(t, err)
			assert.NotNil(t, pool)
		})
	}

	rejected := []struct {
		name   string
		bundle string
	}{
		{name: "empty", bundle: ""},
		{name: "no PEM at all", bundle: "not a certificate"},
		{name: "a block that is not DER", bundle: "-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n"},
		{name: "a private key", bundle: good + keyPEM},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseCACertPool(tt.bundle)
			require.ErrorIs(t, err, ErrInvalidCACertificates)
		})
	}
}

func TestValidateEgressProxy_TLS(t *testing.T) {
	t.Parallel()

	caCertPEM := testCACertPEM(t)

	base := func(tlsCfg *EgressProxyTLSConfig) *EgressProxyConfig {
		return &EgressProxyConfig{Address: "203.0.113.5:1080", TLS: tlsCfg}
	}

	t.Run("nil block passes through", func(t *testing.T) {
		t.Parallel()
		got, err := ValidateEgressProxy(t.Context(), base(nil), literalOnlyResolver())
		require.NoError(t, err)
		assert.Nil(t, got.TLS)
	})

	t.Run("enabled with no options is accepted", func(t *testing.T) {
		t.Parallel()
		got, err := ValidateEgressProxy(t.Context(), base(&EgressProxyTLSConfig{Enabled: true}), literalOnlyResolver())
		require.NoError(t, err)
		require.NotNil(t, got.TLS)
		assert.True(t, got.TLS.Enabled)
	})

	t.Run("server name is canonicalized", func(t *testing.T) {
		t.Parallel()
		got, err := ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: true, ServerName: "  Proxy.Example.COM "}), literalOnlyResolver())
		require.NoError(t, err)
		assert.Equal(t, "proxy.example.com", got.TLS.ServerName)
	})

	t.Run("disabled block canonicalizes to absent", func(t *testing.T) {
		t.Parallel()
		got, err := ValidateEgressProxy(t.Context(), base(&EgressProxyTLSConfig{Enabled: false}), literalOnlyResolver())
		require.NoError(t, err)
		assert.Nil(t, got.TLS, "an explicitly disabled block is the same as no block")
	})

	t.Run("disabled block must carry nothing else", func(t *testing.T) {
		t.Parallel()
		// Accepting these would store a config that reads as though it
		// verifies something while the hop stays in the clear.
		_, err := ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: false, ServerName: "proxy.example.com"}), literalOnlyResolver())
		require.Error(t, err)

		_, err = ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: false, CACert: caCertPEM}), literalOnlyResolver())
		require.Error(t, err)
	})

	t.Run("ca bundle must parse", func(t *testing.T) {
		t.Parallel()
		_, err := ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: true, CACert: "-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----"}),
			literalOnlyResolver())
		require.ErrorIs(t, err, ErrInvalidCACertificates)
	})

	t.Run("parseable ca bundle is kept", func(t *testing.T) {
		t.Parallel()
		got, err := ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: true, CACert: caCertPEM}), literalOnlyResolver())
		require.NoError(t, err)
		assert.Equal(t, strings.TrimSpace(caCertPEM), got.TLS.CACert)
	})

	t.Run("oversized fields are rejected", func(t *testing.T) {
		t.Parallel()
		_, err := ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: true, ServerName: strings.Repeat("a", maxServerNameLen+1)}),
			literalOnlyResolver())
		require.Error(t, err)

		_, err = ValidateEgressProxy(t.Context(),
			base(&EgressProxyTLSConfig{Enabled: true, CACert: strings.Repeat("a", maxCACertLen+1)}),
			literalOnlyResolver())
		require.Error(t, err)
	})

	t.Run("input is not mutated", func(t *testing.T) {
		t.Parallel()
		in := base(&EgressProxyTLSConfig{Enabled: true, ServerName: "PROXY.EXAMPLE.COM"})
		_, err := ValidateEgressProxy(t.Context(), in, literalOnlyResolver())
		require.NoError(t, err)
		assert.Equal(t, "PROXY.EXAMPLE.COM", in.TLS.ServerName)
	})
}

func TestIsIPInDeniedSandboxCIDRs_Exported(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ip   string
		want bool
	}{
		{"10.0.0.5", true},
		{"100.64.0.1", true},
		{"100.127.255.254", true},
		{"172.16.0.1", true},
		{"192.168.5.5", true},
		{"127.0.0.1", true},
		{"169.254.169.254", true},
		{"::1", true},
		{"fe80::1", true},
		{"fc00::1", true},
		{"1.0.0.1", false},
		{"8.8.8.8", false},
		{"100.63.255.255", false},
		{"100.128.0.0", false},
		{"203.0.113.5", false},
		{"2001:db8::1", false},
		// The unspecified and "this network" block address are denied as BYOP endpoints.
		{"0.0.0.0", true},
		{"0.1.2.3", true},
		{"::", true},
	}
	for _, c := range cases {
		t.Run(c.ip, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, c.want, IsIPInDeniedSandboxCIDRs(net.ParseIP(c.ip)))
		})
	}
}
