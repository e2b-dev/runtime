package factories

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// selfSignedCAPEM returns a base64-encoded PEM CA (ECDSA P-256: keygen is microseconds where RSA-2048 cost seconds per run).
func selfSignedCAPEM(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// TLS is on iff RedisTLSEnabled; a CA only customizes verification and requires the flag.
func TestResolveTLSConfig(t *testing.T) {
	t.Parallel()

	ca := selfSignedCAPEM(t)

	t.Run("neither set leaves TLS off", func(t *testing.T) {
		t.Parallel()

		// The aws path and plaintext single-node deployments: nothing set, nothing changes.
		cfg, err := resolveTLSConfig(t.Context(), RedisConfig{}, "redis.example:6379")
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("a CA without the flag is refused at startup", func(t *testing.T) {
		t.Parallel()

		// A trust anchor for a plaintext connection is always a misconfiguration.
		_, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSCABase64: ca}, "redis.example:6379")
		require.ErrorContains(t, err, "REDIS_TLS_ENABLED")
	})

	t.Run("a garbage CA without the flag is refused the same way", func(t *testing.T) {
		t.Parallel()

		_, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSCABase64: "!!!not-base64!!!"}, "redis.example:6379")
		require.ErrorContains(t, err, "REDIS_TLS_ENABLED")
	})

	t.Run("flag alone verifies against the system trust store", func(t *testing.T) {
		t.Parallel()

		// Azure Managed Redis: publicly-signed cert; ServerName pinned (private-endpoint IP redirects have no IP SANs).
		cfg, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSEnabled: true}, "cache.redis.azure.net:10000")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Nil(t, cfg.RootCAs)
		assert.Equal(t, "cache.redis.azure.net", cfg.ServerName)
		assert.Equal(t, uint16(0x0303), cfg.MinVersion) // TLS 1.2
	})

	t.Run("flag plus CA trusts the CA", func(t *testing.T) {
		t.Parallel()

		// The GCP shape (after terraform also sets the flag wherever it provisions the CA).
		cfg, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSCABase64: ca, RedisTLSEnabled: true}, "redis.example:6379")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.NotNil(t, cfg.RootCAs)
		assert.Equal(t, "redis.example", cfg.ServerName)
	})

	t.Run("undecodable CA with the flag is an error, not a silent plaintext fallback", func(t *testing.T) {
		t.Parallel()

		_, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSCABase64: "!!!not-base64!!!", RedisTLSEnabled: true}, "redis.example:6379")
		require.Error(t, err)
	})

	t.Run("valid base64 that is not a certificate is an error", func(t *testing.T) {
		t.Parallel()

		_, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSCABase64: base64.StdEncoding.EncodeToString([]byte("not a pem")), RedisTLSEnabled: true}, "redis.example:6379")
		require.Error(t, err)
	})
}

// Both TLS shapes pin the configured host (per-connection inference breaks IP-redirecting clusters).
func TestResolveTLSConfigServerName(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ addr, want string }{
		"host and port":     {"cache.redis.azure.net:10000", "cache.redis.azure.net"},
		"host without port": {"cache.redis.azure.net", "cache.redis.azure.net"},
		// A bare ":" split produced "[" here and failed the handshake on hostname mismatch.
		"ipv6 literal": {"[::1]:6379", "::1"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg, err := resolveTLSConfig(t.Context(), RedisConfig{RedisTLSEnabled: true}, tc.addr)
			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.Equal(t, tc.want, cfg.ServerName)
		})
	}
}

// A password with no TLS puts the credential on the wire in cleartext; construction must refuse it.
func TestNewRedisClientRefusesPlaintextPassword(t *testing.T) {
	t.Parallel()

	t.Run("cluster", func(t *testing.T) {
		t.Parallel()

		_, err := NewRedisClient(t.Context(), RedisConfig{RedisClusterURL: "redis.example:6379", RedisPassword: "secret"})
		require.ErrorContains(t, err, "plaintext")
	})

	t.Run("single-node", func(t *testing.T) {
		t.Parallel()

		_, err := NewRedisClient(t.Context(), RedisConfig{RedisURL: "redis.example:6379", RedisPassword: "secret"})
		require.ErrorContains(t, err, "plaintext")
	})

	t.Run("a password spelled in the URL meets the same rule", func(t *testing.T) {
		t.Parallel()

		// Otherwise the URL form would be a way around the rule REDIS_PASSWORD obeys.
		_, err := NewRedisClient(t.Context(), RedisConfig{RedisURL: "redis://:secret@redis.example:6379"})
		require.ErrorContains(t, err, "plaintext")
	})

	t.Run("a password with TLS is allowed", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, refusePlaintextPassword("secret", &tls.Config{MinVersion: tls.VersionTLS12}))
	})
}

// REDIS_URL accepts the redis:// URL form as well as the bare host:port that predates it.
func TestParseRedisURL(t *testing.T) {
	t.Parallel()

	t.Run("a bare host:port is an address, not a URL", func(t *testing.T) {
		t.Parallel()

		opts, err := parseRedisURL("redis.example:6379")
		require.NoError(t, err)
		assert.Equal(t, "redis.example:6379", opts.Addr)
		assert.Empty(t, opts.Password)
		assert.Nil(t, opts.TLSConfig)
	})

	t.Run("a URL yields address, credentials and database", func(t *testing.T) {
		t.Parallel()

		// The reported failure: the whole string went to Addr and dialing said "too many colons".
		opts, err := parseRedisURL("redis://user:secret@redis.example:6379/2")
		require.NoError(t, err)
		assert.Equal(t, "redis.example:6379", opts.Addr)
		assert.Equal(t, "user", opts.Username)
		assert.Equal(t, "secret", opts.Password)
		assert.Equal(t, 2, opts.DB)
	})

	t.Run("rediss carries TLS", func(t *testing.T) {
		t.Parallel()

		opts, err := parseRedisURL("rediss://redis.example:6380")
		require.NoError(t, err)
		require.NotNil(t, opts.TLSConfig)
		assert.Equal(t, "redis.example", opts.TLSConfig.ServerName)
	})

	t.Run("a scheme that fails to parse is an error, not an address", func(t *testing.T) {
		t.Parallel()

		// Falling back to host:port here would turn a typo into an unexplained dial failure.
		for name, url := range map[string]string{
			"unsupported scheme": "http://redis.example:6379",
			"bad database":       "redis://redis.example:6379/not-a-number",
			"unknown parameter":  "redis://redis.example:6379?nonsense=1",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				_, err := parseRedisURL(url)
				require.ErrorContains(t, err, "REDIS_URL")
			})
		}
	})

	t.Run("a parse error does not echo the credentials", func(t *testing.T) {
		t.Parallel()

		// net/url echoes slices of the input back in its errors: the invalid
		// escape quotes the fragment-stripped URL, and an unencoded '#' in
		// the password turns the rest into an "invalid port" detail carrying
		// the credential. None of it may reach the returned error.
		for name, url := range map[string]string{
			"invalid escape":      "redis://user:secret%zz@redis.example:6379",
			"with fragment":       "redis://user:secret%zz@redis.example:6379#fragment",
			"'#' in the password": "redis://:secret#pass@redis.example:6379",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				_, err := parseRedisURL(url)
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "secret")
				assert.Contains(t, err.Error(), "<redacted>")
			})
		}
	})
}

// The URL and the explicit REDIS_* settings describe one endpoint; the options reconcile them.
func TestNewRedisOptions(t *testing.T) {
	t.Parallel()

	t.Run("pool settings win over the URL query", func(t *testing.T) {
		t.Parallel()

		opts, err := newRedisOptions(t.Context(), RedisConfig{RedisURL: "redis://redis.example:6379?pool_size=3"}, 40, 10)
		require.NoError(t, err)
		assert.Equal(t, 40, opts.PoolSize)
		assert.Equal(t, 10, opts.MinIdleConns)
		assert.Equal(t, time.Duration(-1), opts.ConnMaxIdleTime)
		assert.Equal(t, connMaxLifetime, opts.ConnMaxLifetime)
	})

	t.Run("rediss enables the configured CA and pins the URL host", func(t *testing.T) {
		t.Parallel()

		// rediss:// alone means TLS, so a CA alongside it is honored rather than refused.
		opts, err := newRedisOptions(t.Context(), RedisConfig{
			RedisURL:         "rediss://:secret@redis.example:6380",
			RedisTLSCABase64: selfSignedCAPEM(t),
		}, 40, 10)
		require.NoError(t, err)
		require.NotNil(t, opts.TLSConfig)
		assert.NotNil(t, opts.TLSConfig.RootCAs)
		assert.Equal(t, "redis.example", opts.TLSConfig.ServerName)
		assert.Equal(t, "secret", opts.Password)
	})

	t.Run("REDIS_TLS_ENABLED applies to a plain redis URL", func(t *testing.T) {
		t.Parallel()

		opts, err := newRedisOptions(t.Context(), RedisConfig{
			RedisURL:        "redis://:secret@redis.example:6379",
			RedisTLSEnabled: true,
		}, 40, 10)
		require.NoError(t, err)
		require.NotNil(t, opts.TLSConfig)
		assert.Equal(t, "redis.example", opts.TLSConfig.ServerName)
	})

	t.Run("REDIS_PASSWORD supplies the password a URL omits", func(t *testing.T) {
		t.Parallel()

		opts, err := newRedisOptions(t.Context(), RedisConfig{
			RedisURL:        "rediss://redis.example:6380",
			RedisPassword:   "secret",
			RedisTLSEnabled: true,
		}, 40, 10)
		require.NoError(t, err)
		assert.Equal(t, "secret", opts.Password)
	})

	t.Run("two passwords that disagree are refused", func(t *testing.T) {
		t.Parallel()

		_, err := newRedisOptions(t.Context(), RedisConfig{
			RedisURL:        "rediss://:from-url@redis.example:6380",
			RedisPassword:   "from-env",
			RedisTLSEnabled: true,
		}, 40, 10)
		require.ErrorContains(t, err, "REDIS_PASSWORD")
	})

	t.Run("a bare host:port keeps behaving as before", func(t *testing.T) {
		t.Parallel()

		opts, err := newRedisOptions(t.Context(), RedisConfig{RedisURL: "redis.example:6379"}, 40, 10)
		require.NoError(t, err)
		assert.Equal(t, "redis.example:6379", opts.Addr)
		assert.Empty(t, opts.Password)
		assert.Nil(t, opts.TLSConfig)
	})
}
