package factories

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var ErrRedisDisabled = errors.New("redis is disabled")

type RedisConfig struct {
	RedisURL         string
	RedisClusterURL  string
	RedisTLSCABase64 string
	// RedisPassword authenticates deployments that require it (Azure Managed Redis access key); aws/gcp leave it empty.
	RedisPassword string
	// RedisTLSEnabled turns on TLS without a custom CA, for publicly-signed endpoints like Azure Managed Redis.
	RedisTLSEnabled bool
	// PoolSize overrides the default connection pool size.
	// When non-positive, defaults to 40.
	PoolSize int
	// MinIdleConns overrides the minimum number of idle connections maintained in the pool
	// (per cluster node for cluster clients).
	// When non-positive, defaults to min(defaultMinIdleConns, PoolSize).
	MinIdleConns int
}

const (
	defaultPoolSize     = 40
	defaultMinIdleConns = 10

	// connMaxLifetime controls the maximum age of a connection before it is recycled.
	// Combined with connMaxLifetimeJitter, this spreads connection recycling evenly over time
	// instead of expiring in bursts (which happens with idle-time-based eviction under LIFO reuse).
	connMaxLifetime = 30 * time.Minute
	// connMaxLifetimeJitter adds random offset in [-jitter, +jitter] to each connection's lifetime,
	// so connections expire between 20-40 minutes instead of all at exactly 30 minutes.
	connMaxLifetimeJitter = 10 * time.Minute
)

// resolvePoolSize computes the effective pool size and minimum idle connections
// from the given config, applying defaults and floors.
func resolvePoolSize(config RedisConfig) (poolSize, minIdleConns int) {
	poolSize = defaultPoolSize
	if config.PoolSize > 0 {
		poolSize = config.PoolSize
	}

	minIdleConns = min(defaultMinIdleConns, poolSize)
	if config.MinIdleConns > 0 {
		minIdleConns = min(config.MinIdleConns, poolSize)
	}

	return poolSize, minIdleConns
}

// resolveTLSConfig builds the TLS config for a Redis endpoint, or nil when TLS is off.
// TLS is on iff RedisTLSEnabled; a CA only customizes verification and requires the flag.
func resolveTLSConfig(ctx context.Context, config RedisConfig, addr string) (*tls.Config, error) {
	if config.RedisTLSCABase64 != "" && !config.RedisTLSEnabled {
		// A trust anchor for a plaintext connection is always a misconfiguration; fail once, named, at startup.
		return nil, errors.New("REDIS_TLS_CA_BASE64 is set but REDIS_TLS_ENABLED is not: a CA without TLS is meaningless -- set REDIS_TLS_ENABLED=true or remove the CA")
	}

	if !config.RedisTLSEnabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	// Pin ServerName to the host: IP-redirecting clusters (Azure Managed Redis private endpoints) present hostname-only certs with no IP SANs.
	tlsConfig.ServerName = addr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		tlsConfig.ServerName = host
	}

	if config.RedisTLSCABase64 != "" {
		cert, err := base64.StdEncoding.DecodeString(config.RedisTLSCABase64)
		if err != nil {
			logger.L().Error(ctx, "Failed to decode Redis TLS CA certificate from base64", zap.Error(err))

			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(cert) {
			logger.L().Error(ctx, "Failed to parse Redis TLS CA certificate")

			return nil, errors.New("failed to parse Redis TLS CA certificate")
		}

		tlsConfig.RootCAs = certPool
		logger.L().Info(ctx, "Redis will be started with TLS enabled (custom CA)")

		return tlsConfig, nil
	}

	logger.L().Info(ctx, "Redis will be started with TLS enabled (system CA)")

	return tlsConfig, nil
}

// refusePlaintextPassword: go-redis sends AUTH on every connection, so a password without TLS puts the credential on the wire in cleartext -- fail once at startup with a named error instead.
// It takes the effective password rather than the config, so a password spelled inside REDIS_URL meets the same rule as REDIS_PASSWORD.
func refusePlaintextPassword(password string, tlsConfig *tls.Config) error {
	if password != "" && tlsConfig == nil {
		return errors.New("a Redis password is set but TLS is not enabled: refusing to send credentials over plaintext (set REDIS_TLS_ENABLED or use the rediss:// scheme, or drop the password)")
	}

	return nil
}

// parseRedisURL reads REDIS_URL, which is either a redis:// / rediss:// URL carrying
// credentials and a database number, or the bare host:port that predates it.
// A scheme means the value is meant as a URL, so failing to parse one is a
// misconfiguration; only a schemeless value falls through to host:port.
func parseRedisURL(redisURL string) (*redis.Options, error) {
	if !strings.Contains(redisURL, "://") {
		return &redis.Options{Addr: redisURL}, nil
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		// Parser errors echo slices of the input -- the URL, host:port
		// fragments, escape sequences -- and any slice may carry the
		// credential (an unencoded '#' in a password puts it in the "invalid
		// port" detail, for example). Callers log this error at startup, so
		// no parser text passes through at all.
		return nil, errors.New(`failed to parse REDIS_URL: "<redacted>" is not a valid redis URL`)
	}

	return opts, nil
}

// newRedisOptions builds the single-node client options, reconciling what REDIS_URL
// carries with what the explicit REDIS_* settings ask for.
func newRedisOptions(ctx context.Context, config RedisConfig, poolSize, minIdleConns int) (*redis.Options, error) {
	opts, err := parseRedisURL(config.RedisURL)
	if err != nil {
		return nil, err
	}

	// rediss:// asks for TLS in the URL. Fold it into the flag so a single path resolves
	// the CA, pins the ServerName, and decides whether a password may go on the wire.
	tlsRequest := config
	tlsRequest.RedisTLSEnabled = config.RedisTLSEnabled || opts.TLSConfig != nil

	// The parsed address, not the raw URL: resolveTLSConfig pins ServerName to the host.
	tlsConfig, err := resolveTLSConfig(ctx, tlsRequest, opts.Addr)
	if err != nil {
		return nil, err
	}

	if config.RedisPassword != "" {
		// Two passwords that disagree is a misconfiguration either way it resolves; name it at startup.
		if opts.Password != "" && opts.Password != config.RedisPassword {
			return nil, errors.New("REDIS_PASSWORD and the password in REDIS_URL differ: set the password in one place")
		}

		opts.Password = config.RedisPassword
	}

	if err := refusePlaintextPassword(opts.Password, tlsConfig); err != nil {
		return nil, err
	}

	opts.TLSConfig = tlsConfig

	// The resolved pool settings are authoritative, so any pool parameters in the URL query are overwritten.
	opts.PoolSize = poolSize
	opts.MinIdleConns = minIdleConns
	opts.ConnMaxIdleTime = -1
	opts.ConnMaxLifetime = connMaxLifetime
	opts.ConnMaxLifetimeJitter = connMaxLifetimeJitter

	return opts, nil
}

func NewRedisClient(ctx context.Context, config RedisConfig) (redis.UniversalClient, error) {
	var redisClient redis.UniversalClient

	poolSize, minIdleConns := resolvePoolSize(config)

	switch {
	case config.RedisClusterURL != "":
		// For managed Redis Cluster in GCP we should use Cluster Client, because
		// > Redis node endpoints can change and can be recycled as nodes are added and removed over time.
		// https://cloud.google.com/memorystore/docs/cluster/cluster-node-specification#cluster_endpoints
		// https://cloud.google.com/memorystore/docs/cluster/client-library-code-samples#go-redis

		clusterOpts := &redis.ClusterOptions{
			Addrs:        []string{config.RedisClusterURL},
			Password:     config.RedisPassword,
			PoolSize:     poolSize,
			MinIdleConns: minIdleConns,
			// Disable idle-time eviction; use lifetime-based recycling with jitter instead.
			// Under the default LIFO reuse, ConnMaxIdleTime causes thundering-herd bursts because
			// the cold (bottom-of-stack) connections all idle-expire simultaneously.
			ConnMaxIdleTime:       -1,
			ConnMaxLifetime:       connMaxLifetime,
			ConnMaxLifetimeJitter: connMaxLifetimeJitter,
		}

		tlsConfig, err := resolveTLSConfig(ctx, config, config.RedisClusterURL)
		if err != nil {
			return nil, err
		}
		if err := refusePlaintextPassword(config.RedisPassword, tlsConfig); err != nil {
			return nil, err
		}
		clusterOpts.TLSConfig = tlsConfig

		redisClient = redis.NewClusterClient(clusterOpts)
	case config.RedisURL != "":
		opts, err := newRedisOptions(ctx, config, poolSize, minIdleConns)
		if err != nil {
			return nil, err
		}

		redisClient = redis.NewClient(opts)
	default:
		return nil, ErrRedisDisabled
	}

	// Enable tracing.
	if err := redisotel.InstrumentTracing(
		redisClient,
		redisotel.WithDBStatement(false),
		redisotel.WithCallerEnabled(false),
	); err != nil {
		closeErr := redisClient.Close()

		return nil, errors.Join(fmt.Errorf("failed to enable redis tracing: %w", err), closeErr)
	}

	// Enable metrics (pool stats, command latency histograms)
	if err := redisotel.InstrumentMetrics(redisClient); err != nil {
		closeErr := redisClient.Close()

		return nil, errors.Join(fmt.Errorf("failed to enable redis metrics: %w", err), closeErr)
	}

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		closeErr := redisClient.Close()

		return nil, errors.Join(fmt.Errorf("failed to ping redis: %w", err), closeErr)
	}

	return redisClient, nil
}

func CloseCleanly(client redis.UniversalClient) error {
	if err := client.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
		return err
	}

	return nil
}
