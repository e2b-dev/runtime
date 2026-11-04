package pool

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/db/pkg/retry"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
)

func New(ctx context.Context, databaseURL string, poolName string, options ...Option) (types.DBTX, *pgxpool.Pool, error) {
	config, retryConfig, err := newConfig(databaseURL, otelpgx.NewTracer(), options...)
	if err != nil {
		return nil, nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err = recordStats(pool, attribute.String("pool.name", poolName)); err != nil {
		pool.Close()

		return nil, nil, fmt.Errorf("failed to record stats: %w", err)
	}

	return retry.Wrap(pool, retryConfig), pool, nil
}

// Client owns a PostgreSQL connection pool and the session-scoped operations
// that must stay on one of its connections.
type Client struct {
	pool *pgxpool.Pool
}

// Connect creates a checked PostgreSQL client. Unlike New, it returns the raw
// pool without statement-level retries, so callers can make retry decisions at
// transaction boundaries where the outcome is unambiguous.
func Connect(ctx context.Context, databaseURL string, poolName string, options ...Option) (*Client, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection pool config: %w", err)
	}
	Configure(config, options...)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	if err := recordStats(pool, attribute.String("pool.name", poolName)); err != nil {
		pool.Close()

		return nil, fmt.Errorf("failed to record stats: %w", err)
	}

	return NewClient(pool), nil
}

// Configure applies the settings Connect gives its pool, for a pool created
// elsewhere and wrapped by NewClient.
func Configure(config *pgxpool.Config, options ...Option) {
	configure(config, otelpgx.NewTracer(otelpgx.WithDisableConnectionDetailsInAttributes()), options...)
}

// NewClient wraps a pool configured by Configure. Closing the client closes
// the pool.
func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// Close closes every connection in the pool.
func (c *Client) Close() {
	c.pool.Close()
}

// Pool returns the underlying pool for libraries that integrate directly with
// pgxpool.
func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}

func newConfig(
	databaseURL string,
	tracer pgx.QueryTracer,
	options ...Option,
) (*pgxpool.Config, retry.Config, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, retry.Config{}, fmt.Errorf("failed to parse connection pool config: %w", err)
	}

	return config, configure(config, tracer, options...), nil
}

func configure(config *pgxpool.Config, tracer pgx.QueryTracer, options ...Option) retry.Config {
	retryConfig := retry.DefaultConfig()
	for _, opt := range options {
		opt(config, &retryConfig)
	}

	config.ConnConfig.Tracer = tracer
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	previousAfterConnect := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if previousAfterConnect != nil {
			if err := previousAfterConnect(ctx, conn); err != nil {
				return err
			}
		}

		typeMap := conn.TypeMap()
		typeMap.RegisterType(&pgtype.Type{
			Name: "uuid[]",
			OID:  pgtype.UUIDArrayOID,
			Codec: &pgtype.ArrayCodec{
				ElementType: &pgtype.Type{
					Name:  "uuid",
					OID:   pgtype.UUIDOID,
					Codec: pgtype.UUIDCodec{},
				},
			},
		})
		typeMap.RegisterDefaultPgType([]uuid.UUID{}, "uuid[]")

		return nil
	}

	return retryConfig
}

func recordStats(pool otelpgx.PoolStats, poolName attribute.KeyValue) error {
	return otelpgx.RecordStats(pool, otelpgx.WithStatsAttributes(poolName))
}
