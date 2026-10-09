package client

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq" //nolint:blank-imports

	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/pool"
	database "github.com/e2b-dev/infra/packages/db/queries"
)

const poolName = "main"

type Client struct {
	*database.Queries

	Dashboard *dashboardqueries.Queries
	conn      *pgxpool.Pool
}

func NewClient(ctx context.Context, databaseURL string, options ...pool.Option) (*Client, error) {
	return newClient(ctx, databaseURL, poolName, options...)
}

// NewReadClient is NewClient for a read replica. Its pool metrics carry
// pool.name "read" so they stay apart from the primary's.
func NewReadClient(ctx context.Context, databaseURL string, options ...pool.Option) (*Client, error) {
	return newClient(ctx, databaseURL, "read", options...)
}

func newClient(ctx context.Context, databaseURL string, name string, options ...pool.Option) (*Client, error) {
	dbClient, connPool, err := pool.New(ctx, databaseURL, name, options...)
	if err != nil {
		return nil, err
	}

	return &Client{
		Queries:   database.New(dbClient),
		Dashboard: dashboardqueries.New(dbClient),
		conn:      connPool,
	}, nil
}

func (db *Client) Close() error {
	db.conn.Close()

	return nil
}

// Pool exposes the underlying pgx pool for the River outbox client.
func (db *Client) Pool() *pgxpool.Pool {
	return db.conn
}

// WithTx starts a read-write transaction and returns a transactional Client.
func (db *Client) WithTx(ctx context.Context) (*Client, pgx.Tx, error) {
	tx, err := db.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, err
	}

	client := &Client{
		Queries:   db.Queries.WithTx(tx),
		Dashboard: db.Dashboard.WithTx(tx),
		conn:      db.conn,
	}

	return client, tx, nil
}
