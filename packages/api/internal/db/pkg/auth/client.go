package authdb

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq" //nolint:blank-imports

	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/pool"
)

const poolName = "auth"

type Client struct {
	*authqueries.Queries

	conn *pgxpool.Pool
}

func NewClient(ctx context.Context, databaseURL string, options ...pool.Option) (*Client, error) {
	client, conn, err := pool.New(ctx, databaseURL, poolName, options...)
	if err != nil {
		return nil, err
	}

	return &Client{Queries: authqueries.New(client), conn: conn}, nil
}

func (db *Client) Close() error {
	db.conn.Close()

	return nil
}

// WithTx runs the given function in a transaction.
func (db *Client) WithTx(ctx context.Context) (*authqueries.Queries, pgx.Tx, error) {
	tx, err := db.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, err
	}

	return db.Queries.WithTx(tx), tx, nil
}

// TestsRawSQL executes raw SQL for tests
func (db *Client) TestsRawSQL(ctx context.Context, sql string, args ...any) error {
	_, err := db.conn.Exec(ctx, sql, args...)

	return err
}

// TestsRawSQLQuery executes raw SQL query and processes rows with the given function
func (db *Client) TestsRawSQLQuery(ctx context.Context, sql string, processRows func(pgx.Rows) error, args ...any) error {
	rows, err := db.conn.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return processRows(rows)
}
