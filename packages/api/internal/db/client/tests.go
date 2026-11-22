package client

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func (db *Client) TestsRawSQL(ctx context.Context, sql string, args ...any) error {
	_, err := db.conn.Exec(ctx, sql, args...)

	return err
}

func (db *Client) TestsRawSQLQuery(ctx context.Context, sql string, processRows func(pgx.Rows) error, args ...any) error {
	rows, err := db.conn.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return processRows(rows)
}
