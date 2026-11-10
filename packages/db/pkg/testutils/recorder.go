package testutils

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/pool"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
)

// QueryRecorder records the sqlc name of every statement run through it, so a
// test can count the statements a code path issues.
type QueryRecorder struct {
	types.DBTX

	mu    sync.Mutex
	names []string
}

// RecordedAuthDB returns an auth client on this test's database whose
// statements the returned recorder sees. Its Close and transactions are not
// usable; it serves queries only.
func (db *Database) RecordedAuthDB(t *testing.T) (*authdb.Client, *QueryRecorder) {
	t.Helper()

	dbtx, conn, err := pool.New(t.Context(), db.connStr, "recorded")
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	recorder := &QueryRecorder{DBTX: dbtx}

	return &authdb.Client{Queries: authqueries.New(recorder)}, recorder
}

// Take returns the names recorded since the last call.
func (r *QueryRecorder) Take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := r.names
	r.names = nil

	return names
}

func (r *QueryRecorder) record(sql string) {
	name, _, _ := strings.Cut(strings.TrimPrefix(sql, "-- name: "), " ")

	r.mu.Lock()
	defer r.mu.Unlock()
	r.names = append(r.names, name)
}

func (r *QueryRecorder) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.record(sql)

	return r.DBTX.Exec(ctx, sql, args...)
}

func (r *QueryRecorder) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.record(sql)

	return r.DBTX.Query(ctx, sql, args...)
}

func (r *QueryRecorder) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	r.record(sql)

	return r.DBTX.QueryRow(ctx, sql, args...)
}
