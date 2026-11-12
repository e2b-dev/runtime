package dbmigrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testLockName = "test-migrations"

var (
	errOperation = errors.New("operation failed")
	driverID     atomic.Uint64
)

type lockDriver struct {
	mu       sync.Mutex
	queries  []string
	unlocked bool
}

func (d *lockDriver) Open(string) (driver.Conn, error) {
	return &lockConnection{driver: d}, nil
}

func (d *lockDriver) record(query string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = append(d.queries, query)
}

func (d *lockDriver) recordedQueries() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]string(nil), d.queries...)
}

type lockConnection struct {
	driver *lockDriver
}

func (c *lockConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *lockConnection) Close() error {
	return nil
}

func (c *lockConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *lockConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) != 1 || args[0].Value != testLockName {
		return nil, fmt.Errorf("unexpected lock arguments: %v", args)
	}

	c.driver.record(query)
	if strings.Contains(query, "pg_advisory_unlock") {
		return &singleValueRow{value: c.driver.unlocked}, nil
	}

	return &singleValueRow{value: nil}, nil
}

type singleValueRow struct {
	read  bool
	value driver.Value
}

func (r *singleValueRow) Columns() []string {
	return []string{"value"}
}

func (r *singleValueRow) Close() error {
	return nil
}

func (r *singleValueRow) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	values[0] = r.value

	return nil
}

func openLockDatabase(t *testing.T, unlocked bool) (*sql.DB, *lockDriver) {
	t.Helper()

	fake := &lockDriver{unlocked: unlocked}
	name := fmt.Sprintf("dbmigrate-test-%d", driverID.Add(1))
	sql.Register(name, fake)
	databaseConnection, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, databaseConnection.Close())
	})

	return databaseConnection, fake
}

func TestWithPostgresSessionLockReleasesAfterOperationError(t *testing.T) {
	t.Parallel()
	databaseConnection, fake := openLockDatabase(t, true)

	err := WithPostgresSessionLock(t.Context(), databaseConnection, testLockName, func(ctx context.Context) error {
		assert.Same(t, t.Context(), ctx, "operation received a different context")
		fake.record("operation")

		return errOperation
	})
	require.ErrorIs(t, err, errOperation)
	queries := fake.recordedQueries()
	require.Len(t, queries, 3)
	require.Contains(t, queries[0], "pg_advisory_lock")
	require.Equal(t, "operation", queries[1])
	require.Contains(t, queries[2], "pg_advisory_unlock")
}

func TestWithPostgresSessionLockReleasesAfterContextCancellation(t *testing.T) {
	t.Parallel()
	databaseConnection, fake := openLockDatabase(t, true)

	ctx, cancel := context.WithCancel(t.Context())
	err := WithPostgresSessionLock(ctx, databaseConnection, testLockName, func(context.Context) error {
		fake.record("operation")
		cancel()

		return nil
	})
	require.NoError(t, err)
	queries := fake.recordedQueries()
	require.Len(t, queries, 3)
	require.Equal(t, "operation", queries[1])
	require.Contains(t, queries[2], "pg_advisory_unlock", "lock was not released")
}

func TestWithPostgresSessionLockReportsLostLock(t *testing.T) {
	t.Parallel()
	databaseConnection, _ := openLockDatabase(t, false)

	err := WithPostgresSessionLock(t.Context(), databaseConnection, testLockName, func(context.Context) error {
		return nil
	})
	require.ErrorIs(t, err, ErrLockNotHeld)
}
