package main

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
)

func TestOpenDatabaseConnections(t *testing.T) {
	t.Parallel()
	databaseURL, err := url.Parse(dbtest.ConnString(t, dbtest.EmptyPool(t)))
	require.NoError(t, err)
	query := databaseURL.Query()
	query.Set("pool_max_conns", "30")
	query.Set("synchronous_commit", "off")
	databaseURL.RawQuery = query.Encode()
	pool, err := openDatabase(t.Context(), databaseURL.String(), 2)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.Equal(t, int32(2), pool.Config().MaxConns)

	for range 2 {
		first, err := pool.Acquire(t.Context())
		require.NoError(t, err)
		t.Cleanup(first.Release)
		second, err := pool.Acquire(t.Context())
		require.NoError(t, err)
		t.Cleanup(second.Release)
		var synchronousCommit string
		require.NoError(t, first.QueryRow(t.Context(), "SHOW synchronous_commit").Scan(&synchronousCommit))
		require.Equal(t, "on", synchronousCommit)
		require.NoError(t, second.QueryRow(t.Context(), "SHOW synchronous_commit").Scan(&synchronousCommit))
		require.Equal(t, "on", synchronousCommit)
		first.Release()
		second.Release()
		pool.Reset()
	}
}

func TestOpenDatabaseRejectsReadOnly(t *testing.T) {
	t.Parallel()
	databaseURL, err := url.Parse(dbtest.ConnString(t, dbtest.EmptyPool(t)))
	require.NoError(t, err)
	query := databaseURL.Query()
	query.Set("default_transaction_read_only", "on")
	databaseURL.RawQuery = query.Encode()
	pool, err := openDatabase(t.Context(), databaseURL.String(), 1)
	if pool != nil {
		t.Cleanup(pool.Close)
	}
	require.ErrorContains(t, err, "must be a writable primary")
	require.Nil(t, pool)
}

func TestOpenDatabasePoolLimits(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		query   string
		maximum int32
	}{
		"zero":         {maximum: 0},
		"negative":     {maximum: -1},
		"minimum":      {query: "pool_min_conns=3", maximum: 2},
		"minimum idle": {query: "pool_min_idle_conns=3", maximum: 2},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, err := openDatabase(t.Context(), "postgres://localhost/scheduling?"+tc.query, tc.maximum)
			if pool != nil {
				t.Cleanup(pool.Close)
			}
			require.Error(t, err)
			require.Nil(t, pool)
		})
	}
}
