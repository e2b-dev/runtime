package client_test

import (
	"context"
	"io"
	"io/fs"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestCheckMigrationVersionAcceptsAMigratedDatabase(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	require.NoError(t, client.CheckMigrationVersion(t.Context(), db.ConnStr(), 1))
}

func TestCheckMigrationVersionWaitsForAnUnreachableDatabase(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	// Drop every connection for the first second, then proxy to the real server.
	reachableAt := time.Now().Add(time.Second)
	connStr := proxyDatabase(t, db.ConnStr(), func(conn net.Conn) bool {
		if time.Now().Before(reachableAt) {
			_ = conn.Close()

			return false
		}

		return true
	})

	require.NoError(t, client.CheckMigrationVersion(t.Context(), connStr, 1))
}

func TestCheckMigrationVersionTimesOutAStalledHandshake(t *testing.T) {
	t.Parallel()

	for name, connectTimeout := range map[string]string{"unset": "0", "longer than an attempt": "600"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			connURL, err := url.Parse(db.ConnStr())
			require.NoError(t, err)
			query := connURL.Query()
			query.Set("connect_timeout", connectTimeout)
			connURL.RawQuery = query.Encode()
			// Leave the first connection unanswered, then proxy to the real server.
			stalled := false
			connStr := proxyDatabase(t, connURL.String(), func(conn net.Conn) bool {
				if !stalled {
					stalled = true
					t.Cleanup(func() { _ = conn.Close() })

					return false
				}

				return true
			})

			done := make(chan error, 1)
			go func() { done <- client.CheckMigrationVersion(t.Context(), connStr, 1) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Minute):
				t.Fatal("the check hung on a stalled handshake")
			}
		})
	}
}

func TestCheckMigrationVersionRefusesADatabaseWithoutMigrationsAtOnce(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), "DROP TABLE "+testutils.TrackingTable))

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	err := client.CheckMigrationVersion(ctx, db.ConnStr(), 1)

	require.ErrorContains(t, err, "has the migrator run")
	require.NotErrorIs(t, err, context.DeadlineExceeded, "a missing tracking table is not retried")
}

func TestCheckMigrationVersionRefusesAClientMisconfigurationAtOnce(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	connURL, err := url.Parse(db.ConnStr())
	require.NoError(t, err)
	query := connURL.Query()
	query.Set("sslmode", "verify-full")
	query.Set("sslrootcert", filepath.Join(t.TempDir(), "missing.crt"))
	connURL.RawQuery = query.Encode()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	err = client.CheckMigrationVersion(ctx, connURL.String(), 1)

	require.ErrorIs(t, err, fs.ErrNotExist)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "a client misconfiguration is not retried")
}

// proxyDatabase routes connStr through a local proxy that forwards each
// connection accept approves.
func proxyDatabase(t *testing.T, connStr string, accept func(net.Conn) bool) string {
	t.Helper()

	connURL, err := url.Parse(connStr)
	require.NoError(t, err)
	backendAddr := connURL.Host

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	connURL.Host = listener.Addr().String()

	ctx := t.Context()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if accept(conn) {
				go proxyTo(ctx, conn, backendAddr)
			}
		}
	}()

	return connURL.String()
}

func proxyTo(ctx context.Context, conn net.Conn, addr string) {
	defer conn.Close()

	backend, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return
	}
	defer backend.Close()

	go func() { _, _ = io.Copy(backend, conn) }()
	_, _ = io.Copy(conn, backend)
}
