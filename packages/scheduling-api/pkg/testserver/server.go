package testserver

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Start serves the production handlers on TCP against a separately migrated test database.
func Start(t *testing.T) (scheduling.SchedulingServiceClient, func()) {
	t.Helper()
	pool := dbtest.MigratedPool(t)
	srv, err := server.New(telemetry.NewNoopClient(), store.New(pool, pool), mtls.ServerConfig{Mode: mtls.StaticMode(mtls.ModeOff)})
	require.NoError(t, err)
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return scheduling.NewSchedulingServiceClient(conn), srv.Stop
}
