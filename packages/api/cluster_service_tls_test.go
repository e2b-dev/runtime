package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

// The cluster service hands out every cluster's edge token, so a plaintext
// hop is refused wherever the token would cross the network.
func TestClusterServiceHopRefusesAnUnverifiedServer(t *testing.T) {
	t.Parallel()

	proc, _ := servicetest.NewProcess(t, nil)
	const (
		server = "spiffe://test.example/cluster-service"
		remote = "cluster-service.example.test:5010"
	)
	off := mtls.HopSettings{Mode: "off"}
	for name, tc := range map[string]struct {
		settings mtls.HopSettings
		address  string
		local    bool
		valid    bool
	}{
		// A blank mode is on, which needs the server's name.
		"blank mode":                      {mtls.HopSettings{}, remote, false, false},
		"blank mode in local development": {mtls.HopSettings{}, remote, true, false},
		"blank mode with a server":        {mtls.HopSettings{Expect: []string{server}}, remote, false, true},
		"on with a server":                {mtls.HopSettings{Mode: "on", Expect: []string{server}}, remote, false, true},
		"an unknown mode":                 {mtls.HopSettings{Mode: "auto", Expect: []string{server}}, remote, false, false},
		"a wildcard server":               {mtls.HopSettings{Expect: []string{"*"}}, remote, false, false},
		"off to a remote address":         {off, remote, false, false},
		"off to a schemed target":         {off, "dns:///localhost:5010", false, false},
		"off in local development":        {off, remote, true, true},
		"off to localhost":                {off, "localhost:5010", false, true},
		"off to an IPv4 loopback address": {off, "127.0.0.1:5010", false, true},
		"off to an IPv6 loopback address": {off, "[::1]:5010", false, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			creds, err := newClusterServiceHop(proc, tc.settings, tc.address, tc.local)
			if !tc.valid {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, creds)
		})
	}
}

func TestClusterServiceHopOffDialsPlaintext(t *testing.T) {
	t.Parallel()

	proc, _ := servicetest.NewProcess(t, nil)
	address := servicetest.GRPCServer(t)
	creds, err := newClusterServiceHop(proc, mtls.HopSettings{Mode: "off"}, address, false)
	require.NoError(t, err)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	healthCheck(t, conn)
}
