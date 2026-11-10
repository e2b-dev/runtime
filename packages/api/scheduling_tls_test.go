package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type schedulingTLSService struct {
	scheduling.UnimplementedSchedulingServiceServer
}

func (schedulingTLSService) Get(context.Context, *scheduling.GetRequest) (*scheduling.GetResponse, error) {
	return scheduling.GetResponse_builder{}.Build(), nil
}

func TestSchedulingClientUsesDedicatedTLSHop(t *testing.T) {
	t.Parallel()
	const apiID = "spiffe://test.example/api"
	const schedulingID = "spiffe://test.example/scheduling-api"
	ca := mtlstest.NewRootCA(t, "trusted")
	procFor := func(t *testing.T, identity string, trusted bool) *mtls.Process {
		t.Helper()
		issuer := ca
		if !trusted {
			issuer = mtlstest.NewRootCA(t, "untrusted")
		}
		dir := mtlstest.WriteFiles(t, issuer.Leaf(t, mtlstest.LeafSpec{SPIFFEID: identity}), ca.BundlePEM())
		proc, err := mtls.NewProcess(t.Context(), telemetry.NewNoopClient().MeterProvider, nil, nil,
			mtls.WithFileConfig(mtls.FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile}))
		require.NoError(t, err)

		return proc
	}
	serverConfig, err := procFor(t, schedulingID, true).Listener("grpc", "", mtls.ListenerSettings{Mode: "required", Allow: []string{apiID}}, mtls.WithoutWildcards())
	require.NoError(t, err)
	srv := e2bgrpc.NewGRPCServer(telemetry.NewNoopClient(), grpcTLSOptions(serverConfig)...)
	scheduling.RegisterSchedulingServiceServer(srv, schedulingTLSService{})
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	for _, tc := range []struct {
		name, identity, expected string
		allowed                  bool
		trusted                  bool
	}{
		{"API to scheduling", apiID, schedulingID, true, true},
		{"wrong server identity", apiID, "spiffe://test.example/orchestrator", false, true},
		{"wrong caller identity", "spiffe://test.example/other", schedulingID, false, true},
		{"untrusted certificate", apiID, schedulingID, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tlsConfig, err := newServiceTLS(procFor(t, tc.identity, tc.trusted), cfg.TLSConfig{Scheduling: mtls.HopSettings{Expect: []string{tc.expected}}}, false, true)
			require.NoError(t, err)
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(tlsConfig.scheduling))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			callCtx, stop := context.WithTimeout(t.Context(), 3*time.Second)
			defer stop()
			_, err = scheduling.NewSchedulingServiceClient(conn).Get(callCtx, scheduling.GetRequest_builder{}.Build())
			if tc.allowed {
				require.NoError(t, err, "authenticated RPC reached the service")
			} else {
				require.Error(t, err)
			}
		})
	}
	t.Run("plaintext rejected", func(t *testing.T) {
		t.Parallel()
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		_, err = scheduling.NewSchedulingServiceClient(conn).Get(ctx, scheduling.GetRequest_builder{}.Build())
		require.Error(t, err)
	})
}

func TestSchedulingTLSConfiguration(t *testing.T) {
	t.Parallel()
	proc, _ := servicetest.NewProcess(t, nil)
	for _, settings := range []mtls.HopSettings{
		{},
		{Mode: "invalid"},
		{Expect: []string{"*"}},
		{Expect: []string{"spiffe://test.example/scheduling-api"}},
	} {
		_, err := newServiceTLS(proc, cfg.TLSConfig{Scheduling: settings}, false, true)
		require.Error(t, err)
	}
	tlsConfig, err := newServiceTLS(proc, cfg.TLSConfig{Scheduling: mtls.HopSettings{Mode: "off"}}, false, true)
	require.NoError(t, err)
	conn, err := grpc.NewClient(servicetest.GRPCServer(t), grpc.WithTransportCredentials(tlsConfig.scheduling))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	healthCheck(t, conn)
	tlsConfig, err = newServiceTLS(proc, cfg.TLSConfig{}, false, false)
	require.NoError(t, err)
	require.Nil(t, tlsConfig.scheduling)
}

func TestSchedulingTLSModeWithCertificates(t *testing.T) {
	t.Parallel()
	ca := mtlstest.NewRootCA(t, "trusted")
	dir := mtlstest.WriteFiles(t, ca.Leaf(t, mtlstest.LeafSpec{SPIFFEID: "spiffe://test.example/api"}), ca.BundlePEM())
	proc, err := mtls.NewProcess(t.Context(), telemetry.NewNoopClient().MeterProvider, nil, nil,
		mtls.WithFileConfig(mtls.FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile}))
	require.NoError(t, err)
	for _, tc := range []struct {
		name, mode string
		valid      bool
	}{
		{"unset", "", true},
		{"on", "on", true},
		{"off", "off", true},
		{"whitespace", " ", false},
		{"invalid", "invalid", false},
		{"uppercase", "ON", false},
		{"padded off", " off ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tlsConfig, err := newServiceTLS(proc, cfg.TLSConfig{Scheduling: mtls.HopSettings{
				Mode: tc.mode, Expect: []string{"spiffe://test.example/scheduling-api"},
			}}, false, true)
			if !tc.valid {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, tlsConfig.scheduling)
		})
	}
}
