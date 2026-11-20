package main

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc/credentials"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/handlers"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

// The mode flags of the api's listeners and client hops; the env value
// under the same chart key is each one's fallback.
const (
	httpModeFlag         = "mtls-api-http-mode"
	internalGRPCModeFlag = "mtls-api-internal-grpc-mode"
	publicGRPCModeFlag   = "mtls-api-public-grpc-mode"

	secretsStoreHopFlag = "mtls-api-secrets-store-client-mode"
	webhooksHopFlag     = "mtls-api-webhooks-client-mode"
	analyticsHopFlag    = "mtls-api-analytics-client-mode"
	orchestratorHopFlag = "mtls-api-orchestrator-client-mode"
)

// serviceTLS is the api's mutual TLS configuration: its three listeners,
// the HTTP port's health paths, and the credentials of its client hops.
type serviceTLS struct {
	http, internalGRPC, publicGRPC mtls.ServerConfig
	httpHealthPaths                []string
	hops                           handlers.Hops
}

// newServiceTLS builds the api's listeners and hops from the chart's
// settings; no wildcard is admitted on its ports.
func newServiceTLS(proc *mtls.Process, c cfg.TLSConfig, analyticsHop bool) (serviceTLS, error) {
	var s serviceTLS
	var err error
	if s.http, err = proc.Listener("http", httpModeFlag, c.HTTP, mtls.WithoutWildcards()); err != nil {
		return serviceTLS{}, err
	}
	// The env list can only add to the probe route the api mounts.
	s.httpHealthPaths = append(c.HTTP.HealthPaths, "/health")
	if s.internalGRPC, err = proc.Listener("internal_grpc", internalGRPCModeFlag, c.InternalGRPC, mtls.WithoutWildcards()); err != nil {
		return serviceTLS{}, err
	}
	if s.publicGRPC, err = proc.Listener("public_grpc", publicGRPCModeFlag, c.PublicGRPC, mtls.WithoutWildcards()); err != nil {
		return serviceTLS{}, err
	}

	hop := func(key, flag string, settings mtls.HopSettings) (credentials.TransportCredentials, error) {
		hopCfg, err := proc.Hop(key, flag, settings)
		if err != nil {
			return nil, err
		}

		return mtls.NewClientCredentials(hopCfg), nil
	}
	if s.hops.SecretsStore, err = hop("secrets_store", secretsStoreHopFlag, c.SecretsStore); err != nil {
		return serviceTLS{}, err
	}
	if s.hops.Webhooks, err = hop("webhooks", webhooksHopFlag, c.Webhooks); err != nil {
		return serviceTLS{}, err
	}
	// Built only when the collector is dialled through the hop.
	if analyticsHop {
		if s.hops.Analytics, err = hop("analytics", analyticsHopFlag, c.Analytics); err != nil {
			return serviceTLS{}, err
		}
	}
	if s.hops.Orchestrator, err = hop("orchestrator", orchestratorHopFlag, c.Orchestrator); err != nil {
		return serviceTLS{}, err
	}

	return s, nil
}

// newSchedulingHop builds the hop to the scheduling API. A blank mode is on,
// and on needs the certificate files.
func newSchedulingHop(proc *mtls.Process, settings mtls.HopSettings) (credentials.TransportCredentials, error) {
	if settings.Mode == "" {
		settings.Mode = "on"
	}
	if settings.Mode != "on" && settings.Mode != "off" {
		return nil, errors.New("E2B_TLS_SCHEDULING_CLIENT_MODE must be on or off")
	}
	hopConfig, err := proc.Hop("scheduling", "", settings)
	if err != nil {
		return nil, err
	}
	if settings.Mode != "off" && hopConfig.Files.Bundle() == nil {
		return nil, errors.New("scheduling TLS certificate files are unavailable")
	}

	return mtls.NewClientCredentials(hopConfig), nil
}

const clusterServiceHop = "cluster_service"

// newClusterServiceHop builds the hop to the cluster service, which hands its
// callers each cluster's edge token. A blank mode is on. Off dials plaintext,
// so it is refused outside local development unless the address is loopback,
// where the token never leaves the machine. The hop itself refuses on without
// certificates or an expected server name.
func newClusterServiceHop(proc *mtls.Process, settings mtls.HopSettings, address string, local bool) (credentials.TransportCredentials, error) {
	prefix := mtls.EnvPrefix(clusterServiceHop)
	if strings.TrimSpace(settings.Mode) == "" {
		settings.Mode = "on"
	}
	mode, err := mtls.ParseClientMode(settings.Mode)
	if err != nil {
		return nil, fmt.Errorf("%sCLIENT_MODE: %w", prefix, err)
	}
	if mode == mtls.ClientOff && !local && !isLoopbackAddress(address) {
		return nil, fmt.Errorf("%sCLIENT_MODE may be off only in local development or for a loopback address", prefix)
	}
	hop, err := proc.Hop(clusterServiceHop, "", settings)
	if err != nil {
		return nil, err
	}

	return mtls.NewClientCredentials(hop), nil
}

// isLoopbackAddress reports whether a host:port target names this machine. A
// schemed target such as dns:///name:port is not.
func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

// grpcTLSOptions puts a gRPC port behind its listener's credentials and
// per-call checks, which run after the constructor's recovery, logging
// and deadline.
func grpcTLSOptions(c mtls.ServerConfig) []e2bgrpc.ServerOption {
	return []e2bgrpc.ServerOption{
		e2bgrpc.WithTransportCredentials(mtls.NewServerCredentials(c)),
		e2bgrpc.WithUnaryInterceptors(mtls.UnaryServerInterceptor(c)),
		e2bgrpc.WithStreamInterceptors(mtls.StreamServerInterceptor(c)),
	}
}
