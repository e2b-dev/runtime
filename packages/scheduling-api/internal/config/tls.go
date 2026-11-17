package config

import (
	"errors"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

func ServerTLS(proc *mtls.Process, settings mtls.ListenerSettings) (mtls.ServerConfig, error) {
	if settings.Mode == "" {
		settings.Mode = "required"
	}
	if settings.Mode != "required" && settings.Mode != "off" {
		return mtls.ServerConfig{}, errors.New("E2B_TLS_GRPC_MODE must be required or off")
	}
	cfg, err := proc.Listener("grpc", "", settings, mtls.WithoutWildcards())
	if err != nil {
		return mtls.ServerConfig{}, err
	}
	if settings.Mode == "required" {
		if cfg.Allow.Len() == 0 {
			return mtls.ServerConfig{}, errors.New("E2B_TLS_GRPC_ALLOW must name permitted API identities")
		}
		if cfg.Files.Bundle() == nil {
			return mtls.ServerConfig{}, errors.New("scheduling TLS certificate files are unavailable")
		}
	}

	return cfg, nil
}
