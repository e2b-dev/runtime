package cfg

import "github.com/e2b-dev/infra/packages/shared/pkg/mtls"

// TLSConfig is the mutual TLS settings the chart renders for the api's
// listeners and client hops, each under its chart key's prefix.
type TLSConfig struct {
	PlacementAllow []string              `env:"E2B_TLS_PLACEMENT_ALLOW"`
	HTTP           mtls.ListenerSettings `envPrefix:"E2B_TLS_HTTP_"`
	InternalGRPC   mtls.ListenerSettings `envPrefix:"E2B_TLS_INTERNAL_GRPC_"`
	PublicGRPC     mtls.ListenerSettings `envPrefix:"E2B_TLS_PUBLIC_GRPC_"`

	SecretsStore     mtls.HopSettings `envPrefix:"E2B_TLS_SECRETS_STORE_"`
	Webhooks         mtls.HopSettings `envPrefix:"E2B_TLS_WEBHOOKS_"`
	Analytics        mtls.HopSettings `envPrefix:"E2B_TLS_ANALYTICS_"`
	Orchestrator     mtls.HopSettings `envPrefix:"E2B_TLS_ORCHESTRATOR_"`
	SandboxMigration mtls.HopSettings `envPrefix:"E2B_TLS_SANDBOX_MIGRATION_"`
}
