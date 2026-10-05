package cfg

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/golang-jwt/jwt/v5"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

const (
	// ServiceDiscoveryProviderNomad queries Nomad's HTTP API (the original / Nomad-based deploy).
	ServiceDiscoveryProviderNomad = "nomad"
	// ServiceDiscoveryProviderKubernetes lists pods via the K8s API (the K8s deploy).
	ServiceDiscoveryProviderKubernetes = "kubernetes"
	// ServiceDiscoveryProviderNomadKubernetes queries both and returns the
	// deduplicated union. Used while orchestrators and template managers run on
	// both platforms at once.
	ServiceDiscoveryProviderNomadKubernetes = "nomad+kubernetes"
	// ServiceDiscoveryProviderLocal returns a single statically configured
	// orchestrator address. Used to develop the API against the darwin dummy
	// orchestrator on macOS, where neither Nomad nor Kubernetes is available.
	ServiceDiscoveryProviderLocal = "local"
)

type Config struct {
	featureflags.Config

	AdminToken string `env:"ADMIN_TOKEN"`

	AnalyticsCollectorAPIToken string `env:"ANALYTICS_COLLECTOR_API_TOKEN"`
	// AnalyticsCollectorHost is the hostname, or "host:port" address, of the
	// analytics collector. Without a port, 443 is used. Empty disables
	// analytics reporting entirely.
	AnalyticsCollectorHost string `env:"ANALYTICS_COLLECTOR_HOST"`
	// AnalyticsCollectorTLS controls whether the collector connection uses
	// TLS. Set to false only for a plaintext collector on loopback, as the
	// local development one is; the API token is sent unencrypted then.
	AnalyticsCollectorTLS bool `env:"ANALYTICS_COLLECTOR_TLS" envDefault:"true"`

	ClickhouseConnectionString  string   `env:"CLICKHOUSE_CONNECTION_STRING"`
	ClickhouseConnectionStrings []string `env:"CLICKHOUSE_CONNECTION_STRINGS" envSeparator:";"`

	LokiPassword string `env:"LOKI_PASSWORD"`
	// LokiURL is optional: without it the api has no Loki client, and sandbox
	// and build log reads must be routed to ClickHouse by logs-read-config.
	LokiURL  string `env:"LOKI_URL"`
	LokiUser string `env:"LOKI_USER"`

	// ServiceDiscoveryProvider selects how the API discovers orchestrator and
	// template-manager instances. Left unset it is nomad, except in a local
	// environment, which has no Nomad agent — see serviceDiscoveryProvider. It
	// carries no envDefault precisely so that "unset" stays distinguishable
	// from "set to nomad", which an operator running a local Nomad agent needs.
	// Allowed values:
	//   "nomad"            (default) - query the local Nomad agent's HTTP API.
	//   "kubernetes"                 - list pods via the K8s API.
	//   "nomad+kubernetes"           - the deduplicated union of both.
	ServiceDiscoveryProvider string `env:"SERVICE_DISCOVERY_PROVIDER"`

	NomadAddress string `env:"NOMAD_ADDRESS" envDefault:"http://localhost:4646"`
	NomadToken   string `env:"NOMAD_TOKEN"`

	// NomadOrchestratorServiceNames is the comma-separated list of
	// Nomad-native service names whose registrations enumerate orchestrator
	// instances (GET /v1/service/<name> per name, results unioned). Every
	// orchestrator jobspec registers one of these services, regardless of
	// job type or node pool. Used when ServiceDiscoveryProvider is nomad or
	// nomad+kubernetes.
	NomadOrchestratorServiceNames []string `env:"NOMAD_ORCHESTRATOR_SERVICE_NAMES" envDefault:"orchestrator" envSeparator:","`

	// NomadOrchestratorLegacyDiscoveryEnabled enables a node-pool-based
	// discovery FALLBACK unioned with the service-based discovery above: all
	// ready Nomad nodes in the "default" pool are assumed to run an
	// orchestrator on the well-known port. It covers orchestrator jobs
	// deployed from jobspecs that register their service with an empty
	// Address (pre-port-label-fix), which service discovery skips as
	// unroutable, and removes any rollout ordering constraint between the
	// API and orchestrator releases. Set to false once no legacy orchestrator
	// jobs remain. Used when ServiceDiscoveryProvider is nomad or
	// nomad+kubernetes.
	NomadOrchestratorLegacyDiscoveryEnabled bool `env:"NOMAD_ORCHESTRATOR_LEGACY_DISCOVERY_ENABLED" envDefault:"true"`

	// LocalOrchestratorAddress is the "host:port" address of a statically
	// configured orchestrator instance. Required when
	// ServiceDiscoveryProvider=local. Used for local dev against the darwin
	// dummy orchestrator.
	LocalOrchestratorAddress string `env:"LOCAL_ORCHESTRATOR_ADDRESS" envDefault:"127.0.0.1:5008"`

	// Used when ServiceDiscoveryProvider is kubernetes or nomad+kubernetes.
	// Empty K8sAPIEndpoint uses the pod's own ServiceAccount and only works
	// inside the cluster; set it to reach a cluster from outside, on Google ADC.
	K8sAPIEndpoint                     string `env:"K8S_API_ENDPOINT"`
	K8sNamespace                       string `env:"K8S_NAMESPACE"                           envDefault:"default"`
	K8sOrchestratorPodLabelSelector    string `env:"K8S_ORCHESTRATOR_POD_LABEL_SELECTOR"     envDefault:"app.kubernetes.io/name=orchestrator"`
	K8sTemplateManagerPodLabelSelector string `env:"K8S_TEMPLATE_MANAGER_POD_LABEL_SELECTOR" envDefault:"app.kubernetes.io/name=template-manager"`

	PostgresConnectionString string `env:"POSTGRES_CONNECTION_STRING,required,notEmpty"`
	DBMaxOpenConnections     int32  `env:"DB_MAX_OPEN_CONNECTIONS"                      envDefault:"40"`
	DBMinIdleConnections     int32  `env:"DB_MIN_IDLE_CONNECTIONS"                      envDefault:"5"`

	AuthDBConnectionString   string `env:"AUTH_DB_CONNECTION_STRING"`
	AuthDBMinIdleConnections int32  `env:"AUTH_DB_MIN_IDLE_CONNECTIONS" envDefault:"5"`
	AuthDBMaxOpenConnections int32  `env:"AUTH_DB_MAX_OPEN_CONNECTIONS" envDefault:"20"`

	PosthogAPIKey string `env:"POSTHOG_API_KEY"`

	RedisURL         string `env:"REDIS_URL"`
	RedisClusterURL  string `env:"REDIS_CLUSTER_URL"`
	RedisTLSCABase64 string `env:"REDIS_TLS_CA_BASE64"`
	RedisTLSEnabled  bool   `env:"REDIS_TLS_ENABLED"`
	RedisPassword    string `env:"REDIS_PASSWORD"`
	RedisPoolSize    int    `env:"REDIS_POOL_SIZE"     envDefault:"160"`

	APIInternalGrpcPort uint16 `env:"API_INTERNAL_GRPC_PORT" envDefault:"5009"`
	APIEdgeGrpcPort     uint16 `env:"API_EDGE_GRPC_PORT"     envDefault:"5109"`

	ClientProxyOIDCIssuerURL string `env:"CLIENT_PROXY_OIDC_ISSUER_URL"`

	SandboxAccessTokenHashSeed string `env:"SANDBOX_ACCESS_TOKEN_HASH_SEED"`

	// SecretsStoreBackendGrpcAddress is the gRPC target of the secrets store
	// management backend, either "host:port" or a schemed target such as
	// "dns:///name:port" for a headless Service. Optional: when empty the API
	// keeps serving every other route and answers the secret management routes
	// with the same forbidden response the feature gate produces.
	SecretsStoreBackendGrpcAddress string `env:"SECRETS_STORE_BACKEND_GRPC_ADDRESS"`

	// WebhooksBackendGrpcAddress is the sandbox events and webhook management
	// backend. Unset leaves those routes registered and answering as they do
	// when the backend is absent, so the surface does not depend on the wiring.
	WebhooksBackendGrpcAddress string `env:"WEBHOOKS_BACKEND_GRPC_ADDRESS"`

	VolumesToken VolumesTokenConfig

	AuthProvider      sharedauth.ProviderConfig `env:"AUTH_PROVIDER_CONFIG"`
	AdminAuthProvider sharedauth.ProviderConfig `env:"ADMIN_AUTH_PROVIDER_CONFIG"`

	DefaultPersistentVolumeType     string `env:"DEFAULT_PERSISTENT_VOLUME_TYPE"`
	PlaceholderPersistentVolumeType string `env:"PLACEHOLDER_PERSISTENT_VOLUME_TYPE" envDefault:"__DEFAULT_VOLUME_TYPE__"`

	// DefaultPersistentVolumeTypeByRegion is the per-region default volume type,
	// e.g. "us-west3:zonalfilestore-us-west3". A team's region is resolved from
	// the region= node labels of the nodes its scheduling labels select; Terraform
	// derives this map from the volume types themselves and fails the plan when a
	// region with several types lacks an explicit default.
	DefaultPersistentVolumeTypeByRegion map[string]string `env:"DEFAULT_PERSISTENT_VOLUME_TYPE_BY_REGION"`

	// DomainName is the public domain of this deployment. It builds API-facing
	// URLs, such as the volume token audience. It is not a LaunchDarkly key.
	DomainName string `env:"DOMAIN_NAME" envDefault:""`

	// BestOfKHugepageMemory includes hugepage-pool load in best-of-K
	// placement (max of CPU and pool). A node that reports no pool scores
	// 0.5, so clusters without a pool should set this false and rank on CPU.
	BestOfKHugepageMemory bool `env:"BEST_OF_K_HUGEPAGE_MEMORY" envDefault:"true"`
}

type FailureCondition string

const (
	FailureConditionInvalidServiceDiscoveryProvider FailureCondition = "invalid_service_discovery_provider"
)

type FailureError struct {
	Condition FailureCondition
	err       error
}

func (e *FailureError) Error() string {
	return e.err.Error()
}

func (e *FailureError) Unwrap() error {
	return e.err
}

func ParseFailureCondition(err error) (FailureCondition, bool) {
	var failureErr *FailureError
	if !errors.As(err, &failureErr) {
		return "", false
	}

	return failureErr.Condition, true
}

func newFailureError(condition FailureCondition, message string) error {
	return &FailureError{
		Condition: condition,
		err:       errors.New(message),
	}
}

type JWTSigningKey any

type VolumesTokenConfig struct {
	// Enabled explicitly toggles volume content token signing. When true (the
	// default), all signing env vars are required. Set to false when volumes are
	// disabled so the signing env vars may be omitted entirely.
	Enabled bool `env:"VOLUME_TOKEN_ENABLED" envDefault:"true"`

	Issuer         string            `env:"VOLUME_TOKEN_ISSUER"`
	SigningMethod  jwt.SigningMethod `env:"VOLUME_TOKEN_SIGNING_METHOD"`
	SigningKey     JWTSigningKey     `env:"VOLUME_TOKEN_SIGNING_KEY"`
	SigningKeyName string            `env:"VOLUME_TOKEN_SIGNING_KEY_NAME"`
	Duration       time.Duration     `env:"VOLUME_TOKEN_DURATION"         envDefault:"1h"`
}

// IsConfigured reports whether volume content token signing is enabled.
func (c VolumesTokenConfig) IsConfigured() bool {
	return c.Enabled
}

// validate ensures that when signing is enabled all required signing env vars
// are present. A partial config is a deployment mistake and should fail fast at
// startup.
func (c VolumesTokenConfig) validate() error {
	if !c.Enabled {
		return nil
	}

	var missing []string
	if c.Issuer == "" {
		missing = append(missing, "VOLUME_TOKEN_ISSUER")
	}
	if c.SigningMethod == nil {
		missing = append(missing, "VOLUME_TOKEN_SIGNING_METHOD")
	}
	if c.SigningKey == nil {
		missing = append(missing, "VOLUME_TOKEN_SIGNING_KEY")
	}
	if c.SigningKeyName == "" {
		missing = append(missing, "VOLUME_TOKEN_SIGNING_KEY_NAME")
	}

	if len(missing) > 0 {
		return fmt.Errorf("VOLUME_TOKEN_ENABLED is set but the following are missing: %s", strings.Join(missing, ", "))
	}

	return nil
}

var (
	ErrInvalidJWTSigningKey = errors.New("JWT signing key must be in the format '$TYPE:base64($VALUE)'")
	ErrUnknownKeyType       = errors.New("unknown JWT signing key type")

	parserFuncs = map[reflect.Type]env.ParserFunc{
		reflect.TypeFor[sharedauth.ProviderConfig](): func(v string) (any, error) {
			return sharedauth.ParseProviderConfig(v)
		},
		reflect.TypeFor[JWTSigningKey](): func(v string) (any, error) {
			keyPieces := strings.SplitN(v, ":", 2)
			if len(keyPieces) != 2 {
				return nil, ErrInvalidJWTSigningKey
			}

			keyType := keyPieces[0]
			keyValue, err := base64.StdEncoding.DecodeString(keyPieces[1])
			if err != nil {
				return nil, errors.New("JWT signing key must be base64 encoded")
			}

			switch strings.ToUpper(keyType) {
			case "ECDSA":
				return jwt.ParseECPrivateKeyFromPEM(keyValue)
			case "RSA":
				return jwt.ParseRSAPrivateKeyFromPEM(keyValue)
			case "HMAC":
				return keyValue, nil
			case "ED25519":
				return jwt.ParseEdPrivateKeyFromPEM(keyValue)
			default:
				return nil, fmt.Errorf("%s: %w", keyType, ErrUnknownKeyType)
			}
		},
		reflect.TypeFor[jwt.SigningMethod](): func(v string) (any, error) {
			method := jwt.GetSigningMethod(v)
			if method == nil {
				return nil, fmt.Errorf("unknown signing method: %s", v)
			}

			return method, nil
		},
	}
)

func Parse() (Config, error) {
	config, err := env.ParseAsWithOptions[Config](env.Options{
		FuncMap: parserFuncs,
	})
	if err != nil {
		return Config{}, err
	}

	if config.AuthDBConnectionString == "" {
		config.AuthDBConnectionString = config.PostgresConnectionString
	}

	if !slices.Contains([]string{"", ServiceDiscoveryProviderNomad, ServiceDiscoveryProviderKubernetes, ServiceDiscoveryProviderNomadKubernetes, ServiceDiscoveryProviderLocal}, config.ServiceDiscoveryProvider) {
		return config, newFailureError(
			FailureConditionInvalidServiceDiscoveryProvider,
			fmt.Sprintf("invalid service discovery provider: %s", config.ServiceDiscoveryProvider),
		)
	}

	if err := config.VolumesToken.validate(); err != nil {
		return config, err
	}

	return config, nil
}
