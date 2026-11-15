// Package management holds the state changes the control-plane management
// interface applies. They live outside the handlers so they are reachable
// without gin: what these operations get wrong is never the HTTP.
//
// Each one owns the cache evictions its own write invalidates, and reports
// failures as sentinel errors rather than database ones, so the routes above
// map outcomes to status codes without knowing what backs them.
package management

import (
	"go.opentelemetry.io/otel/metric"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

// Auth and project state use separately configured pools; transactions cannot span them.
type Service struct {
	db        *authdb.Client
	projectDB *sqlcdb.Client
	cache     sharedauth.Service
	// clusters is nil without a cluster service.
	clusters clustersgrpc.ClusterServiceClient
	applyLag applyLag
}

func NewService(db *authdb.Client, projectDB *sqlcdb.Client, cache sharedauth.Service, clusters clustersgrpc.ClusterServiceClient, meterProvider metric.MeterProvider) *Service {
	return &Service{db: db, projectDB: projectDB, cache: cache, clusters: clusters, applyLag: newApplyLag(meterProvider)}
}
