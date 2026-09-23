// Package management holds the state changes the control-plane management
// interface applies. They live outside the handlers so they are reachable
// without gin: what these operations get wrong is never the HTTP.
//
// Each one owns the cache evictions its own write invalidates, and reports
// failures as sentinel errors rather than database ones, so the routes above
// map outcomes to status codes without knowing what backs them.
package management

import (
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
)

// Auth and project state use separately configured pools; transactions cannot span them.
type Service struct {
	db        *authdb.Client
	projectDB *sqlcdb.Client
	cache     sharedauth.Service
}

func NewService(db *authdb.Client, projectDB *sqlcdb.Client, cache sharedauth.Service) *Service {
	return &Service{db: db, projectDB: projectDB, cache: cache}
}
