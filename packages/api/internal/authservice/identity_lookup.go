package authservice

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
)

// identityCacheTTL bounds how long a successful (iss, sub) -> user_id lookup
// is reused without re-querying the database. Negative results (missing rows
// or query errors) are NOT cached so that newly provisioned users can sign in
// immediately and transient db errors don't get pinned.
const identityCacheTTL = 1 * time.Minute

// authIdentityLookup adapts *authqueries.Queries to auth.IdentityLookup so the
// oidc package stays free of any db-layer imports. Missing rows are mapped to
// auth.ErrIdentityNotFound; all other errors are wrapped.
type authIdentityLookup struct {
	queries *authqueries.Queries
}

// newAuthIdentityLookup constructs an auth.IdentityLookup backed by the
// supplied authqueries handle. The returned lookup memoizes successful results
// in-process for identityCacheTTL to avoid a DB round-trip on every JWT
// verification.
func newAuthIdentityLookup(queries *authqueries.Queries) auth.IdentityLookup {
	base := &authIdentityLookup{queries: queries}

	return newCachingIdentityLookup(base)
}

func (l *authIdentityLookup) GetUserIdentity(ctx context.Context, iss, sub string) (uuid.UUID, error) {
	row, err := l.queries.GetUserIdentity(ctx, authqueries.GetUserIdentityParams{
		OidcIss: iss,
		OidcSub: sub,
	})
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			return uuid.Nil, auth.ErrIdentityNotFound
		}

		return uuid.Nil, fmt.Errorf("query user identity: %w", err)
	}

	return row.UserID, nil
}

// cachingIdentityLookup wraps an auth.IdentityLookup with an in-memory cache.
// Only successful lookups are cached; ErrIdentityNotFound and other errors are
// always forwarded to the underlying lookup so newly provisioned users and
// recovery from transient errors aren't blocked by stale negative entries.
type cachingIdentityLookup struct {
	delegate auth.IdentityLookup
	cache    *cache.MemoryCache[uuid.UUID]
}

func newCachingIdentityLookup(delegate auth.IdentityLookup) *cachingIdentityLookup {
	return &cachingIdentityLookup{
		delegate: delegate,
		cache:    cache.NewMemoryCache[uuid.UUID](cache.Config[uuid.UUID]{TTL: identityCacheTTL}),
	}
}

// identityCacheKey joins iss and sub with a NUL byte so the resulting key is
// unambiguous regardless of what characters appear inside either field.
func identityCacheKey(iss, sub string) string {
	return iss + "\x00" + sub
}

func (l *cachingIdentityLookup) GetUserIdentity(ctx context.Context, iss, sub string) (uuid.UUID, error) {
	// GetOrSet only stores successful results and singleflights concurrent
	// misses for the same key, so a burst of requests for the same uncached
	// identity translates to a single underlying DB query.
	return l.cache.GetOrSet(ctx, identityCacheKey(iss, sub), func(ctx context.Context, _ string) (uuid.UUID, error) {
		return l.delegate.GetUserIdentity(ctx, iss, sub)
	})
}
