package identity

import "context"

// Directory is the subject-keyed admin API of a single identity provider
// (e.g. one Ory project). It never touches the database; issuer routing is the
// Service's concern.
type Directory interface {
	ListIdentities(ctx context.Context, subjects []string) ([]Identity, error)
	SearchByEmail(ctx context.Context, email string) ([]Identity, error)
}
