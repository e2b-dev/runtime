package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/cfg"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/identity"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// emptyLinkage answers "this user has no linked identity", which is all the
// configured branch needs: it never reaches the identity provider.
type emptyLinkage struct{}

func (emptyLinkage) IdentitiesForUsers(_ context.Context, _ []string, _ []uuid.UUID) ([]identity.LinkedIdentity, error) {
	return nil, nil
}

func (emptyLinkage) UsersForSubjects(_ context.Context, _ string, _ []string) ([]identity.LinkedIdentity, error) {
	return nil, nil
}

func TestNewIdentityServiceWithoutOryIsUnavailable(t *testing.T) {
	t.Parallel()

	service, err := newIdentityService(t.Context(), logger.NewNopLogger(), cfg.Config{}, http.DefaultClient, emptyLinkage{})
	require.NoError(t, err)

	_, err = service.ProfilesByUserID(t.Context(), []uuid.UUID{uuid.New()})
	require.ErrorIs(t, err, identity.ErrNoIdentityProvider)
}

func TestNewIdentityServiceWithOryIsBacked(t *testing.T) {
	t.Parallel()

	config := cfg.Config{
		OrySDKURL:          "https://tenant.projects.oryapis.com",
		OryProjectAPIToken: "pat",
	}

	service, err := newIdentityService(t.Context(), logger.NewNopLogger(), config, http.DefaultClient, emptyLinkage{})
	require.NoError(t, err)

	profiles, err := service.ProfilesByUserID(t.Context(), []uuid.UUID{uuid.New()})
	require.NoError(t, err)
	require.Empty(t, profiles)
}
