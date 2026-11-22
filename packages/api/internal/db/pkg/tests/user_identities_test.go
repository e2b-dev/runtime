package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

func TestUserIdentitiesAggregateByUserIDAndRejectDuplicateIssuer(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	userID := uuid.New()

	require.NoError(t, db.AuthDB.UpsertPublicUser(ctx, userID))
	_, err := db.AuthDB.UpsertPublicIdentity(ctx, authqueries.UpsertPublicIdentityParams{
		OidcIss: "https://issuer-a.example.test",
		OidcSub: "subject-a",
		UserID:  userID,
	})
	require.NoError(t, err)
	_, err = db.AuthDB.UpsertPublicIdentity(ctx, authqueries.UpsertPublicIdentityParams{
		OidcIss: "https://issuer-b.example.test",
		OidcSub: "subject-b",
		UserID:  userID,
	})
	require.NoError(t, err)

	rows, err := db.AuthDB.GetUserIdentitiesByUserIDsAndIssuers(ctx, authqueries.GetUserIdentitiesByUserIDsAndIssuersParams{
		OidcIssuers: []string{"https://issuer-a.example.test"},
		UserIds:     []uuid.UUID{userID},
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "https://issuer-a.example.test", rows[0].OidcIss)
	require.Equal(t, "subject-a", rows[0].OidcSub)

	rows, err = db.AuthDB.GetUserIdentitiesByUserIDsAndIssuers(ctx, authqueries.GetUserIdentitiesByUserIDsAndIssuersParams{
		OidcIssuers: []string{"https://issuer-b.example.test"},
		UserIds:     []uuid.UUID{userID},
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "https://issuer-b.example.test", rows[0].OidcIss)
	require.Equal(t, "subject-b", rows[0].OidcSub)

	_, err = db.AuthDB.UpsertPublicIdentity(ctx, authqueries.UpsertPublicIdentityParams{
		OidcIss: "https://issuer-a.example.test",
		OidcSub: "subject-a-2",
		UserID:  userID,
	})
	require.Error(t, err)
}
