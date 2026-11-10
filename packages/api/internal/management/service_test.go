package management

import (
	"context"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func newService(db *testutils.Database) (*Service, *recordingCache) {
	cache := &recordingCache{}

	return NewService(db.AuthDB, db.SqlcClient, cache, noop.NewMeterProvider()), cache
}

func teamMembers(t *testing.T, db *testutils.Database, teamID uuid.UUID) []uuid.UUID {
	t.Helper()

	var members []uuid.UUID
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT user_id FROM public.users_teams WHERE team_id = $1 ORDER BY user_id",
		func(rows pgx.Rows) error {
			for rows.Next() {
				var userID uuid.UUID
				if err := rows.Scan(&userID); err != nil {
					return err
				}
				members = append(members, userID)
			}

			return nil
		}, teamID)
	require.NoError(t, err)

	return members
}

func teamMemberIsDefault(t *testing.T, db *testutils.Database, teamID, userID uuid.UUID) bool {
	t.Helper()

	var isDefault bool
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT is_default FROM public.users_teams WHERE team_id = $1 AND user_id = $2",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&isDefault)
		}, teamID, userID)
	require.NoError(t, err)

	return isDefault
}

func publicUserExists(t *testing.T, db *testutils.Database, userID uuid.UUID) bool {
	t.Helper()

	var exists bool
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT EXISTS (SELECT 1 FROM public.users WHERE id = $1)",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&exists)
		}, userID)
	require.NoError(t, err)

	return exists
}

func identityOwner(t *testing.T, db *testutils.Database, identity ProjectMemberIdentity) uuid.UUID {
	t.Helper()

	var userID uuid.UUID
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT user_id FROM public.user_identities WHERE oidc_iss = $1 AND oidc_sub = $2",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&userID)
		}, identity.Issuer, identity.Subject)
	require.NoError(t, err)

	return userID
}

type memberKey struct {
	userID uuid.UUID
	teamID string
}

// recordingCache captures the evictions, which are the half of these operations
// with no trace in the database.
type recordingCache struct {
	noopAuthService

	members []memberKey
	teams   []uuid.UUID
}

func (c *recordingCache) reset() {
	c.members = nil
	c.teams = nil
}

func (c *recordingCache) InvalidateTeamMemberCache(_ context.Context, userID uuid.UUID, teamID string) {
	c.members = append(c.members, memberKey{userID: userID, teamID: teamID})
}

func (c *recordingCache) InvalidateTeamCache(_ context.Context, teamID uuid.UUID) error {
	c.teams = append(c.teams, teamID)

	return nil
}

// noopAuthService covers what these tests never reach, so recordingCache only
// implements the two methods it asserts on.
type noopAuthService struct{}

var _ sharedauth.Service = noopAuthService{}

func (noopAuthService) ValidateAPIKey(context.Context, *gin.Context, string) (*authtypes.Team, *sharedauth.APIError) {
	return nil, nil
}

func (noopAuthService) ValidateAuthProviderToken(context.Context, *gin.Context, string) (uuid.UUID, *sharedauth.APIError) {
	return uuid.Nil, nil
}

func (noopAuthService) ValidateAuthProviderTeam(context.Context, *gin.Context, string) (*authtypes.Team, *sharedauth.APIError) {
	return nil, nil
}

func (noopAuthService) GetTeamByID(context.Context, uuid.UUID) (*authtypes.Team, error) {
	return nil, nil
}

func (noopAuthService) InvalidateTeamMemberCache(context.Context, uuid.UUID, string) {}

func (noopAuthService) InvalidateAPIKeyCache(context.Context, string) {}

func (noopAuthService) InvalidateTeamCache(context.Context, uuid.UUID) error {
	return nil
}

func (noopAuthService) Close(context.Context) error {
	return nil
}
