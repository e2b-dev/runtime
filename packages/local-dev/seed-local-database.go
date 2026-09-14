package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

const (
	defaultOidcIssuer  = "http://localhost:4444/"
	defaultOidcSubject = "local-dev-user"

	// SEED_TEAM_API_KEY=random asks for a freshly generated team API key.
	randomTeamAPIKey = "random"
	// The shortest key accepted from SEED_TEAM_API_KEY or its file, in bytes
	// after the prefix; the fixed development key is exactly this long.
	minTeamAPIKeyBytes = 16
	teamAPIKeyFileMode = 0o600
	// seedTeamAPIKeyName marks the rows this program manages, so a rotation
	// revokes earlier seed keys and never a key someone created elsewhere.
	seedTeamAPIKeyName = "local dev seed token"
)

var (
	teamID         = uuid.MustParse("0b8a3ded-4489-4722-afd1-1d82e64ec2d5")
	teamTokenValue = "53ae1fed82754c17ad8077fbc8bcdd90"
	defaultUserID  = uuid.MustParse("fb69f46f-eb51-4a87-a14e-306f7a3fd89c")
)

func main() {
	ctx := context.Background()

	if err := run(ctx); err != nil {
		panic(err)
	}
}

func run(ctx context.Context) error {
	userID := defaultUserID
	if raw := strings.TrimSpace(os.Getenv("SEED_USER_ID")); raw != "" {
		var err error
		userID, err = uuid.Parse(raw)
		if err != nil || userID == uuid.Nil {
			return errors.New("SEED_USER_ID must be a nonzero UUID")
		}
	}

	connectionString := os.Getenv("POSTGRES_CONNECTION_STRING")

	if connectionString == "" {
		connectionString = "postgresql://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
	}

	oidcIssuer := os.Getenv("OIDC_ISSUER")
	if oidcIssuer == "" {
		oidcIssuer = defaultOidcIssuer
	}

	oidcSubject := os.Getenv("OIDC_SUBJECT")
	if oidcSubject == "" {
		oidcSubject = defaultOidcSubject
	}

	authDb, err := authdb.NewClient(ctx, connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer authDb.Close()

	// create user
	if err := upsertUser(ctx, authDb, userID); err != nil {
		return fmt.Errorf("failed to upsert user: %w", err)
	}

	// create team
	teamID, err := upsertTeam(ctx, authDb)
	if err != nil {
		return fmt.Errorf("failed to upsert team: %w", err)
	}

	if err = ensureUserIsOnTeam(ctx, authDb, userID, teamID); err != nil {
		return fmt.Errorf("failed to ensure user is on team: %w", err)
	}

	if err = upsertUserIdentity(ctx, authDb, userID, oidcIssuer, oidcSubject); err != nil {
		return fmt.Errorf("failed to upsert user identity: %w", err)
	}

	// create team token
	teamAPIKeyFile := os.Getenv("SEED_TEAM_API_KEY_FILE")
	teamAPIKey, err := resolveTeamAPIKey(os.Getenv("SEED_TEAM_API_KEY"), teamAPIKeyFile, generateTeamAPIKey)
	if err != nil {
		return err
	}

	if err = rejectForeignTeamAPIKey(ctx, authDb, teamID, teamAPIKey); err != nil {
		return err
	}

	if err = upsertTeamAPIKey(ctx, authDb, userID, teamID, keys.ApiKeyPrefix, teamAPIKey); err != nil {
		return fmt.Errorf("failed to upsert token: %w", err)
	}

	if teamAPIKey != keys.ApiKeyPrefix+teamTokenValue {
		if err = revokeOtherSeedKeys(ctx, authDb, teamID, teamAPIKey); err != nil {
			return fmt.Errorf("failed to revoke the earlier seed keys: %w", err)
		}
	}

	if teamAPIKeyFile != "" {
		fmt.Printf("team api key %s written to %s\n", keys.MaskToken(keys.ApiKeyPrefix, teamAPIKey), teamAPIKeyFile)
	}

	// create local cluster
	// if err = upsertLocalCluster(ctx, db); err != nil {
	//	return fmt.Errorf("failed to upsert local cluster: %w", err)
	// }

	return nil
}

// resolveTeamAPIKey picks the team API key the seed inserts.
//
// value is SEED_TEAM_API_KEY: unset or empty keeps the fixed development key,
// "random" generates one, anything else is the key itself (a value that is only
// whitespace is a malformed key, not the default). file is SEED_TEAM_API_KEY_FILE:
// when set, the resolved key is written there, mode 0600, for other processes
// to read, and in random mode a key already in the file is reused, so a stack
// keeps its key across re-runs of the seed; an empty file counts as no key yet,
// which is what an interrupted first run leaves behind. Random mode needs the
// file: a generated key that nobody records is lost, and the seed does not
// print secrets.
func resolveTeamAPIKey(value, file string, generate func() (string, error)) (string, error) {
	var key string

	trimmed := strings.TrimSpace(value)

	switch {
	case value == "":
		key = keys.ApiKeyPrefix + teamTokenValue
	case trimmed == randomTeamAPIKey:
		if file == "" {
			return "", errors.New("SEED_TEAM_API_KEY=random needs SEED_TEAM_API_KEY_FILE, the file the generated key is kept in")
		}

		stored, err := os.ReadFile(file)
		switch {
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			return "", fmt.Errorf("failed to read SEED_TEAM_API_KEY_FILE %s: %w", file, err)
		case err == nil && strings.TrimSpace(string(stored)) != "":
			// Reused, and rewritten below so the file ends up 0600 and normalised
			// whatever created it.
			key, err = parseTeamAPIKey(string(stored))
			if err != nil {
				return "", fmt.Errorf("SEED_TEAM_API_KEY_FILE %s: %w", file, err)
			}
		default:
			key, err = generate()
			if err != nil {
				return "", fmt.Errorf("failed to generate a team api key: %w", err)
			}
		}
	default:
		var err error

		key, err = parseTeamAPIKey(trimmed)
		if err != nil {
			return "", fmt.Errorf("SEED_TEAM_API_KEY: %w", err)
		}
	}

	if file != "" {
		if err := writeTeamAPIKeyFile(file, key); err != nil {
			return "", fmt.Errorf("failed to write SEED_TEAM_API_KEY_FILE %s: %w", file, err)
		}
	}

	return key, nil
}

// writeTeamAPIKeyFile truncates or creates file, forces its mode to 0600 and
// only then writes the key, so a pre-existing world-readable file never holds
// it (os.WriteFile applies the mode only when it creates the file).
func writeTeamAPIKeyFile(file, key string) error {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, teamAPIKeyFileMode)
	if err != nil {
		return err
	}

	if err := f.Chmod(teamAPIKeyFileMode); err != nil {
		_ = f.Close()

		return err
	}

	if _, err := f.WriteString(key + "\n"); err != nil {
		_ = f.Close()

		return err
	}

	return f.Close()
}

// parseTeamAPIKey accepts the form the SDK sends: the e2b_ prefix followed by
// the hex of at least minTeamAPIKeyBytes bytes.
func parseTeamAPIKey(raw string) (string, error) {
	key := strings.TrimSpace(raw)
	if !strings.HasPrefix(key, keys.ApiKeyPrefix) {
		return "", fmt.Errorf("a team api key starts with %q", keys.ApiKeyPrefix)
	}

	value, err := hex.DecodeString(strings.TrimPrefix(key, keys.ApiKeyPrefix))
	if err != nil {
		return "", errors.New("a team api key is hex after the prefix")
	}

	if len(value) < minTeamAPIKeyBytes {
		return "", fmt.Errorf("a team api key has at least %d hex characters after the prefix", 2*minTeamAPIKeyBytes)
	}

	return key, nil
}

func generateTeamAPIKey() (string, error) {
	key, err := keys.GenerateKey(keys.ApiKeyPrefix)
	if err != nil {
		return "", err
	}

	return key.PrefixedRawValue, nil
}

// rejectForeignTeamAPIKey refuses a key that already authenticates another
// team. The hash column is unique, so inserting it for this team would be
// swallowed as a duplicate and the seed would then revoke this team's own key
// while the configured one kept working as the other team's.
func rejectForeignTeamAPIKey(ctx context.Context, db *authdb.Client, teamID uuid.UUID, key string) error {
	hash, _, err := createTokenHash(keys.ApiKeyPrefix, key)
	if err != nil {
		return err
	}

	var others int
	err = db.TestsRawSQLQuery(ctx, `SELECT count(*) FROM team_api_keys WHERE api_key_hash = $1 AND team_id <> $2`, func(rows pgx.Rows) error {
		for rows.Next() {
			if err := rows.Scan(&others); err != nil {
				return err
			}
		}

		return rows.Err()
	}, hash, teamID)
	if err != nil {
		return fmt.Errorf("failed to check the team api key's owner: %w", err)
	}

	if others > 0 {
		return errors.New("SEED_TEAM_API_KEY is already the key of another team; choose another key")
	}

	return nil
}

// revokeOtherSeedKeys deletes every other key this program inserted for the
// team, so a rotation (a new explicit key, or a new generated one after the
// file was removed) leaves exactly one seed key, and an in-place upgrade of
// an install seeded with the fixed development key stops accepting it. Keys
// created elsewhere carry other names and are never touched.
func revokeOtherSeedKeys(ctx context.Context, db *authdb.Client, teamID uuid.UUID, key string) error {
	hash, _, err := createTokenHash(keys.ApiKeyPrefix, key)
	if err != nil {
		return err
	}

	return db.TestsRawSQL(ctx, `DELETE FROM team_api_keys WHERE team_id = $1 AND name = $2 AND api_key_hash <> $3`, teamID, seedTeamAPIKeyName, hash)
}

func upsertTeamAPIKey(ctx context.Context, db *authdb.Client, userID, teamID uuid.UUID, tokenPrefix, token string) error {
	tokenHash, tokenMask, err := createTokenHash(tokenPrefix, token)
	if err != nil {
		return fmt.Errorf("failed to create token hash: %w", err)
	}

	if _, err = db.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
		TeamID:           teamID,
		CreatedBy:        &userID,
		ApiKeyHash:       tokenHash,
		ApiKeyPrefix:     tokenMask.Prefix,
		ApiKeyLength:     int32(tokenMask.ValueLength),
		ApiKeyMaskPrefix: tokenMask.MaskedValuePrefix,
		ApiKeyMaskSuffix: tokenMask.MaskedValueSuffix,
		Name:             seedTeamAPIKeyName,
	}); ignoreConstraints(err) != nil {
		return fmt.Errorf("failed to create team api key: %w", err)
	}

	return nil
}

func upsertUserIdentity(ctx context.Context, db *authdb.Client, userID uuid.UUID, oidcIssuer, oidcSubject string) error {
	if _, err := db.UpsertPublicIdentity(ctx, authqueries.UpsertPublicIdentityParams{
		OidcIss: oidcIssuer,
		OidcSub: oidcSubject,
		UserID:  userID,
	}); err != nil {
		return fmt.Errorf("failed to upsert user identity: %w", err)
	}

	return nil
}

func ensureUserIsOnTeam(ctx context.Context, db *authdb.Client, userID, teamID uuid.UUID) error {
	if err := db.TestsRawSQL(ctx, `
INSERT INTO users_teams (user_id, team_id, is_default)
VALUES ($1, $2, NOT EXISTS (SELECT 1 FROM users_teams WHERE user_id = $1 AND is_default))
ON CONFLICT (team_id, user_id) DO UPDATE
SET is_default = users_teams.is_default OR EXCLUDED.is_default;`, userID, teamID); err != nil {
		return fmt.Errorf("failed to add user to team: %w", err)
	}

	return nil
}

func ignoreConstraints(err error) error {
	// sqlc check
	if pgconnErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		if pgconnErr.Code == "23505" {
			return nil
		}
	}

	return err
}

func upsertTeam(ctx context.Context, db *authdb.Client) (uuid.UUID, error) {
	err := db.TestsRawSQL(ctx, `
INSERT INTO teams (id, email, name, tier, is_blocked, slug)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET
	email = EXCLUDED.email,
	name = EXCLUDED.name,
	tier = EXCLUDED.tier,
	slug = EXCLUDED.slug
`, teamID, "team@e2b-dev.local", "local-dev team", "base_v1", false, "local-dev-team")
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to upsert team: %w", err)
	}

	return teamID, nil
}

func upsertUser(ctx context.Context, db *authdb.Client, userID uuid.UUID) error {
	err := db.TestsRawSQL(ctx, `
INSERT INTO auth.users (id, email)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE SET
	email = EXCLUDED.email
`, userID, "user@e2b-dev.local")
	if err != nil {
		return fmt.Errorf("failed to upsert user: %w", err)
	}

	err = db.UpsertPublicUser(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to upsert public user: %w", err)
	}

	return nil
}

func createTokenHash(prefix, accessToken string) (string, keys.MaskedIdentifier, error) {
	hasher := keys.NewSHA256Hashing()
	tokenWithoutPrefix := strings.TrimPrefix(accessToken, prefix)
	accessTokenBytes, err := hex.DecodeString(tokenWithoutPrefix)
	if err != nil {
		return "", keys.MaskedIdentifier{}, errors.New("failed to hex decode string")
	}
	accessTokenHash := hasher.Hash(accessTokenBytes)
	accessTokenMask, err := keys.MaskKey(prefix, tokenWithoutPrefix)
	if err != nil {
		return "", keys.MaskedIdentifier{}, errors.New("failed to mask key")
	}

	return accessTokenHash, accessTokenMask, nil
}
