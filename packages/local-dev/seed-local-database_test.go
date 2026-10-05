package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	infradb "github.com/e2b-dev/infra/packages/db"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

func TestRun(t *testing.T) {
	postgresContainer, err := postgres.Run(t.Context(),
		"postgres:18-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("password"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := postgresContainer.Terminate(context.WithoutCancel(t.Context()))
		assert.NoError(t, err)
	})

	connectionString, err := postgresContainer.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)
	t.Setenv("POSTGRES_CONNECTION_STRING", connectionString)

	db, err := sql.Open("pgx", connectionString)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := db.Close()
		assert.NoError(t, err)
	})

	// run the db migration, through a provider carrying its own store rather
	// than goose's package-level dialect and tracking-table globals. The
	// files come embedded from the db module: go test does not track a file
	// outside this module as a test input, so a migration edit read from the
	// sibling checkout would leave a cached pass standing.
	store, err := database.NewStore(goose.DialectPostgres, "_migrations")
	require.NoError(t, err)

	provider, err := goose.NewProvider(
		"", // Has to be empty when using a custom store
		db,
		infradb.Migrations(),
		goose.WithStore(store),
	)
	require.NoError(t, err)

	_, err = provider.Up(t.Context())
	require.NoError(t, err)

	canonicalUserID := uuid.New()
	t.Setenv("SEED_USER_ID", canonicalUserID.String())

	err = run(t.Context())
	require.NoError(t, err)
	var owner uuid.UUID
	err = db.QueryRowContext(t.Context(), "SELECT user_id FROM public.user_identities WHERE oidc_iss = $1 AND oidc_sub = $2", defaultOidcIssuer, defaultOidcSubject).Scan(&owner)
	require.NoError(t, err)
	require.Equal(t, canonicalUserID, owner)
	var member bool
	err = db.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM public.users_teams WHERE user_id = $1 AND team_id = $2)", canonicalUserID, teamID).Scan(&member)
	require.NoError(t, err)
	require.True(t, member)

	var defaultTeam uuid.UUID
	require.NoError(t, run(t.Context()))
	err = db.QueryRowContext(t.Context(), "SELECT team_id FROM users_teams WHERE user_id=$1 AND is_default", canonicalUserID).Scan(&defaultTeam)
	require.NoError(t, err)
	require.Equal(t, teamID, defaultTeam)

	_, err = db.ExecContext(t.Context(), "UPDATE users_teams SET is_default=false WHERE user_id=$1", canonicalUserID)
	require.NoError(t, err)
	require.NoError(t, run(t.Context()))
	err = db.QueryRowContext(t.Context(), "SELECT team_id FROM users_teams WHERE user_id=$1 AND is_default", canonicalUserID).Scan(&defaultTeam)
	require.NoError(t, err)
	require.Equal(t, teamID, defaultTeam)

	existingTeam := uuid.New()
	_, err = db.ExecContext(t.Context(), "INSERT INTO teams (id,email,name,tier,is_blocked,slug) VALUES ($1,'test@example.com','Existing project','base_v1',false,$2)", existingTeam, existingTeam.String())
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "UPDATE users_teams SET is_default=false WHERE user_id=$1", canonicalUserID)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "INSERT INTO users_teams (user_id,team_id,is_default) VALUES ($1,$2,true)", canonicalUserID, existingTeam)
	require.NoError(t, err)
	require.NoError(t, run(t.Context()))
	err = db.QueryRowContext(t.Context(), "SELECT team_id FROM users_teams WHERE user_id=$1 AND is_default", canonicalUserID).Scan(&defaultTeam)
	require.NoError(t, err)
	require.Equal(t, existingTeam, defaultTeam)

	// A generated key: the row carries its hash, the file keeps the key, and a
	// second run reuses it instead of adding another row.
	keyFile := filepath.Join(t.TempDir(), "team-api-key")
	t.Setenv("SEED_TEAM_API_KEY", "random")
	t.Setenv("SEED_TEAM_API_KEY_FILE", keyFile)
	printed := captureStdout(t, func() { require.NoError(t, run(t.Context())) })
	stored, err := os.ReadFile(keyFile)
	require.NoError(t, err)
	generated, err := parseTeamAPIKey(string(stored))
	require.NoError(t, err)
	require.Contains(t, printed, keys.MaskToken(keys.ApiKeyPrefix, generated), "the seed names the key it wrote, masked")
	require.NotContains(t, printed, generated, "the seed never prints the raw key")
	require.NotEqual(t, keys.ApiKeyPrefix+teamTokenValue, generated)
	generatedBytes, err := hex.DecodeString(strings.TrimPrefix(generated, keys.ApiKeyPrefix))
	require.NoError(t, err)
	generatedHash := keys.NewSHA256Hashing().Hash(generatedBytes)
	var rows int
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE team_id = $1 AND api_key_hash = $2", teamID, generatedHash).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	require.NoError(t, run(t.Context()))
	again, err := os.ReadFile(keyFile)
	require.NoError(t, err)
	require.Equal(t, string(stored), string(again))
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE team_id = $1", teamID).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 1, rows, "the generated key only: a run that resolves another key revokes the fixed development key the earlier runs inserted")
	fixedBytes, err := hex.DecodeString(teamTokenValue)
	require.NoError(t, err)
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE api_key_hash = $1", keys.NewSHA256Hashing().Hash(fixedBytes)).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 0, rows, "the fixed development key no longer authenticates")

	// Rotation. A pinned key replaces the generated one, and a fresh random key
	// (file removed, as an operator rotating a leaked key would) replaces the
	// pinned one: at every step the team has exactly one seed-managed key.
	const pinnedKey = "e2b_ffeeddccbbaa99887766554433221100"
	t.Setenv("SEED_TEAM_API_KEY", pinnedKey)
	require.NoError(t, run(t.Context()))
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE team_id = $1", teamID).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 1, rows, "the pinned key only")
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE api_key_hash = $1", hashOf(t, pinnedKey)).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE api_key_hash = $1", generatedHash).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 0, rows, "the earlier generated key is revoked")

	require.NoError(t, os.Remove(keyFile))
	t.Setenv("SEED_TEAM_API_KEY", "random")
	require.NoError(t, run(t.Context()))
	rotated, err := os.ReadFile(keyFile)
	require.NoError(t, err)
	require.NotEqual(t, string(stored), string(rotated))
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE team_id = $1", teamID).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 1, rows, "the new generated key only")
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE api_key_hash = $1", hashOf(t, pinnedKey)).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 0, rows, "the pinned key is revoked")

	// A key that already belongs to another team is refused before anything is
	// written or revoked: accepting it would revoke this team's key while the
	// configured key kept authenticating as the other team.
	const foreignKey = "e2b_0f0e0d0c0b0a09080706050403020100"
	otherTeam := uuid.New()
	_, err = db.ExecContext(t.Context(), "INSERT INTO teams (id,email,name,tier,is_blocked,slug) VALUES ($1,'other@example.com','Other team','base_v1',false,$2)", otherTeam, otherTeam.String())
	require.NoError(t, err)
	authDb, err := authdb.NewClient(t.Context(), connectionString)
	require.NoError(t, err)
	t.Cleanup(func() { _ = authDb.Close() })
	require.NoError(t, upsertTeamAPIKey(t.Context(), authDb, canonicalUserID, otherTeam, keys.ApiKeyPrefix, foreignKey))

	t.Setenv("SEED_TEAM_API_KEY", foreignKey)
	require.ErrorContains(t, run(t.Context()), "another team")
	err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM team_api_keys WHERE team_id = $1 AND api_key_hash = $2", teamID, hashOf(t, strings.TrimSpace(string(rotated)))).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 1, rows, "the seeded team keeps its own key")
	err = db.QueryRowContext(t.Context(), "SELECT team_id FROM team_api_keys WHERE api_key_hash = $1", hashOf(t, foreignKey)).Scan(&owner)
	require.NoError(t, err)
	require.Equal(t, otherTeam, owner, "the other team's key is untouched")
}

// hashOf is the stored form of a prefixed team API key.
func hashOf(t *testing.T, key string) string {
	t.Helper()

	raw, err := hex.DecodeString(strings.TrimPrefix(key, keys.ApiKeyPrefix))
	require.NoError(t, err)

	return keys.NewSHA256Hashing().Hash(raw)
}

func TestRunRejectsInvalidSeedUserID(t *testing.T) {
	t.Setenv("SEED_USER_ID", "not-a-uuid")
	require.ErrorContains(t, run(t.Context()), "SEED_USER_ID")
}

func TestResolveTeamAPIKey(t *testing.T) {
	t.Parallel()

	fixedKey := keys.ApiKeyPrefix + teamTokenValue

	const (
		generatedKey = "e2b_00112233445566778899aabbccddeeff0011223344"
		pinnedKey    = "e2b_ffeeddccbbaa99887766554433221100"
		storedKey    = "e2b_a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
	)

	generate := func() (string, error) { return generatedKey, nil }
	noGenerate := func() (string, error) { return "", errors.New("generate must not be called") }

	tests := []struct {
		name      string
		value     string
		withFile  bool
		fileHolds string
		fileMode  os.FileMode // 0 means 0600
		fileIsDir bool
		generate  func() (string, error)
		wantKey   string
		wantFile  string
		wantErr   string
	}{
		{name: "unset keeps the fixed development key", value: "", generate: noGenerate, wantKey: fixedKey},
		{name: "unset writes the fixed key to the file", value: "", withFile: true, generate: noGenerate, wantKey: fixedKey, wantFile: fixedKey + "\n"},
		{name: "random without a file is refused", value: "random", generate: generate, wantErr: "SEED_TEAM_API_KEY_FILE"},
		{name: "random generates a key and writes it", value: "random", withFile: true, generate: generate, wantKey: generatedKey, wantFile: generatedKey + "\n"},
		{name: "random reuses the key already in the file", value: "random", withFile: true, fileHolds: storedKey + "\n", generate: noGenerate, wantKey: storedKey, wantFile: storedKey + "\n"},
		{name: "random refuses a file that does not hold a key", value: "random", withFile: true, fileHolds: "not a key\n", generate: noGenerate, wantFile: "not a key\n", wantErr: "SEED_TEAM_API_KEY_FILE"},
		{name: "random treats an empty file as no key yet", value: "random", withFile: true, fileHolds: "\n", generate: generate, wantKey: generatedKey, wantFile: generatedKey + "\n"},
		{name: "random refuses a file it cannot read", value: "random", withFile: true, fileIsDir: true, generate: noGenerate, wantErr: "SEED_TEAM_API_KEY_FILE"},
		{name: "an explicit key tightens the mode of a pre-existing file", value: pinnedKey, withFile: true, fileHolds: storedKey + "\n", fileMode: 0o644, generate: noGenerate, wantKey: pinnedKey, wantFile: pinnedKey + "\n"},
		{name: "random reuse tightens the mode of a pre-existing file", value: "random", withFile: true, fileHolds: storedKey, fileMode: 0o644, generate: noGenerate, wantKey: storedKey, wantFile: storedKey + "\n"},
		{name: "a whitespace-only value is refused, not taken as the default", value: " \n", generate: noGenerate, wantErr: "SEED_TEAM_API_KEY"},
		{name: "an explicit key is used and replaces the file", value: pinnedKey, withFile: true, fileHolds: storedKey + "\n", generate: noGenerate, wantKey: pinnedKey, wantFile: pinnedKey + "\n"},
		{name: "whitespace around an explicit key is ignored", value: " " + pinnedKey + "\n", generate: noGenerate, wantKey: pinnedKey},
		{name: "an explicit key without the prefix is refused", value: strings.TrimPrefix(pinnedKey, keys.ApiKeyPrefix), generate: noGenerate, wantErr: "SEED_TEAM_API_KEY"},
		{name: "an explicit key that is not hex is refused", value: "e2b_zz223344556677889900aabbccddeeff", generate: noGenerate, wantErr: "SEED_TEAM_API_KEY"},
		{name: "an explicit key shorter than 16 bytes is refused", value: "e2b_00112233445566778899aabbccddee", generate: noGenerate, wantErr: "SEED_TEAM_API_KEY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := ""
			if tt.withFile {
				file = filepath.Join(t.TempDir(), "team-api-key")
				switch {
				case tt.fileIsDir:
					require.NoError(t, os.Mkdir(file, 0o700))
				case tt.fileHolds != "":
					mode := tt.fileMode
					if mode == 0 {
						mode = 0o600
					}
					require.NoError(t, os.WriteFile(file, []byte(tt.fileHolds), mode))
				}
			}

			got, err := resolveTeamAPIKey(tt.value, file, tt.generate)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantKey, got)
			}

			if file == "" || tt.fileIsDir {
				return
			}
			if tt.wantFile == "" {
				require.NoFileExists(t, file)

				return
			}
			content, err := os.ReadFile(file)
			require.NoError(t, err)
			require.Equal(t, tt.wantFile, string(content))
			info, err := os.Stat(file)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		})
	}
}

func TestGenerateTeamAPIKeyIsAccepted(t *testing.T) {
	t.Parallel()

	generated, err := generateTeamAPIKey()
	require.NoError(t, err)
	parsed, err := parseTeamAPIKey(generated)
	require.NoError(t, err)
	require.Equal(t, generated, parsed)
	require.Len(t, generated, len(keys.ApiKeyPrefix)+2*20, "keys.GenerateKey draws 20 random bytes")

	other, err := generateTeamAPIKey()
	require.NoError(t, err)
	require.NotEqual(t, generated, other)
}

// captureStdout runs fn with os.Stdout redirected into a pipe and returns what
// it printed. Not for parallel tests: os.Stdout is process-wide.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	require.NoError(t, err)

	original := os.Stdout
	os.Stdout = writer

	fn()

	os.Stdout = original
	require.NoError(t, writer.Close())

	printed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	return string(printed)
}
