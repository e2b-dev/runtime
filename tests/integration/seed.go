package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/envd/pkg"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

type SeedData struct {
	APIKey  string
	EnvID   string
	BuildID uuid.UUID
	TeamID  uuid.UUID
	UserID  uuid.UUID
	RAMMB   int
}

func main() {
	ctx := context.Background()

	exitCode := run(ctx)
	if exitCode != 0 {
		log.Printf("Seed failed with exit code %d", exitCode)
		os.Exit(exitCode)
	}

	fmt.Println("Seed completed successfully.")
}

func run(ctx context.Context) int {
	connectionString := os.Getenv("POSTGRES_CONNECTION_STRING")
	if connectionString == "" {
		log.Printf("POSTGRES_CONNECTION_STRING is not set")

		return 1
	}

	db, err := client.NewClient(ctx, connectionString)
	if err != nil {
		log.Printf("Failed to connect to database: %v", err)

		return 1
	}
	defer db.Close()
	authDb, err := authdb.NewClient(ctx, connectionString)
	if err != nil {
		log.Printf("Failed to connect to database: %v", err)

		return 1
	}
	defer authDb.Close()

	// The build rows must declare the memory the template's memfile was
	// actually built with: the VM sizes itself from the row, and a row
	// larger than the memfile makes resumes read past its end. CI always
	// sets the env (build-sandbox-template writes it to .env.test); the
	// fallback matches the orchestrator Makefile's MEMORY_MB default so a
	// local build-then-seed without the env stays consistent too.
	ramMB := 256
	if v := os.Getenv("TESTS_SANDBOX_RAM_MB"); v != "" {
		ramMB, err = strconv.Atoi(v)
		if err != nil {
			log.Printf("Invalid TESTS_SANDBOX_RAM_MB %q: %v", v, err)

			return 1
		}
	}

	data := SeedData{
		APIKey:  os.Getenv("TESTS_E2B_API_KEY"),
		EnvID:   os.Getenv("TESTS_SANDBOX_TEMPLATE_ID"),
		BuildID: uuid.MustParse(os.Getenv("TESTS_SANDBOX_BUILD_ID")),
		TeamID:  uuid.MustParse(os.Getenv("TESTS_SANDBOX_TEAM_ID")),
		UserID:  uuid.MustParse(os.Getenv("TESTS_SANDBOX_USER_ID")),
		RAMMB:   ramMB,
	}

	err = seed(ctx, db, authDb, data)
	if err != nil {
		log.Printf("Failed to execute seed: %v", err)

		return 1
	}

	fmt.Println("Seed completed successfully.")

	return 0
}

func seed(ctx context.Context, db *client.Client, authdb *authdb.Client, data SeedData) error {
	hasher := keys.NewSHA256Hashing()

	// User
	err := authdb.TestsRawSQL(ctx, `
INSERT INTO auth.users (id, email)
VALUES ($1, $2)
`, data.UserID, "user-test-integration@e2b.dev")
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	err = authdb.UpsertPublicUser(ctx, data.UserID)
	if err != nil {
		return fmt.Errorf("failed to create public user: %w", err)
	}

	// Team
	err = authdb.TestsRawSQL(ctx, `
INSERT INTO teams (id, email, name, tier, is_blocked, slug)
VALUES ($1, $2, $3, $4, $5, $6)
`, data.TeamID, "test-integration@e2b.dev", "E2B", "base_v1", false, "e2b-integration")
	if err != nil {
		return fmt.Errorf("failed to create team: %w", err)
	}

	// Grant the integration test team extra capacity so parallel tests don't
	// hit the base tier's 20-sandbox cap.
	err = db.TestsRawSQL(ctx, `
INSERT INTO addons (team_id, name, extra_concurrent_sandboxes, extra_concurrent_template_builds, added_by)
VALUES ($1, 'integration-tests-extra-capacity', 200, 50, '00000000-0000-0000-0000-000000000000')
`, data.TeamID)
	if err != nil {
		return fmt.Errorf("failed to create test team addon: %w", err)
	}

	// User-Team
	err = authdb.TestsRawSQL(ctx, `
INSERT INTO users_teams (user_id, team_id, is_default)
VALUES ($1, $2, $3)
`, data.UserID, data.TeamID, true)
	if err != nil {
		return fmt.Errorf("failed to create user-team: %w", err)
	}

	// Team API Key
	keyWithoutPrefix := strings.TrimPrefix(data.APIKey, keys.ApiKeyPrefix)
	apiKeyBytes, err := hex.DecodeString(keyWithoutPrefix)
	if err != nil {
		return fmt.Errorf("failed to decode api key: %w", err)
	}
	apiKeyHash := hasher.Hash(apiKeyBytes)
	apiKeyMask, err := keys.MaskKey(keys.ApiKeyPrefix, keyWithoutPrefix)
	if err != nil {
		return fmt.Errorf("failed to mask api key: %w", err)
	}
	_, err = authdb.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
		TeamID:           data.TeamID,
		CreatedBy:        &data.UserID,
		ApiKeyHash:       apiKeyHash,
		ApiKeyPrefix:     apiKeyMask.Prefix,
		ApiKeyLength:     int32(apiKeyMask.ValueLength),
		ApiKeyMaskPrefix: apiKeyMask.MaskedValuePrefix,
		ApiKeyMaskSuffix: apiKeyMask.MaskedValueSuffix,
		Name:             "Integration Tests API Key",
	})
	if err != nil {
		return fmt.Errorf("failed to create team api key: %w", err)
	}

	// Env
	err = db.TestsRawSQL(ctx, `
INSERT INTO envs (id, team_id, public, build_count, spawn_count, updated_at, source)
VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, 'template')
`, data.EnvID, data.TeamID, true, 2, 0)
	if err != nil {
		return fmt.Errorf("failed to create env: %w", err)
	}

	type buildData struct {
		id        uuid.UUID
		createdAt *time.Time
	}

	oldBuildTime := time.Now().Add(-time.Hour)
	// Important: Insert builds in chronological order (oldest first, newest last)
	// The query uses ORDER BY eba.created_at DESC to get the latest build,
	// so the last inserted build assignment will be selected.
	builds := []buildData{
		// An older build, so we have multiple builds - inserted FIRST
		{
			id:        uuid.New(),
			createdAt: &oldBuildTime,
		},
		// Primary build - inserted LAST so it has the latest assignment timestamp
		{
			id:        data.BuildID,
			createdAt: nil,
		},
	}

	for _, build := range builds {
		if build.createdAt != nil {
			err = db.TestsRawSQL(ctx, `
INSERT INTO env_builds (
	id, dockerfile, status, vcpu, ram_mb, free_disk_size_mb,
	total_disk_size_mb, kernel_version, firecracker_version, envd_version,
	cluster_node_id, version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)
`, build.id, "FROM e2bdev/base:latest", dbtypes.BuildStatusUploaded,
				2, data.RAMMB, 512, 1982, "vmlinux-6.1.158-c1a568c", "v1.14-0.2.0", pkg.Version,
				"integration-test-node", templates.TemplateV2LatestVersion, build.createdAt)
		} else {
			err = db.TestsRawSQL(ctx, `
INSERT INTO env_builds (
	id, dockerfile, status, vcpu, ram_mb, free_disk_size_mb,
	total_disk_size_mb, kernel_version, firecracker_version, envd_version,
	cluster_node_id, version, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP)
`, build.id, "FROM e2bdev/base:latest", dbtypes.BuildStatusUploaded,
				2, data.RAMMB, 512, 1982, "vmlinux-6.1.158-c1a568c", "v1.14-0.2.0", pkg.Version,
				"integration-test-node", templates.TemplateV2LatestVersion)
		}
		if err != nil {
			return fmt.Errorf("failed to create env build: %w", err)
		}

		// Create the build assignment (trigger will backfill env_id for backward compat)
		var assignmentCreatedAt *time.Time
		if build.createdAt != nil {
			assignmentCreatedAt = build.createdAt
		}

		if assignmentCreatedAt != nil {
			err = db.TestsRawSQL(ctx, `
INSERT INTO env_build_assignments (env_id, build_id, tag, created_at)
VALUES ($1, $2, 'default', $3)
`, data.EnvID, build.id, assignmentCreatedAt)
		} else {
			err = db.TestsRawSQL(ctx, `
INSERT INTO env_build_assignments (env_id, build_id, tag)
VALUES ($1, $2, 'default')
`, data.EnvID, build.id)
		}
		if err != nil {
			return fmt.Errorf("failed to create env build assignment: %w", err)
		}
	}

	err = db.TestsRawSQL(ctx, `
INSERT INTO env_aliases (alias, is_renamable, env_id)
VALUES ($1, $2, $3)
`, "base", true, data.EnvID)
	if err != nil {
		return fmt.Errorf("failed to create env alias: %w", err)
	}

	return nil
}
