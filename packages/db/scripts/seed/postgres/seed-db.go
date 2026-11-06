package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

func main() {
	ctx := context.Background()
	hasher := keys.NewSHA256Hashing()

	// Prompt user for values
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\nPlease enter the following values:")
	fmt.Println()

	fmt.Printf("Email: ")
	email, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input:", err)

		return
	}

	email = strings.TrimSpace(email)
	if email == "" {
		fmt.Println("Error: Email cannot be empty")

		return
	}

	teamUUID := uuid.New()

	teamAPIKey, err := keys.GenerateKey(keys.ApiKeyPrefix)
	if err != nil {
		fmt.Printf("Error: %v\n", err)

		return
	}

	fmt.Println()
	fmt.Println("Seeding database with:")
	fmt.Printf("  Email: %s\n", email)
	fmt.Printf("  Team ID: %s\n", teamUUID)
	fmt.Printf("  Team API Key: %s\n", teamAPIKey.PrefixedRawValue)
	fmt.Println()

	connectionString := os.Getenv("POSTGRES_CONNECTION_STRING")
	db, err := client.NewClient(ctx, connectionString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	authDb, err := authdb.NewClient(ctx, connectionString)
	if err != nil {
		panic(err)
	}
	defer authDb.Close()

	// Clean up existing data for idempotent re-seeding.
	// Delete child rows that have ON DELETE NO ACTION constraints
	// before deleting the team and user rows.
	err = authDb.TestsRawSQL(ctx, `
DELETE FROM envs WHERE team_id IN (SELECT id FROM teams WHERE email = $1)
`, email)
	if err != nil {
		panic(err)
	}

	err = authDb.TestsRawSQL(ctx, `
DELETE FROM snapshots WHERE team_id IN (SELECT id FROM teams WHERE email = $1)
`, email)
	if err != nil {
		panic(err)
	}

	err = authDb.TestsRawSQL(ctx, `
DELETE FROM volumes WHERE team_id IN (SELECT id FROM teams WHERE email = $1)
`, email)
	if err != nil {
		panic(err)
	}

	err = authDb.TestsRawSQL(ctx, `
DELETE FROM addons WHERE added_by IN (SELECT id FROM auth.users WHERE email = $1)
`, email)
	if err != nil {
		panic(err)
	}

	// Now safe to delete team (team_api_keys cascade automatically).
	err = authDb.TestsRawSQL(ctx, `
DELETE FROM teams WHERE email = $1
`, email)
	if err != nil {
		panic(err)
	}

	err = authDb.TestsRawSQL(ctx, `
DELETE FROM public.users WHERE id IN (SELECT id FROM auth.users WHERE email = $1)
`, email)
	if err != nil {
		panic(err)
	}

	// Delete the auth user separately.
	err = authDb.TestsRawSQL(ctx, `
DELETE FROM auth.users WHERE email = $1
`, email)
	if err != nil {
		panic(err)
	}

	userID := uuid.New()
	err = authDb.TestsRawSQL(ctx, `
INSERT INTO auth.users (id, email)
VALUES ($1, $2)
`, userID, email)
	if err != nil {
		panic(err)
	}

	err = authDb.UpsertPublicUser(ctx, userID)
	if err != nil {
		panic(err)
	}

	// Create team
	err = authDb.TestsRawSQL(ctx, `
INSERT INTO teams (id, email, name, is_blocked, slug)
VALUES ($1, $2, $3, $4, $5)
`, teamUUID, email, "E2B", false, "e2b")
	if err != nil {
		panic(err)
	}

	// Create user team
	err = authDb.TestsRawSQL(ctx, `
INSERT INTO users_teams (user_id, team_id, is_default)
VALUES ($1, $2, $3)
`, userID, teamUUID, true)
	if err != nil {
		panic(err)
	}

	// Create team api key
	keyWithoutPrefix := strings.TrimPrefix(teamAPIKey.PrefixedRawValue, keys.ApiKeyPrefix)
	apiKeyBytes, err := hex.DecodeString(keyWithoutPrefix)
	if err != nil {
		panic(err)
	}
	apiKeyHash := hasher.Hash(apiKeyBytes)
	apiKeyMask, err := keys.MaskKey(keys.ApiKeyPrefix, keyWithoutPrefix)
	if err != nil {
		panic(err)
	}
	_, err = authDb.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
		TeamID:           teamUUID,
		CreatedBy:        &userID,
		ApiKeyHash:       apiKeyHash,
		ApiKeyPrefix:     apiKeyMask.Prefix,
		ApiKeyLength:     int32(apiKeyMask.ValueLength),
		ApiKeyMaskPrefix: apiKeyMask.MaskedValuePrefix,
		ApiKeyMaskSuffix: apiKeyMask.MaskedValueSuffix,
		Name:             "Seed API Key",
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Database seeded.\n")
}
