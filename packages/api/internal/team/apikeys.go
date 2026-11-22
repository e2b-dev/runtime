package team

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/authservice"
	authdb "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth"
	authqueries "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type CreateAPIKeyResponse struct {
	*authqueries.TeamApiKey

	RawAPIKey string
}

func CreateAPIKey(ctx context.Context, authDB *authdb.Client, teamID uuid.UUID, createdBy *uuid.UUID, name string) (CreateAPIKeyResponse, error) {
	teamApiKey, err := keys.GenerateKey(keys.ApiKeyPrefix)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when generating team API key", err)

		return CreateAPIKeyResponse{}, fmt.Errorf("error when generating team API key: %w", err)
	}

	apiKey, err := authDB.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
		TeamID:           teamID,
		CreatedBy:        createdBy,
		ApiKeyHash:       teamApiKey.HashedValue,
		ApiKeyPrefix:     teamApiKey.Masked.Prefix,
		ApiKeyLength:     int32(teamApiKey.Masked.ValueLength),
		ApiKeyMaskPrefix: teamApiKey.Masked.MaskedValuePrefix,
		ApiKeyMaskSuffix: teamApiKey.Masked.MaskedValueSuffix,
		Name:             name,
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when creating API key", err)

		return CreateAPIKeyResponse{}, fmt.Errorf("error when creating API key: %w", err)
	}

	return CreateAPIKeyResponse{
		TeamApiKey: &apiKey,
		RawAPIKey:  teamApiKey.PrefixedRawValue,
	}, nil
}

func DeleteAPIKey(ctx context.Context, authDB *authdb.Client, authService authservice.Service, teamID uuid.UUID, apiKeyID uuid.UUID) (bool, error) {
	hashes, err := authDB.DeleteTeamAPIKey(ctx, authqueries.DeleteTeamAPIKeyParams{
		ID:     apiKeyID,
		TeamID: teamID,
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when deleting API key", err)

		return false, fmt.Errorf("error when deleting API key: %w", err)
	}

	// Invalidate the auth cache so the deleted key stops authenticating
	// immediately instead of after the cache TTL expires.
	for _, hash := range hashes {
		authService.InvalidateAPIKeyCache(ctx, hash)
	}

	return len(hashes) > 0, nil
}
