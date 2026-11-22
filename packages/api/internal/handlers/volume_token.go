package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

var ErrVolumesTokenNotConfigured = errors.New("volumes content token signing is not supported by this deployment")

// generateVolumeContentToken mints the JWT the SDK presents to the volume
// content API. audience is the origin the token is valid for
// (`https://api.<domain>`, see volumeTokenAudience).
func generateVolumeContentToken(config cfg.VolumesTokenConfig, volume queries.Volume, team *types.Team, audience string) (string, *api.APIError) {
	if !config.IsConfigured() {
		return "", &api.APIError{
			Err:       ErrVolumesTokenNotConfigured,
			ClientMsg: ErrVolumesTokenNotConfigured.Error(),
			Code:      http.StatusNotImplemented,
		}
	}

	now := time.Now()
	expiration := now.Add(config.Duration)

	claims := jwt.MapClaims{
		// registered
		"aud": audience,
		"exp": jwt.NewNumericDate(expiration),
		"iat": jwt.NewNumericDate(now),
		"iss": config.Issuer,
		"jti": uuid.NewString(),
		"nbf": jwt.NewNumericDate(now),
		"sub": team.ID.String(),

		// custom
		"teamid":  team.ID.String(),
		"volid":   volume.ID.String(),
		"voltype": volume.VolumeType,
	}

	token := jwt.NewWithClaims(config.SigningMethod, claims)
	// `kid` is the standard header verifiers use to select a key from a JWKS;
	// `tokid` is kept for backwards compatibility.
	token.Header["kid"] = config.SigningKeyName
	token.Header["tokid"] = config.SigningKeyName
	signedToken, err := token.SignedString(config.SigningKey)
	if err != nil {
		return "", &api.APIError{
			Err:       fmt.Errorf("failed to sign token: %w", err),
			ClientMsg: "failed to sign token",
			Code:      http.StatusInternalServerError,
		}
	}

	return signedToken, nil
}
