package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

type failingAPIKeyStore struct {
	staticAuthStore

	err error
}

func (s failingAPIKeyStore) GetTeamByHashedAPIKey(context.Context, string) (*types.Team, error) {
	return nil, s.err
}

// Only a key no team owns is a 401. A lookup that failed has not shown the
// key is unknown, so it answers 5xx: a caller that treats the 401 as final
// must not do so for a database or cache outage.
func TestValidateAPIKeyTellsAnUnknownKeyFromAFailedLookup(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		storeErr error
		wantCode int
	}{
		"unknown key": {
			storeErr: fmt.Errorf("failed to get team from API key: %w", pgx.ErrNoRows),
			wantCode: http.StatusUnauthorized,
		},
		"failed lookup": {
			storeErr: errors.New("failed to get team from API key: connection refused"),
			wantCode: http.StatusInternalServerError,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			service := &AuthService{
				store:     failingAPIKeyStore{err: tc.storeErr},
				teamCache: newAuthCache(redis_utils.SetupInstance(t)),
			}
			ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())

			team, apiErr := service.ValidateAPIKey(t.Context(), ginCtx, keys.ApiKeyPrefix+strings.Repeat("a", 40))

			require.Nil(t, team)
			require.NotNil(t, apiErr)
			require.Equal(t, tc.wantCode, apiErr.Code)
		})
	}
}
