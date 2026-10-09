package idempotency

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

type executorKey struct{}

const headerName = "Idempotency-Key"

// Middleware provides a route-gated executor after authentication and request admission.
func Middleware(client redis.UniversalClient, flags *featureflags.Client) gin.HandlerFunc {
	return middleware(&redisStore{client: client}, flags)
}

func middleware(store store, flags *featureflags.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		values, present := c.Request.Header[headerName]
		if !present {
			return
		}
		ctx := c.Request.Context()
		team, ok := auth.GetTeamInfo(c)
		if !ok || c.FullPath() == "" {
			return
		}
		routes := flags.JSONFlag(ctx, featureflags.APIIdempotencyRoutes)
		if !routes.GetByKey(c.Request.Method + " " + c.FullPath()).BoolValue() {
			return
		}
		if len(values) != 1 || len(values[0]) < 1 || len(values[0]) > 255 || strings.ContainsFunc(values[0], func(r rune) bool {
			return (r < 'a' || r > 'z') && (r < '0' || r > '9')
		}) {
			reject(c, http.StatusBadRequest, "Idempotency-Key must contain one value of 1 to 255 lowercase ASCII letters or digits")

			return
		}

		retentionSeconds, err := flags.IntFlagWithError(ctx, featureflags.APIIdempotencyTTLSeconds)
		if err != nil {
			unavailable(c, err)

			return
		}
		if retentionSeconds <= 0 {
			unavailable(c, fmt.Errorf("%s must be greater than zero", featureflags.APIIdempotencyTTLSeconds.Key()))

			return
		}
		c.Set(executorKey{}, &executor{
			store:            store,
			key:              recordKey(team.ID.String(), values[0]),
			retentionSeconds: retentionSeconds,
		})
	}
}
