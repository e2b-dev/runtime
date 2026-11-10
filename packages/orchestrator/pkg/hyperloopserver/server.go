//go:build linux

package hyperloopserver

import (
	"context"
	"fmt"
	"net"
	"net/http"

	limits "github.com/gin-contrib/size"
	"github.com/gin-gonic/gin"
	middleware "github.com/oapi-codegen/gin-middleware"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/hyperloopserver/contracts"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/hyperloopserver/handlers"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const maxUploadLimit = 1 << 28 // 256 MiB

func NewHyperloopServer(ctx context.Context, port uint16, logger logger.Logger, sandboxes *sandbox.Map, featureFlags *featureflags.Client) (*http.Server, error) {
	sandboxCollectorAddr := env.LogsCollectorAddress()
	store := handlers.NewHyperloopStore(logger, sandboxes, sandboxCollectorAddr, featureFlags)
	swagger, err := contracts.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("error getting swagger spec: %w", err)
	}

	engine := gin.New()
	engine.Use(
		gin.Recovery(),
		limits.RequestSizeLimiter(maxUploadLimit),
		// Nothing reads the decoded body; don't hold it for the whole request.
		middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{DiscardValidatedValue: true}),
	)

	server := &http.Server{
		Handler: engine,
		Addr:    fmt.Sprintf("0.0.0.0:%d", port),

		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	httpserver.ConfigureH2C(server)

	contracts.RegisterHandlersWithOptions(engine, store, contracts.GinServerOptions{})

	return server, nil
}
