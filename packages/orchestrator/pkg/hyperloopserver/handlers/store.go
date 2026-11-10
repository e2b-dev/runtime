//go:build linux

package handlers

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const CollectorExporterTimeout = 10 * time.Second

const defaultMaxInflightShadowWrites = 1024

type APIStore struct {
	logger          logger.Logger
	staleWarnLogger logger.Logger
	sandboxes       *sandbox.Map

	collectorClient http.Client
	logWriteConfig  *featureflags.LogWriteConfigResolver

	// shadowInflight bounds concurrent shadow forwards (best-effort, non-blocking
	// acquire; dropped when the resolved route limit is reached).
	shadowInflight atomic.Int64
}

func NewHyperloopStore(logger logger.Logger, sandboxes *sandbox.Map, sandboxCollectorAddr string, featureFlags *featureflags.Client) *APIStore {
	staleWarnLogger := logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(core, staleLogWarningInterval, 1, 0)
	}))

	return &APIStore{
		logger:          logger,
		staleWarnLogger: staleWarnLogger,
		sandboxes:       sandboxes,

		collectorClient: http.Client{
			Timeout: CollectorExporterTimeout,
		},
		logWriteConfig: featureflags.NewLogWriteConfigResolver(featureFlags, featureflags.LogWriteDefaultsFromEnv(sandboxCollectorAddr)),
	}
}

func (h *APIStore) sendAPIStoreError(c *gin.Context, code int, message string) {
	apierrors.SendAPIStoreError(c, code, message)
}
