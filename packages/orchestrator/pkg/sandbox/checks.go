//go:build linux

package sandbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox")

const (
	healthCheckInterval = 20 * time.Second
	healthCheckTimeout  = 100 * time.Millisecond
)

type Checks struct {
	sandbox *Sandbox

	mu        sync.Mutex
	cancelCtx context.CancelCauseFunc
	// stopped records that Stop ran. Start is launched in its own goroutine, so
	// a short-lived sandbox can Stop before Start is scheduled — at which point
	// cancelCtx is still nil and Stop has nothing to cancel. Without this flag
	// Start would then enter an uncancellable health-check loop that leaks.
	stopped bool

	healthy atomic.Bool
}

var ErrChecksStopped = errors.New("checks stopped")

func NewChecks(sandbox *Sandbox) *Checks {
	// Create background context, passed ctx is from create/resume request and will be canceled after the request is processed.
	h := &Checks{
		sandbox: sandbox,
		healthy: atomic.Bool{}, // defaults to `false`
	}

	// By default, the sandbox should be healthy, if the status change we report it.
	h.healthy.Store(true)

	return h
}

func (c *Checks) Start(ctx context.Context) {
	c.mu.Lock()
	if c.stopped {
		// Stop already ran before this goroutine was scheduled; don't start the
		// health-check loop, it would never be cancelled.
		c.mu.Unlock()

		return
	}
	ctx, c.cancelCtx = context.WithCancelCause(ctx)
	c.mu.Unlock()

	go c.sandbox.labelBalloonMode(ctx)
	go c.sandbox.runPeriodicHinting(ctx)
	c.logHealth(ctx)
}

func (c *Checks) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stopped = true

	if c.cancelCtx != nil {
		c.cancelCtx(ErrChecksStopped)
	}
}

func (c *Checks) logHealth(ctx context.Context) {
	healthTicker := time.NewTicker(healthCheckInterval)
	defer func() {
		healthTicker.Stop()
	}()

	// Get metrics and health status on sandbox startup
	go c.Healthcheck(ctx, false)
	go c.seedOOMWatermark(ctx)

	for {
		select {
		case <-healthTicker.C:
			c.Healthcheck(ctx, false)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Checks) Healthcheck(ctx context.Context, alwaysReport bool) {
	ok, err := c.getHealth(ctx, healthCheckTimeout)
	// Sandbox stopped
	if errors.Is(err, ErrChecksStopped) {
		return
	}

	if !ok && c.healthy.CompareAndSwap(true, false) {
		sbxlogger.E(c.sandbox).Healthcheck(ctx, sbxlogger.Fail)
		sbxlogger.I(c.sandbox).Warn(ctx, "healthcheck failed", zap.Error(err), logger.WithEnvdVersion(c.sandbox.Config.Envd.Version))

		return
	}

	if ok && c.healthy.CompareAndSwap(false, true) {
		sbxlogger.E(c.sandbox).Healthcheck(ctx, sbxlogger.Success)

		return
	}

	if alwaysReport {
		if ok {
			sbxlogger.E(c.sandbox).Healthcheck(ctx, sbxlogger.ReportSuccess)
		} else {
			sbxlogger.E(c.sandbox).Healthcheck(ctx, sbxlogger.ReportFail)
			sbxlogger.I(c.sandbox).Error(ctx, "control healthcheck failed", zap.Error(err), logger.WithEnvdVersion(c.sandbox.Config.Envd.Version))
		}
	}
}
