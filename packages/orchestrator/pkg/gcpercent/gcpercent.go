// Package gcpercent sets the process's Go GC percent from a feature flag.
//
// The controller writes the knob and reports why it holds the value it does;
// it reads no heap quantity. The runtime derives the heap goal from the live
// heap at every cycle, so any accepted percent keeps the goal above the live
// heap, and judging a value's cost is left to metrics and alert rules.
package gcpercent

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// Interval is how often the flag is evaluated.
	Interval = 30 * time.Second

	// Disabled is the flag value that keeps the original percent in force.
	Disabled = -1

	// MinPercent and MaxPercent bound the accepted percents. Below the floor
	// the collection rate grows as 1/percent for little memory returned;
	// above the ceiling the pacer runs looser than the runtime default.
	MinPercent = 10
	MaxPercent = 100

	// warnInterval is how often a refused value or a failed evaluation is
	// logged again while it persists.
	warnInterval = 10 * time.Minute

	gogcSample = "/gc/gogc:percent"
)

// Outcome says why the process runs at the GC percent in force.
type Outcome string

const (
	// OutcomeApplied: the controller applied an accepted percent.
	OutcomeApplied Outcome = "applied"
	// OutcomeDisabled: the flag is -1 and the original percent is in force.
	OutcomeDisabled Outcome = "disabled"
	// OutcomeRefused: the flag holds a value outside {-1} ∪ [10, 100] and the
	// original percent is in force.
	OutcomeRefused Outcome = "refused"
	// OutcomeError: the evaluation served no value and the original percent
	// is in force.
	OutcomeError Outcome = "error"
)

// outcomes is every Outcome, in the precedence order resolve applies.
var outcomes = []Outcome{OutcomeError, OutcomeRefused, OutcomeDisabled, OutcomeApplied}

// resolve maps one evaluation to its outcome. The order is the precedence:
// a failed evaluation hands back the fallback, -1, so it must be decided
// before the value is read.
func resolve(value int, served bool) Outcome {
	switch {
	case !served:
		return OutcomeError
	case value != Disabled && (value < MinPercent || value > MaxPercent):
		return OutcomeRefused
	case value == Disabled:
		return OutcomeDisabled
	default:
		return OutcomeApplied
	}
}

// flagSource evaluates the flag and reports whether a value was served.
type flagSource func(ctx context.Context) (int, bool)

// Controller evaluates the flag on a ticker and writes the GC percent that
// should be in force whenever it changes.
type Controller struct {
	flag     flagSource
	set      func(int) int
	original int
	log      logger.Logger

	// Owned by the Run goroutine.
	inForce     int
	warned      Outcome
	warnedValue int
	lastWarnAt  time.Time

	mu        sync.Mutex
	evaluated bool
	outcome   Outcome
}

// New builds a controller over the orchestrator's GC percent flag and
// registers its outcome gauge. The original percent is read from the runtime
// here and is what every non-applied outcome restores.
func New(meterProvider metric.MeterProvider, flags *featureflags.Client) (*Controller, error) {
	original, err := readGCPercent()
	if err != nil {
		return nil, err
	}

	source := func(ctx context.Context) (int, bool) {
		return flags.IntFlagOverride(ctx, featureflags.OrchestratorGOGCPercentFlag)
	}

	return newController(meterProvider, source, debug.SetGCPercent, original, logger.L())
}

func newController(meterProvider metric.MeterProvider, flag flagSource, set func(int) int, original int, log logger.Logger) (*Controller, error) {
	c := &Controller{
		flag:     flag,
		set:      set,
		original: original,
		log:      log,
		inForce:  original,
	}

	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/gcpercent")
	gauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorGOGCOutcomeGaugeName)
	if err != nil {
		return nil, fmt.Errorf("create GC outcome gauge: %w", err)
	}

	outcomeAttrs := make([]metric.ObserveOption, len(outcomes))
	for i, o := range outcomes {
		outcomeAttrs[i] = metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", string(o))))
	}

	// Every outcome is observed on every collection, so a change moves the
	// values of existing series instead of ending one series and starting
	// another, which a push exporter would leave both visible for.
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		c.mu.Lock()
		evaluated, current := c.evaluated, c.outcome
		c.mu.Unlock()

		if !evaluated {
			return nil
		}
		for i, outcome := range outcomes {
			var v int64
			if outcome == current {
				v = 1
			}
			o.ObserveInt64(gauge, v, outcomeAttrs[i])
		}

		return nil
	}, gauge)
	if err != nil {
		return nil, fmt.Errorf("register GC outcome gauge callback: %w", err)
	}

	return c, nil
}

// Run evaluates the flag at once and then every Interval until ctx is done.
// The orchestrator passes a context that is never cancelled, so pacing stays
// in force through the sandbox drain and ends with the process.
func (c *Controller) Run(ctx context.Context) {
	c.tick(ctx)

	ticker := time.NewTicker(Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Controller) tick(ctx context.Context) {
	value, served := c.flag(ctx)
	outcome := resolve(value, served)

	target := c.original
	if outcome == OutcomeApplied {
		target = value
	}

	if target != c.inForce {
		previous := c.set(target)
		if previous != c.inForce {
			c.log.Warn(ctx, "GC percent was changed outside the controller; overwriting it",
				zap.Int("found", previous), zap.Int("last_written", c.inForce), zap.Int("gc_percent", target))
		}
		c.inForce = target
		c.log.Info(ctx, "GC percent changed",
			zap.Int("gc_percent", target), zap.Int("previous", previous), zap.String("outcome", string(outcome)))
	}

	c.mu.Lock()
	c.evaluated, c.outcome = true, outcome
	c.mu.Unlock()

	c.warn(ctx, outcome, value)
}

// warn logs a refused value or a failed evaluation when the outcome or the
// value changes, and again every warnInterval while both hold.
func (c *Controller) warn(ctx context.Context, outcome Outcome, value int) {
	if outcome != OutcomeRefused && outcome != OutcomeError {
		c.warned = ""

		return
	}

	now := time.Now()
	if outcome == c.warned && value == c.warnedValue && now.Sub(c.lastWarnAt) < warnInterval {
		return
	}
	c.warned, c.warnedValue, c.lastWarnAt = outcome, value, now

	if outcome == OutcomeRefused {
		c.log.Warn(ctx, "GC percent flag value refused; keeping the original percent",
			zap.Int("value", value), zap.Int("min", MinPercent), zap.Int("max", MaxPercent), zap.Int("gc_percent", c.original))

		return
	}

	c.log.Warn(ctx, "GC percent flag evaluation served no value; keeping the original percent",
		zap.String("flag", featureflags.OrchestratorGOGCPercentFlag.Key()), zap.Int("gc_percent", c.original))
}

// readGCPercent reads the GC percent in force without writing it.
func readGCPercent() (int, error) {
	sample := []rtmetrics.Sample{{Name: gogcSample}}
	rtmetrics.Read(sample)
	if sample[0].Value.Kind() != rtmetrics.KindUint64 {
		return 0, errors.New("runtime does not report " + gogcSample)
	}

	// GOGC=off reports -1 through the unsigned sample.
	return int(int64(sample[0].Value.Uint64())), nil
}
