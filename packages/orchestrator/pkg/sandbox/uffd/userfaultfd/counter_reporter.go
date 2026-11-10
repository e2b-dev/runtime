//go:build linux

package userfaultfd

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// counterReportInterval is the shortest window a counterReporter accumulates
// before Log writes it. The serve loop calls Log on every pass that handles
// page faults, so without a window a busy sandbox wrote about one line per
// fault batch.
const counterReportInterval = 30 * time.Second

// counterReporter tracks counts per type/key and logs them all in a single
// message, at most once per counterReportInterval and once more on Close.
type counterReporter struct {
	counts    map[string]uint64
	startTime time.Time
	endTime   time.Time
	logger    logger.Logger
	msg       string
	interval  time.Duration
	now       func() time.Time
}

func newCounterReporter(logger logger.Logger, msg string) *counterReporter {
	return &counterReporter{
		counts:    make(map[string]uint64),
		startTime: time.Time{},
		endTime:   time.Time{},
		logger:    logger,
		msg:       msg,
		interval:  counterReportInterval,
		now:       time.Now,
	}
}

func (c *counterReporter) Increase(eventType string) {
	if c.totalCount() == 0 {
		c.startTime = c.now()
	}

	c.counts[eventType]++

	c.endTime = c.now()
}

func (c *counterReporter) totalCount() uint64 {
	var total uint64
	for _, count := range c.counts {
		total += count
	}

	return total
}

func (c *counterReporter) log(ctx context.Context, closing bool) {
	total := c.totalCount()
	if total > 0 {
		fields := []zap.Field{
			zap.Uint64("count", total),
			logger.Time("start", c.startTime),
			logger.Time("end", c.endTime),
			zap.Bool("closing", closing),
		}

		for eventType, count := range c.counts {
			fields = append(fields, zap.Uint64(eventType, count))
		}

		c.logger.Debug(ctx, c.msg, fields...)

		c.counts = make(map[string]uint64)
	}
}

// Close writes whatever has accumulated, however recent.
func (c *counterReporter) Close(ctx context.Context) {
	c.log(ctx, true)
}

// Log writes the accumulated counts once the oldest of them is at least
// interval old; until then it keeps accumulating.
func (c *counterReporter) Log(ctx context.Context) {
	if c.totalCount() == 0 || c.now().Sub(c.startTime) < c.interval {
		return
	}

	c.log(ctx, false)
}
