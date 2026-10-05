//go:build linux

package sandbox

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

const (
	oomSeedTimeout = 2 * time.Second

	// Long enough for a restarted envd to read its kernel log.
	oomSeedAttempts   = 5
	oomSeedRetryDelay = 2 * time.Second
)

// Without a complete list the seed would take old kills for new ones.
var errOOMKillsUnknown = errors.New("envd doesn't know the guest's OOM kills")

// OOMWatermark remembers the newest kill logged for a sandbox, so each is logged once.
//
// The seed marks the snapshot's kills as seen. Kills after the last poll before a pause are lost.
type OOMWatermark struct {
	mu         sync.Mutex
	started    bool
	seq        int64
	seedFailed bool // the first poll sets the watermark instead
}

// Unseen returns the kills newer than the watermark, oldest first, and nothing before the seed.
func (w *OOMWatermark) Unseen(kills []envd.OOMKill) []envd.OOMKill {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.started && !w.seedFailed {
		return nil
	}

	return w.advance(kills)
}

// Seed sets the watermark unless a checkpoint left it set.
func (w *OOMWatermark) Seed(kills []envd.OOMKill) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.started {
		w.advance(kills)
	}
}

// seedDone reports whether the watermark is set or the seed has given up.
func (w *OOMWatermark) seedDone() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.started || w.seedFailed
}

// SeedFailed lets the first poll set the watermark.
func (w *OOMWatermark) SeedFailed() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seedFailed = true
}

// advance must be called with mu held.
func (w *OOMWatermark) advance(kills []envd.OOMKill) []envd.OOMKill {
	var unseen []envd.OOMKill
	for _, kill := range kills {
		if w.started && kill.Seq <= w.seq {
			continue
		}

		w.seq = max(w.seq, kill.Seq)
		if w.started {
			unseen = append(unseen, kill)
		}
	}
	w.started = true

	return unseen
}

// LogsOOMKills reports whether the sandbox's OOM kills reach its logs; a build's belong to the build.
func (s *Sandbox) LogsOOMKills() bool {
	return s.Runtime.SandboxType != sandboxtypes.SandboxTypeBuild
}

// seedOOMWatermark sets the watermark right after start, so early kills are still logged.
func (c *Checks) seedOOMWatermark(ctx context.Context) {
	if !c.sandbox.LogsOOMKills() {
		return
	}

	fetch := func(ctx context.Context) (*Metrics, error) { return c.GetMetrics(ctx, oomSeedTimeout) }
	err := c.sandbox.OOMKills.seedFrom(ctx, fetch, oomSeedRetryDelay)
	switch {
	case ctx.Err() != nil:
		// The checks stopped; nothing to report.
	case errors.Is(err, errOOMKillsUnknown):
		// Expected on an older envd, which never reports kills, so not a warning.
		logger.L().Debug(ctx, "envd doesn't know the guest's OOM kills, none will be logged until it does",
			logger.WithSandboxID(c.sandbox.Runtime.SandboxID))
	case err != nil:
		logger.L().Warn(ctx, "Failed to seed the OOM kill watermark, kills until the first metrics poll will not be logged",
			zap.Error(err), logger.WithSandboxID(c.sandbox.Runtime.SandboxID))
	}
}

// seedFrom sets the watermark from the first complete list fetch returns, trying oomSeedAttempts
// times. If none comes, it lets the first poll set the watermark and returns the last error; if
// ctx ends first, it returns ctx's error. After a checkpoint it does nothing if the watermark is set
// or an earlier seed gave up.
func (w *OOMWatermark) seedFrom(ctx context.Context, fetch func(context.Context) (*Metrics, error), retryDelay time.Duration) error {
	if w.seedDone() {
		return nil
	}

	var err error
	for attempt := range oomSeedAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}

		var m *Metrics
		m, err = fetch(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && m.OomKills == nil {
			err = errOOMKillsUnknown
		}
		if err == nil {
			w.Seed(*m.OomKills)

			return nil
		}
	}

	w.SeedFailed()

	return err
}
