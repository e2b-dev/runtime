//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/retry"
)

const (
	// reapplyWorkers bounds how many slot chains a flag change rebuilds at once.
	reapplyWorkers = 4

	// A slot whose re-apply failed is retried after retryInitialDelay, doubled
	// on every further failure up to retryMaxDelay, each wait spread by
	// retryJitter of itself either way.
	retryInitialDelay = 250 * time.Millisecond
	retryMaxDelay     = 30 * time.Second
	retryJitter       = 0.5
)

// NonDNSUDPModeFollower reads the non-DNS UDP mode flag for a pool's slots
// and keeps the handed-out ones in the mode it serves: every chain build
// consults the flag, and a change of the flag re-applies it to every slot a
// tenant holds, evaluated with that tenant's flag contexts, so a flip reaches
// running sandboxes instead of waiting for their release. A slot whose
// re-apply failed stays eligible: it is retried with backoff until a retry
// succeeds or the slot goes back to the pool. Without a flag client every
// slot builds in off mode and nothing follows.
type NonDNSUDPModeFollower struct {
	flags *featureflags.Client

	mu        sync.Mutex
	handedOut map[*Slot]struct{}
	// failed holds the handed-out slots whose last re-apply failed and
	// backoff the waits before their retries; failedRetries counts the
	// retries in a row that left a slot failing, up to the one whose wait
	// reaches the cap, and retryCapped records the one warning for it. A flag
	// change starts the schedule over and clears the warning.
	failed        map[*Slot]struct{}
	backoff       retry.Backoff
	failedRetries int
	retryCapped   bool
}

func NewNonDNSUDPModeFollower(flags *featureflags.Client) *NonDNSUDPModeFollower {
	return &NonDNSUDPModeFollower{
		flags:     flags,
		handedOut: make(map[*Slot]struct{}),
		failed:    make(map[*Slot]struct{}),
		backoff:   retry.Backoff{Initial: retryInitialDelay, Max: retryMaxDelay, Multiplier: 2, Jitter: retryJitter},
	}
}

// nominalRetryWait is the wait the schedule names before its jitter once
// failedRetries retries in a row left a slot failing: the initial wait
// doubled per retry, up to the cap. The count stops at the cap, so the
// doubling cannot overflow.
func nominalRetryWait(failedRetries int) time.Duration {
	return min(retryInitialDelay<<failedRetries, retryMaxDelay)
}

// Provider is what a slot consults at every chain build; nil without a flag
// client.
func (f *NonDNSUDPModeFollower) Provider() NonDNSUDPModeProvider {
	if f.flags == nil {
		return nil
	}

	return func(ctx context.Context) string {
		return f.flags.StringFlag(ctx, featureflags.SandboxFirewallNonDNSUDPFlag)
	}
}

// HandOut records a slot taken for a tenant, followed until TakeBack.
func (f *NonDNSUDPModeFollower) HandOut(slot *Slot) {
	slot.setHandedOut(true)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.handedOut[slot] = struct{}{}
}

// TakeBack forgets a slot on its way back to the pool, a pending retry
// included; its release applies the mode the next tenant needs.
func (f *NonDNSUDPModeFollower) TakeBack(slot *Slot) {
	slot.setHandedOut(false)

	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.handedOut, slot)
	delete(f.failed, slot)
}

func (f *NonDNSUDPModeFollower) snapshot() []*Slot {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slotsOf(f.handedOut)
}

func slotsOf(set map[*Slot]struct{}) []*Slot {
	slots := make([]*Slot, 0, len(set))
	for slot := range set {
		slots = append(slots, slot)
	}

	return slots
}

// Start runs Follow in the background; the returned function stops it and
// waits until it has finished.
func (f *NonDNSUDPModeFollower) Start(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { f.Follow(ctx) })

	return func() {
		cancel()
		wg.Wait()
	}
}

// Follow re-applies the mode to the handed-out slots when it starts and on
// every change of the flag, and retries the slots whose re-apply failed,
// until ctx ends.
func (f *NonDNSUDPModeFollower) Follow(ctx context.Context) {
	if f.flags == nil {
		return
	}

	changes, stop := f.flags.WatchFlag(featureflags.SandboxFirewallNonDNSUDPFlag.Key())
	defer func() {
		stop()
		for range changes { //nolint:revive // intentional drain
		}
	}()

	var (
		retryTimer *time.Timer
		retry      <-chan time.Time
	)
	defer func() {
		if retryTimer != nil {
			retryTimer.Stop()
		}
	}()

	// A change made before the watch began sends no event, so the current
	// mode is applied once up front.
	f.Reapply(ctx)
	for {
		if retryTimer != nil {
			retryTimer.Stop()
		}
		retryTimer, retry = nil, nil
		if delay, pending := f.nextRetry(); pending {
			retryTimer = time.NewTimer(delay)
			retry = retryTimer.C
		}

		select {
		case <-ctx.Done():
			return
		case _, ok := <-changes:
			if !ok {
				return
			}

			f.Reapply(ctx)
		case <-retry:
			f.retryFailed(ctx)
		}
	}
}

// nextRetry is the wait before the next retry, and whether a slot awaits
// one; asking advances the schedule.
func (f *NonDNSUDPModeFollower) nextRetry() (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.failed) == 0 {
		return 0, false
	}

	return f.backoff.Next(), true
}

// walkResult is what a walk over slots found: how many it reached before
// its context ended, how many it rebuilt, and the slots whose re-apply
// failed, with one error per failed slot in the same order.
type walkResult struct {
	examined int
	rebuilt  int
	failed   []*Slot
	errs     []error
}

// walk re-applies the mode to the slots, a few at a time, each under its
// slot's lock, and dispatches no more once ctx ends. observe, when set, is
// told every slot's outcome as it lands.
func walk(ctx context.Context, slots []*Slot, observe func(slot *Slot, rebuilt bool, err error)) walkResult {
	var (
		result   walkResult
		resultMu sync.Mutex
		group    errgroup.Group
	)
	group.SetLimit(reapplyWorkers)
	for _, slot := range slots {
		if ctx.Err() != nil {
			break
		}

		result.examined++
		group.Go(func() error {
			rebuilt, err := slot.reapplyNonDNSUDPMode(ctx)
			if observe != nil {
				observe(slot, rebuilt, err)
			}

			resultMu.Lock()
			defer resultMu.Unlock()
			switch {
			case err != nil:
				result.failed = append(result.failed, slot)
				result.errs = append(result.errs, fmt.Errorf("slot %d: %w", slot.Idx, err))
			case rebuilt:
				result.rebuilt++
			}

			return nil
		})
	}
	_ = group.Wait()

	return result
}

// keepFailedLocked replaces the retry set with the slots of failed that are
// still handed out. The caller holds f.mu.
func (f *NonDNSUDPModeFollower) keepFailedLocked(failed []*Slot) {
	clear(f.failed)
	for _, slot := range failed {
		if _, handedOut := f.handedOut[slot]; handedOut {
			f.failed[slot] = struct{}{}
		}
	}
}

// Reapply evaluates the mode for every handed-out slot with its tenant's flag
// contexts and rebuilds the chains whose mode differs, a few at a time, each
// under its slot's lock. One line reports the walk: the slots it examined,
// which is fewer than the handed-out ones when ctx ends mid-walk, those it
// rebuilt and those that failed; at debug level when no slot is handed out.
// The failed slots are kept for a retry, with the backoff started over.
func (f *NonDNSUDPModeFollower) Reapply(ctx context.Context) {
	slots := f.snapshot()
	result := walk(ctx, slots, nil)

	f.mu.Lock()
	f.keepFailedLocked(result.failed)
	f.backoff.Reset()
	f.failedRetries = 0
	f.retryCapped = false
	f.mu.Unlock()

	fields := []zap.Field{
		zap.Int("examined", result.examined),
		zap.Int("rebuilt", result.rebuilt),
		zap.Int("failed", len(result.failed)),
	}
	switch {
	case len(result.failed) > 0:
		logger.L().Error(ctx, "re-applied the non-DNS UDP mode to the handed-out slots; some chains were not rebuilt and will be retried",
			append(fields, zap.Error(errors.Join(result.errs...)))...)
	case len(slots) == 0:
		logger.L().Debug(ctx, reappliedMessage, fields...)
	default:
		logger.L().Info(ctx, reappliedMessage, fields...)
	}
}

const reappliedMessage = "re-applied the non-DNS UDP mode to the handed-out slots"

// retryFailed re-applies the mode to the slots whose last re-apply failed,
// each evaluated afresh against the flag and its installed chain. A slot
// that succeeds, needs nothing, or was taken back is forgotten; the rest wait
// for the next retry, twice as long, up to the cap.
func (f *NonDNSUDPModeFollower) retryFailed(ctx context.Context) {
	f.mu.Lock()
	slots := slotsOf(f.failed)
	f.mu.Unlock()

	result := walk(ctx, slots, func(slot *Slot, rebuilt bool, err error) {
		outcome := "nothing to do"
		switch {
		case err != nil:
			outcome = "failed"
		case rebuilt:
			outcome = "rebuilt"
		}
		logger.L().Debug(ctx, "retried the non-DNS UDP mode on a slot whose last update failed",
			zap.Int("slot_index", slot.Idx), zap.String("outcome", outcome), zap.Error(err))
	})

	f.mu.Lock()
	defer f.mu.Unlock()

	f.keepFailedLocked(result.failed)
	if len(f.failed) == 0 || f.retryCapped {
		return
	}

	f.failedRetries++
	if nominalRetryWait(f.failedRetries) < retryMaxDelay {
		return
	}

	f.retryCapped = true
	logger.L().Warn(ctx, "the non-DNS UDP mode retries reached their longest wait; some slots still run a chain in another mode",
		zap.Int("failed", len(f.failed)), zap.Duration("retry_delay", retryMaxDelay), zap.Error(errors.Join(result.errs...)))
}
