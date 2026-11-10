//go:build linux

package v2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network/v2")
	meter  = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network/v2")

	newSlotsAvailableCounter = utils.Must(meter.Int64UpDownCounter("orchestrator.network.v2.slots_pool.new",
		metric.WithDescription("Number of new v2 network slots ready to be used."),
		metric.WithUnit("{slot}"),
	))
	reusableSlotsAvailableCounter = utils.Must(meter.Int64UpDownCounter("orchestrator.network.v2.slots_pool.reused",
		metric.WithDescription("Number of reused v2 network slots ready to be used."),
		metric.WithUnit("{slot}"),
	))
	acquiredSlots = utils.Must(meter.Int64Counter("orchestrator.network.v2.slots_pool.acquired",
		metric.WithDescription("Number of v2 network slots acquired."),
		metric.WithUnit("{slot}"),
	))
	returnedSlotCounter = utils.Must(meter.Int64Counter("orchestrator.network.v2.slots_pool.returned",
		metric.WithDescription("Number of v2 network slots returned."),
		metric.WithUnit("{slot}"),
	))
	releasedSlotCounter = utils.Must(meter.Int64Counter("orchestrator.network.v2.slots_pool.released",
		metric.WithDescription("Number of v2 network slots released."),
		metric.WithUnit("{slot}"),
	))
)

const defaultPopulateRetryDelay = 100 * time.Millisecond

type slotCreationStage string

const (
	slotCreationStageAcquire slotCreationStage = "acquire"
	slotCreationStageCreate  slotCreationStage = "create"
)

type slotCreationError struct {
	stage slotCreationStage
	err   error
}

func (e *slotCreationError) Error() string { return e.err.Error() }
func (e *slotCreationError) Unwrap() error { return e.err }

type V2PoolOption func(*V2Pool)

func WithPoolMetrics(metrics *Metrics) V2PoolOption {
	return func(pool *V2Pool) { pool.metrics = metrics }
}

func WithPopulateRetryDelay(delay time.Duration) V2PoolOption {
	return func(pool *V2Pool) { pool.populateRetryDelay = delay }
}

// WithNonDNSUDPFlag has every slot build its chain in the mode the flag
// serves and the pool follow the flag for the slots it has handed out.
// Without it every slot builds in off mode.
func WithNonDNSUDPFlag(flags *featureflags.Client) V2PoolOption {
	return func(pool *V2Pool) { pool.modeFollower = network.NewNonDNSUDPModeFollower(flags) }
}

// V2Pool implements network.PoolInterface using the v2 nftables datapath.
type V2Pool struct {
	config      network.Config
	storage     network.Storage
	hostFw      *HostFirewall
	observer    *VethObserver
	registry    *SlotV2Registry
	newSlots    chan *network.Slot
	reusedSlots chan *network.Slot
	done        chan struct{}
	doneOnce    sync.Once

	closeMu sync.RWMutex
	closed  bool

	// admissionsWG tracks Get calls admitted before Close published closed. It
	// covers slot selection, the final handoff, configuration, and initiation of
	// a configuration-failure return. If that return runs asynchronously,
	// returnsWG owns its cleanup; Close joins it after admissionsWG.
	admissionsWG sync.WaitGroup

	// returnsWG tracks in-flight asynchronous slot returns; Close waits for
	// it before draining the pool.
	returnsWG sync.WaitGroup
	// First unresolved failures only, guarded by closeMu; returns report every error.
	releaseErr    error
	cleanupErr    error
	retainedSlots map[*network.Slot]struct{}

	metrics            *Metrics
	populateRetryDelay time.Duration
	modeFollower       *network.NonDNSUDPModeFollower
}

// Compile-time check that V2Pool satisfies network.PoolInterface.
var _ network.PoolInterface = (*V2Pool)(nil)

// ValidateV2Prerequisites checks that required kernel parameters are set for v2 networking.
// Returns an error if any prerequisite is missing. Call before NewV2Pool.
func ValidateV2Prerequisites() error {
	checks := []struct {
		path     string
		expected string
		desc     string
	}{
		{"/proc/sys/net/ipv4/ip_forward", "1", "IPv4 forwarding"},
	}

	var errs []error
	for _, c := range checks {
		val, err := os.ReadFile(c.path)
		if err != nil {
			errs = append(errs, fmt.Errorf("cannot read %s: %w", c.path, err))

			continue
		}
		if strings.TrimSpace(string(val)) != c.expected {
			errs = append(errs, fmt.Errorf("%s: %s must be %s, got %q", c.desc, c.path, c.expected, strings.TrimSpace(string(val))))
		}
	}

	if _, err := network.IptablesHandle(); err != nil {
		errs = append(errs, fmt.Errorf("iptables (needed for the per-slot FORWARD accepts): %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("v2 networking prerequisites not met: %w", errors.Join(errs...))
	}

	return nil
}

func NewV2Pool(storage network.Storage, config network.Config, hostFw *HostFirewall, observer *VethObserver, options ...V2PoolOption) *V2Pool {
	pool := &V2Pool{
		config:             config,
		storage:            storage,
		hostFw:             hostFw,
		observer:           observer,
		registry:           NewSlotV2Registry(),
		newSlots:           make(chan *network.Slot, network.NewSlotsPoolSize-1),
		reusedSlots:        make(chan *network.Slot, network.ReusedSlotsPoolSize),
		done:               make(chan struct{}),
		populateRetryDelay: defaultPopulateRetryDelay,
		modeFollower:       network.NewNonDNSUDPModeFollower(nil),
	}
	for _, option := range options {
		option(pool)
	}
	if pool.metrics == nil && hostFw != nil {
		pool.metrics = hostFw.metrics
	}

	return pool
}

func (p *V2Pool) createNetworkSlot(ctx context.Context) (*network.Slot, error) {
	slot, err := p.storage.Acquire(ctx)
	if err != nil {
		return nil, &slotCreationError{stage: slotCreationStageAcquire, err: fmt.Errorf("failed to acquire network slot: %w", err)}
	}

	slot.UseNonDNSUDPMode(p.modeFollower.Provider())
	slotV2 := NewSlotV2(slot)
	p.registry.Store(slotV2)

	if err := CreateNetworkV2(ctx, slot, slotV2, p.hostFw, p.observer); err != nil {
		p.registry.Delete(slot.Idx)
		releaseErr := p.storage.Release(slot)

		return nil, &slotCreationError{stage: slotCreationStageCreate, err: fmt.Errorf("failed to create v2 network: %w", errors.Join(err, releaseErr))}
	}

	return slot, nil
}

func (p *V2Pool) Populate(ctx context.Context) {
	defer close(p.newSlots)

	stopFollowing := p.modeFollower.Start(ctx)
	defer stopFollowing()

	for {
		select {
		case <-p.done:
			return
		case <-ctx.Done():
			return
		default:
			slot, err := p.createNetworkSlot(ctx)
			if err != nil {
				select {
				case <-p.done:
					return
				case <-ctx.Done():
					return
				default:
				}

				var creationErr *slotCreationError
				if errors.As(err, &creationErr) && p.metrics != nil {
					p.metrics.recordCreationFailure(ctx, creationErr.stage)
				}
				logger.L().Error(ctx, "[v2 network slot pool]: failed to create network", zap.Error(err))

				timer := time.NewTimer(p.populateRetryDelay)
				select {
				case <-p.done:
					if !timer.Stop() {
						<-timer.C
					}

					return
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}

					return
				case <-timer.C:
					continue
				}
			}

			select {
			case p.newSlots <- slot:
				newSlotsAvailableCounter.Add(ctx, 1)

				continue
			case <-p.done:
			case <-ctx.Done():
			}

			if err := p.cleanup(context.WithoutCancel(ctx), slot); err != nil {
				logger.L().Error(ctx, "[v2 network slot pool]: failed to cleanup created slot while closing", zap.Error(err), zap.Int("slot_index", slot.Idx))
			}

			return
		}
	}
}

func (p *V2Pool) Get(ctx context.Context, netConfig *orchestrator.SandboxNetworkConfig, class sandboxtypes.EgressClass) (*network.Slot, error) {
	if !p.tryTrackAdmission() {
		return nil, network.ErrClosed
	}
	defer p.admissionsWG.Done()

	slot, err := p.take(ctx)
	if err != nil {
		return nil, err
	}

	p.modeFollower.HandOut(slot)
	if err := p.configureSlot(ctx, slot, netConfig, class); err != nil {
		// Never handed out, so nobody listens for the release notification.
		if rerr := p.ReturnAsync(context.WithoutCancel(ctx), slot, func(context.Context, string) error { return nil }, 0); rerr != nil {
			logger.L().Error(ctx, "failed to return v2 slot to pool", zap.Error(rerr), zap.Int("slot_index", slot.Idx))
		}

		return nil, err
	}

	return slot, nil
}

// take prefers a reused slot and otherwise waits for whichever queue delivers
// first.
func (p *V2Pool) take(ctx context.Context) (*network.Slot, error) {
	var (
		slot   *network.Slot
		reused bool
	)

	select {
	case <-p.done:
		return nil, network.ErrClosed
	case s := <-p.reusedSlots:
		reusableSlotsAvailableCounter.Add(ctx, -1)
		slot = s
		reused = true
	default:
		select {
		case <-p.done:
			return nil, network.ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		case s := <-p.reusedSlots:
			// ReturnAsync may publish a reusable slot after the fast-path miss.
			reusableSlotsAvailableCounter.Add(ctx, -1)
			slot = s
			reused = true
		case s, ok := <-p.newSlots:
			// Populate closes the channel on its way out, and a closed channel
			// wins this select as readily as done does.
			if !ok {
				return nil, network.ErrClosed
			}

			newSlotsAvailableCounter.Add(ctx, -1)
			slot = s
		}
	}

	if err := p.claimSlot(ctx, slot); err != nil {
		return nil, err
	}

	poolName := "new"
	event := "new v2 network slot"
	if reused {
		poolName = "reused"
		event = "reused v2 network slot"
	}
	acquiredSlots.Add(ctx, 1, metric.WithAttributes(attribute.String("pool", poolName)))
	telemetry.ReportEvent(ctx, event)

	return slot, nil
}

func (p *V2Pool) configureSlot(ctx context.Context, slot *network.Slot, netConfig *orchestrator.SandboxNetworkConfig, class sandboxtypes.EgressClass) error {
	// Slots are created before their tenant is known, so a build re-stamps the
	// rule CreateNetworkV2 seeded. No-op when both classes resolve alike.
	if err := ApplyEgressDSCP(slot, p.config.EgressDSCP(class)); err != nil {
		return fmt.Errorf("error setting v2 slot egress DSCP for %s: %w", class, err)
	}

	if err := slot.ConfigureInternet(ctx, netConfig); err != nil {
		return fmt.Errorf("error setting v2 slot internet access: %w", err)
	}

	return nil
}

// tryTrackAdmission registers a complete Get, including configuration and
// initiation of its failure cleanup, before Close can publish closed and wait.
// It reports false for every later caller.
func (p *V2Pool) tryTrackAdmission() bool {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	if p.closed {
		return false
	}

	p.admissionsWG.Add(1)

	return true
}

// claimSlot is the final handoff linearization point. If Close published
// closed first, the admission still owns the received slot and must clean it
// up before returning ErrClosed; Close joins that work through admissionsWG.
func (p *V2Pool) claimSlot(ctx context.Context, slot *network.Slot) error {
	p.closeMu.RLock()
	closed := p.closed
	p.closeMu.RUnlock()

	if !closed {
		return nil
	}

	return p.cleanupWith(context.WithoutCancel(ctx), slot, network.ErrClosed)
}

func (p *V2Pool) tryTrackReturn() bool {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	if p.closed {
		return false
	}

	p.returnsWG.Add(1)

	return true
}

// ReturnAsync recycles a slot in the background, logging errors instead of
// returning them. Close waits for all in-flight returns before draining the
// pool. If the pool is already closed the slot is cleaned up synchronously.
func (p *V2Pool) ReturnAsync(ctx context.Context, slot *network.Slot, releasedFn network.ReleaseNotify, returnDelay time.Duration) error {
	p.modeFollower.TakeBack(slot)

	if !p.tryTrackReturn() {
		return p.returnSlot(ctx, slot, releasedFn, returnDelay)
	}

	go func() {
		defer p.returnsWG.Done()

		err := p.returnSlot(ctx, slot, releasedFn, returnDelay)
		switch {
		case err == nil:
		case errors.Is(err, network.ErrSlotRetained):
			logger.L().Error(ctx, "v2 network slot retained and unavailable for reuse", zap.Error(err), zap.Int("slot_index", slot.Idx))
		case errors.Is(err, network.ErrClosed), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			logger.L().Warn(ctx, "v2 network slot returned during pool shutdown", zap.Error(err), zap.Int("slot_index", slot.Idx))
		default:
			logger.L().Error(ctx, "failed to return v2 network slot to pool", zap.Error(err), zap.Int("slot_index", slot.Idx))
		}
	}()

	return nil
}

// returnSlot recycles a slot that was used by a sandbox. It waits returnDelay
// before making the slot reusable to let inflight requests on the previous
// sandbox drain. Notifications must finish before teardown and succeed before reuse.
func (p *V2Pool) returnSlot(ctx context.Context, slot *network.Slot, releasedFn network.ReleaseNotify, returnDelay time.Duration) (retErr error) {
	started := time.Now()
	cleanupStarted := started
	defer func() { network.RecordSlotReturnDuration(ctx, started, cleanupStarted, 2, retErr) }()

	return telemetry.Observe0(ctx, tracer, "clean network-slot", func(ctx context.Context) error {
		// If the pool is closed or the context is cancelled during the delay we
		// still fall through and clean up the slot to avoid leaking it.
		var cause error
		select {
		case <-ctx.Done():
			cause = ctx.Err()
		case <-p.done:
			cause = network.ErrClosed
		case <-time.After(returnDelay):
		}

		cleanupStarted = time.Now()

		// Every path notifies before the slot can be torn down or reused.
		if err := releasedFn(context.WithoutCancel(ctx), slot.HostIPString()); err != nil {
			p.closeMu.Lock()
			closed := p.closed
			var releaseErr error
			if closed {
				releaseErr = errors.Join(network.ErrClosed, cause, err)
			} else {
				releaseErr = errors.Join(cause, fmt.Errorf("%w: v2 slot '%d': %w", network.ErrSlotRetained, slot.Idx, err))
			}
			if p.releaseErr == nil {
				p.releaseErr = releaseErr
			}
			if !closed {
				if p.retainedSlots == nil {
					p.retainedSlots = make(map[*network.Slot]struct{})
				}
				p.retainedSlots[slot] = struct{}{}
			}
			p.closeMu.Unlock()
			if closed {
				return p.cleanupWith(ctx, slot, releaseErr)
			}
			telemetry.SetAttributes(ctx, attribute.Bool("network.slot.retained", true))

			return releaseErr
		}
		if cause != nil {
			return p.cleanupWith(ctx, slot, cause)
		}

		return p.recycle(ctx, slot)
	}, trace.WithAttributes(attribute.Int("network_version", 2)))
}

func (p *V2Pool) recycle(ctx context.Context, slot *network.Slot) error {
	// Undo any build-specific DSCP before the next tenant can inherit it.
	if err := ApplyEgressDSCP(slot, p.config.EgressDSCP(sandboxtypes.EgressClassSandbox)); err != nil {
		return p.cleanupWith(ctx, slot, fmt.Errorf("error resetting v2 slot egress DSCP: %w", err))
	}

	if err := slot.ResetInternet(ctx); err != nil {
		return p.cleanupWith(ctx, slot, fmt.Errorf("error resetting v2 slot internet access: %w", err))
	}

	reused, cause := p.tryReuse(ctx, slot)
	if reused {
		return nil
	}

	// cause is nil when the pool is simply full.
	return p.cleanupWith(ctx, slot, cause)
}

// tryReuse attempts to push the slot into the reused pool. The RLock pairs
// with Close's Lock so a send can never race the drain.
func (p *V2Pool) tryReuse(ctx context.Context, slot *network.Slot) (reused bool, cause error) {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	if p.closed {
		return false, network.ErrClosed
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-p.done:
		return false, network.ErrClosed
	case p.reusedSlots <- slot:
		returnedSlotCounter.Add(ctx, 1)
		reusableSlotsAvailableCounter.Add(ctx, 1)

		return true, nil
	default:
		return false, nil
	}
}

// cleanupWith tears the slot down and attaches any cleanup error to cause.
func (p *V2Pool) cleanupWith(ctx context.Context, slot *network.Slot, cause error) error {
	if cerr := p.cleanup(ctx, slot); cerr != nil {
		cleanupErr := fmt.Errorf("cleanup v2 slot '%d': %w", slot.Idx, cerr)
		p.closeMu.Lock()
		if p.cleanupErr == nil {
			p.cleanupErr = cleanupErr
		}
		p.closeMu.Unlock()

		return errors.Join(cause, cleanupErr)
	}

	return cause
}

func (p *V2Pool) cleanup(ctx context.Context, slot *network.Slot) error {
	var errs []error

	slotV2, ok := p.registry.Load(slot.Idx)
	if ok {
		if err := RemoveNetworkV2(ctx, slot, slotV2, p.hostFw, p.observer); err != nil {
			errs = append(errs, fmt.Errorf("cannot remove v2 network for slot '%d': %w", slot.Idx, err))
		}
		p.registry.Delete(slot.Idx)
	} else {
		// Fallback: try basic cleanup even without v2 metadata
		if err := slot.RemoveNetwork(ctx); err != nil {
			errs = append(errs, fmt.Errorf("cannot remove network for slot '%d': %w", slot.Idx, err))
		}
	}

	if err := p.storage.Release(slot); err != nil {
		errs = append(errs, fmt.Errorf("failed to release slot '%d': %w", slot.Idx, err))
	}

	releasedSlotCounter.Add(ctx, 1)

	return errors.Join(errs...)
}

// Close leaves the pool unchanged when ctx is canceled before Close publishes
// closed. Once it has published closed, Close ignores later cancellation so it
// can join admissions and returns before removing shared network state.
func (p *V2Pool) Close(ctx context.Context) error {
	p.closeMu.Lock()
	if err := ctx.Err(); err != nil {
		p.closeMu.Unlock()

		return err
	}
	p.closed = true
	p.doneOnce.Do(func() {
		close(p.done)
	})
	p.closeMu.Unlock()

	ctx = context.WithoutCancel(ctx)
	logger.L().Info(ctx, "Closing v2 network pool")

	// Admissions register while open under closeMu. A successful claim may
	// still be configuring its caller-owned slot after Close publishes closed.
	// Join the complete Get, including rejected-claim cleanup or initiation of
	// a configuration-failure return; returnsWG below joins asynchronous returns.
	p.admissionsWG.Wait()

	p.returnsWG.Wait()

	p.closeMu.Lock()
	errs := []error{p.releaseErr, p.cleanupErr}
	retained := p.retainedSlots
	p.retainedSlots = nil
	p.closeMu.Unlock()
	for slot := range retained {
		if err := p.cleanup(ctx, slot); err != nil {
			errs = append(errs, fmt.Errorf("failed to cleanup retained v2 slot '%d': %w", slot.Idx, err))
		}
	}

	for slot := range p.newSlots {
		newSlotsAvailableCounter.Add(ctx, -1)

		if err := p.cleanup(ctx, slot); err != nil {
			errs = append(errs, fmt.Errorf("failed to cleanup v2 slot '%d': %w", slot.Idx, err))
		}
	}

drain:
	for {
		select {
		case slot := <-p.reusedSlots:
			reusableSlotsAvailableCounter.Add(ctx, -1)

			if err := p.cleanup(ctx, slot); err != nil {
				errs = append(errs, fmt.Errorf("failed to cleanup v2 slot '%d': %w", slot.Idx, err))
			}
		default:
			break drain
		}
	}

	if p.hostFw != nil {
		if err := p.hostFw.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close host firewall: %w", err))
		}
	}

	if err := p.observer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close observer: %w", err))
	}

	return errors.Join(errs...)
}
