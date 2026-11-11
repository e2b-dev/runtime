//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	NewSlotsPoolSize    = 32
	ReusedSlotsPoolSize = 100

	// ReturnDelay is how long we wait before returning a slot to the reused pool,
	// to let inflight requests on the previous sandbox drain and reduce reuse churn.
	ReturnDelay = 3 * time.Second
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network")

	slotReturnDuration = utils.Must(telemetry.GetHistogram(meter, telemetry.NetworkSlotReturnDurationName))

	newSlotsAvailableCounter = utils.Must(meter.Int64UpDownCounter("orchestrator.network.slots_pool.new",
		metric.WithDescription("Number of new network slots ready to be used."),
		metric.WithUnit("{slot"),
	))
	reusableSlotsAvailableCounter = utils.Must(meter.Int64UpDownCounter("orchestrator.network.slots_pool.reused",
		metric.WithDescription("Number of reused network slots ready to be used."),
		metric.WithUnit("{slot}"),
	))
	acquiredSlots = utils.Must(meter.Int64Counter("orchestrator.network.slots_pool.acquired",
		metric.WithDescription("Number of network slots acquired."),
		metric.WithUnit("{slot}"),
	))
	returnedSlotCounter = utils.Must(meter.Int64Counter("orchestrator.network.slots_pool.returned",
		metric.WithDescription("Number of network slots returned."),
		metric.WithUnit("{slot}"),
	))
	releasedSlotCounter = utils.Must(meter.Int64Counter("orchestrator.network.slots_pool.released",
		metric.WithDescription("Number of network slots released."),
		metric.WithUnit("{slot}"),
	))
)

// ReleaseNotify tells the slot's users that its sandbox released it. An error
// means a user still holds the slot, so it must not be reused.
type ReleaseNotify func(ctx context.Context, ip string) error

type Config struct {
	// Using reserver IPv4 in range that is used for experiments and documentation
	// https://en.wikipedia.org/wiki/Reserved_IP_addresses
	OrchestratorInSandboxIPAddress string `env:"SANDBOX_ORCHESTRATOR_IP" envDefault:"192.0.2.1"`

	HyperloopProxyPort uint16 `env:"SANDBOX_HYPERLOOP_PROXY_PORT" envDefault:"5010"`
	NFSProxyPort       uint16 `env:"SANDBOX_NFS_PROXY_PORT"       envDefault:"5011"`
	PortmapperPort     uint16 `env:"SANDBOX_PORTMAPPER_PORT"      envDefault:"5012"`

	// Comma-separated CIDRs to allow through the predefined firewall deny list.
	// These are allowed before the private-range deny rules, so they can
	// reach hosts in the 10.0.0.0/8, 172.16.0.0/12, etc. blocks.
	// The exemption applies to every sandbox on the node. A wide prefix such as
	// 0.0.0.0/0, a link-local prefix or 100.64.0.0/10 also opens cloud metadata
	// endpoints such as 169.254.169.254.
	AllowSandboxInternalCIDRs []string `env:"ALLOW_SANDBOX_INTERNAL_CIDRS" envDefault:"" envSeparator:","`

	// TCP firewall ports - separate ports for different traffic types to avoid
	// protocol detection blocking on server-first protocols like SSH.
	// - HTTP port: for traffic destined to port 80 (HTTP Host header inspection)
	// - TLS port: for traffic destined to port 443 (TLS SNI inspection)
	// - Other port: for all other traffic (CIDR-only check, no protocol inspection)
	SandboxTCPFirewallHTTPPort  uint16 `env:"SANDBOX_TCP_FIREWALL_HTTP_PORT"  envDefault:"5016"`
	SandboxTCPFirewallTLSPort   uint16 `env:"SANDBOX_TCP_FIREWALL_TLS_PORT"   envDefault:"5017"`
	SandboxTCPFirewallOtherPort uint16 `env:"SANDBOX_TCP_FIREWALL_OTHER_PORT" envDefault:"5018"`

	// 0 disables; valid range 0..63 (DSCP is 6 bits). CS1=8 is the canonical Scavenger class (RFC 3662).
	SandboxEgressDSCP uint8 `env:"SANDBOX_EGRESS_DSCP" envDefault:"0"`

	// Egress DSCP for template builds. Nil (not 0) inherits SANDBOX_EGRESS_DSCP;
	// an explicit 0 disables marking for builds only. Range 0..63.
	// Set-but-empty behaves as unset (inherits): the env parser skips it.
	BuildSandboxEgressDSCP *uint8 `env:"BUILD_SANDBOX_EGRESS_DSCP"`

	// NetworkVersion selects v1 (iptables per-sandbox) or v2 (nftables, host sets).
	NetworkVersion int `env:"NETWORK_VERSION" envDefault:"1"`

	// VethPrefix is the prefix used for host-side sandbox veth interfaces (e.g. "e2b-").
	VethPrefix string `env:"NETWORK_VETH_PREFIX" envDefault:"e2b-"`

	// ReusedSlotsPoolSize is how many returned slots the pool keeps for the
	// next sandbox; unset or 0 keeps the default. A slot returned to a full
	// pool is torn down and the pool makes new slots one at a time, so a node
	// that recycles more sandboxes at once than this slows its next burst.
	// Slots still pooled when the node stops are torn down one at a time. A
	// pool raised above the default stops at the shutdown's teardown deadline:
	// the stop then exits non-zero and the next start reclaims the rest before
	// it serves. With that reclaim disabled it keeps going. Slots still on
	// their way back to the pool are not bounded.
	ReusedSlotsPoolSize int `env:"NETWORK_REUSED_SLOTS_POOL_SIZE"`
}

// ReusedPoolSize is the reuse pool's capacity: the configured size, or the
// default when none is set.
func (c Config) ReusedPoolSize() int {
	if c.ReusedSlotsPoolSize > 0 {
		return c.ReusedSlotsPoolSize
	}

	return ReusedSlotsPoolSize
}

const (
	maxDSCP = 63 // DSCP is the top 6 bits of the IPv4 TOS / IPv6 traffic-class byte.

	// DefaultVethPrefix is the default host-side veth device prefix.
	DefaultVethPrefix = "e2b-"
	// LegacyVethPrefix is the legacy host-side veth device prefix.
	LegacyVethPrefix = "veth-"

	maxVethPrefixLen = 10 // max IFNAMSIZ-1 (15) minus max 5 digits for slot index (vrtSlotsSize is 32766)
)

func isValidVethPrefixChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '.' || r == '-'
}

// ValidateVethPrefix verifies that prefix is non-empty, contains only valid
// interface name characters ([A-Za-z0-9_.-]), has length <= 10 so that
// prefix + 5-digit index fits within Linux IFNAMSIZ-1 (15 bytes), and ends
// with a non-alphanumeric separator ('-', '_', or '.') so a slot device name
// <prefix><digits> cannot be a prefix-match for other host devices that
// continue with letters.
func ValidateVethPrefix(prefix string) error {
	if prefix == "" {
		return errors.New("NETWORK_VETH_PREFIX must not be empty")
	}
	if len(prefix) > maxVethPrefixLen {
		return fmt.Errorf("NETWORK_VETH_PREFIX=%q too long (%d chars, max %d)", prefix, len(prefix), maxVethPrefixLen)
	}
	for _, r := range prefix {
		if !isValidVethPrefixChar(r) {
			return fmt.Errorf("NETWORK_VETH_PREFIX=%q contains invalid character %q (allowed: [A-Za-z0-9_.-])", prefix, r)
		}
	}
	last := prefix[len(prefix)-1]
	if last != '-' && last != '_' && last != '.' {
		return fmt.Errorf("NETWORK_VETH_PREFIX=%q must end with a separator ('-', '_', or '.')", prefix)
	}

	return nil
}

// EgressDSCP returns the DSCP class to stamp on egress for the given kind of
// sandbox. 0 means "leave the field alone".
func (c Config) EgressDSCP(class sandboxtypes.EgressClass) uint8 {
	if class == sandboxtypes.EgressClassBuild && c.BuildSandboxEgressDSCP != nil {
		return *c.BuildSandboxEgressDSCP
	}

	return c.SandboxEgressDSCP
}

// EgressTOS is the per-class IPv4 TOS / IPv6 traffic-class byte (DSCP in the
// top 6 bits; 0 = leave the field alone): Config resolved to the form the
// connection proxies consume.
type EgressTOS struct {
	Sandbox int
	Build   int
}

// For picks the byte for the class.
func (e EgressTOS) For(class sandboxtypes.EgressClass) int {
	if class == sandboxtypes.EgressClassBuild {
		return e.Build
	}

	return e.Sandbox
}

// EgressTOS resolves both configured DSCP classes into TOS bytes.
func (c Config) EgressTOS() EgressTOS {
	return EgressTOS{
		Sandbox: int(c.EgressDSCP(sandboxtypes.EgressClassSandbox)) << 2,
		Build:   int(c.EgressDSCP(sandboxtypes.EgressClassBuild)) << 2,
	}
}

// untenantedDSCP is the class an idle pooled slot carries between tenants:
// CreateNetwork seeds it and recycle restores it — the two must agree.
func (c Config) untenantedDSCP() uint8 {
	return c.EgressDSCP(sandboxtypes.EgressClassSandbox)
}

// DSCP builds the pointer BuildSandboxEgressDSCP takes: DSCP(0) is an
// explicit "disable for builds", distinct from nil (inherit).
func DSCP(v uint8) *uint8 { return new(v) }

func (c Config) Validate() error {
	if c.NetworkVersion != 1 && c.NetworkVersion != 2 {
		return fmt.Errorf("NETWORK_VERSION=%d unsupported (must be 1 or 2)", c.NetworkVersion)
	}

	if c.SandboxEgressDSCP > maxDSCP {
		return fmt.Errorf("SANDBOX_EGRESS_DSCP=%d out of range (0..%d)", c.SandboxEgressDSCP, maxDSCP)
	}

	if c.BuildSandboxEgressDSCP != nil && *c.BuildSandboxEgressDSCP > maxDSCP {
		return fmt.Errorf("BUILD_SANDBOX_EGRESS_DSCP=%d out of range (0..%d)", *c.BuildSandboxEgressDSCP, maxDSCP)
	}

	if c.ReusedSlotsPoolSize < 0 || c.ReusedSlotsPoolSize > vrtSlotsSize {
		return fmt.Errorf("NETWORK_REUSED_SLOTS_POOL_SIZE=%d out of range (0..%d)", c.ReusedSlotsPoolSize, vrtSlotsSize)
	}

	prefix := c.VethPrefix
	if prefix == "" {
		prefix = DefaultVethPrefix
	}

	return ValidateVethPrefix(prefix)
}

func ParseConfig() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

type Pool struct {
	config Config

	done     chan struct{}
	doneOnce sync.Once

	closeMu sync.RWMutex
	closed  bool

	newSlots    chan *Slot
	reusedSlots chan *Slot

	// admissionsWG tracks Get calls admitted before Close published closed. It
	// covers slot selection, the final handoff, configuration, and initiation of
	// a configuration-failure return. If that return runs asynchronously,
	// returnsWG owns its cleanup; Close joins it after admissionsWG.
	admissionsWG sync.WaitGroup

	// returnsWG tracks in-flight asynchronous slot returns; Close waits for
	// it before draining the pool.
	returnsWG sync.WaitGroup
	// First unresolved failures only, guarded by closeMu; returns report every error.
	releaseErr error
	cleanupErr error
	// Retained slots cannot be reused, but Close still owns their teardown.
	retainedSlots map[*Slot]struct{}

	slotStorage Storage

	modeFollower *NonDNSUDPModeFollower
}

type PoolOption func(*Pool)

// WithNonDNSUDPFlag has every slot build its chain in the mode the flag
// serves and the pool follow the flag for the slots it has handed out.
// Without it every slot builds in off mode.
func WithNonDNSUDPFlag(flags *featureflags.Client) PoolOption {
	return func(pool *Pool) { pool.modeFollower = NewNonDNSUDPModeFollower(flags) }
}

var ErrClosed = errors.New("cannot read from a closed pool")

// ErrSlotRetained reports a slot kept allocated because its release
// notification failed. It stays out of reuse and is physically removed by Close.
var ErrSlotRetained = errors.New("network slot retained after failed release")

func NewPool(newSlotsPoolSize, reusedSlotsPoolSize int, slotStorage Storage, config Config, options ...PoolOption) *Pool {
	newSlots := make(chan *Slot, newSlotsPoolSize-1)
	reusedSlots := make(chan *Slot, reusedSlotsPoolSize)

	pool := &Pool{
		config:       config,
		done:         make(chan struct{}),
		newSlots:     newSlots,
		reusedSlots:  reusedSlots,
		slotStorage:  slotStorage,
		modeFollower: NewNonDNSUDPModeFollower(nil),
	}
	for _, option := range options {
		option(pool)
	}

	return pool
}

func (p *Pool) createNetworkSlot(ctx context.Context) (*Slot, error) {
	ips, err := p.slotStorage.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire network slot: %w", err)
	}
	ips.UseNonDNSUDPMode(p.modeFollower.Provider())

	err = ips.CreateNetwork(ctx)
	if err != nil {
		releaseErr := p.slotStorage.Release(ips)
		err = errors.Join(err, releaseErr)

		return nil, fmt.Errorf("failed to create network: %w", err)
	}

	return ips, nil
}

func (p *Pool) Populate(ctx context.Context) {
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
				logger.L().Error(ctx, "[network slot pool]: failed to create network", zap.Error(err))

				continue
			}

			select {
			case p.newSlots <- slot:
				newSlotsAvailableCounter.Add(ctx, 1)

				continue
			case <-p.done:
			case <-ctx.Done():
			}

			if err := p.cleanup(context.WithoutCancel(ctx), slot); err != nil {
				logger.L().Error(ctx, "[network slot pool]: failed to cleanup created slot while closing", zap.Error(err), zap.Int("slot_index", slot.Idx))
			}

			return
		}
	}
}

func (p *Pool) Get(ctx context.Context, network *orchestrator.SandboxNetworkConfig, class sandboxtypes.EgressClass) (*Slot, error) {
	if !p.tryTrackAdmission() {
		return nil, ErrClosed
	}
	defer p.admissionsWG.Done()

	var (
		slot   *Slot
		reused bool
	)

	select {
	case <-p.done:
		return nil, ErrClosed
	case s := <-p.reusedSlots:
		reusableSlotsAvailableCounter.Add(ctx, -1)
		slot = s
		reused = true
	default:
		select {
		case <-p.done:
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		case s, ok := <-p.newSlots:
			if !ok {
				// Populate closes this channel when it exits; a closed channel is
				// not a new slot.
				return nil, ErrClosed
			}

			newSlotsAvailableCounter.Add(ctx, -1)
			slot = s
		case s := <-p.reusedSlots:
			// ReturnAsync may publish a reusable slot after the fast-path miss
			// while this caller waits for new-slot production.
			reusableSlotsAvailableCounter.Add(ctx, -1)
			slot = s
			reused = true
		}
	}

	if err := p.claimSlot(ctx, slot); err != nil {
		return nil, err
	}

	p.modeFollower.HandOut(slot)

	poolName := "new"
	event := "new network slot"
	if reused {
		poolName = "reused"
		event = "reused network slot"
	}
	acquiredSlots.Add(ctx, 1, metric.WithAttributes(attribute.String("pool", poolName)))
	telemetry.ReportEvent(ctx, event)

	if err := p.configureSlot(ctx, slot, network, class); err != nil {
		// Never handed out, so nobody listens for the release notification.
		if rerr := p.ReturnAsync(context.WithoutCancel(ctx), slot, func(context.Context, string) error { return nil }, 0); rerr != nil {
			logger.L().Error(ctx, "failed to return slot to the pool", zap.Error(rerr), zap.Int("slot_index", slot.Idx))
		}

		return nil, err
	}

	return slot, nil
}

func (p *Pool) configureSlot(ctx context.Context, slot *Slot, network *orchestrator.SandboxNetworkConfig, class sandboxtypes.EgressClass) error {
	// Slots are created before their tenant is known, so a build re-stamps the
	// rule CreateNetwork installed. No-op when both classes resolve alike.
	if err := slot.applyEgressDSCP(ctx, p.config.EgressDSCP(class)); err != nil {
		return fmt.Errorf("error setting slot egress DSCP for %s: %w", class, err)
	}

	if err := slot.ConfigureInternet(ctx, network); err != nil {
		return fmt.Errorf("error setting slot internet access: %w", err)
	}

	return nil
}

// returnSlot recycles a slot that was used by a sandbox. It waits returnDelay
// before making the slot reusable to let inflight requests on the previous
// sandbox drain. Notifications must finish before teardown and succeed before reuse.
func (p *Pool) returnSlot(ctx context.Context, slot *Slot, releasedFn ReleaseNotify, returnDelay time.Duration) (retErr error) {
	started := time.Now()
	cleanupStarted := started
	defer func() { RecordSlotReturnDuration(ctx, started, cleanupStarted, 1, retErr) }()

	return telemetry.Observe0(ctx, tracer, "clean network-slot", func(ctx context.Context) error {
		var cause error
		select {
		case <-ctx.Done():
			cause = ctx.Err()
		case <-p.done:
			cause = ErrClosed
		case <-time.After(returnDelay):
		}

		cleanupStarted = time.Now()

		// Every path notifies before the slot can be torn down or reused.
		if err := releasedFn(context.WithoutCancel(ctx), slot.HostIPString()); err != nil {
			p.closeMu.Lock()
			closed := p.closed
			var releaseErr error
			if closed {
				releaseErr = errors.Join(ErrClosed, cause, err)
			} else {
				releaseErr = errors.Join(cause, fmt.Errorf("%w: slot '%d': %w", ErrSlotRetained, slot.Idx, err))
			}
			if p.releaseErr == nil {
				p.releaseErr = releaseErr
			}
			if !closed {
				if p.retainedSlots == nil {
					p.retainedSlots = make(map[*Slot]struct{})
				}
				p.retainedSlots[slot] = struct{}{}
			}
			p.closeMu.Unlock()
			if closed {
				// No reuse is possible after Close. Remove the veth/netns even
				// if a subscriber's independent cleanup is still pending.
				return p.cleanupWith(ctx, slot, releaseErr)
			}
			telemetry.SetAttributes(ctx, attribute.Bool("network.slot.retained", true))

			return releaseErr
		}
		if cause != nil {
			return p.cleanupWith(ctx, slot, cause)
		}

		return p.recycle(ctx, slot)
	}, trace.WithAttributes(attribute.Int("network_version", 1)))
}

// RecordSlotReturnDuration records both the full return attempt and cleanup
// after the reuse delay. Both network pools use the same metric and result labels.
func RecordSlotReturnDuration(ctx context.Context, started, cleanupStarted time.Time, version int, err error) {
	result := "error"
	switch err {
	case nil:
		result = "success"
	case ErrClosed, context.Canceled, context.DeadlineExceeded: //nolint:errorlint // Wrapped or joined errors also represent a failed cleanup operation.
		// Successful cleanup returns the shutdown cause directly. A joined
		// cleanup failure must still be reported as an error.
		result = "shutdown"
	default:
		if errors.Is(err, ErrSlotRetained) {
			result = "retained"
		}
	}
	finished := time.Now()
	attrs := metric.WithAttributes(attribute.Int("network_version", version), attribute.String("result", result))
	slotReturnDuration.Record(ctx, finished.Sub(started).Milliseconds(), attrs, metric.WithAttributes(attribute.String("phase", "total")))
	slotReturnDuration.Record(ctx, finished.Sub(cleanupStarted).Milliseconds(), attrs, metric.WithAttributes(attribute.String("phase", "cleanup")))
}

// tryTrackAdmission registers a complete Get, including configuration and
// initiation of its failure cleanup, before Close can publish closed and wait.
// It reports false for every later caller.
func (p *Pool) tryTrackAdmission() bool {
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
func (p *Pool) claimSlot(ctx context.Context, slot *Slot) error {
	p.closeMu.RLock()
	closed := p.closed
	p.closeMu.RUnlock()

	if !closed {
		return nil
	}

	return p.cleanupWith(context.WithoutCancel(ctx), slot, ErrClosed)
}

// tryTrackReturn registers an in-flight slot return so Close can wait for
// it. It reports false when the pool is already closed and the caller must
// process the slot inline.
func (p *Pool) tryTrackReturn() bool {
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
func (p *Pool) ReturnAsync(ctx context.Context, slot *Slot, releasedFn ReleaseNotify, returnDelay time.Duration) error {
	p.modeFollower.TakeBack(slot)

	if !p.tryTrackReturn() {
		return p.returnSlot(ctx, slot, releasedFn, returnDelay)
	}

	go func() {
		defer p.returnsWG.Done()

		err := p.returnSlot(ctx, slot, releasedFn, returnDelay)
		switch {
		case err == nil:
		case errors.Is(err, ErrSlotRetained):
			logger.L().Error(ctx, "network slot retained and unavailable for reuse", zap.Error(err), zap.Int("slot_index", slot.Idx))
		case errors.Is(err, ErrClosed), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// Expected when the pool closes or the context ends mid-return.
			logger.L().Warn(ctx, "network slot returned during pool shutdown", zap.Error(err), zap.Int("slot_index", slot.Idx))
		default:
			logger.L().Error(ctx, "failed to return network slot to pool", zap.Error(err), zap.Int("slot_index", slot.Idx))
		}
	}()

	return nil
}

// recycle resets the slot's internet configuration and puts it back into the
// reused pool, or cleans it up if the pool is full or closed.
func (p *Pool) recycle(ctx context.Context, slot *Slot) error {
	// Undo any build-specific DSCP before the next tenant can inherit it.
	if err := slot.applyEgressDSCP(ctx, p.config.untenantedDSCP()); err != nil {
		return p.cleanupWith(ctx, slot, fmt.Errorf("error resetting slot egress DSCP: %w", err))
	}

	if err := slot.ResetInternet(ctx); err != nil {
		return p.cleanupWith(ctx, slot, fmt.Errorf("error resetting slot internet access: %w", err))
	}

	reused, cause := p.tryReuse(ctx, slot)
	if reused {
		return nil
	}

	// cause is nil when the pool is simply full.
	return p.cleanupWith(ctx, slot, cause)
}

// tryReuse attempts to push the slot into the reused pool. The RLock pairs
// with Close's Lock so a send can never race the drain; cleanup stays
// outside the lock so Close is never pinned by slow iptables/netlink
// teardown.
func (p *Pool) tryReuse(ctx context.Context, slot *Slot) (reused bool, cause error) {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	if p.closed {
		return false, ErrClosed
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-p.done:
		return false, ErrClosed
	case p.reusedSlots <- slot:
		returnedSlotCounter.Add(ctx, 1)
		reusableSlotsAvailableCounter.Add(ctx, 1)

		return true, nil
	default:
		return false, nil
	}
}

// cleanupWith tears the slot down and attaches any cleanup error to cause.
func (p *Pool) cleanupWith(ctx context.Context, slot *Slot, cause error) error {
	if cerr := p.cleanup(ctx, slot); cerr != nil {
		cleanupErr := fmt.Errorf("cleanup slot '%d': %w", slot.Idx, cerr)
		p.closeMu.Lock()
		if p.cleanupErr == nil {
			p.cleanupErr = cleanupErr
		}
		p.closeMu.Unlock()

		return errors.Join(cause, cleanupErr)
	}

	return cause
}

func (p *Pool) cleanup(ctx context.Context, slot *Slot) error {
	var errs []error

	err := slot.RemoveNetwork(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("cannot remove network when releasing slot '%d': %w", slot.Idx, err))
	}

	err = p.slotStorage.Release(slot)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to release slot '%d': %w", slot.Idx, err))
	}

	releasedSlotCounter.Add(ctx, 1)

	return errors.Join(errs...)
}

// Close leaves the pool unchanged when ctx is canceled before Close publishes
// closed. Once it has published closed, Close ignores later cancellation so it
// can join admissions and returns before removing shared network state. Past a
// teardown deadline on ctx, slots still pooled stay in place and Close returns
// ErrSlotsLeft.
func (p *Pool) Close(ctx context.Context) error {
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
	logger.L().Info(ctx, "Closing network pool")

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
	// Not bounded by the teardown deadline: this teardown is the only read of
	// a retained slot's firewall counters.
	for slot := range retained {
		if err := p.cleanup(ctx, slot); err != nil {
			errs = append(errs, fmt.Errorf("failed to cleanup retained slot '%d': %w", slot.Idx, err))
		}
	}

	left := 0
	for slot := range p.newSlots {
		newSlotsAvailableCounter.Add(ctx, -1)

		if TeardownDeadlinePassed(ctx) {
			left++

			continue
		}
		err := p.cleanup(ctx, slot)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to cleanup slot '%d': %w", slot.Idx, err))
		}
	}

drain:
	for {
		select {
		case slot := <-p.reusedSlots:
			reusableSlotsAvailableCounter.Add(ctx, -1)

			if TeardownDeadlinePassed(ctx) {
				left++

				continue
			}
			err := p.cleanup(ctx, slot)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to cleanup slot '%d': %w", slot.Idx, err))
			}
		default:
			break drain
		}
	}

	if left > 0 {
		errs = append(errs, fmt.Errorf("%w: %d", ErrSlotsLeft, left))
	}

	return errors.Join(errs...)
}
