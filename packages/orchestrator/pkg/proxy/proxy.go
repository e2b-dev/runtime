//go:build linux

package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// This timeout should be > 600 (GCP LB upstream idle timeout) to prevent race condition
	// Also it's a good practice to set it to higher values as you progress in the stack
	// https://cloud.google.com/load-balancing/docs/https#timeouts_and_retries%23:~:text=The%20load%20balancer%27s%20backend%20keepalive,is%20greater%20than%20600%20seconds
	idleTimeout     = 620 * time.Second
	shutdownTimeout = 30 * time.Second
	// retirementTimeout bounds the wait for a lifecycle's handlers and dials
	// to settle at network release, independently of the caller's
	// cancellation, as the egress side waits: a slot whose requests have not
	// settled is retained, never reused.
	retirementTimeout = time.Minute
)

var _ sandbox.MapSubscriber = (*SandboxProxy)(nil)

type SandboxProxy struct {
	proxy     *reverseproxy.Proxy
	limiter   *connlimit.ConnectionLimiter
	sandboxes *sandbox.Map
	ownersMu  sync.Mutex
	owners    map[string]*pool.Owner
	// retirement is the budget of one release's wait, retirementTimeout.
	retirement time.Duration
}

func (p *SandboxProxy) resolveDestination(r *http.Request) (*pool.Destination, error) {
	var sbx *sandbox.Sandbox
	d, err := reverseproxy.ResolveIngress(r, func(id string, port uint64) (reverseproxy.IngressTarget, error) {
		// The lookup and the owner it admits into are taken under one lock,
		// so a stop or reset cannot slip between them.
		p.ownersMu.Lock()
		defer p.ownersMu.Unlock()
		var found bool
		if sbx, found = p.sandboxes.Get(id); !found {
			return reverseproxy.IngressTarget{}, reverseproxy.NewErrSandboxNotFound(id)
		}
		// Full-router ingress enters through the sandbox's Router, which this
		// proxy does not forward for, so it never reaches the guest from here.
		if catalog.NetworkMode(sbx.NetworkPlacement.GetNetworkMode()) == catalog.NetworkModeFullRouter {
			return reverseproxy.IngressTarget{}, reverseproxy.NewErrSandboxPlacementRefused(id, "full-router ingress is not served by the orchestrator proxy", proxygrpc.PlacementFeatureUnsupported, http.StatusServiceUnavailable)
		}
		owner := p.owners[sbx.LifecycleID]
		if owner == nil {
			owner = p.proxy.NewOwner()
			p.owners[sbx.LifecycleID] = owner
		}

		return reverseproxy.IngressTarget{
			Config: sbx.Config.GetNetworkIngress(),
			Destination: pool.Destination{
				Url:                                &url.URL{Host: net.JoinHostPort(sbx.Slot.HostIPString(), strconv.FormatUint(port, 10))},
				DefaultToPortError:                 true,
				IncludeSandboxIdInProxyErrorLogger: true,
				ConnectionKey:                      sbx.LifecycleID,
				Owner:                              owner,
			},
		}, nil
	})
	if err != nil {
		return nil, err
	}
	// Built once the request is admitted: a refused one never logs through it.
	d.RequestLogger = logger.L().With(
		append(
			logger.ProxyRequestFields(r, sbx.Runtime.SandboxID, d.SandboxPort),
			logger.WithTeamID(sbx.Runtime.TeamID),
			logger.WithSandboxIP(sbx.Slot.HostIPString()),
		)...,
	)

	return d, nil
}

func NewSandboxProxy(meterProvider metric.MeterProvider, port uint16, sandboxes *sandbox.Map, featureFlags *featureflags.Client) (*SandboxProxy, error) {
	limiter := connlimit.NewConnectionLimiter()
	metrics := NewMetrics(meterProvider)

	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy")

	connLimitConfig := &reverseproxy.ConnectionLimitConfig{
		Limiter: limiter,
		GetMaxLimit: func(ctx context.Context) int {
			return featureFlags.IntFlag(ctx, featureflags.SandboxMaxIncomingConnections)
		},
		OnConnectionAcquired: metrics.RecordConnectionsPerSandbox,
		OnConnectionReleased: metrics.RecordConnectionDuration,
		OnConnectionBlocked:  metrics.RecordConnectionBlocked,
	}

	outcomeCounter, err := telemetry.GetCounter(meter, telemetry.OrchestratorProxyRequestOutcomeCounterName)
	if err != nil {
		return nil, fmt.Errorf("error registering orchestrator proxy request outcome metric (%s): %w", telemetry.OrchestratorProxyRequestOutcomeCounterName, err)
	}
	recordOutcome := func(ctx context.Context, outcome pool.RequestOutcome) {
		outcomeCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", string(outcome))))
	}

	sandboxProxy := &SandboxProxy{limiter: limiter, sandboxes: sandboxes, owners: make(map[string]*pool.Owner), retirement: retirementTimeout}
	proxy := reverseproxy.New(
		port,
		// Retry 5 times to handle port forwarding delays in sandbox envd.
		reverseproxy.SandboxProxyRetries,
		idleTimeout,
		sandboxProxy.resolveDestination,
		connLimitConfig,
		// We are not using keepalives for orchestrator proxy,
		// because the servers inside of the sandbox can be unstable (restarts),
		// and we are also on the same host, so the overhead is minimal.
		true,
		recordOutcome,
	)

	_, err = telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorProxyServerConnectionsMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(proxy.CurrentServerConnections())

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error registering orchestrator proxy connections metric (%s): %w", telemetry.OrchestratorProxyServerConnectionsMeterCounterName, err)
	}

	_, err = telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorProxyPoolConnectionsMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(proxy.CurrentPoolConnections())

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error registering orchestrator proxy connections metric (%s): %w", telemetry.OrchestratorProxyPoolConnectionsMeterCounterName, err)
	}

	_, err = telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorProxyPoolSizeMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(int64(proxy.CurrentPoolSize()))

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error registering orchestrator proxy pool size metric (%s): %w", telemetry.OrchestratorProxyPoolSizeMeterCounterName, err)
	}

	sandboxProxy.proxy = proxy

	// Subscribe to sandbox events for cleanup
	sandboxes.Subscribe(sandboxProxy)

	return sandboxProxy, nil
}

func (p *SandboxProxy) Start(ctx context.Context) error {
	return p.proxy.ListenAndServe(ctx)
}

func (p *SandboxProxy) Close(ctx context.Context) error {
	var err error
	forced := ctx.Err() != nil
	if !forced {
		shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()

		err = p.proxy.Shutdown(shutdownCtx)
		if err != nil {
			forced = true
			logger.L().Warn(ctx, "sandbox proxy graceful shutdown interrupted", zap.Error(err))
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				err = nil
			}
		}
	}
	if forced {
		err = errors.Join(err, p.proxy.Close())
	}
	logger.L().Info(ctx, "sandbox proxy shutdown complete", zap.Bool("forced", forced), zap.Error(err))
	if err != nil {
		return fmt.Errorf("failed to shutdown proxy server: %w", err)
	}

	return nil
}

// RemoveFromPool retires the lifecycle's current generation of connections,
// as an envd restart needs, and waits for it to settle on the proxy's budget,
// so the caller, a template build, continues against a settled generation.
// The lifecycle admits into a fresh owner from the moment the old one is
// detached, so requests arriving during the wait are served, not refused.
func (p *SandboxProxy) RemoveFromPool(ctx context.Context, connectionKey string) error {
	p.ownersMu.Lock()
	owner := p.owners[connectionKey]
	delete(p.owners, connectionKey)
	p.ownersMu.Unlock()
	if owner == nil {
		return nil
	}

	return p.retire(ctx, owner)
}

func (p *SandboxProxy) GetAddr() string {
	return p.proxy.Addr
}

// OnInsert is called when a sandbox is inserted into the map.
func (p *SandboxProxy) OnInsert(_ context.Context, _ *sandbox.Sandbox) {}

// OnStopping is called when a sandbox leaves the live registry.
func (p *SandboxProxy) OnStopping(_ context.Context, sbx *sandbox.Sandbox) {
	p.ownersMu.Lock()
	owner := p.owners[sbx.LifecycleID]
	p.ownersMu.Unlock()
	if owner != nil {
		owner.Stop()
	}
}

// OnNetworkRelease is called when a sandbox's network slot is released.
// Keyed by LifecycleID so the removal is scoped to this sandbox lifecycle.
func (p *SandboxProxy) OnNetworkRelease(ctx context.Context, sbx *sandbox.Sandbox) error {
	if err := p.closeOwner(ctx, sbx.LifecycleID); err != nil {
		return err
	}
	p.limiter.Remove(sbx.LifecycleID)

	return nil
}

// closeOwner retires the lifecycle's owner for its network release. The
// sealed owner stays discoverable until its work has settled, so a request
// for the ending lifecycle is refused meanwhile and a failed release can be
// retried against the same work.
func (p *SandboxProxy) closeOwner(ctx context.Context, lifecycleID string) error {
	p.ownersMu.Lock()
	owner := p.owners[lifecycleID]
	p.ownersMu.Unlock()
	if owner == nil {
		return nil
	}
	if err := p.retire(ctx, owner); err != nil {
		return err
	}
	p.ownersMu.Lock()
	defer p.ownersMu.Unlock()
	if p.owners[lifecycleID] == owner {
		delete(p.owners, lifecycleID)
	}

	return nil
}

// retire stops the owner and waits for its work on the proxy's own budget.
func (p *SandboxProxy) retire(ctx context.Context, owner *pool.Owner) error {
	retireCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.retirement)
	defer cancel()

	return owner.Close(retireCtx)
}
