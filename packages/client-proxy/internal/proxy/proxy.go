package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	// Also it's a good practice to set it to a value higher than the idle timeout of the backend service
	// https://cloud.google.com/load-balancing/docs/https#timeouts_and_retries%23:~:text=The%20load%20balancer%27s%20backend%20keepalive,is%20greater%20than%20600%20seconds
	idleTimeout = 610 * time.Second
)

var (
	ErrNodeNotFound         = errors.New("node not found")
	ErrNodeRouteUnavailable = errors.New("node route unavailable")
)

type autoResumeResult uint8

const (
	autoResumeSucceeded autoResumeResult = iota
	autoResumeNotAllowed
	autoResumePermissionDenied
	autoResumeResourceExhausted
	autoResumeErrored
)

func catalogSandboxNodeIP(s *catalog.SandboxInfo) (string, error) {
	return normalizeNodeIP(s.OrchestratorIP)
}

func normalizeNodeIP(nodeIP string) (string, error) {
	nodeIP = strings.TrimSpace(nodeIP)
	if nodeIP == "" {
		return "", ErrNodeRouteUnavailable
	}

	return nodeIP, nil
}

func clientProxyMaskRequestHost(ctx context.Context, featureFlags *featureflags.Client, host string, sandboxID string, port uint64) *string {
	domain, sharedHost := reverseproxy.SandboxSharedHostDomain(host)
	if !sharedHost || featureFlags.BoolFlag(ctx, featureflags.OrchAcceptsCombinedHostFlag) {
		return nil
	}

	orchestratorHost := fmt.Sprintf("%d-%s.%s", port, sandboxID, domain)

	return &orchestratorHost
}

func catalogResolution(ctx context.Context, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string, c catalog.SandboxesCatalog, pausedChecker PausedSandboxResumer) (string, error) {
	s, err := c.GetSandbox(ctx, sandboxId)
	if err != nil {
		if errors.Is(err, catalog.ErrSandboxNotFound) {
			nodeIP, res, pausedErr := handlePausedSandbox(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken, pausedChecker)
			if pausedErr != nil {
				return "", pausedErr
			}
			if res == autoResumeSucceeded {
				return nodeIP, nil
			}

			return "", ErrNodeNotFound
		}

		return "", fmt.Errorf("failed to get sandbox from catalog: %w", err)
	}

	return catalogSandboxNodeIP(s)
}

func handlePausedSandbox(
	ctx context.Context,
	sandboxId string,
	sandboxPort uint64,
	trafficAccessToken string,
	envdAccessToken string,
	pausedChecker PausedSandboxResumer,
) (string, autoResumeResult, error) {
	if pausedChecker == nil {
		return "", autoResumeNotAllowed, nil
	}

	logger.L().Info(ctx, "catalog miss, attempting resume via api", logger.WithSandboxID(sandboxId))
	nodeIP, err := pausedChecker.Resume(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.PermissionDenied {
				return "", autoResumePermissionDenied, reverseproxy.NewErrSandboxResumePermissionDenied(sandboxId)
			}
			if st.Code() == codes.NotFound {
				return "", autoResumeNotAllowed, nil
			}
			if st.Code() == codes.FailedPrecondition && st.Message() == proxygrpc.SandboxStillTransitioningMessage {
				return "", autoResumeErrored, reverseproxy.NewErrSandboxStillTransitioning(sandboxId)
			}
			if st.Code() == codes.ResourceExhausted {
				return "", autoResumeResourceExhausted, reverseproxy.NewErrSandboxResourceExhausted(sandboxId, st.Message())
			}
		}

		return "", autoResumeErrored, err
	}

	nodeIP, err = normalizeNodeIP(nodeIP)
	if err != nil {
		return "", autoResumeErrored, err
	}

	return nodeIP, autoResumeSucceeded, nil
}

// selectCatalog picks the routing source per request: the orchestrator-owned
// record when OrchestratorRoutingPrioritizedFlag is on, else the API-owned one.
func selectCatalog(ctx context.Context, featureFlags *featureflags.Client, apiCatalog, orchestratorCatalog catalog.SandboxesCatalog) catalog.SandboxesCatalog {
	if orchestratorCatalog != nil && featureFlags.BoolFlag(ctx, featureflags.OrchestratorRoutingPrioritizedFlag) {
		return orchestratorCatalog
	}

	return apiCatalog
}

func NewClientProxy(meterProvider metric.MeterProvider, serviceName string, port, orchestratorProxyPort uint16, catalog catalog.SandboxesCatalog, orchestratorCatalog catalog.SandboxesCatalog, pausedSandboxResumer PausedSandboxResumer, featureFlagsClient *featureflags.Client) (*reverseproxy.Proxy, error) {
	getTargetFromRequest := reverseproxy.GetTargetFromRequest()
	proxy := reverseproxy.New(
		port,
		// Retries that are needed to handle port forwarding delays in sandbox envd are handled by the orchestrator proxy
		reverseproxy.ClientProxyRetries,
		idleTimeout,
		func(r *http.Request) (*pool.Destination, error) {
			ctx := r.Context()
			sandboxId, port, err := getTargetFromRequest(r)
			if err != nil {
				return nil, err
			}

			l := logger.L().With(logger.ProxyRequestFields(r, sandboxId, port)...)

			trafficAccessToken := r.Header.Get(proxygrpc.MetadataTrafficAccessToken)
			envdAccessToken := r.Header.Get(proxygrpc.MetadataEnvdHTTPAccessToken)
			routingSource := selectCatalog(ctx, featureFlagsClient, catalog, orchestratorCatalog)
			nodeIP, err := catalogResolution(ctx, sandboxId, port, trafficAccessToken, envdAccessToken, routingSource, pausedSandboxResumer)
			if err != nil {
				if resumeDeniedErr, ok := errors.AsType[*reverseproxy.SandboxResumePermissionDeniedError](err); ok {
					l.Warn(ctx, "sandbox resume denied", zap.Error(err))

					return nil, resumeDeniedErr
				}

				if resourceExhaustedErr, ok := errors.AsType[*reverseproxy.SandboxResourceExhaustedError](err); ok {
					l.Warn(ctx, "sandbox resource exhausted", zap.Error(err))

					return nil, resourceExhaustedErr
				}

				if stillTransitioningErr, ok := errors.AsType[*reverseproxy.SandboxStillTransitioningError](err); ok {
					l.Warn(ctx, "sandbox still transitioning", zap.Error(err))

					return nil, stillTransitioningErr
				}

				if errors.Is(err, ErrNodeRouteUnavailable) {
					l.Warn(ctx, "sandbox route unavailable", zap.Error(err))
				} else if !errors.Is(err, ErrNodeNotFound) {
					l.Warn(ctx, "failed to resolve node ip with Redis resolution", zap.Error(err))
				}

				return nil, reverseproxy.NewErrSandboxNotFound(sandboxId)
			}

			url := &url.URL{
				Scheme: "http",
				Host:   net.JoinHostPort(nodeIP, strconv.Itoa(int(orchestratorProxyPort))),
			}

			l = l.With(
				zap.String("target_hostname", url.Hostname()),
				zap.String("target_port", url.Port()),
			)

			return &pool.Destination{
				SandboxId:     sandboxId,
				RequestLogger: l,
				SandboxPort:   port,
				ConnectionKey: pool.ClientProxyConnectionKey,
				Url:           url,
				MaskRequestHost: clientProxyMaskRequestHost(
					ctx,
					featureFlagsClient,
					r.Host,
					sandboxId,
					port,
				),
			}, nil
		},
		nil,
		false,
	)

	meter := meterProvider.Meter(serviceName)
	_, err := telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyPoolConnectionsMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(proxy.CurrentPoolConnections())

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy connections metric (%s): %w", telemetry.ClientProxyPoolConnectionsMeterCounterName, err)
	}

	_, err = telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyPoolSizeMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(int64(proxy.CurrentPoolSize()))

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy pool size metric (%s): %w", telemetry.ClientProxyPoolSizeMeterCounterName, err)
	}

	_, err = telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyServerConnectionsMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(proxy.CurrentServerConnections())

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy server connections metric (%s): %w", telemetry.ClientProxyServerConnectionsMeterCounterName, err)
	}

	return proxy, nil
}
