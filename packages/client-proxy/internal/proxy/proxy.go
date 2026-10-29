package proxy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
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

// routeHost returns the host and port a route's traffic goes to: the
// orchestrator proxy for legacy and egress-router routes, the Router's
// ingress endpoint saved in a full-router placement. A full-router route
// without that endpoint, or an unknown mode, never falls back to the
// orchestrator.
func routeHost(route *catalog.SandboxInfo, orchestratorProxyPort uint16) (string, error) {
	if placement := route.NetworkPlacement; placement != nil {
		switch placement.NetworkMode {
		case catalog.NetworkModeEgressRouter:
		case catalog.NetworkModeFullRouter:
			if placement.IngressAddress == "" || placement.IngressPort == 0 || placement.IngressPort > math.MaxUint16 {
				return "", fmt.Errorf("%w: full-router placement without its Router's ingress", ErrNodeRouteUnavailable)
			}

			return net.JoinHostPort(placement.IngressAddress, strconv.FormatUint(uint64(placement.IngressPort), 10)), nil
		default:
			return "", fmt.Errorf("%w: unsupported network mode %q", ErrNodeRouteUnavailable, placement.NetworkMode)
		}
	}

	nodeIP := strings.TrimSpace(route.OrchestratorIP)
	if nodeIP == "" {
		return "", ErrNodeRouteUnavailable
	}

	return net.JoinHostPort(nodeIP, strconv.Itoa(int(orchestratorProxyPort))), nil
}

func clientProxyMaskRequestHost(ctx context.Context, featureFlags *featureflags.Client, host string, sandboxID string, port uint64) *string {
	domain, sharedHost := reverseproxy.SandboxSharedHostDomain(host)
	if !sharedHost || featureFlags.BoolFlag(ctx, featureflags.OrchAcceptsCombinedHostFlag) {
		return nil
	}

	orchestratorHost := fmt.Sprintf("%d-%s.%s", port, sandboxID, domain)

	return &orchestratorHost
}

// catalogResolution returns the sandbox's route: the routing record on a
// hit, and on a miss the route the resume answered with, without reading
// the record again.
func catalogResolution(ctx context.Context, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string, c catalog.SandboxesCatalog, pausedChecker SandboxResumer) (*catalog.SandboxInfo, error) {
	s, err := c.GetSandbox(ctx, sandboxId)
	if err != nil {
		if errors.Is(err, catalog.ErrSandboxNotFound) {
			route, res, pausedErr := handlePausedSandbox(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken, pausedChecker)
			if pausedErr != nil {
				return nil, pausedErr
			}
			if res == autoResumeSucceeded {
				return route, nil
			}

			return nil, ErrNodeNotFound
		}

		return nil, fmt.Errorf("failed to get sandbox from catalog: %w", err)
	}

	return s, nil
}

func handlePausedSandbox(
	ctx context.Context,
	sandboxId string,
	sandboxPort uint64,
	trafficAccessToken string,
	envdAccessToken string,
	pausedChecker SandboxResumer,
) (*catalog.SandboxInfo, autoResumeResult, error) {
	if pausedChecker == nil {
		return nil, autoResumeNotAllowed, nil
	}

	route, err := pausedChecker.Resume(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
	if err != nil {
		if st, ok := resumeStatus(err); ok {
			if st.Code() == codes.PermissionDenied {
				return nil, autoResumePermissionDenied, reverseproxy.NewErrSandboxResumePermissionDenied(sandboxId)
			}
			if st.Code() == codes.NotFound {
				return nil, autoResumeNotAllowed, nil
			}
			if st.Code() == codes.FailedPrecondition && st.Message() == proxygrpc.SandboxStillTransitioningMessage {
				// The SDK reads an envd-port 409 as "already exists" (a
				// make_dir then reports success), so envd traffic gets
				// the same page as 503, which it reads as unavailable.
				if sandboxPort == uint64(consts.DefaultEnvdServerPort) {
					return nil, autoResumeErrored, reverseproxy.NewErrSandboxStillTransitioningUnavailable(sandboxId)
				}

				return nil, autoResumeErrored, reverseproxy.NewErrSandboxStillTransitioning(sandboxId)
			}
			if st.Code() == codes.ResourceExhausted {
				return nil, autoResumeResourceExhausted, reverseproxy.NewErrSandboxResourceExhausted(sandboxId, st.Message())
			}
			// The code the API sent decides a placement outcome: a refusal
			// no retry helps gets the refused page whatever its gRPC code
			// (a node's refusal travels as 500 or 400), a placement timeout
			// (504) and a create that failed on the node (500) the capacity
			// page, since the not-found page they would fall to reads as
			// the sandbox being gone.
			code, permanent := resumeRefusalCode(st)
			if permanent {
				// The API's own status for the refusal, carried on its
				// detail; from an API that sends none, a bad request stays
				// a 400 the SDKs do not retry and anything else is the 503
				// page.
				statusCode := resumeHTTPStatus(st)
				if statusCode == 0 {
					statusCode = http.StatusServiceUnavailable
					if st.Code() == codes.InvalidArgument {
						statusCode = http.StatusBadRequest
					}
				}

				return nil, autoResumeErrored, reverseproxy.NewErrSandboxPlacementRefused(sandboxId, st.Message(), code, statusCode)
			}
			if st.Code() == codes.Unavailable || proxygrpc.RetryablePlacementFailure(code) {
				return nil, autoResumeErrored, reverseproxy.NewErrSandboxCapacity(sandboxId, st.Message())
			}
		}

		return nil, autoResumeErrored, err
	}

	return route, autoResumeSucceeded, nil
}

// NewClientProxy returns the proxy and, when resumes are enabled, the closer
// that stops the resume flights still running after the proxy has shut down.
func NewClientProxy(meterProvider metric.MeterProvider, serviceName string, port, orchestratorProxyPort uint16, catalog catalog.SandboxesCatalog, sandboxResumer SandboxResumer, featureFlagsClient *featureflags.Client) (*reverseproxy.Proxy, ResumeCloser, error) {
	meter := meterProvider.Meter(serviceName)

	var resumer SandboxResumer
	var resumeCloser ResumeCloser
	if sandboxResumer != nil {
		flights, err := newResumeFlights(meter, sandboxResumer, featureFlagsClient)
		if err != nil {
			return nil, nil, err
		}

		resumer = flights
		resumeCloser = flights
	}

	outcomeCounter, err := telemetry.GetCounter(meter, telemetry.ClientProxyRequestOutcomeCounterName)
	if err != nil {
		return nil, nil, fmt.Errorf("error registering client proxy request outcome metric (%s): %w", telemetry.ClientProxyRequestOutcomeCounterName, err)
	}
	recordOutcome := func(ctx context.Context, outcome pool.RequestOutcome, node string) {
		if node == "" {
			node = "unknown"
		}
		outcomeCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("outcome", string(outcome)),
			attribute.String("node", node),
		))
	}

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
			var host string
			route, err := catalogResolution(ctx, sandboxId, port, trafficAccessToken, envdAccessToken, catalog, resumer)
			if err == nil {
				host, err = routeHost(route, orchestratorProxyPort)
			}
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

				// Logged here with the request's fields; the lot-full shed
				// is counted and stays quiet.
				if capacityErr, ok := errors.AsType[*reverseproxy.SandboxCapacityError](err); ok {
					fields := []zap.Field{zap.Error(err), zap.String("reason", capacityErr.Message), zap.Bool("permanent", capacityErr.Permanent)}
					if capacityErr.Message == reasonParkingLotFull {
						l.Debug(ctx, capacityErr.Error(), fields...)
					} else {
						l.Warn(ctx, capacityErr.Error(), fields...)
					}

					return nil, capacityErr
				}

				// The caller left while it waited: nothing went wrong on
				// this side, and nobody is left to read a page.
				if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
					return nil, err
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
				Host:   host,
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
				// Label outcomes by the worker instance so an unreachable-upstream
				// spike points straight at a drained/stale node.
				Node: route.OrchestratorID,
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
		recordOutcome,
	)

	_, err = telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyPoolConnectionsMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(proxy.CurrentPoolConnections())

			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error registering client proxy connections metric (%s): %w", telemetry.ClientProxyPoolConnectionsMeterCounterName, err)
	}

	_, err = telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyPoolSizeMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(int64(proxy.CurrentPoolSize()))

			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error registering client proxy pool size metric (%s): %w", telemetry.ClientProxyPoolSizeMeterCounterName, err)
	}

	_, err = telemetry.GetObservableUpDownCounter(
		meter, telemetry.ClientProxyServerConnectionsMeterCounterName, func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(proxy.CurrentServerConnections())

			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error registering client proxy server connections metric (%s): %w", telemetry.ClientProxyServerConnectionsMeterCounterName, err)
	}

	return proxy, resumeCloser, nil
}
