package featureflags

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldreason"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/interfaces"
	"github.com/launchdarkly/go-server-sdk/v7/ldcomponents"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// launchDarklyOfflineStore is a test fixture that provides dynamically updatable feature flag state
var launchDarklyOfflineStore = ldtestdata.DataSource()

var launchDarklyApiKey = os.Getenv("LAUNCH_DARKLY_API_KEY")

const waitForInit = 5 * time.Second

// Option configures NewClient and NewClientWithLogLevel.
type Option func(*options)

type options struct{ startOnInitTimeout bool }

// WithStartOnInitTimeout keeps the client when LaunchDarkly has not answered
// within waitForInit: it is Live and serves fallbacks until the stream
// connects, then follows the flags. A permanent failure, such as a rejected
// SDK key, still fails. Opt in only where running on fallbacks is safer than
// not starting.
func WithStartOnInitTimeout() Option {
	return func(o *options) { o.startOnInitTimeout = true }
}

// undefinedDeploymentEnvironment is the place key of a process whose
// configuration names no deployment environment. It keeps the context
// present, so the gap is visible in LaunchDarkly instead of silent.
const undefinedDeploymentEnvironment = "undefined"

// Config identifies the place a process runs in. Services embed it in their
// configuration struct, so the environment parser fills it with the rest.
type Config struct {
	// DeploymentEnvironment is the OpenTelemetry deployment.environment value
	// of this process, for example "staging".
	DeploymentEnvironment string `env:"DEPLOYMENT_ENVIRONMENT"`
}

type Client struct {
	ld                    *ldclient.LDClient
	static                bool
	deploymentEnvironment string
	serviceName           string
	contextProviders      []ContextProvider
}

func newClient(ld *ldclient.LDClient, static bool, deploymentEnvironment, serviceName string) *Client {
	if deploymentEnvironment == "" {
		deploymentEnvironment = undefinedDeploymentEnvironment
	}

	return &Client{ld: ld, static: static, deploymentEnvironment: deploymentEnvironment, serviceName: serviceName}
}

// applicationInfo names the process to LaunchDarkly. Every deployment
// environment reports into one LaunchDarkly environment, and the Monitoring
// tab splits evaluations by application only, so the application ID carries
// the environment next to the service name. An empty service name sends no
// metadata: the SDK drops it without an ID anyway.
func applicationInfo(deploymentEnvironment, serviceName, serviceVersion string) interfaces.ApplicationInfo {
	if serviceName == "" {
		return interfaces.ApplicationInfo{}
	}
	if deploymentEnvironment == "" {
		deploymentEnvironment = undefinedDeploymentEnvironment
	}

	return interfaces.ApplicationInfo{
		ApplicationID:      serviceName + "-" + deploymentEnvironment,
		ApplicationVersion: serviceVersion,
	}
}

// ContextProvider supplies an additional LD context on every flag evaluation.
// Services register providers to inject specific contexts without leaking that
// specificity into the shared client.
type ContextProvider func(ctx context.Context) ldcontext.Context

func NewClientWithDatasource(source *ldtestdata.TestDataSource) (*Client, error) {
	ldClient, err := ldclient.MakeCustomClient(
		"",
		ldclient.Config{
			DataSource: source,
			// Disable all outbound network traffic: no analytics events and no
			// diagnostic events. The SDK key is empty here so any network call
			// would fail and produce noise anyway.
			Events:           ldcomponents.NoEvents(),
			DiagnosticOptOut: true,
		},
		0)
	if err != nil {
		return nil, err
	}

	return &Client{ld: ldClient}, nil
}

// NewClient creates a client that adds a DeploymentEnvironmentKind context and,
// when serviceName is not empty, a ServiceKind context to every evaluation.
// The same names, with serviceVersion, identify the process as a LaunchDarkly
// application; see applicationInfo.
func NewClient(deploymentEnvironment, serviceName, serviceVersion string, opts ...Option) (*Client, error) {
	if launchDarklyApiKey == "" {
		c, err := NewClientWithDatasource(launchDarklyOfflineStore)
		if err != nil {
			return nil, err
		}

		return newClient(c.ld, true, deploymentEnvironment, serviceName), nil
	}

	cfg := ldclient.Config{
		ApplicationInfo:  applicationInfo(deploymentEnvironment, serviceName, serviceVersion),
		ServiceEndpoints: serviceEndpoints(),
	}

	return connect(launchDarklyApiKey, cfg, waitForInit, deploymentEnvironment, serviceName, opts...)
}

// connect makes the SDK client and waits up to wait for its first payload.
// On ErrInitializationTimeout the SDK keeps connecting in the background, and
// the client is kept if the caller opted in. Otherwise an error closes the
// client, stopping its event processor and data source. Close does not stop a
// stream still retrying its first connection: it keeps running until it
// connects.
//
//nolint:contextcheck // the constructors take no context; the warning is a one-off at startup
func connect(sdkKey string, cfg ldclient.Config, wait time.Duration, deploymentEnvironment, serviceName string, opts ...Option) (*Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	ldClient, err := ldclient.MakeCustomClient(sdkKey, cfg, wait)
	if err != nil {
		if !o.startOnInitTimeout || !errors.Is(err, ldclient.ErrInitializationTimeout) {
			if ldClient != nil {
				_ = ldClient.Close()
			}

			return nil, err
		}

		logger.L().Warn(context.Background(), "LaunchDarkly client did not initialize in time; starting on flag fallbacks",
			zap.String("service", serviceName), zap.Duration("waited", wait), zap.Error(err))
	}

	return newClient(ldClient, false, deploymentEnvironment, serviceName), nil
}

// NewClientWithLogLevel creates a client with a specific log level.
// Use ldlog.Error to suppress INFO/WARN logs in CLI tools.
func NewClientWithLogLevel(deploymentEnvironment, serviceName, serviceVersion string, logLevel ldlog.LogLevel, opts ...Option) (*Client, error) {
	cfg := ldclient.Config{
		ApplicationInfo:  applicationInfo(deploymentEnvironment, serviceName, serviceVersion),
		Logging:          ldcomponents.Logging().MinLevel(logLevel),
		ServiceEndpoints: serviceEndpoints(),
	}

	if launchDarklyApiKey == "" {
		cfg.DataSource = launchDarklyOfflineStore
		// Disable all outbound network traffic when no API key is configured.
		cfg.Events = ldcomponents.NoEvents()
		cfg.DiagnosticOptOut = true
		ldClient, err := ldclient.MakeCustomClient("", cfg, 0)
		if err != nil {
			return nil, err
		}

		return newClient(ldClient, true, deploymentEnvironment, serviceName), nil
	}

	return connect(launchDarklyApiKey, cfg, waitForInit, deploymentEnvironment, serviceName, opts...)
}

// LAUNCH_DARKLY_BASE_URL redirects streaming, polling and events together.
func serviceEndpoints() interfaces.ServiceEndpoints {
	if baseURL := os.Getenv("LAUNCH_DARKLY_BASE_URL"); baseURL != "" {
		return ldcomponents.RelayProxyEndpoints(baseURL)
	}

	return interfaces.ServiceEndpoints{}
}

// Live reports whether flag values can change at runtime. A client built
// without an API key serves the offline store's values for the life of the
// process, and a nil client serves fallbacks.
func (c *Client) Live() bool {
	return c != nil && c.ld != nil && !c.static
}

// RegisterContextProvider registers a provider whose contexts are appended to
// every flag evaluation.
func (c *Client) RegisterContextProvider(provider ContextProvider) {
	c.contextProviders = append(c.contextProviders, provider)
}

func (c *Client) BoolFlag(ctx context.Context, flag BoolFlag, contexts ...ldcontext.Context) bool {
	return getFlag(ctx, c.ld, c.ld.BoolVariationCtx, flag, c.allContexts(ctx, contexts))
}

// BoolFlagOverride returns whether a flag was successfully evaluated. Callers
// with different legacy defaults can distinguish an explicit false from a fallback.
func (c *Client) BoolFlagOverride(ctx context.Context, flag BoolFlag, contexts ...ldcontext.Context) (bool, bool) {
	if c.ld == nil {
		return false, false
	}
	value, detail, err := c.ld.BoolVariationDetailCtx(ctx, flag.Key(), mergeContexts(ctx, c.allContexts(ctx, contexts)), flag.Fallback())

	return value, err == nil && !detail.IsDefaultValue()
}

func (c *Client) JSONFlag(ctx context.Context, flag JSONFlag, contexts ...ldcontext.Context) ldvalue.Value {
	return getFlag(ctx, c.ld, c.ld.JSONVariationCtx, flag, c.allContexts(ctx, contexts))
}

func (c *Client) WatchJSONFlag(ctx context.Context, flag JSONFlag, contexts ...ldcontext.Context) (<-chan interfaces.FlagValueChangeEvent, func()) {
	if c.ld == nil {
		ch := make(chan interfaces.FlagValueChangeEvent)
		close(ch)

		return ch, func() {}
	}

	listener := c.ld.GetFlagTracker().AddFlagValueChangeListener(
		flag.Key(),
		mergeContexts(ctx, c.allContexts(ctx, contexts)),
		flag.Fallback(),
	)

	return listener, func() {
		c.ld.GetFlagTracker().RemoveFlagValueChangeListener(listener)
	}
}

func (c *Client) IntFlag(ctx context.Context, flag IntFlag, contexts ...ldcontext.Context) int {
	return getFlag(ctx, c.ld, c.ld.IntVariationCtx, flag, c.allContexts(ctx, contexts))
}

// IntFlagWithError uses the fallback only when no flag value is available.
// Invalid values and other evaluation failures are returned as errors.
func (c *Client) IntFlagWithError(ctx context.Context, flag IntFlag, contexts ...ldcontext.Context) (int, error) {
	if c.ld == nil {
		return flag.Fallback(), nil
	}
	value, detail, err := c.ld.IntVariationDetailCtx(ctx, flag.Key(), mergeContexts(ctx, c.allContexts(ctx, contexts)), flag.Fallback())
	switch detail.Reason.GetErrorKind() {
	case ldreason.EvalErrorFlagNotFound, ldreason.EvalErrorClientNotReady:
		return flag.Fallback(), nil
	}
	if err != nil {
		return 0, fmt.Errorf("evaluate integer flag %q: %w", flag.Key(), err)
	}
	if detail.Reason.GetKind() == ldreason.EvalReasonError {
		return 0, fmt.Errorf("evaluate integer flag %q: %s", flag.Key(), detail.Reason.GetErrorKind())
	}
	if !detail.Value.IsInt() {
		return 0, fmt.Errorf("flag %q must be an integer", flag.Key())
	}

	return value, nil
}

// IntFlagOverride returns the flag's value and whether LaunchDarkly served it.
// On a failed evaluation — a client in LaunchDarkly's offline mode or not yet
// initialised, a value of the wrong type — the value is the fallback and the
// second result is false, so a caller can tell a served value from one equal
// to the fallback. A key the environment does not define counts as served at
// the fallback: nobody has chosen a value, which is what the fallback stands
// for, and an environment that never creates the flag is not failing. The
// keyless client (the package's offline store) serves every NewIntFlag flag
// at its fallback, which counts as served too.
func (c *Client) IntFlagOverride(ctx context.Context, flag IntFlag, contexts ...ldcontext.Context) (int, bool) {
	if c.ld == nil {
		return flag.Fallback(), false
	}
	value, detail, err := c.ld.IntVariationDetailCtx(ctx, flag.Key(), mergeContexts(ctx, c.allContexts(ctx, contexts)), flag.Fallback())
	if detail.Reason.GetErrorKind() == ldreason.EvalErrorFlagNotFound {
		return flag.Fallback(), true
	}

	return value, err == nil && !detail.IsDefaultValue()
}

func (c *Client) StringFlag(ctx context.Context, flag StringFlag, contexts ...ldcontext.Context) string {
	return getFlag(ctx, c.ld, c.ld.StringVariationCtx, flag, c.allContexts(ctx, contexts))
}

type typedFlag[T any] interface {
	Key() string
	Fallback() T
}

func getFlag[T any](
	ctx context.Context,
	ld *ldclient.LDClient,
	getFromLaunchDarkly func(ctx context.Context, key string, context ldcontext.Context, defaultVal T) (T, error),
	flag typedFlag[T],
	contexts []ldcontext.Context,
) T {
	if ld == nil {
		logger.L().Info(ctx, "LaunchDarkly client is not initialized, returning fallback")

		return flag.Fallback()
	}

	// A client kept on an initialization timeout fails every evaluation with
	// ErrClientNotInitialized until its stream connects. That is expected, so
	// it serves the fallback without a warning on each read.
	if !ld.Initialized() {
		return flag.Fallback()
	}

	value, err := getFromLaunchDarkly(ctx, flag.Key(), mergeContexts(ctx, contexts), flag.Fallback())
	if err != nil {
		logger.L().Warn(ctx, "error evaluating flag", zap.Error(err), zap.String("flag", flag.Key()))
	}

	return value
}

func (c *Client) Close(ctx context.Context) error {
	if c.ld == nil {
		return nil
	}

	err := c.ld.Close()
	if err != nil {
		logger.L().Error(ctx, "Error during launch-darkly client shutdown", zap.Error(err))

		return err
	}

	return nil
}

// allContexts puts the process identity before the contexts of one
// evaluation: mergeContexts lets a later context of the same kind win, so a
// caller that names a service explicitly overrides the process default.
func (c *Client) allContexts(ctx context.Context, contexts []ldcontext.Context) []ldcontext.Context {
	var all []ldcontext.Context
	if c.deploymentEnvironment != "" {
		all = append(all, DeploymentEnvironmentContext(c.deploymentEnvironment))
	}
	if c.serviceName != "" {
		all = append(all, ServiceContext(c.serviceName))
	}
	for _, provider := range c.contextProviders {
		all = append(all, provider(ctx))
	}

	return append(all, contexts...)
}
