package handlers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/idna"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/fcgate"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	apiorch "github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/networktransform"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	InstanceIDPrefix            = "i"
	metricTemplateAlias         = metrics.MetricPrefix + "template.alias"
	metricMemoryOverride        = metrics.MetricPrefix + "memory_override"
	metricPauseMode             = metrics.MetricPrefix + "pause.mode"
	metricResumeGate            = metrics.MetricPrefix + "resume.gate"
	minEnvdVersionForSecureFlag = "0.2.0" // Minimum version of envd that supports secure flag

	// Network validation error messages
	ErrMsgDomainsRequireBlockAll = "When specifying allowed domains in allow out, you must include 'ALL_TRAFFIC' in deny out to block all other traffic."

	maxHTTPSPorts = 128
	// maxDNSServers caps dnsServers. Resolvers only use the first few anyway.
	maxDNSServers                     = 10
	maxNetworkRuleTransformsPerDomain = 1
	maxNetworkRuleDomainLen           = 128
	maxNetworkRuleHeaderNameLen       = 64
	maxNetworkRuleHeadersPerRule      = 20

	maxIamTokens = 5
)

// PostSandboxes creates a sandbox with secured envd access; the body's secure field is ignored.
func (a *APIStore) PostSandboxes(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.PostSandboxesJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	body.Secure = new(true)
	sbx, apiErr := a.createSandbox(c, body, sandbox.SandboxTimeoutDefault)
	if apiErr != nil {
		// Only POST /v2/sandboxes exposes Should-Retry.
		apiErr.Retryable = apierrors.UndefinedRetry
		apierrors.SendAPIError(c, apiErr)

		return
	}
	c.JSON(http.StatusCreated, sbx)
}

// PostV2Sandboxes creates a sandbox with secured envd access; the request has no secure field to opt out.
func (a *APIStore) PostV2Sandboxes(c *gin.Context, _ api.PostV2SandboxesParams) {
	ctx := c.Request.Context()

	var body api.PostV2SandboxesJSONRequestBody
	// ShouldBind, unlike Bind, leaves the response uncommitted so Should-Retry can still be set.
	if err := c.ShouldBind(&body); err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("Error when parsing request: %s", err),
			Retryable: apierrors.NoRetry,
		})

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	fingerprintBody := body
	// Keep pre-upgrade idempotency keys valid for projects without a saved default.
	if n := body.Network; n != nil && n.AllowPublicTraffic == nil && auth.MustGetTeamInfo(c).ProjectSettings.SandboxSettings.DefaultAllowPublicTraffic == nil {
		network := *n
		network.AllowPublicTraffic = new(true)
		fingerprintBody.Network = &network
	}

	idempotency.Execute(c, fingerprintBody, func() *apierrors.APIError {
		sbx, apiErr := a.createSandbox(c, newSandboxFromV2(body), sandbox.SandboxTimeoutDefaultV2)
		if apiErr != nil {
			return apiErr
		}
		c.JSON(http.StatusCreated, sbx)

		return nil
	})
}

func newSandboxFromV2(body api.NewSandboxV2) api.NewSandbox {
	secure := true

	return api.NewSandbox{
		TemplateID:          body.TemplateID,
		Timeout:             body.Timeout,
		CpuCount:            body.CpuCount,
		AutoPause:           body.AutoPause,
		AutoPauseMemory:     body.AutoPauseMemory,
		AutoResume:          body.AutoResume,
		Secure:              &secure,
		AllowInternetAccess: body.AllowInternetAccess,
		Network:             body.Network,
		Metadata:            body.Metadata,
		EnvVars:             body.EnvVars,
		Mcp:                 body.Mcp,
		Iam:                 body.Iam,
		VolumeMounts:        body.VolumeMounts,
	}
}

// createSandbox runs the shared create flow; defaultTimeout applies when the body omits timeout.
func (a *APIStore) createSandbox(c *gin.Context, body api.NewSandbox, defaultTimeout time.Duration) (*api.Sandbox, *api.APIError) {
	ctx := c.Request.Context()

	// Get team from context, use TeamContextKey
	teamInfo := auth.MustGetTeamInfo(c)

	c.Set("teamID", teamInfo.Team.ID.String())

	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	telemetry.ReportEvent(ctx, "Parsed body")

	identifier, tag, err := id.ParseName(body.TemplateID)
	if err != nil {
		telemetry.ReportError(ctx, "invalid template reference", err)

		return nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("Invalid template reference: %s", err),
			Retryable: apierrors.NoRetry,
		}
	}

	clusterID := clusters.WithClusterFallback(teamInfo.Team.ClusterID)
	aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, teamInfo.Team.Slug)
	if err != nil {
		apiErr := templatecache.ErrorToAPIError(err, identifier)
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "error when resolving template alias", apiErr.Err, attribute.String("identifier", identifier))
		apiErr.Retryable = apierrors.NoRetry
		if apiErr.Code == http.StatusInternalServerError {
			apiErr.Retryable = apierrors.CanRetry
		}

		return nil, apiErr
	}

	env, build, err := a.templateCache.Get(ctx, aliasInfo.TemplateID, tag, teamInfo.Team.ID, clusterID)
	if err != nil {
		visible := aliasInfo.TeamID == teamInfo.Team.ID
		if metadata, mErr := a.templateCache.GetMetadata(ctx, aliasInfo.TemplateID); mErr == nil {
			visible = visible || metadata.Public
		}

		ref := templatecache.TemplateRef{
			Identifier: aliasInfo.MatchedIdentifier,
			Visible:    visible,
		}

		apiErr := ref.APIError(err)
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "error when getting template", apiErr.Err, telemetry.WithTemplateID(aliasInfo.TemplateID))
		apiErr.Retryable = apierrors.NoRetry
		if apiErr.Code == http.StatusInternalServerError {
			apiErr.Retryable = apierrors.CanRetry
		}

		return nil, apiErr
	}

	telemetry.ReportEvent(ctx, "Checked team access")

	c.Set("envID", env.TemplateID)
	setTemplateNameMetric(ctx, c, a.featureFlags, env.TemplateID, env.Names)

	sandboxID := InstanceIDPrefix + id.Generate()

	c.Set("instanceID", sandboxID)

	sbxlogger.E(&sbxlogger.SandboxMetadata{
		SandboxID:  sandboxID,
		TemplateID: env.TemplateID,
		TeamID:     teamInfo.Team.ID.String(),
	}).Debug(ctx, "Started creating sandbox")

	alias := firstAlias(env.Aliases)
	telemetry.SetAttributes(ctx,
		telemetry.WithSandboxID(sandboxID),
		telemetry.WithTemplateID(env.TemplateID),
		telemetry.WithBuildID(build.ID.String()),
		attribute.String("env.alias", alias),
		telemetry.WithKernelVersion(build.KernelVersion),
		telemetry.WithFirecrackerVersion(build.FirecrackerVersion),
	)

	autoPause := sharedUtils.DerefOrDefault(body.AutoPause, sandbox.AutoPauseDefault)
	// autoPauseMemory defaults to true (full memory snapshot). When false, a
	// timeout auto-pause takes a filesystem-only snapshot (cold-boots on resume).
	autoPauseFilesystemOnly := !sharedUtils.DerefOrDefault(body.AutoPauseMemory, true)
	envVars := sharedUtils.DerefOrDefault(body.EnvVars, nil)
	mcp := sharedUtils.DerefOrDefault(body.Mcp, nil)
	metadata := sharedUtils.DerefOrDefault(body.Metadata, nil)
	apiVolumeMounts := sharedUtils.DerefOrDefault(body.VolumeMounts, nil)

	timeout, apiErr := validateAndParseTimeoutWithDefault(body.Timeout, teamInfo.Limits.MaxLengthHours, defaultTimeout)
	if apiErr != nil {
		apiErr.Retryable = apierrors.NoRetry

		return nil, apiErr
	}

	autoResume := buildAutoResumeConfig(body.AutoResume)
	if autoResume != nil {
		minAutoResumeTimeout := time.Duration(a.featureFlags.IntFlag(ctx, featureflags.MinAutoResumeTimeoutSeconds)) * time.Second
		autoResume.Timeout = calculateTimeoutSeconds(timeout, minAutoResumeTimeout, teamInfo)
	}

	// autoPauseMemory only controls the snapshot kind of a timeout auto-pause, so
	// it is meaningless without autoPause; reject it rather than silently storing
	// a no-op policy.
	if autoPauseFilesystemOnly && !autoPause {
		return nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "autoPauseMemory=false only applies when autoPause is true.",
			Retryable: apierrors.NoRetry,
		}
	}

	// A filesystem-only auto-pause produces a snapshot that traffic cannot
	// auto-resume (it must be resumed explicitly), so the two are incompatible.
	if autoPauseFilesystemOnly && autoResume != nil && autoResume.Policy == types.SandboxAutoResumeAny {
		return nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "autoPauseMemory=false (filesystem-only auto-pause) cannot be combined with autoResume: a filesystem-only snapshot cannot be auto-resumed by traffic and must be resumed explicitly.",
			Retryable: apierrors.NoRetry,
		}
	}

	// A filesystem-only auto-pause also needs the sandbox's Firecracker
	// release to carry the feature. No record exists yet, so fcgate checks
	// the declared build version and falls back to the same flag resolution
	// the orchestrator will apply at start — refusing up front instead of
	// storing a policy the timeout eviction would have to degrade later.
	// (The evictor still degrades gracefully if resolution shifts between
	// create and timeout.)
	if autoPauseFilesystemOnly && !fcgate.SupportsFilesystemSnapshotsDeclared(ctx, a.featureFlags, build.FirecrackerVersion) {
		return nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("autoPauseMemory=false requires a Firecracker release with filesystem-only snapshot support; this template's version is %q. Rebuild the template on a current release or omit autoPauseMemory.", build.FirecrackerVersion),
			Retryable: apierrors.NoRetry,
		}
	}

	var envdAccessToken *string = nil
	if body.Secure != nil && *body.Secure == true {
		accessToken, tokenErr := a.getEnvdAccessToken(build.EnvdVersion, sandboxID)
		if tokenErr != nil {
			telemetry.ReportError(ctx, "secure envd access token error", tokenErr.Err, telemetry.WithSandboxID(sandboxID), telemetry.WithBuildID(build.ID.String()))

			return nil, tokenErr
		}

		envdAccessToken = &accessToken
	}

	iamCfg, iamErr := buildSandboxIam(body.Iam)
	if iamErr != nil {
		telemetry.ReportError(ctx, "invalid iam config", iamErr.Err, telemetry.WithSandboxID(sandboxID))
		iamErr.Retryable = apierrors.NoRetry

		return nil, iamErr
	}

	if iamCfg != nil && !a.featureFlags.BoolFlag(ctx, featureflags.SandboxIamTokensFlag, featureflags.TeamContext(teamInfo.Team.ID.String())) {
		return nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "Sandbox IAM workload tokens are not available for your team.",
			Retryable: apierrors.NoRetry,
		}
	}

	allowInternetAccess := body.AllowInternetAccess

	var network *types.SandboxNetworkConfig
	if n := body.Network; n != nil {
		limits := a.networkRuleLimits(ctx, teamInfo.Team.ID)
		if err := validateNetworkConfig(ctx, a.featureFlags, teamInfo.Team.ID, sharedUtils.DerefOrDefault(build.EnvdVersion, ""), limits, n); err != nil {
			telemetry.ReportError(ctx, "invalid network config", err.Err, telemetry.WithSandboxID(sandboxID))
			err.Retryable = apierrors.NoRetry

			return nil, err
		}

		network = sandboxNetworkConfig(n, teamInfo.ProjectSettings.SandboxSettings)

		if ep := n.EgressProxy; ep != nil {
			if !a.featureFlags.BoolFlag(ctx, featureflags.BYOPProxyEnabledFlag) {
				telemetry.ReportEvent(ctx, "egressProxy rejected by BYOPProxyEnabledFlag")

				return nil, &api.APIError{
					Code:      http.StatusForbidden,
					ClientMsg: "Egress proxy (network.egressProxy) is not enabled for this team.",
					Retryable: apierrors.NoRetry,
				}
			}

			canonical, err := sandbox_network.ValidateEgressProxy(ctx, apiEgressProxyToConfig(ep), nil)
			if err != nil {
				telemetry.ReportError(ctx, "invalid egress proxy config", err, telemetry.WithSandboxID(sandboxID))

				return nil, &api.APIError{
					Code:      http.StatusBadRequest,
					ClientMsg: fmt.Sprintf("Invalid egress proxy config: %s", err),
					Retryable: apierrors.NoRetry,
				}
			}

			apiorch.ApplyValidatedEgressProxy(network.Egress, canonical)
		}
	} else {
		network = sandboxNetworkConfig(nil, teamInfo.ProjectSettings.SandboxSettings)
	}

	sbxVolumeMounts, err := convertAPIVolumesToOrchestratorVolumes(
		ctx, a.sqlcDB, a.featureFlags, teamInfo.ID, apiVolumeMounts, build,
	)
	if err != nil {
		if errors.Is(err, errVolumesNotSupported) || errors.Is(err, errNoEnvdVersion) {
			return nil, &api.APIError{Code: http.StatusBadRequest, ClientMsg: err.Error(), Retryable: apierrors.NoRetry}
		}

		if errors.Is(err, ErrVolumeMountsDisabled) {
			return nil, &api.APIError{Code: http.StatusBadRequest, ClientMsg: "Volume mounts are not enabled.", Retryable: apierrors.NoRetry}
		}

		if vne, ok := errors.AsType[InvalidVolumeMountsError](err); ok {
			return nil, &api.APIError{Code: http.StatusBadRequest, ClientMsg: vne.Error(), Retryable: apierrors.NoRetry}
		}

		telemetry.ReportError(ctx, "failed to convert volume mounts", err, telemetry.WithSandboxID(sandboxID))

		return nil, &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "failed to convert volume mounts", Retryable: apierrors.CanRetry}
	}

	getSandboxData := func(_ context.Context) (apiorch.SandboxMetadata, *api.APIError) {
		// The data can't be influenced by action on the same sandbox as other operations,
		// so it's safe to reuse the data
		return apiorch.SandboxMetadata{
			Metadata:                metadata,
			EnvVars:                 envVars,
			Build:                   *build,
			AllowInternetAccess:     allowInternetAccess,
			Network:                 network,
			Alias:                   alias,
			TemplateID:              env.TemplateID,
			BaseTemplateID:          env.TemplateID,
			AutoPause:               autoPause,
			AutoPauseFilesystemOnly: autoPauseFilesystemOnly,
			AutoResume:              autoResume,
			VolumeMounts:            sbxVolumeMounts,
			EnvdAccessToken:         envdAccessToken,
			Iam:                     iamCfg,
		}, nil
	}

	sbx, createErr := a.startSandbox(
		ctx,
		sandboxID,
		timeout,
		teamInfo,
		getSandboxData,
		&c.Request.Header,
		false,
		false,
		mcp,
		apiorch.WithCPUCount(body.CpuCount),
	)
	if createErr != nil {
		return nil, createErr
	}

	if n := body.Network; n != nil && n.Rules != nil && len(*n.Rules) > 0 {
		domains := make([]string, 0, len(*n.Rules))
		for domain := range *n.Rules {
			domains = append(domains, domain)
		}

		a.posthog.CreateAnalyticsSandboxEvent(ctx, teamInfo.Team.ID.String(), sandboxID, "sandbox with network transform rules created",
			a.posthog.GetPackageToPosthogProperties(&c.Request.Header).
				Set("sandbox_id", sandboxID).
				Set("domains", domains),
		)
	}

	return sbx, nil
}

// Preserve the legacy nil network for public creates without network options.
// Persist private ingress so pause and resume retain it.
func sandboxNetworkConfig(n *api.SandboxNetworkConfig, settings types.SandboxSettings) *types.SandboxNetworkConfig {
	projectAllowsPublicTraffic := sharedUtils.DerefOrDefault(settings.DefaultAllowPublicTraffic, types.AllowPublicAccessDefault)
	if n == nil {
		if projectAllowsPublicTraffic {
			return nil
		}

		return &types.SandboxNetworkConfig{
			Ingress: &types.SandboxNetworkIngressConfig{AllowPublicAccess: new(false)},
		}
	}

	return &types.SandboxNetworkConfig{
		Ingress: &types.SandboxNetworkIngressConfig{
			AllowPublicAccess: new(sharedUtils.DerefOrDefault(n.AllowPublicTraffic, projectAllowsPublicTraffic)),
			MaskRequestHost:   n.MaskRequestHost,
			HTTPSPorts:        sharedUtils.DerefOrDefault(n.HttpsPorts, nil),
		},
		Egress: &types.SandboxNetworkEgressConfig{
			AllowedAddresses: sharedUtils.DerefOrDefault(n.AllowOut, nil),
			DeniedAddresses:  sharedUtils.DerefOrDefault(n.DenyOut, nil),
			Rules:            apiRulesToDBRules(n.Rules),
			DnsServers:       n.DnsServers,
		},
	}
}

// iamTokenTypeJWTSVID is the only workload token type accepted in this version.
const iamTokenTypeJWTSVID = "JWT-SVID"

// buildSandboxIam validates the optional iam.tokens map and returns the sandbox
// workload identity configuration to persist. An absent or empty map means
// workload identity is disabled and returns nil. The audience is preserved
// exactly as received.
func buildSandboxIam(iam *api.SandboxIam) (*types.SandboxIam, *api.APIError) {
	if iam == nil || iam.Tokens == nil || len(*iam.Tokens) == 0 {
		return nil, nil
	}

	reject := func(field, msg string) *api.APIError {
		return &api.APIError{
			Code:      http.StatusBadRequest,
			Err:       fmt.Errorf("%s: %s", field, msg),
			ClientMsg: fmt.Sprintf("%s: %s", field, msg),
		}
	}

	if len(*iam.Tokens) > maxIamTokens {
		return nil, reject("iam.tokens", fmt.Sprintf("too many tokens: %d (max %d)", len(*iam.Tokens), maxIamTokens))
	}

	tokens := make(map[string]types.SandboxIamToken, len(*iam.Tokens))
	for name, def := range *iam.Tokens {
		if name == "" {
			return nil, reject("iam.tokens", "token name must not be empty")
		}

		if def.Audience == "" {
			return nil, reject(fmt.Sprintf("iam.tokens.%s.audience", name), "audience is required")
		}

		if def.TokenType != iamTokenTypeJWTSVID {
			return nil, reject(fmt.Sprintf("iam.tokens.%s.tokenType", name), fmt.Sprintf("only %q is supported", iamTokenTypeJWTSVID))
		}

		tokens[name] = types.SandboxIamToken{Audience: def.Audience, TokenType: def.TokenType}
	}

	return &types.SandboxIam{Tokens: tokens}, nil
}

func buildAutoResumeConfig(autoResume *api.SandboxAutoResumeConfig) *types.SandboxAutoResumeConfig {
	if autoResume == nil {
		return nil
	}

	policy := types.SandboxAutoResumeOff
	if autoResume.Enabled {
		policy = types.SandboxAutoResumeAny
	}

	return &types.SandboxAutoResumeConfig{
		Policy: policy,
	}
}

func dedupeVolumeNames(items []api.SandboxVolumeMount) []string {
	itemsSet := make(map[string]struct{}, len(items))
	for _, item := range items {
		itemsSet[item.Name] = struct{}{}
	}

	results := make([]string, 0, len(itemsSet))
	for name := range itemsSet {
		results = append(results, name)
	}

	return results
}

var ErrVolumeMountsDisabled = errors.New("volume mounts are not enabled")

type featureFlagsClient interface {
	BoolFlag(ctx context.Context, flagName featureflags.BoolFlag, contexts ...ldcontext.Context) bool
}

type InvalidMount struct {
	Index  int
	Reason string
}

type InvalidVolumeMountsError struct {
	InvalidMounts []InvalidMount
}

func (im InvalidVolumeMountsError) Error() string {
	var errs []string

	for _, mount := range im.InvalidMounts {
		errs = append(errs, fmt.Sprintf("\t- volume mount #%d: %s", mount.Index, mount.Reason))
	}

	return fmt.Sprintf("invalid mounts:\n%s", strings.Join(errs, "\n"))
}

var errVolumesNotSupported = errors.New("volumes are not supported")

var errNetworkRulesNotSupported = errors.New("network transform rules are not supported")

var errNoEnvdVersion = errors.New("template must be rebuilt: envd version is not set")

const minEnvdVersionForNetworkRules = "0.5.13"

const minEnvdVersionForVolumes = "0.5.14"

// checkEnvdVersionRequirement returns errNoEnvdVersion when buildVersion is empty, a parse
// error when the version string is invalid, or a wrapped featureErr when the build does not
// meet requiredMinVersion. The caller decides how to convert the returned error into an API
// response so each call-site can produce its own status code / message.
func checkEnvdVersionRequirement(buildVersion, requiredMinVersion string, featureErr error) error {
	if buildVersion == "" {
		return errNoEnvdVersion
	}

	ok, err := sharedUtils.IsGTEVersion(buildVersion, requiredMinVersion)
	if err != nil {
		return fmt.Errorf("invalid envd version %q: %w", buildVersion, err)
	}

	if !ok {
		return fmt.Errorf("%w; template must be rebuilt. Template envd version is %s, must be at least %s", featureErr, buildVersion, requiredMinVersion)
	}

	return nil
}

func convertAPIVolumesToOrchestratorVolumes(ctx context.Context, sqlClient *sqlcdb.Client, featureFlags featureFlagsClient, teamID uuid.UUID, volumeMounts []api.SandboxVolumeMount, env *queries.EnvBuild) ([]*orchestrator.SandboxVolumeMount, error) {
	// are any volumes configured?
	if len(volumeMounts) == 0 {
		return []*orchestrator.SandboxVolumeMount{}, nil // only b/c you should never return (nil, nil)
	}

	// are volumes enabled?
	if !featureFlags.BoolFlag(ctx, featureflags.PersistentVolumesFlag) {
		return nil, ErrVolumeMountsDisabled
	}

	// does your envd version support volumes?
	envdVersion := sharedUtils.DerefOrDefault(env.EnvdVersion, "")
	if err := checkEnvdVersionRequirement(envdVersion, minEnvdVersionForVolumes, errVolumesNotSupported); err != nil {
		return nil, err
	}

	// get volumes from the database
	dbVolumesMap, err := getDBVolumesMap(ctx, sqlClient, teamID, volumeMounts)
	if err != nil {
		return nil, fmt.Errorf("failed to get db volumes map: %w", err)
	}

	invalidVolumeMounts := make([]InvalidMount, 0)
	results := make([]*orchestrator.SandboxVolumeMount, 0, len(volumeMounts))

	usedPaths := make(map[string]struct{})

	for index, v := range volumeMounts {
		actualVolume, ok := dbVolumesMap[v.Name]
		if !ok {
			invalidVolumeMounts = append(invalidVolumeMounts, InvalidMount{Index: index, Reason: fmt.Sprintf("volume '%s' not found", v.Name)})

			continue
		}

		if reason, ok := isValidMountPath(v.Path); !ok {
			invalidVolumeMounts = append(invalidVolumeMounts, InvalidMount{Index: index, Reason: reason})

			continue
		}

		if _, ok := usedPaths[v.Path]; ok {
			invalidVolumeMounts = append(invalidVolumeMounts, InvalidMount{Index: index, Reason: fmt.Sprintf("path '%s' is already used", v.Path)})

			continue
		}
		usedPaths[v.Path] = struct{}{}

		results = append(results, &orchestrator.SandboxVolumeMount{
			Id:   actualVolume.ID.String(),
			Path: v.Path,
			Type: actualVolume.VolumeType,
			Name: actualVolume.Name,
		})
	}

	if len(invalidVolumeMounts) > 0 {
		return nil, InvalidVolumeMountsError{InvalidMounts: invalidVolumeMounts}
	}

	return results, nil
}

func isValidMountPath(path string) (string, bool) {
	if path == "" {
		return "path cannot be empty", false
	}

	if !filepath.IsAbs(path) {
		return "path must be absolute", false
	}

	if filepath.Clean(path) != path {
		return "path must not contain any '.' or '..' components", false
	}

	return "", true
}

func getDBVolumesMap(ctx context.Context, sqlcDB *sqlcdb.Client, teamID uuid.UUID, volumeMounts []api.SandboxVolumeMount) (map[string]queries.Volume, error) {
	dbVolumes, err := sqlcDB.GetVolumesByName(ctx, queries.GetVolumesByNameParams{
		TeamID:      teamID,
		VolumeNames: dedupeVolumeNames(volumeMounts),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get volumes from db: %w", err)
	}

	dbVolumesMap := make(map[string]queries.Volume, len(dbVolumes))
	for _, v := range dbVolumes {
		dbVolumesMap[v.Name] = v
	}

	return dbVolumesMap, nil
}

func (a *APIStore) getEnvdAccessToken(envdVersion *string, sandboxID string) (string, *api.APIError) {
	if envdVersion == nil {
		return "", &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "You need to re-build template to allow using secured access. Please visit https://e2b.dev/docs/sandbox/secured-access for more information.",
			Err:       errors.New("envd version is required during envd access token creation"),
			Retryable: apierrors.NoRetry,
		}
	}

	// check if the envd version is at least 0.2.0
	ok, err := sharedUtils.IsGTEVersion(*envdVersion, minEnvdVersionForSecureFlag)
	if err != nil {
		return "", &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "error during envd version check",
			Err:       err,
			Retryable: apierrors.NoRetry,
		}
	}
	if !ok {
		return "", &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "Template is not compatible with secured access. Please visit https://e2b.dev/docs/sandbox/secured-access for more information.",
			Err:       errors.New("envd version is not supported for secure flag"),
			Retryable: apierrors.NoRetry,
		}
	}

	key, err := a.accessTokenGenerator.GenerateEnvdAccessToken(sandboxID)
	if err != nil {
		return "", &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "error during sandbox access token generation",
			Err:       err,
			Retryable: apierrors.CanRetry,
		}
	}

	return key, nil
}

func setTemplateNameMetric(ctx context.Context, c *gin.Context, ff *featureflags.Client, templateID string, names []string) {
	trackedTemplates := featureflags.GetTrackedTemplatesSet(ctx, ff)

	// Check template ID first
	if _, exists := trackedTemplates[templateID]; exists {
		c.Set(metricTemplateAlias, templateID)

		return
	}

	// Then check names (namespace/alias format when namespaced)
	for _, name := range names {
		if _, exists := trackedTemplates[name]; exists {
			c.Set(metricTemplateAlias, name)

			return
		}
	}

	// Fallback to 'other' if no match of tracked templates found
	c.Set(metricTemplateAlias, "other")
}

func firstAlias(aliases []string) string {
	if len(aliases) == 0 {
		return ""
	}

	return aliases[0]
}

func splitHostPortOptional(hostport string) (host string, port string, err error) {
	host, port, err = net.SplitHostPort(hostport)
	if err != nil {
		if strings.Contains(err.Error(), "missing port") {
			return hostport, "", nil
		}

		return "", "", err
	}

	return host, port, nil
}

func apiRulesToDBRules(apiRules *map[string][]api.SandboxNetworkRule) map[string][]types.SandboxNetworkRule {
	if apiRules == nil {
		return nil
	}

	dbRules := make(map[string][]types.SandboxNetworkRule, len(*apiRules))
	for domain, rules := range *apiRules {
		dbDomainRules := make([]types.SandboxNetworkRule, 0, len(rules))
		for _, r := range rules {
			dbRule := types.SandboxNetworkRule{}

			if r.Transform != nil {
				dbRule.Transform = &types.SandboxNetworkTransform{
					Headers: sharedUtils.DerefOrDefault(r.Transform.Headers, nil),
				}
			}

			dbDomainRules = append(dbDomainRules, dbRule)
		}

		dbRules[strings.ToLower(domain)] = dbDomainRules
	}

	return dbRules
}

// apiEgressProxyToConfig maps the API egress proxy object onto the validation
// input. It does no checking of its own; ValidateEgressProxy owns that.
func apiEgressProxyToConfig(ep *api.SandboxEgressProxyConfig) *sandbox_network.EgressProxyConfig {
	if ep == nil {
		return nil
	}

	cfg := &sandbox_network.EgressProxyConfig{
		Address:  ep.Address,
		Username: sharedUtils.DerefOrDefault(ep.Username, ""),
		Password: sharedUtils.DerefOrDefault(ep.Password, ""),
	}

	if t := ep.Tls; t != nil {
		cfg.TLS = &sandbox_network.EgressProxyTLSConfig{
			Enabled:    t.Enabled,
			ServerName: sharedUtils.DerefOrDefault(t.ServerName, ""),
			CACert:     sharedUtils.DerefOrDefault(t.CaCert, ""),
		}
	}

	return cfg
}

func validateNetworkConfig(ctx context.Context, featureFlags featureFlagsClient, teamID uuid.UUID, envdVersion string, limits networkRuleLimits, network *api.SandboxNetworkConfig) *api.APIError {
	if network == nil {
		return nil
	}

	if httpsPorts := network.HttpsPorts; httpsPorts != nil {
		if len(*httpsPorts) > maxHTTPSPorts {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("too many HTTPS ports: %d (max %d)", len(*httpsPorts), maxHTTPSPorts),
				ClientMsg: fmt.Sprintf("HTTPS ports can have at most %d entries.", maxHTTPSPorts),
			}
		}

		seen := make(map[uint32]struct{}, len(*httpsPorts))
		for _, port := range *httpsPorts {
			if port == 0 || port > 65535 {
				return &api.APIError{
					Code:      http.StatusBadRequest,
					Err:       fmt.Errorf("invalid HTTPS port %d", port),
					ClientMsg: fmt.Sprintf("HTTPS port must be between 1 and 65535: %d", port),
				}
			}
			if port == uint32(consts.DefaultEnvdServerPort) {
				return &api.APIError{
					Code:      http.StatusBadRequest,
					Err:       errors.New("envd port cannot use HTTPS backend routing"),
					ClientMsg: fmt.Sprintf("HTTPS backend routing is not supported for reserved port %d", port),
				}
			}
			if _, ok := seen[port]; ok {
				return &api.APIError{
					Code:      http.StatusBadRequest,
					Err:       fmt.Errorf("duplicate HTTPS port %d", port),
					ClientMsg: fmt.Sprintf("HTTPS port %d is specified more than once.", port),
				}
			}

			seen[port] = struct{}{}
		}
	}

	if maskRequestHost := network.MaskRequestHost; maskRequestHost != nil {
		hostname, _, err := splitHostPortOptional(*maskRequestHost)
		if err != nil {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("invalid mask request host (%s): %w", *maskRequestHost, err),
				ClientMsg: fmt.Sprintf("mask request host is not valid: %s", *maskRequestHost),
			}
		}

		host, err := idna.Display.ToASCII(hostname)
		if err != nil {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("invalid mask request host (%s): %w", *maskRequestHost, err),
				ClientMsg: fmt.Sprintf("mask request host is not valid: %s", *maskRequestHost),
			}
		}

		if !strings.EqualFold(host, hostname) {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("mask request host is not ASCII (%s)!=(%s)", host, hostname),
				ClientMsg: fmt.Sprintf("mask request host '%s' is not ASCII. Please use ASCII characters only.", hostname),
			}
		}
	}

	denyOut := sharedUtils.DerefOrDefault(network.DenyOut, nil)
	allowOut := sharedUtils.DerefOrDefault(network.AllowOut, nil)

	if err := validateEgressRules(allowOut, denyOut); err != nil {
		return err
	}

	if err := validateDNSServers(network.DnsServers); err != nil {
		return err
	}

	return validateNetworkRules(ctx, featureFlags, teamID, envdVersion, limits, network.Rules)
}

// validateEgressRules validates egress allow/deny rules:
// - denyOut entries must be valid IPs or CIDRs (not domains)
// - allowOut entries must be valid IPs, CIDRs, or domain names
// - when allowOut contains domains, denyOut must include 0.0.0.0/0
func validateEgressRules(allowOut, denyOut []string) *api.APIError {
	for _, cidr := range denyOut {
		if !sandbox_network.IsSpecifiedIPOrCIDR(cidr) {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("invalid denied CIDR %s", cidr),
				ClientMsg: fmt.Sprintf("invalid denied CIDR %s", cidr),
			}
		}
	}

	if len(allowOut) > 0 {
		allowedAddresses, allowedDomains := sandbox_network.ParseAddressesAndDomains(allowOut)

		for _, addr := range allowedAddresses {
			if !sandbox_network.IsSpecifiedIPOrCIDR(addr) {
				return &api.APIError{
					Code:      http.StatusBadRequest,
					Err:       fmt.Errorf("invalid allowed address %s", addr),
					ClientMsg: fmt.Sprintf("invalid allowed address %s", addr),
				}
			}
		}
		hasBlockAll := slices.Contains(denyOut, sandbox_network.AllInternetTrafficCIDR)

		if len(allowedDomains) > 0 && !hasBlockAll {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       errors.New("allow out contains domains but deny out is missing 0.0.0.0/0 (ALL_TRAFFIC)"),
				ClientMsg: ErrMsgDomainsRequireBlockAll,
			}
		}
	}

	return nil
}

// validateDNSServers checks that every DNS server is a plain IPv4 address.
func validateDNSServers(dnsServers *[]string) *api.APIError {
	servers := sharedUtils.DerefOrDefault(dnsServers, nil)
	if len(servers) > maxDNSServers {
		return &api.APIError{
			Code:      http.StatusBadRequest,
			Err:       fmt.Errorf("too many DNS servers: %d (max %d)", len(servers), maxDNSServers),
			ClientMsg: fmt.Sprintf("DNS servers can have at most %d entries.", maxDNSServers),
		}
	}

	for _, server := range servers {
		addr, err := netip.ParseAddr(server)
		if err != nil || !addr.Is4() || addr.IsUnspecified() {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("invalid DNS server %q", server),
				ClientMsg: fmt.Sprintf("invalid DNS server %s: must be an IPv4 address", server),
			}
		}
	}

	return nil
}

// networkRuleLimits holds the per-team network rule limits read from feature flags.
type networkRuleLimits struct {
	MaxDomains        int
	MaxHeaderValueLen int
}

func (a *APIStore) networkRuleLimits(ctx context.Context, teamID uuid.UUID) networkRuleLimits {
	teamCtx := featureflags.TeamContext(teamID.String())

	return networkRuleLimits{
		MaxDomains:        a.featureFlags.IntFlag(ctx, featureflags.MaxNetworkRuleDomains, teamCtx),
		MaxHeaderValueLen: a.featureFlags.IntFlag(ctx, featureflags.MaxNetworkRuleHeaderValueLen, teamCtx),
	}
}

func validateNetworkRules(ctx context.Context, featureFlags featureFlagsClient, teamID uuid.UUID, envdVersion string, limits networkRuleLimits, rules *map[string][]api.SandboxNetworkRule) *api.APIError {
	if rules == nil {
		return nil
	}

	if !featureFlags.BoolFlag(ctx, featureflags.NetworkTransformRulesFlag, featureflags.TeamContext(teamID.String())) {
		return &api.APIError{
			Code:      http.StatusBadRequest,
			Err:       fmt.Errorf("team %s is not allowed to use network transform rules", teamID),
			ClientMsg: "Network transform rules are not available for your team.",
		}
	}

	if err := checkEnvdVersionRequirement(envdVersion, minEnvdVersionForNetworkRules, errNetworkRulesNotSupported); err != nil {
		if errors.Is(err, errNetworkRulesNotSupported) || errors.Is(err, errNoEnvdVersion) {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       err,
				ClientMsg: err.Error(),
			}
		}

		return &api.APIError{
			Code:      http.StatusInternalServerError,
			Err:       err,
			ClientMsg: "internal error while validating network rules",
		}
	}

	if len(*rules) > limits.MaxDomains {
		return &api.APIError{
			Code:      http.StatusBadRequest,
			Err:       fmt.Errorf("too many rule domains: %d (max %d)", len(*rules), limits.MaxDomains),
			ClientMsg: fmt.Sprintf("Network rules can have at most %d domains.", limits.MaxDomains),
		}
	}

	seenDomains := make(map[string]struct{}, len(*rules))
	for domain, domainRules := range *rules {
		if len(domain) == 0 {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       errors.New("rule domain must not be empty"),
				ClientMsg: "Rule domain must not be empty.",
			}
		}

		if len(domain) > maxNetworkRuleDomainLen {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("rule domain %q exceeds max length %d", domain, maxNetworkRuleDomainLen),
				ClientMsg: fmt.Sprintf("Rule domain %q exceeds maximum length of %d characters.", domain, maxNetworkRuleDomainLen),
			}
		}

		validDomain := govalidator.IsDNSName(domain)
		if strings.HasPrefix(domain, "*.") {
			validDomain = sandbox_network.IsValidWildcardDomainPattern(domain)
		}

		if !validDomain {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("rule domain %q is not a valid domain", domain),
				ClientMsg: fmt.Sprintf("Rule domain %q is not a valid domain name.", domain),
			}
		}

		normalizedDomain := strings.ToLower(domain)
		if _, ok := seenDomains[normalizedDomain]; ok {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       errors.New("network rule domain keys must be unique ignoring case"),
				ClientMsg: "Network rule domain keys must be unique ignoring case.",
			}
		}
		seenDomains[normalizedDomain] = struct{}{}

		if len(domainRules) > maxNetworkRuleTransformsPerDomain {
			return &api.APIError{
				Code:      http.StatusBadRequest,
				Err:       fmt.Errorf("domain %q has %d transforms (max %d)", domain, len(domainRules), maxNetworkRuleTransformsPerDomain),
				ClientMsg: fmt.Sprintf("Domain %q can have at most %d transform rule.", domain, maxNetworkRuleTransformsPerDomain),
			}
		}

		markerSeen := make(map[string]struct{})
		for _, rule := range domainRules {
			if rule.Transform == nil {
				continue
			}

			headers := sharedUtils.DerefOrDefault(rule.Transform.Headers, nil)
			if len(headers) > maxNetworkRuleHeadersPerRule {
				return &api.APIError{
					Code:      http.StatusBadRequest,
					Err:       fmt.Errorf("domain %q has %d headers (max %d)", domain, len(headers), maxNetworkRuleHeadersPerRule),
					ClientMsg: fmt.Sprintf("Domain %q can have at most %d headers per rule.", domain, maxNetworkRuleHeadersPerRule),
				}
			}

			for name, value := range headers {
				if len(name) == 0 {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("header name in rule for domain %q must not be empty", domain),
						ClientMsg: fmt.Sprintf("Header name in rule for domain %q must not be empty.", domain),
					}
				}

				if !httpguts.ValidHeaderFieldName(name) {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("header name %q in rule for domain %q contains invalid characters", name, domain),
						ClientMsg: fmt.Sprintf("Header name %q in rule for domain %q must contain only valid HTTP token characters.", name, domain),
					}
				}

				if len(name) > maxNetworkRuleHeaderNameLen {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("header name %q in rule for domain %q exceeds max length %d", name, domain, maxNetworkRuleHeaderNameLen),
						ClientMsg: fmt.Sprintf("Header name %q in rule for domain %q exceeds maximum length of %d characters.", name, domain, maxNetworkRuleHeaderNameLen),
					}
				}

				if !httpguts.ValidHeaderFieldValue(value) {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("value for header %q in rule for domain %q contains invalid characters", name, domain),
						ClientMsg: fmt.Sprintf("Value for header %q in rule for domain %q contains invalid characters.", name, domain),
					}
				}

				if len(value) > limits.MaxHeaderValueLen {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("value for header %q in rule for domain %q exceeds max length %d", name, domain, limits.MaxHeaderValueLen),
						ClientMsg: fmt.Sprintf("Value for header %q in rule for domain %q exceeds maximum length of %d characters.", name, domain, limits.MaxHeaderValueLen),
					}
				}

				placeholders, err := networktransform.ParsePlaceholders(value)
				if err != nil {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("malformed E2B placeholder in network transform header: %w", err),
						ClientMsg: "Network transform header contains a malformed E2B placeholder.",
					}
				}

				for _, placeholder := range placeholders {
					if placeholder.Kind == networktransform.PlaceholderCustomerSecret {
						markerSeen[placeholder.Name] = struct{}{}
					}
				}
				if len(markerSeen) > networktransform.MaxMarkerNames {
					return &api.APIError{
						Code:      http.StatusBadRequest,
						Err:       fmt.Errorf("domain %q references %d secrets (max %d)", domain, len(markerSeen), networktransform.MaxMarkerNames),
						ClientMsg: fmt.Sprintf("Rule domain %q references more than %d secrets.", domain, networktransform.MaxMarkerNames),
					}
				}
			}
		}
	}

	return nil
}
