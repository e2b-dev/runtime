package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/builds"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	ut "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Semantic error codes for the explicit filesystem-boot (memory:false) path,
// emitted in APIError.ErrorCode so callers and metrics can distinguish these
// from ambient 4xx/5xx on the same routes.
const (
	ErrCodeStartInFlight             = "sandbox_start_in_flight"
	ErrCodeFilesystemBootUnconfirmed = "sandbox_filesystem_boot_unconfirmed"
)

// SandboxDataFetcher is a callback that fetches sandbox metadata.
// It is called after the concurrency lock is acquired to ensure fresh data.
type SandboxDataFetcher func(ctx context.Context) (SandboxMetadata, *api.APIError)

type SandboxMetadata struct {
	Metadata            map[string]string
	EnvVars             map[string]string
	Build               queries.EnvBuild
	AllowInternetAccess *bool
	Network             *types.SandboxNetworkConfig
	Alias               string
	TemplateID          string
	BaseTemplateID      string
	AutoPause           bool
	// AutoPauseFilesystemOnly makes a timeout auto-pause take a filesystem-only
	// snapshot instead of a full memory one. Only meaningful when AutoPause.
	AutoPauseFilesystemOnly bool
	AutoResume              *types.SandboxAutoResumeConfig
	VolumeMounts            []*orchestrator.SandboxVolumeMount
	EnvdAccessToken         *string
	// Iam records the sandbox workload identity configuration requested at create
	// time. Nil means workload identity is disabled.
	Iam    *types.SandboxIam
	NodeID *string
	// SnapshotSandboxID is the sandbox ID the resume snapshot is stored under.
	// It differs from the ID of the sandbox being started when forking.
	SnapshotSandboxID string
	// FilesystemBoot demands a cold boot of a memory-inclusive snapshot
	// (explicit memory:false resume). Set only by the resume data fetchers;
	// template creates and auto-resume never set it.
	FilesystemBoot bool
	// FilesystemOnlySnapshot marks a resume whose snapshot persists only the
	// rootfs. Unlike FilesystemBoot it describes the stored artifact rather than
	// the request, so every path that resumes such a snapshot sets it.
	FilesystemOnlySnapshot bool
}

// iamToProto maps the sandbox workload identity configuration into the
// orchestrator config. It returns nil when nothing is configured so older nodes
// and stored configs stay unchanged.
func iamToProto(iam *types.SandboxIam) *orchestrator.SandboxIam {
	if iam == nil || len(iam.Tokens) == 0 {
		return nil
	}

	protoTokens := make(map[string]*orchestrator.SandboxIamToken, len(iam.Tokens))
	for name, def := range iam.Tokens {
		protoTokens[name] = &orchestrator.SandboxIamToken{
			Audience:  def.Audience,
			TokenType: def.TokenType,
		}
	}

	return &orchestrator.SandboxIam{Tokens: protoTokens}
}

// buildEgressConfig constructs the orchestrator egress configuration from
// allow/deny entry lists. It splits allowed entries into CIDRs and domains,
// and adds the default nameserver when domains are present so the sandbox can
// resolve them.
func buildEgressConfig(allowedEntries, deniedEntries []string, rules map[string][]types.SandboxNetworkRule) *orchestrator.SandboxNetworkEgressConfig {
	allowedAddresses, allowedDomains := sandbox_network.ParseAddressesAndDomains(allowedEntries)

	if len(allowedDomains) > 0 {
		allowedAddresses = append(allowedAddresses, sandbox_network.DefaultNameserver)
	}

	var orchRules map[string]*orchestrator.SandboxNetworkDomainRules
	if rules != nil {
		orchRules = make(map[string]*orchestrator.SandboxNetworkDomainRules, len(rules))
		for domain, domainRules := range rules {
			orchRuleList := make([]*orchestrator.SandboxNetworkRule, 0, len(domainRules))
			for _, r := range domainRules {
				orchRule := &orchestrator.SandboxNetworkRule{}
				if r.Transform != nil {
					orchRule.Transform = &orchestrator.SandboxNetworkTransform{
						Headers: r.Transform.Headers,
					}
				}
				orchRuleList = append(orchRuleList, orchRule)
			}
			orchRules[domain] = &orchestrator.SandboxNetworkDomainRules{Rules: orchRuleList}
		}
	}

	return &orchestrator.SandboxNetworkEgressConfig{
		AllowedCidrs:   sandbox_network.AddressStringsToCIDRs(allowedAddresses),
		DeniedCidrs:    sandbox_network.AddressStringsToCIDRs(deniedEntries),
		AllowedDomains: allowedDomains,
		Rules:          orchRules,
	}
}

// applyEgressProxy copies BYOP SOCKS5 fields from src to dst. No-op on nil.
func applyEgressProxy(dst *orchestrator.SandboxNetworkEgressConfig, src *types.SandboxNetworkEgressConfig) {
	if dst == nil || src == nil {
		return
	}
	dst.EgressProxyAddress = src.EgressProxyAddress
	dst.EgressProxyUsername = src.EgressProxyUsername
	dst.EgressProxyPassword = src.EgressProxyPassword
}

// buildNetworkConfig constructs the orchestrator network configuration from the input parameters
func buildNetworkConfig(network *types.SandboxNetworkConfig, allowInternetAccess *bool, trafficAccessToken *string) *orchestrator.SandboxNetworkConfig {
	orchNetwork := &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{},
		Ingress: &orchestrator.SandboxNetworkIngressConfig{
			TrafficAccessToken: trafficAccessToken,
		},
	}

	if network != nil && network.Egress != nil {
		egress := buildEgressConfig(network.Egress.AllowedAddresses, network.Egress.DeniedAddresses, network.Egress.Rules)
		applyEgressProxy(egress, network.Egress)
		orchNetwork.Egress = egress
	}

	if network != nil && network.Ingress != nil {
		orchNetwork.Ingress.MaskRequestHost = network.Ingress.MaskRequestHost
		orchNetwork.Ingress.HttpsPorts = network.Ingress.HTTPSPorts
	}

	// Handle the case where internet access is explicitly disabled
	// This should be applied after copying the network config to preserve allowed addresses
	if allowInternetAccess != nil && !*allowInternetAccess {
		// Block all internet access - this overrides any other blocked addresses
		orchNetwork.Egress.DeniedCidrs = []string{sandbox_network.AllInternetTrafficCIDR}
	}

	return orchNetwork
}

func (o *Orchestrator) CreateSandbox(
	ctx context.Context,
	sandboxID,
	executionID string,
	team *teamtypes.Team,
	getSandboxData SandboxDataFetcher,
	startTime time.Time,
	endTime time.Time,
	timeout time.Duration,
	isResume bool,
	demandFilesystemBoot bool,
	creationMeta sandbox.CreationMetadata,
) (sbx sandbox.Sandbox, apiErr *api.APIError) {
	ctx, childSpan := tracer.Start(ctx, "create-sandbox")
	defer childSpan.End()

	// Calculate total concurrent instances including addons
	totalConcurrentInstances := team.Limits.SandboxConcurrency

	// Check if team has reached max instances
	finishStart, waitForStart, err := o.sandboxStore.Reserve(ctx, team.Team.ID, sandboxID, int(totalConcurrentInstances))
	if err != nil {
		var limitErr *sandbox.LimitExceededError

		switch {
		case errors.As(err, &limitErr):
			return sandbox.Sandbox{}, &api.APIError{
				Code: http.StatusTooManyRequests,
				ClientMsg: fmt.Sprintf(
					"you have reached the maximum number of concurrent E2B sandboxes (%d). If you need more, "+
						"please visit 'https://e2b.dev/docs/billing'", totalConcurrentInstances),
				Err: fmt.Errorf("team '%s' has reached the maximum number of instances (%d)", team.ID, totalConcurrentInstances),
			}
		case errors.Is(err, sandbox.ErrSandboxKilled):
			// A DELETE claimed this sandbox ID for removal while this resume was
			// starting. The kill wins: the snapshot is being (or has been)
			// deleted, so publishing this sandbox would resurrect a sandbox the
			// client was told was gone. Refuse instead.
			return sandbox.Sandbox{}, &api.APIError{
				Code:      http.StatusNotFound,
				ClientMsg: fmt.Sprintf("Sandbox '%s' was deleted", sandboxID),
				Err:       fmt.Errorf("resume of '%s' refused: %w", sandboxID, err),
			}
		default:
			logger.L().Error(ctx, "failed to reserve sandbox for team", logger.WithSandboxID(sandboxID), zap.Error(err))

			return sandbox.Sandbox{}, &api.APIError{
				Code:      http.StatusInternalServerError,
				ClientMsg: fmt.Sprintf("Failed to create sandbox: %s", err),
				Err:       err,
			}
		}
	}

	if waitForStart != nil {
		// A joined request rides whatever start is already in flight — which may
		// be a memory restore (e.g. a traffic-triggered auto-resume) that an
		// explicit memory:false must never be silently answered with.
		if demandFilesystemBoot {
			return sandbox.Sandbox{}, &api.APIError{
				Code:      http.StatusConflict,
				ErrorCode: ErrCodeStartInFlight,
				ClientMsg: "Sandbox is already starting; memory: false cannot be applied to a start already in flight — retry once it is running or paused",
				Err:       fmt.Errorf("filesystem-boot resume of '%s' cannot join an in-flight start", sandboxID),
			}
		}

		// Mark as a joined request for telemetry purposes
		joined.Mark(ctx)

		logger.L().Info(ctx, "sandbox is already being started, waiting for it to be ready", logger.WithSandboxID(sandboxID))

		sbx, err = waitForStart(ctx)
		if err != nil {
			logger.L().Warn(ctx, "Error waiting for sandbox to start", zap.Error(err), logger.WithSandboxID(sandboxID))

			if apiErr, ok := errors.AsType[*api.APIError](err); ok {
				return sandbox.Sandbox{}, apiErr
			}

			return sandbox.Sandbox{}, &api.APIError{
				Code:      http.StatusInternalServerError,
				ClientMsg: "Error waiting for sandbox to start",
				Err:       err,
			}
		}

		return sbx, nil
	}

	telemetry.ReportEvent(ctx, "Reserved sandbox for team")
	defer func() {
		// Don't change this handling
		// https://go.dev/play/p/4oy02s7BDMc
		if apiErr != nil {
			finishStart(sbx, apiErr)
		} else {
			finishStart(sbx, nil)
		}
	}()

	sbxData, fetchErr := getSandboxData(ctx)
	if fetchErr != nil {
		return sandbox.Sandbox{}, fetchErr
	}

	fcSemver, err := fcversion.New(sbxData.Build.FirecrackerVersion)
	if err != nil {
		errMsg := fmt.Errorf("failed to get fcSemver for firecracker fcSemver '%s': %w", sbxData.Build.FirecrackerVersion, err)

		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Failed to get build information for the template",
			Err:       errMsg,
		}
	}

	hasHugePages := fcSemver.HasHugePages()
	telemetry.ReportEvent(ctx, "Got FC info")

	var sbxDomain *string
	if team.ClusterID != nil {
		cluster, ok := o.clusters.GetClusterById(*team.ClusterID)
		if !ok {
			return sandbox.Sandbox{}, &api.APIError{
				Code:      http.StatusInternalServerError,
				ClientMsg: "Error while looking for sandbox cluster information",
				Err:       fmt.Errorf("cannot access cluster %s associated with team id %s that spawned sandbox %s", *team.ClusterID, team.ID, sandboxID),
			}
		}

		sbxDomain = cluster.SandboxDomain
	}

	var trafficAccessToken *string = nil
	network := sbxData.Network
	if network != nil && network.Ingress != nil && network.Ingress.AllowPublicAccess != nil && !*network.Ingress.AllowPublicAccess {
		accessToken, err := o.accessTokenGenerator.GenerateTrafficAccessToken(sandboxID)
		if err != nil {
			return sandbox.Sandbox{}, &api.APIError{
				Code:      http.StatusInternalServerError,
				ClientMsg: "Failed to create traffic access token",
				Err:       fmt.Errorf("failed to create traffic access token for sandbox %s: %w", sandboxID, err),
			}
		}

		trafficAccessToken = &accessToken
	}

	sbxNetwork := buildNetworkConfig(network, sbxData.AllowInternetAccess, trafficAccessToken)

	var orchAutoResume *orchestrator.SandboxAutoResumeConfig
	if sbxData.AutoResume != nil {
		orchAutoResume = &orchestrator.SandboxAutoResumeConfig{
			Policy:         string(sbxData.AutoResume.Policy),
			TimeoutSeconds: sbxData.AutoResume.Timeout,
		}
	}

	sbxRequest := &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			BaseTemplateId:          sbxData.BaseTemplateID,
			TemplateId:              sbxData.TemplateID,
			Alias:                   &sbxData.Alias,
			TeamId:                  team.ID.String(),
			BuildId:                 sbxData.Build.ID.String(),
			SandboxId:               sandboxID,
			ExecutionId:             executionID,
			KernelVersion:           sbxData.Build.KernelVersion,
			FirecrackerVersion:      sbxData.Build.FirecrackerVersion,
			EnvdVersion:             *sbxData.Build.EnvdVersion,
			Metadata:                sbxData.Metadata,
			EnvVars:                 sbxData.EnvVars,
			EnvdAccessToken:         sbxData.EnvdAccessToken,
			MaxSandboxLength:        team.Limits.MaxLengthHours,
			EventsTtlDays:           team.Limits.EventsTTLDays,
			HugePages:               hasHugePages,
			RamMb:                   sbxData.Build.RamMb,
			Vcpu:                    sbxData.Build.Vcpu,
			Snapshot:                isResume,
			AutoPause:               sbxData.AutoPause,
			AutoPauseFilesystemOnly: sbxData.AutoPauseFilesystemOnly,
			AutoResume:              orchAutoResume,
			AllowInternetAccess:     sbxData.AllowInternetAccess,
			Network:                 sbxNetwork,
			TotalDiskSizeMb:         ut.FromPtr(sbxData.Build.TotalDiskSizeMb),
			VolumeMounts:            sbxData.VolumeMounts,
			Iam:                     iamToProto(sbxData.Iam),
		},
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}
	if sbxData.FilesystemBoot {
		// Left absent otherwise, so requests without the rescue are
		// byte-identical to before the field existed.
		sbxRequest.FilesystemBoot = new(true)
	}

	cpuRequirement := o.resolveCPURequirement(ctx, sandboxID, team, sbxData)
	// Recorded before placement runs: a placement the pin starved is the case
	// worth querying.
	telemetry.SetAttributes(ctx, attribute.String("placement.cpu_model_pinned", cpuRequirement.PinnedModel))

	hostIsolation := o.resolveHostIsolation(ctx, sandboxID, team, sbxData)

	var node *nodemanager.Node

	if isResume && sbxData.NodeID != nil {
		telemetry.ReportEvent(ctx, "Placing sandbox on the node where the snapshot was taken")

		clusterID := clusters.WithClusterFallback(team.ClusterID)
		node = o.GetNode(clusterID, *sbxData.NodeID)
		if node != nil && !node.CanAcceptNewRequests() {
			node = nil
		}
		// Snapshot affinity skips the candidate filter below, and the origin node
		// can drift off the snapshot's own side of the partition when a timed-out
		// resume remaps it. Vetted here so it cannot outrank the partition.
		if node != nil && !hostIsolation.Allows(node) {
			node = nil
		}
	}

	nodeClusterID := clusters.WithClusterFallback(team.ClusterID)
	clusterNodes := o.GetClusterNodes(nodeClusterID)

	if hostIsolation.Enabled() {
		isolatedCandidates := hostIsolation.FilterNodes(clusterNodes)
		// Recorded because the partition starving placement surfaces as an
		// ordinary "no nodes available", which alone does not say the fence
		// caused it.
		telemetry.SetAttributes(ctx,
			attribute.Bool("placement.host_isolation.isolated_origin", hostIsolation.IsolatedOrigin()),
			attribute.Int("placement.host_isolation.nodes_before", len(clusterNodes)),
			attribute.Int("placement.host_isolation.nodes_after", len(isolatedCandidates)),
		)

		clusterNodes = isolatedCandidates
	}

	allLabels, labelFilteringEnabled := o.generateRequiredNodeLabels(ctx, sandboxID, team, sbxData)

	placed, err := placement.PlaceSandbox(ctx, o.placementAlgorithm, clusterNodes, node, sbxRequest, cpuRequirement, labelFilteringEnabled, allLabels)
	if err != nil {
		if isResume && placed.TimedOut {
			// Remap by the snapshot's own sandbox ID: when forking, the started
			// sandbox ID has no snapshot row and the remap would silently no-op.
			snapshotSandboxID := sandboxID
			if sbxData.SnapshotSandboxID != "" {
				snapshotSandboxID = sbxData.SnapshotSandboxID
			}
			o.maybeRemapResumeOriginNode(ctx, snapshotSandboxID, team, sbxData.NodeID, placed.WarmedNode)
		}

		return sandbox.Sandbox{}, placementAPIError(err)
	}

	node = placed.Node

	// The sandbox was created successfully
	attributes := []attribute.KeyValue{
		attribute.Bool("is_resume", isResume),
		attribute.Bool("node_affinity_requested", sbxData.NodeID != nil),
		attribute.Bool("node_affinity_success", sbxData.NodeID != nil && node.ID == *sbxData.NodeID),
	}
	o.createdSandboxesCounter.Add(ctx, 1, metric.WithAttributes(attributes...))

	telemetry.SetAttributes(ctx, attribute.String("node.id", node.ID))
	telemetry.ReportEvent(ctx, "Created sandbox")

	// This is to compensate for the time it takes to start the instance
	// Otherwise it could cause the instance to expire before user has a chance to use it
	startTime = time.Now()
	endTime = startTime.Add(timeout)

	// The record carries the version the sandbox actually RUNS when the
	// orchestrator echoes it: the declared build version resolved through the
	// firecracker-versions flag at start and frozen for the sandbox's
	// lifetime. Version-gated paths branch on resolvedFCVersion — a resolved
	// version is exact, the declared fallback (old orchestrators) is only an
	// approximation of the running binary.
	recordFCVersion := placed.Response.GetResolvedFirecrackerVersion()
	resolvedFCVersion := recordFCVersion != ""
	if !resolvedFCVersion {
		recordFCVersion = sbxData.Build.FirecrackerVersion
	}

	sbx = sandbox.NewSandbox(
		sandboxID,
		sbxData.TemplateID,
		consts.ClientID,
		&sbxData.Alias,
		executionID,
		team.ID,
		sbxData.Build.ID,
		sbxData.Metadata,
		time.Duration(team.Limits.MaxLengthHours)*time.Hour,
		startTime,
		endTime,
		sbxData.Build.Vcpu,
		*sbxData.Build.TotalDiskSizeMb,
		sbxData.Build.RamMb,
		sbxData.Build.KernelVersion,
		recordFCVersion,
		*sbxData.Build.EnvdVersion,
		node.ID,
		node.ClusterID,
		sbxData.AutoPause,
		sbxData.AutoPauseFilesystemOnly,
		sbxData.AutoResume,
		sbxData.EnvdAccessToken,
		sbxData.AllowInternetAccess,
		sbxData.BaseTemplateID,
		sbxDomain,
		sbxData.Network,
		trafficAccessToken,
		nodemanager.ConvertOrchestratorMountsToDatabaseMounts(sbxData.VolumeMounts),
		sbxData.Iam,
	)
	sbx.FirecrackerVersionResolved = resolvedFCVersion

	// An orchestrator that predates the filesystem_boot field ignores it and
	// memory-restores; only the echo proves the demand was honored. Kill the
	// wrong-path VM on the node (the snapshot row is untouched) and fail loudly
	// rather than hand back a silent memory restore.
	if demandFilesystemBoot && !placed.Response.GetFilesystemBootApplied() {
		// Torn down synchronously: the 503 invites a retry, and an async kill
		// would race it with the sandbox ID still live on the node.
		killErr := o.removeSandboxFromNode(
			context.WithoutCancel(ctx),
			sbx,
			sandbox.StateActionKill,
			sandbox.KillReasonUnknown,
			false, // kill: no snapshot
			false,
		)
		if killErr != nil {
			logger.L().Error(ctx, "Error removing memory-restored sandbox after unhonored filesystem-boot demand",
				zap.Error(killErr),
				logger.WithSandboxID(sbx.SandboxID),
			)
		}

		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusServiceUnavailable,
			ErrorCode: ErrCodeFilesystemBootUnconfirmed,
			ClientMsg: "This cluster cannot resume without memory yet (memory: false); the resume was rolled back and the snapshot is untouched — retry shortly",
			Err:       fmt.Errorf("filesystem-boot demand for '%s' not confirmed by node %s", sandboxID, node.ID),
		}
	}

	err = o.sandboxStore.Add(ctx, sbx, &creationMeta)
	if err != nil {
		telemetry.ReportError(ctx, "failed to add sandbox to store", err)

		// Clean up the sandbox from the node
		// Copy to a new variable to avoid race conditions
		sbxToRemove := sbx
		go func() {
			killErr := o.removeSandboxFromNode(
				context.WithoutCancel(ctx),
				sbxToRemove,
				sandbox.StateActionKill,
				sandbox.KillReasonUnknown,
				false, // kill: no snapshot
				false,
			)
			if killErr != nil {
				logger.L().Error(ctx, "Error removing sandbox",
					zap.Error(killErr),
					logger.WithSandboxID(sbxToRemove.SandboxID),
					zap.String("kill_reason", sandbox.KillReasonUnknown.String()),
				)
			}
		}()

		return sandbox.Sandbox{}, &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Failed to create sandbox",
			Err:       fmt.Errorf("failed to add sandbox to store: %w", err),
		}
	}

	return sbx, nil
}

// maybeRemapResumeOriginNode repoints the snapshot's origin_node_id to the
// fallback node a resume timed out on. Pinning the next resume attempt to avoid
// re-pulling the snapshot onto another node and spraying load across the cluster
//
// It only acts on placement timeouts (warmedNode is nil otherwise), and only
// when the warming node differs from the origin we already tried. The write runs
// on a detached context because the request context is already past its deadline
// (that is why placement timed out).
func (o *Orchestrator) maybeRemapResumeOriginNode(ctx context.Context, sandboxID string, team *teamtypes.Team, originNodeID *string, warmedNode *nodemanager.Node) {
	if warmedNode == nil {
		return
	}

	newNode := warmedNode
	if originNodeID != nil && *originNodeID == newNode.ID {
		return
	}

	// The request context is already past its deadline (that is why placement
	// timed out), so detach it for everything below: the feature-flag read, the
	// DB write, cache invalidation, the counter, and logging would all otherwise
	// observe a cancelled context.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if !o.featureFlagsClient.BoolFlag(wctx, featureflags.ResumeOriginNodeRemapFlag, featureflags.TeamContext(team.ID.String()), featureflags.SandboxContext(sandboxID)) {
		return
	}

	if err := o.sqlcDB.UpdateSnapshotOriginNode(wctx, queries.UpdateSnapshotOriginNodeParams{
		OriginNodeID: newNode.ID,
		SandboxID:    sandboxID,
	}); err != nil {
		logger.L().Warn(wctx, "failed to remap resume origin node",
			zap.Error(err),
			logger.WithSandboxID(sandboxID),
		)

		return
	}

	// Drop the cached snapshot so the next resume reads the new origin node.
	o.snapshotCache.Invalidate(wctx, sandboxID)
	o.resumeOriginNodeRemapCounter.Add(wctx, 1)

	oldNodeID := ""
	if originNodeID != nil {
		oldNodeID = *originNodeID
	}

	logger.L().Info(wctx, "remapped resume origin node to the node a previous resume timed out on",
		logger.WithSandboxID(sandboxID),
		zap.String("old_origin_node_id", oldNodeID),
		zap.String("new_origin_node_id", newNode.ID),
	)
}

// resolveCPURequirement builds the CPU constraint placement filters nodes with.
//
// A memory restore may move to the newer models IsCompatibleWith treats as
// supersets. A filesystem-only snapshot is instead confined to one CPU model,
// keeping the cold-boot resume on a single generation while that path is
// unproven across them. FsOnlyResumeCPUModelFlag names the model, and empty
// returns filesystem-only resumes to the shared rule.
func (o *Orchestrator) resolveCPURequirement(ctx context.Context, sandboxID string, team *teamtypes.Team, sbxData SandboxMetadata) placement.CPURequirement {
	requirement := placement.CPURequirement{Build: builds.ToMachineInfo(sbxData.Build)}

	if !sbxData.FilesystemOnlySnapshot {
		return requirement
	}

	requirement.PinnedModel = o.featureFlagsClient.StringFlag(ctx, featureflags.FsOnlyResumeCPUModelFlag,
		featureflags.TeamContext(team.ID.String()),
		featureflags.SandboxContext(sandboxID),
		featureflags.ClusterContext(clusters.WithClusterFallback(team.ClusterID)),
	)

	return requirement
}

// resolveHostIsolation builds the host-partition constraint placement filters
// candidates with. The origin is the build's cluster host: the node a template
// build ran on, or — because a pause copies its origin node into the snapshot's
// build row — the node a snapshot was taken on. A build with no recorded host
// lands on the non-isolated side.
func (o *Orchestrator) resolveHostIsolation(ctx context.Context, sandboxID string, team *teamtypes.Team, sbxData SandboxMetadata) placement.HostIsolation {
	return placement.HostIsolation{
		Hosts: featureflags.GetIsolatedSchedulingHosts(ctx, o.featureFlagsClient,
			featureflags.TeamContext(team.ID.String()),
			featureflags.SandboxContext(sandboxID),
			featureflags.ClusterContext(clusters.WithClusterFallback(team.ClusterID)),
		),
		OriginHost: ut.FromPtr(sbxData.Build.ClusterNodeID),
	}
}

func (o *Orchestrator) generateRequiredNodeLabels(ctx context.Context, sandboxID string, team *teamtypes.Team, sbxData SandboxMetadata) ([]string, bool) {
	labelFilteringEnabled := o.featureFlagsClient.BoolFlag(ctx, featureflags.SandboxLabelBasedSchedulingFlag, featureflags.TeamContext(team.ID.String()), featureflags.SandboxContext(sandboxID))
	if !labelFilteringEnabled {
		return nil, false
	}

	// if the team doesn't require a specific label, we default to "default",
	// which corresponds to all unoptimized nodes (nodes that don't expect
	// high cpu, high memory, long lifespan, etc)
	allLabels := append([]string{}, team.SandboxSchedulingLabels...)
	if len(allLabels) == 0 {
		allLabels = append(allLabels, "default")
	}

	clusterID := clusters.WithClusterFallback(team.ClusterID)
	volumeFilteringEnabled := o.featureFlagsClient.BoolFlag(ctx,
		featureflags.SandboxVolumeLabelBasedSchedulingFlag,
		featureflags.TeamContext(team.ID.String()),
		featureflags.ClusterContext(clusterID),
		featureflags.SandboxContext(sandboxID),
	)

	if volumeFilteringEnabled {
		for _, mount := range sbxData.VolumeMounts {
			label := internal.MakeVolumeTypeLabel(mount.GetType())
			allLabels = append(allLabels, label)
		}
	}

	return allLabels, labelFilteringEnabled
}
