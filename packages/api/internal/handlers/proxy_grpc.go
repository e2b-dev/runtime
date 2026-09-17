package handlers

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/oauth"
	apiorchestrator "github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type SandboxService struct {
	proxygrpc.UnimplementedSandboxServiceServer

	api                        *APIStore
	requireEdgeClientProxyAuth bool
	clientProxyOAuth           oauth.Verifier
}

func NewSandboxService(api *APIStore, requireEdgeClientProxyAuth bool, clientProxyOAuth oauth.Verifier) *SandboxService {
	return &SandboxService{
		api:                        api,
		requireEdgeClientProxyAuth: requireEdgeClientProxyAuth,
		clientProxyOAuth:           clientProxyOAuth,
	}
}

func metadataFromIncomingContext(ctx context.Context) metadata.MD {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return md
	}

	return metadata.MD{}
}

func metadataFirstValue(md metadata.MD, key string) (string, bool) {
	vals := md.Get(key)
	if len(vals) == 0 {
		return "", false
	}

	return vals[0], true
}

func isNonEnvdTrafficRequest(ctx context.Context, incomingMetadata metadata.MD, sandboxID string) bool {
	requestPortRaw, found := metadataFirstValue(incomingMetadata, proxygrpc.MetadataSandboxRequestPort)
	if !found {
		return true
	}

	requestPort, parseErr := strconv.ParseUint(requestPortRaw, 10, 64)
	if parseErr != nil {
		logger.L().Warn(
			ctx,
			"invalid sandbox request port metadata for resume",
			zap.Error(parseErr),
			zap.String("request_port", requestPortRaw),
			logger.WithSandboxID(sandboxID),
		)

		return true
	}

	return requestPort != uint64(consts.DefaultEnvdServerPort)
}

func isPrivateIngressTraffic(network *dbtypes.SandboxNetworkConfig) bool {
	return network != nil && network.Ingress != nil && network.Ingress.AllowPublicAccess != nil && !*network.Ingress.AllowPublicAccess
}

func tokensMatch(providedToken string, expectedToken string) bool {
	return subtle.ConstantTimeCompare([]byte(providedToken), []byte(expectedToken)) == 1
}

func denyResumePermission() error {
	return status.Error(codes.PermissionDenied, "permission denied")
}

const autoResumeTransitionWaitBudget = time.Minute

func (s *SandboxService) getAutoResumeSnapshot(ctx context.Context, sandboxID string) (*snapshotcache.SnapshotInfo, *dbtypes.SandboxAutoResumeConfig, error) {
	snap, err := s.api.snapshotCache.Get(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, snapshotcache.ErrSnapshotNotFound) {
			return nil, nil, status.Error(codes.NotFound, "snapshot not found")
		}

		return nil, nil, status.Errorf(codes.Internal, "failed to get snapshot: %v", err)
	}

	var autoResume *dbtypes.SandboxAutoResumeConfig
	if snap.Snapshot.Config != nil {
		autoResume = snap.Snapshot.Config.AutoResume
	}
	if autoResume == nil || autoResume.Policy != dbtypes.SandboxAutoResumeAny {
		return nil, nil, status.Error(codes.NotFound, "sandbox auto-resume disabled")
	}

	// A filesystem-only snapshot can only be resumed by cold-booting (reboot),
	// which loses in-memory state. Refuse to do that implicitly on an incoming
	// request — the caller must resume it explicitly.
	if snapshotIsFilesystemOnly(snap.Snapshot) {
		return nil, nil, status.Error(codes.FailedPrecondition, "filesystem-only snapshot must be resumed explicitly")
	}

	return snap, autoResume, nil
}

func (s *SandboxService) ResumeSandbox(ctx context.Context, req *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	incomingMetadata := metadataFromIncomingContext(ctx)

	var clientProxyClaims oauth.Claims
	if s.requireEdgeClientProxyAuth {
		var authErr error
		clientProxyClaims, authErr = oauth.RequireClaims(ctx, incomingMetadata, s.clientProxyOAuth)
		if authErr != nil {
			return nil, authErr
		}
		if err := oauth.RequireScopeClaims(clientProxyClaims, oauth.RequiredScope); err != nil {
			return nil, err
		}
	}

	sandboxID, err := utils.ShortID(req.GetSandboxId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid sandbox ID")
	}

	snap, autoResume, err := s.getAutoResumeSnapshot(ctx, sandboxID)
	if err != nil {
		return nil, err
	}

	teamID := snap.Snapshot.TeamID

	team, err := s.api.authService.GetTeamByID(ctx, teamID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get team: %v", err)
	}

	if s.requireEdgeClientProxyAuth {
		var authOrgID string
		if team.ClusterID != nil {
			cluster, found := s.api.clusters.GetClusterById(*team.ClusterID)
			if !found {
				return nil, status.Errorf(codes.Internal, "cluster with ID '%s' not found", *team.ClusterID)
			}

			authOrgID = cluster.AuthOrgID
		}

		if err := oauth.RequireOrgClaims(clientProxyClaims, authOrgID); err != nil {
			return nil, err
		}
	}

	if err := auth.CheckTeamBlocked(team); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	sandboxData, sandboxErr := s.api.orchestrator.GetSandbox(ctx, teamID, sandboxID)
	if sandboxErr != nil {
		if !errors.Is(sandboxErr, sandbox.ErrNotFound) {
			return nil, status.Errorf(codes.Internal, "failed to get sandbox state: %v", sandboxErr)
		}
	} else {
		nodeIP, handled, existingErr := s.api.orchestrator.HandleExistingSandboxAutoResume(
			ctx,
			teamID,
			sandboxID,
			sandboxData,
			autoResumeTransitionWaitBudget,
		)
		if existingErr != nil {
			if errors.Is(existingErr, apiorchestrator.ErrSandboxStillTransitioning) {
				return nil, status.Error(codes.FailedPrecondition, proxygrpc.SandboxStillTransitioningMessage)
			}
			if errors.Is(existingErr, sandbox.ErrNotFound) {
				return nil, status.Error(codes.NotFound, "sandbox not found")
			}
			if errors.Is(existingErr, context.Canceled) || errors.Is(existingErr, context.DeadlineExceeded) {
				return nil, status.FromContextError(existingErr).Err()
			}

			return nil, status.Error(codes.Internal, existingErr.Error())
		}
		if handled {
			return &proxygrpc.SandboxResumeResponse{OrchestratorIp: nodeIP}, nil
		}
	}

	minAutoResumeTimeout := time.Duration(s.api.featureFlags.IntFlag(ctx, featureflags.MinAutoResumeTimeoutSeconds)) * time.Second

	timeout := calculateAutoResumeTimeout(autoResume, minAutoResumeTimeout, team)
	timeout, exhausted := clampToFrozenSnapshotLifetime(timeout, snap.Snapshot)
	if exhausted {
		return nil, status.Error(codes.FailedPrecondition, "sandbox lifetime exhausted")
	}

	var envdAccessToken *string
	if snap.Snapshot.EnvSecure {
		accessToken, tokenErr := s.api.getEnvdAccessToken(snap.EnvBuild.EnvdVersion, sandboxID)
		if tokenErr != nil {
			logger.L().Error(ctx, "Secure envd access token error", zap.Error(tokenErr.Err), logger.WithSandboxID(sandboxID))

			return nil, status.Error(codes.Internal, "failed to create envd access token")
		}

		envdAccessToken = &accessToken
	}

	var network *dbtypes.SandboxNetworkConfig
	if snap.Snapshot.Config != nil {
		network = snap.Snapshot.Config.Network
	}

	isNonEnvdTraffic := isNonEnvdTrafficRequest(ctx, incomingMetadata, sandboxID)

	// Validate traffic access token for sandboxes with private ingress.
	if isPrivateIngressTraffic(network) && isNonEnvdTraffic {
		expectedToken, tokenErr := s.api.accessTokenGenerator.GenerateTrafficAccessToken(sandboxID)
		if tokenErr != nil {
			logger.L().Error(ctx, "failed to generate expected traffic access token", zap.Error(tokenErr), logger.WithSandboxID(sandboxID))

			return nil, status.Error(codes.Internal, "failed to validate traffic access token")
		}

		providedToken, _ := metadataFirstValue(incomingMetadata, proxygrpc.MetadataTrafficAccessToken)

		if !tokensMatch(providedToken, expectedToken) {
			return nil, denyResumePermission()
		}
	}

	// Validate envd access token for secure sandboxes on envd traffic
	if !isNonEnvdTraffic && snap.Snapshot.EnvSecure && envdAccessToken != nil {
		providedEnvdToken, _ := metadataFirstValue(incomingMetadata, proxygrpc.MetadataEnvdAccessToken)

		if !tokensMatch(providedEnvdToken, *envdAccessToken) {
			return nil, denyResumePermission()
		}
	}

	headers := http.Header{}
	sbx, apiErr := s.api.startSandboxInternal(
		ctx,
		sandboxID,
		timeout,
		team,
		// Auto-resume never demands a cold boot: no memory override exists here.
		s.api.buildResumeSandboxData(sandboxID, nil, nil),
		&headers,
		true,
		false,
		nil, // mcp
	)
	if apiErr != nil {
		return nil, status.Error(sharedutils.GRPCCodeFromHTTPStatus(apiErr.Code), apiErr.ClientMsg)
	}

	nodeIP := s.api.orchestrator.GetNodeRouteIPAddress(sbx.ClusterID, sbx.NodeID)
	if nodeIP == "" {
		return nil, status.Error(codes.Internal, "sandbox resumed but orchestrator IP is not available yet")
	}

	return &proxygrpc.SandboxResumeResponse{OrchestratorIp: nodeIP}, nil
}
