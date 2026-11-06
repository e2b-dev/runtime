package server

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// resolveSandboxFirecrackerVersion picks the Firecracker binary a sandbox
// runs. The default profile's version goes through the fleet's version map; a
// profile that pins keeps the binary its build was made with, because the map
// does not know the guest OS. A build from a pinned parent records the
// parent's binary in its metadata while its build row keeps the fleet's, so
// the metadata wins when it names one.
func resolveSandboxFirecrackerVersion(ctx context.Context, ff *featureflags.Client, meta metadata.Template, profile guest.Profile, buildVersion string) string {
	if profile.Firecracker().PinToBuild {
		if meta.Template.FirecrackerVersion != "" {
			return meta.Template.FirecrackerVersion
		}

		return buildVersion
	}

	return featureflags.ResolveFirecrackerVersion(ctx, ff, buildVersion)
}

// unclaimedGuestRefusal answers a template whose guest OS no registered
// profile claims with FailedPrecondition, so the API tells a node that cannot
// serve the guest apart from a failed start. It carries no create-refused
// detail, so placement still tries a node that registers the profile. It is
// nil for any other error.
func unclaimedGuestRefusal(err error) error {
	if !errors.Is(err, guest.ErrUnclaimed) {
		return nil
	}

	return status.Error(codes.FailedPrecondition, err.Error())
}

// tlsInterceptionRefusal refuses a new sandbox whose egress rules the proxy
// intercepts when its guest takes no CA bundle. A resume is let through with
// a warning: its rules come from a snapshot, and refusing it would strand the
// sandbox.
func tlsInterceptionRefusal(ctx context.Context, profile guest.Profile, egress *orchestrator.SandboxNetworkEgressConfig, isResume bool) error {
	err := sandbox.RefuseTLSInterception(profile, egress)
	if err == nil {
		return nil
	}
	if isResume {
		logger.L().Warn(ctx, "resumed sandbox intercepts TLS for a guest without the CA bundle", zap.Error(err))

		return nil
	}

	return createRefused(ctx, codes.InvalidArgument, "%s", err.Error())
}
