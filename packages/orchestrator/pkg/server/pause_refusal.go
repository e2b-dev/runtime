package server

import (
	"context"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// refuseUnsupportedFilesystemOnly answers a filesystem-only pause or
// checkpoint of a guest without a filesystem quiesce with FailedPrecondition.
// The capture Sandbox.Pause would refuse persists nothing; the refusal has to
// come before the RPC marks the sandbox stopping, or the deferred stop tears
// down a sandbox whose state was never saved.
func refuseUnsupportedFilesystemOnly(sbx *sandbox.Sandbox, filesystemOnly bool) error {
	g := sbx.Config.GuestProfile()
	if !filesystemOnly || !g.Capabilities().NoFilesystemOnlyPause {
		return nil
	}

	st := unsupportedFilesystemOnlyStatus(codes.FailedPrecondition, g, "sandbox '%s' runs a %s guest, which offers no filesystem quiesce: a filesystem-only pause is not supported", sbx.Runtime.SandboxID, g.Label())

	return st.Err()
}

// autoPauseRefusal refuses a sandbox whose timeout auto-pause is to persist
// only the filesystem when its guest offers no filesystem quiesce: the sweep
// could never take the capture the sandbox was created for, and a memory
// capture in its place would keep what the policy drops. A resume is refused
// the same way, since the policy travels with its snapshot; a resume that
// turns auto-pause off is not, since the snapshot kind then decides nothing.
// The refusal carries the pause refusal's detail and the create-refused
// detail: every node that claims the profile answers the same.
func autoPauseRefusal(ctx context.Context, profile guest.Profile, config *orchestrator.SandboxConfig) error {
	if !config.GetAutoPause() || !config.GetAutoPauseFilesystemOnly() || !profile.Capabilities().NoFilesystemOnlyPause {
		return nil
	}

	st := unsupportedFilesystemOnlyStatus(codes.InvalidArgument, profile, "sandbox '%s' runs a %s guest, which offers no filesystem quiesce: a timeout auto-pause that persists only the filesystem is not supported", config.GetSandboxId(), profile.Label())

	return withCreateRefusedDetail(ctx, st)
}

// unsupportedFilesystemOnlyStatus is a refusal of a filesystem-only capture
// by a guest without the capability, with the detail the API keys on to
// restore the record instead of removing it: a status without it is read as
// today's failure.
func unsupportedFilesystemOnlyStatus(code codes.Code, g guest.Profile, format string, args ...any) *status.Status {
	st := status.Newf(code, format, args...)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   sandboxtypes.PauseRefusedUnsupportedReason,
		Domain:   sandboxtypes.PauseRefusalDomain,
		Metadata: map[string]string{"guest_os": g.Label(), "snapshot_kind": "filesystem_only"},
	})
	if err != nil {
		return st
	}

	return detailed
}
