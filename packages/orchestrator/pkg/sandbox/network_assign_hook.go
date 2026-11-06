//go:build linux

package sandbox

import (
	"context"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// NetworkAssignReason tells NetworkAssignHook.OnNetworkAssign what kind of
// start this is (a brand-new sandbox, a memory-snapshot resume, etc.), so
// an implementation can decide what to do based on it (e.g. only guard
// against a resumed guest's traffic, and skip that work on a fresh boot).
type NetworkAssignReason uint8

const (
	// NetworkAssignReasonUnknown is the zero value: an omitted or not-yet-set
	// reason, distinct from every deliberately chosen one.
	NetworkAssignReasonUnknown NetworkAssignReason = iota
	// NetworkAssignReasonCreate: a brand-new sandbox, cold-booting for the first time.
	NetworkAssignReasonCreate
	// NetworkAssignReasonResume: a memory-snapshot resume; guest state (including any
	// established network connection) may be preserved across this call.
	NetworkAssignReasonResume
	// NetworkAssignReasonReboot: a filesystem-only snapshot boot; guest
	// memory, and therefore any prior network connection state, is not
	// preserved.
	NetworkAssignReasonReboot
	// NetworkAssignReasonThrowawayResume: a short-lived resume that only
	// exists to prefetch a sandbox's resume working set into the page cache
	// ahead of time, so a later real resume is faster (see
	// ThrowawayResumeOptions). The caller reaps it itself right after, so it
	// is never promoted to a live, routable sandbox (WithoutLiveRegistration)
	// and stays network-isolated for its entire lifetime (WithDenyEgress),
	// since nothing should ever reach it or be reached by it.
	NetworkAssignReasonThrowawayResume
)

// String implements fmt.Stringer so a NetworkAssignReason logged via zap.Any (or
// any other %v/%s formatting) reads as a name, not a bare uint8.
func (r NetworkAssignReason) String() string {
	switch r {
	case NetworkAssignReasonCreate:
		return "create"
	case NetworkAssignReasonResume:
		return "resume"
	case NetworkAssignReasonReboot:
		return "reboot"
	case NetworkAssignReasonThrowawayResume:
		return "throwaway_resume"
	default:
		return "unknown"
	}
}

// NetworkAssignHook is an optional extension point for sandbox startup. It
// is called once per start attempt, right after AssignNetwork puts the
// sandbox in the network map and strictly before that sandbox's guest can
// run (or resume running) for this attempt, so an edition can complete the
// sandbox's network. It's a no-op by default (see NoopNetworkAssignHook).
//
// An error fails the start, unless the hook wrapped it with
// OptionalNetworkAssignError: then it is logged and the start goes on. The
// placement returned, if any, says where the sandbox's traffic goes and is
// reported by Create.
type NetworkAssignHook interface {
	OnNetworkAssign(ctx context.Context, sbx *Sandbox, reason NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error)
	// OnNetworkUpdate runs after a placed sandbox's update commits and releases
	// its lock and update admission. Calls can overlap, arrive out of commit
	// order, and race with Close. The hook must safely notify its worker to read
	// the latest committed state, tolerate teardown, and not wait on remote calls.
	// It must not panic: the update has already committed and cannot be rolled back.
	OnNetworkUpdate(ctx context.Context, sbx *Sandbox)
	// OnEnvdReady runs once a start's envd answered /init, with the version it reported when that
	// parses as a version, else "". An error fails the start: the guest may already have run, so
	// the start is not retried on another network.
	OnEnvdReady(ctx context.Context, sbx *Sandbox, envdVersion string) error
}

// NoopNetworkAssignHook is a no-op NetworkAssignHook, used when no
// edition-specific implementation is configured.
type NoopNetworkAssignHook struct{}

var _ NetworkAssignHook = NoopNetworkAssignHook{}

func (NoopNetworkAssignHook) OnNetworkAssign(context.Context, *Sandbox, NetworkAssignReason) (*orchestrator.SandboxNetworkPlacement, error) {
	return nil, nil
}

func (NoopNetworkAssignHook) OnNetworkUpdate(context.Context, *Sandbox) {}

func (NoopNetworkAssignHook) OnEnvdReady(context.Context, *Sandbox, string) error { return nil }

// OptionalNetworkAssignError marks a NetworkAssignHook error the start can
// go on without: the factory logs it instead of failing the start.
func OptionalNetworkAssignError(err error) error {
	return optionalNetworkAssignError{err: err}
}

type optionalNetworkAssignError struct{ err error }

func (e optionalNetworkAssignError) Error() string { return e.err.Error() }
func (e optionalNetworkAssignError) Unwrap() error { return e.err }
