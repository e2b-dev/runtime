package process

import (
	"context"
	"errors"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
)

// DefaultUpgradeBinPath is the only filesystem path a live upgrade will write a
// delivered binary to and re-exec into. Constraining the target (rather than
// trusting a request header or marker file) keeps a malformed or forged upgrade
// request from writing to / executing an arbitrary path. The /upgrade endpoint
// is authenticated, but this is defense-in-depth around a same-PID exec.
const DefaultUpgradeBinPath = "/usr/bin/envd.next"

// HandoverResult summarizes a completed incoming handover. It is surfaced to the
// orchestrator via the /init X-Envd-Handover header so the envd-side outcome
// (which envd otherwise only logs) is observable fleet-wide.
type HandoverResult struct {
	// Every item is total-carried + failed-subset (ok = total - failed).
	Procs          int
	ProcsFailed    int
	Retained       int
	RetainedFailed int
	Watchers       int
	WatchersFailed int
	// Mounts and Forwards are decoded from the blob and handed back to their
	// owners (the API service's mount ledger and the port forwarder), which are
	// constructed after ResumeFromHandover runs — so unlike watchers they are
	// returned rather than applied via a callback.
	Mounts   []*upgrade.MountEntry
	Forwards []*upgrade.ForwardedPort
	Helpers  []int32
}

// ErrWorkloadNotFrozen means the pre-handover freeze was issued but the workload
// had not stopped by the deadline, so the swap was refused.
var ErrWorkloadNotFrozen = errors.New("workload was not frozen before the handover")

// FreezeWorkloadHold freezes the workload and keeps the shared freeze lock held,
// returning a release func, so the freeze stays uninterruptible across the
// handover: a concurrent /init or /unfreeze thaw blocks until release. The caller
// MUST release on any path that does not execve; a successful execve drops the
// lock with the process image.
// Unlike the pause path, this one is STRICT: if the workload did not fully stop
// the handover is refused. execve-ing over a still-running workload is exactly what
// the freeze exists to prevent, and unlike a pause the handover has a safe
// alternative -- abort and thaw, leaving a working envd in place.
func (s *Service) FreezeWorkloadHold() (release func(), err error) {
	release, res, err := s.workloadFreezer.FreezeHold(context.Background(), cgroups.FreezeOptions{MaxWait: s.handoverMaxWait})
	if err != nil {
		s.logger.Warn().Err(err).Msg("handover: freeze failed")

		return release, err
	}
	if !res.AllFrozen() {
		s.logger.Warn().
			Int("frozen", res.Frozen).
			Int("not_frozen", res.NotFrozen).
			Int("failed", res.Failed).
			Dur("wait_duration", res.WaitDuration).
			Msg("handover: refusing to swap over a workload that did not stop")

		return release, ErrWorkloadNotFrozen
	}

	return release, nil
}

// UnfreezeWorkload thaws the user/pty cgroups. Idempotent (thawing a non-frozen
// cgroup is a no-op), so it is safe to call on every upgrade outcome — success,
// failure, or panic — guaranteeing a failed swap never leaves the workload frozen.
func (s *Service) UnfreezeWorkload() {
	if err := s.workloadFreezer.Unfreeze(context.Background()); err != nil {
		s.logger.Warn().Err(err).Msg("handover: unfreeze failed")
	}
}

// ApplyHandoverDefaults restores the carried exec context onto the incoming envd's
// defaults, and reports which fields it applied.
//
// Only ever widens: a blob with no record (an outgoing envd predating the field)
// leaves the compiled-in fallback in place, which is the behaviour that existed
// before this field. A later /init carrying a non-empty user still overrides it --
// the host owns this state and the blob is a backstop, not an authority.
//
// Per field, matching the writer: a blob can carry a workdir and no user, and applying
// neither because one is absent is how a delivered workdir gets lost for good.
func ApplyHandoverDefaults(st *upgrade.HandoverState, d *execcontext.Defaults) (user, workdir bool) {
	hd := st.GetDefaults()
	if hd == nil || d == nil {
		return false, false
	}
	if u := hd.GetUser(); u != "" {
		d.User = u
		d.UserDelivered = true
		user = true
	}
	if hd.GetHasWorkdir() {
		w := hd.GetWorkdir()
		d.Workdir = &w
		workdir = true
	}

	return user, workdir
}
