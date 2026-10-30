//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// errEnvdReinitCut is the cause a post-checkpoint re-init is cancelled with when a Pause
// stops it or a fresh re-init replaces it, and what initEnvd returns, wrapped, when either
// kept it from completing: before its gate chose a loop, or with its /init unwritten or in
// flight. The handle's cutBy names which.
var errEnvdReinitCut = errors.New("envd re-init cut")

// envdReinit is the post-checkpoint re-init's handle, published on the sandbox before its
// goroutine starts, so that a following Pause can stop it before freezing. A held re-init
// that wrote its /init after the pre-pause freeze would have envd serve it behind that
// freeze, and its handler would thaw the workload the snapshot is about to capture.
//
// A Pause marks the handle stopped, so that no further write starts: the held loop checks
// the mark under mu before each /init it writes. It then cancels a held re-init's context,
// whatever it has written, and waits for it to return. A re-init that has not yet chosen
// its loop is only marked: if it then chooses the held loop, begin refuses it and it
// writes nothing. An /init already written
// cannot thaw the freeze that follows on an envd at or above MinEnvdVersionForHeldInit:
// its handler read envd's freeze generation when it entered, the freeze advances it, and
// the handler's thaw is withheld. Only a held re-init is cut: a legacy-loop re-init is left
// as it is, so its /init, if envd serves it after the pre-pause freeze, may still thaw that
// freeze -- today's behaviour, which the flag leaves unchanged.
//
// Every fresh re-init replaces the handle before it (startEnvdReinit): begin then refuses
// that one on either loop, a held one in flight is cancelled, and none writes again, so a
// re-init still undecided or in flight when a fresh one starts never writes beside it. One
// that chose the legacy loop by then is left to run, as before. A Pause's cleanup replaces
// the handle it stopped before starting a fresh one, and starts none when the handle was
// left to run or already replaced.
//
// A Pause's cut also holds publication (envdReinitHolds): from the cut until its freeze
// landed, or until it gave up before freezing, no fresh re-init is published, so the
// handle the cut stopped is the one in place when the guest freezes. A start under a hold
// starts nothing; the Pause holding it re-syncs the clock itself once its outcome is known.
type envdReinit struct {
	mu       sync.Mutex
	decided  bool
	held     bool
	stopped  bool
	replaced bool
	// cutBy is what first marked the re-init to keep it from completing, recorded by the
	// stop (pause) or the replace (replace) that did, whether that cancelled it in flight
	// or had begin refuse it. "" until then.
	cutBy string
	// returned is set once the re-init has returned.
	returned bool
	cancel   context.CancelCauseFunc
	// done is closed once the re-init has returned and recorded how it ended.
	done chan struct{}
}

func newEnvdReinit(cancel context.CancelCauseFunc) *envdReinit {
	return &envdReinit{cancel: cancel, done: make(chan struct{})}
}

// begin records the loop the re-init's gate picked. It reports false for a held re-init a
// Pause already stopped, and for a re-init a fresh one replaced, which then write nothing;
// the mark that refuses it recorded its cause in cutBy when it landed.
func (r *envdReinit) begin(held bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.decided, r.held = true, held

	return !r.replaced && (!held || !r.stopped)
}

// mayWrite reports whether the re-init may write its next /init: false once a Pause has
// stopped a held re-init, or a fresh re-init replaced it.
func (r *envdReinit) mayWrite() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return !r.replaced && (!r.held || !r.stopped)
}

// finish records that the re-init has returned, and reports whether it was held and what
// first kept it from completing (cutBy). A Pause stopping it from then on does not wait;
// one already waiting is released by publishReturned, which the re-init calls once it has
// recorded how it ended.
func (r *envdReinit) finish() (held bool, cutBy string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.returned = true

	return r.held, r.cutBy
}

// publishReturned releases a Pause waiting on the re-init.
func (r *envdReinit) publishReturned() {
	close(r.done)
}

// leftToRunLocked reports whether the re-init chose the legacy loop, which a stop leaves
// to run. The caller holds mu.
func (r *envdReinit) leftToRunLocked() bool {
	return r.decided && !r.held
}

// cutLocked records cause as what first kept the re-init from completing, and cancels a
// decided re-init, whatever it has written. The caller holds mu and calls it only for a
// handle that has not returned and is not left to run on the legacy loop, so a decided one
// here is held and in flight.
func (r *envdReinit) cutLocked(cause string) {
	if r.cutBy == "" {
		r.cutBy = cause
	}
	if r.decided {
		r.cancel(errEnvdReinitCut)
	}
}

// replace marks the handle replaced by a fresh re-init, and cuts it as replace: a held
// one in flight is cancelled, since nothing of it may be served behind a freeze the
// replacing path did not stop it for, and an undecided one is refused by begin. It
// reports false, and marks nothing, for one left to run on the legacy loop, which needs
// none, and for one already replaced, whose replacement is in place. A stop that marked
// the handle first keeps its cause.
func (r *envdReinit) replace() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.replaced || r.leftToRunLocked() {
		return false
	}
	r.replaced = true
	if !r.returned {
		r.cutLocked("replace")
	}

	return true
}

// stop marks the handle stopped and cuts it as pause: a held re-init that has not
// returned is cancelled, and the caller must wait for it to return. An undecided re-init
// is not waited on: a held one sees the mark in begin and writes nothing; a legacy one is
// left as it is. stopped reports whether the mark may keep the re-init from completing:
// it has not returned, and it is held or has not yet chosen its loop. A replace that
// marked the handle first keeps its cause.
func (r *envdReinit) stop() (wait, stopped bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopped = true
	if r.returned || r.leftToRunLocked() {
		return false, false
	}
	r.cutLocked("pause")

	return r.decided, true
}

// startEnvdReinit publishes a re-init handle in place of the previous one, which it
// replaces, and runs bestEffortEnvdReinit on a goroutine: a sick envd must cost the
// checkpoint nothing. A following Pause stops it through cutEnvdReinit. Under a Pause's
// hold (between its cut and its freeze) it publishes nothing and reports false: a handle
// published there would escape the cut and could write its /init across the freeze.
func (s *Sandbox) startEnvdReinit(ctx context.Context) bool {
	initCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	r := newEnvdReinit(cancel)

	s.envdReinitMu.Lock()
	if s.envdReinitHolds > 0 {
		s.envdReinitMu.Unlock()
		cancel(errEnvdReinitCut)

		return false
	}
	prev := s.envdReinit.Swap(r)
	s.envdReinitMu.Unlock()
	if prev != nil {
		prev.replace()
	}

	go s.bestEffortEnvdReinit(ctx, initCtx, r)

	return true
}

// releaseEnvdReinit releases the hold a cutEnvdReinit took, once the Pause's freeze landed
// or the Pause gave up before it; fresh re-inits publish again when no hold is left.
func (s *Sandbox) releaseEnvdReinit() {
	s.envdReinitMu.Lock()
	defer s.envdReinitMu.Unlock()

	s.envdReinitHolds--
}

// cutEnvdReinit stops this sandbox's post-checkpoint re-init before Pause freezes the
// guest: a held one is cancelled, whatever it has written, and waited for under ctx. The
// wait is for the re-init's goroutine to see its cancellation, not for envd to answer. If
// ctx ends first, the error is returned before anything has frozen the guest. It returns
// the handle it stopped, nil when there was none, and cut reports whether the stop may
// keep the re-init from completing (stop's stopped). It takes a hold on publication
// first, whatever it finds, so the handle it stops stays the published one until the
// caller's releaseEnvdReinit; the caller owes one release per cut.
func (s *Sandbox) cutEnvdReinit(ctx context.Context) (r *envdReinit, cut bool, err error) {
	s.envdReinitMu.Lock()
	s.envdReinitHolds++
	r = s.envdReinit.Load()
	s.envdReinitMu.Unlock()
	if r == nil {
		return nil, false, nil
	}
	wait, stopped := r.stop()
	if !wait {
		return r, stopped, nil
	}

	select {
	case <-r.done:
		return r, true, nil
	case <-ctx.Done():
		return r, true, fmt.Errorf("wait for the envd re-init: %w", context.Cause(ctx))
	}
}

// cutEnvdReinitForPause is Pause's cut. When it stops a held or undecided re-init, it
// registers on the Pause's cleanup a fresh re-init for a failure before its VM pause is
// known to have landed, unless the sandbox was killed: the stopped re-init carried the
// previous checkpoint's clock re-sync, and such a Pause starts none of its own. The
// cleanup decides on the handle this cut stopped: one that chose the legacy loop by then
// was left to run, and one another cleanup already replaced has its replacement, so it
// starts none beside either; one still undecided is replaced, and writes nothing once it
// chooses. pauseLanded is read when the cleanup runs; once the VM pause landed, an
// in-place Pause's resume cleanup owns the re-sync, and a destroy-path Pause re-syncs
// nothing, as before. A cut whose wait ended with ctx returns its error with the cleanup
// registered. The cleanup also releases the cut's hold on publication for a Pause that
// gave up before its freeze landed; one whose freeze landed released it there.
func (s *Sandbox) cutEnvdReinitForPause(ctx context.Context, cleanup *Cleanup, pauseLanded *bool) error {
	r, cut, err := s.cutEnvdReinit(ctx)
	cleanup.Add(ctx, func(ctx context.Context) error {
		if *pauseLanded {
			return nil
		}
		s.releaseEnvdReinit()
		if cut && s.GetStopReason() != StopReasonKilled && r.replace() {
			s.startEnvdReinit(ctx)
		}

		return nil
	})

	return err
}

// bestEffortEnvdReinit re-runs the envd /init a real resume makes, after an
// in-place resume (success or error-cleanup path), or from the cleanup of a
// Pause that cut one (cutEnvdReinitForPause). The FC-paused window
// stopped the guest's clocks; envd steps CLOCK_REALTIME from /init's host
// timestamp when it lags by >50ms — less than a single checkpoint's pause
// window — so without this every checkpoint leaves the guest's wall clock
// behind by the paused duration and repeated checkpoints accumulate the
// drift. Best-effort: it is the same /init every resume already makes against
// a live envd, and a failure only leaves the clock lagging until the next
// resume. initCtx is detached from the request so a dying request ctx can't
// skip it.
//
// The episode budget is the envd-timeout flag (~10s) — the same bound a real
// resume gives WaitForEnvd — NOT EnvdInitRequestTimeout: that 50ms value is
// the PER-ATTEMPT deadline inside initEnvd's retry loop, calibrated for
// fail-fast-and-retry, and using it as the total would allow exactly one
// attempt against a guest that is busy working through its post-freeze
// backlog. initCtx carries the /init, so nothing of the re-init outlives that
// budget.
func (s *Sandbox) bestEffortEnvdReinit(ctx, initCtx context.Context, r *envdReinit) {
	defer r.cancel(nil)

	budget := time.Duration(s.featureFlags.IntFlag(ctx, featureflags.EnvdTimeoutMilliseconds)) * time.Millisecond

	// The same race WaitForEnvd runs around this exact retry loop: bound it on
	// sandbox LIVENESS, not just time. Detached from the request, a re-init
	// that only times out would outlive a sandbox killed mid-budget — and
	// outlive the network pool's 3s slot drain, POSTing this sandbox's /init
	// body (access token, env vars, CA bundle) at whichever tenant is handed
	// the recycled slot IP next.
	go func() {
		select {
		case <-time.After(budget):
			r.cancel(errors.New("envd re-init budget exceeded"))
		case <-initCtx.Done():
			return
		case <-s.process.Exit.Done():
			// Exit.Error() is nil for every orchestrator-initiated teardown
			// (SIGTERM/SIGKILL both store nil); a nil %w renders as
			// %!w(<nil>), garbling the dominant cause string.
			if exitErr := s.process.Exit.Error(); exitErr != nil {
				r.cancel(fmt.Errorf("%w: %w", ErrFcProcessExited, exitErr))
			} else {
				r.cancel(ErrFcProcessExited)
			}
		}
	}()

	err := s.initEnvd(initCtx, StartTypeResume, false, envdInitOptions{budget: budget, reinit: r})

	// The re-init was cut only if initEnvd says so: a stop whose cancel the response
	// outran cut nothing.
	cut := errors.Is(err, errEnvdReinitCut)
	held, cutBy := r.finish()
	switch {
	case cut && cutBy != "":
		envdReinitCut.Add(ctx, 1, metric.WithAttributes(attribute.String("by", cutBy), s.teamAttr()))
	case held && err != nil && errors.Is(err, ErrFcProcessExited):
		envdReinitCut.Add(ctx, 1, metric.WithAttributes(attribute.String("by", "kill"), s.teamAttr()))
	}
	switch {
	case err == nil || cut:
	case errors.Is(err, errEnvdRefusedByNetwork):
		// The sandbox keeps running.
		s.log().Warn(ctx, "envd re-init: the sandbox's network refused the envd that answered", zap.Error(err))
	default:
		s.log().Warn(ctx, "envd re-init after in-place resume failed (guest clock may lag)", zap.Error(err))
	}
	// Last, after the counter and the warning: a waiter reads both once done is closed.
	r.publishReturned()
}
