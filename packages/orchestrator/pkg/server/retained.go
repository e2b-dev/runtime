//go:build linux

package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// errRetainedPaused marks a retained capture that failed after its guest
// paused: the caller owns the paused guest.
var errRetainedPaused = errors.New("the retained guest is paused")

// RetainedCheckpoint is a snapshot whose sandbox stays paused with its
// process alive (CaptureRetained). This node keeps the snapshot in its cache
// until its upload lands or, after a failed upload, until the owner calls
// Release. When it offered it to peers, the offer stays registered while the
// upload runs and, after a failed upload, until its key expires.
type RetainedCheckpoint struct {
	buildID    string
	upload     *sandbox.Upload
	peers      peerclient.Registry
	advertised bool
	landed     atomic.Bool
	// ended is set once the upload ended either way, released once the owner
	// called Release. The pin drops once both hold, or the upload landed.
	ended, released atomic.Bool
	unpin           func()
}

// newRetainedCheckpoint returns the checkpoint of res's snapshot, holding the
// cache pin unpin releases. It has res's upload completion record a landed
// upload and release the pin before the completion finishes the upload and
// withdraws the peer offer, so Readable holds once Uploaded returned nil.
func newRetainedCheckpoint(buildID string, res *snapshotResult, peers peerclient.Registry, unpin func()) *RetainedCheckpoint {
	checkpoint := &RetainedCheckpoint{buildID: buildID, upload: res.upload, peers: peers, advertised: res.advertised, unpin: sync.OnceFunc(unpin)}
	completeUpload := res.completeUpload
	res.completeUpload = func(ctx context.Context, err error) {
		checkpoint.ended.Store(true)
		if err == nil {
			checkpoint.landed.Store(true)
		}
		if err == nil || checkpoint.released.Load() {
			checkpoint.unpin()
		}
		completeUpload(ctx, err)
	}

	return checkpoint
}

// Release gives up this node's cache pin on the snapshot. The owner calls it
// once nothing reads the snapshot from this node. The pin drops at once if
// the upload ended, else when it ends: the upload reads the snapshot from the
// cache. A landed upload drops it on its own. Repeated calls do nothing.
func (c *RetainedCheckpoint) Release() {
	c.released.Store(true)
	if c.ended.Load() {
		c.unpin()
	}
}

// Uploaded waits until the snapshot's upload ended and returns its error,
// nil once the snapshot landed in storage.
func (c *RetainedCheckpoint) Uploaded(ctx context.Context) error {
	return c.upload.Wait(ctx)
}

// Readable reports whether another node can read the snapshot now: it landed
// in storage, or the peer offer this node registered is still registered.
func (c *RetainedCheckpoint) Readable(ctx context.Context) bool {
	if c.landed.Load() {
		return true
	}
	if !c.advertised {
		return false
	}
	_, offered, err := c.peers.Lookup(ctx, c.buildID)

	return err == nil && offered
}

// ReserveStart takes sandboxID for a start on this node within its sandbox
// limit, as Create does.
func (s *Server) ReserveStart(sandboxID string) (*sandbox.Reservation, error) {
	return s.sandboxFactory.Sandboxes.ReserveWithin(sandboxID, s.info.MaxSandboxes.Load())
}

// errCaptureNotMigrating refuses a CaptureRetained of a sandbox not marked
// with Map.BeginMigration.
var errCaptureNotMigrating = errors.New("the sandbox is not being migrated")

// ErrCaptureGuestRunning marks a failed CaptureRetained that left the guest
// running.
var ErrCaptureGuestRunning = errors.New("the capture failed and the guest runs")

// CaptureRetained snapshots the memory and rootfs of sbx into buildID through
// the pause path and leaves the guest paused, its process alive and its
// health checks off (sandbox.WithRetainedPause). The snapshot is in this
// node's cache, offered to peers when peer transfer is on, and uploads in the
// background. It refuses what Pause refuses before pausing, with the guest
// untouched, and a capture that fails after that resumed the guest: both are
// ErrCaptureGuestRunning. A guest it could not resume it tore down
// (sandbox.ErrSandboxLost). It runs under Map.BeginMigration and refuses a
// sandbox not being migrated.
func (s *Server) CaptureRetained(ctx context.Context, sbx *sandbox.Sandbox, buildID string) (*RetainedCheckpoint, error) {
	if !sbx.MigrationInFlight() {
		return nil, errCaptureNotMigrating
	}
	ctx = featureflags.AddToContext(ctx, sandboxFlagContexts(sbx)...)
	if err := sbx.EnsurePausable(); err != nil {
		return nil, guestRuns(err)
	}
	if graceMs := s.featureFlags.IntFlag(ctx, featureflags.PauseAdmissionGraceMs); graceMs >= 0 {
		if _, _, err := sbx.AwaitSnapshotAdmission(ctx, time.Duration(graceMs)*time.Millisecond, true); err != nil {
			return nil, guestRuns(err)
		}
	}
	if healthMs := s.featureFlags.IntFlag(ctx, featureflags.PauseEnvdHealthTimeoutMs); healthMs > 0 {
		if _, _, err := sbx.AwaitEnvdAdmission(ctx, time.Duration(healthMs)*time.Millisecond); err != nil {
			return nil, guestRuns(err)
		}
	}
	diskReservation, err := s.admitPauseDisk(ctx, sbx, false)
	if err != nil {
		return nil, guestRuns(err)
	}

	res, err := s.snapshotAndCacheSandbox(ctx, sbx, buildID, map[string]string{storage.ObjectMetadataTemplateID: sbx.Runtime.TemplateID},
		storage.ObjectOriginPause, false, false, false, sandboxStaysPaused)
	if err != nil {
		diskReservation.Release()
		// A failed Pause resumed the guest, or tore it down when it could
		// not. After Pause the guest is paused, and resumed here.
		if errors.Is(err, errRetainedPaused) {
			err = errors.Join(err, s.resumeRetained(ctx, sbx))
		}

		return nil, guestRuns(err)
	}
	s.releasePauseDiskWhenWritten(context.WithoutCancel(ctx), res, diskReservation)
	completeUpload := res.completeUpload
	res.completeUpload = func(ctx context.Context, err error) {
		completeUpload(ctx, err)
		diskReservation.Release()
	}
	// The cache keeps the snapshot while a target may read it from here:
	// until the upload landed or, after it failed, until the owner calls
	// RetainedCheckpoint.Release.
	_, release, err := s.templateCache.GetTemplatePinned(ctx, buildID, true, false)
	if err != nil {
		res.abandonUpload(context.WithoutCancel(ctx), err)
		diskReservation.Release()

		return nil, guestRuns(errors.Join(errRetainedPaused, err, s.resumeRetained(ctx, sbx)))
	}
	checkpoint := newRetainedCheckpoint(buildID, res, s.peerRegistry, release)
	s.uploadSnapshotAsync(ctx, sbx, res)

	return checkpoint, nil
}

// guestRuns marks err ErrCaptureGuestRunning unless the guest was torn down.
func guestRuns(err error) error {
	if errors.Is(err, sandbox.ErrSandboxLost) {
		return err
	}

	return errors.Join(ErrCaptureGuestRunning, err)
}

// resumeRetained runs a retained guest again, as WithRetainedPause describes
// for an abandoned move, or tears it down when it cannot.
func (s *Server) resumeRetained(ctx context.Context, sbx *sandbox.Sandbox) error {
	ctx = context.WithoutCancel(ctx)
	if err := s.sandboxFactory.ResumeVM(ctx, sbx); err != nil {
		sbx.SetStopReason(sandbox.StopReasonKilled)

		return errors.Join(sandbox.ErrSandboxLost, err, sbx.Stop(ctx))
	}
	checks := sandbox.NewChecks(sbx)
	sbx.SetChecks(checks)
	go checks.Start(ctx)

	return nil
}

// errLifecycleTracked refuses a RestorePaused under a lifecycle already
// tracked.
var errLifecycleTracked = errors.New("the lifecycle is already tracked")

// RestorePaused restores buildID's snapshot of the execution api describes
// into one VM under lifecycleID, paused, as Create resumes a snapshot, on the
// kernel and Firecracker the snapshot was taken with: the guest has executed
// nothing when it returns. It runs on a slot from the pool, and the
// edition's network assignment hook runs before the load as for any resume.
// The sandbox keeps startedAt and endAt. Its lifecycle is tracked: it is closed once its process
// ends. The caller runs it with Factory.ResumeVM and PromoteRestored, or
// stops it. A lifecycle already tracked is refused before anything is
// assigned to it, and a configuration with an egress proxy passes Create's
// gate for it.
func (s *Server) RestorePaused(ctx context.Context, api *orchestrator.SandboxConfig, buildID, lifecycleID string, startedAt, endAt time.Time) (*sandbox.Sandbox, error) {
	// A second restore would replace the first VM's route and leave it with
	// no lifecycle entry. The check and the tracking are not atomic: the
	// caller restores once per migration and never runs two attempts at once.
	if s.sandboxFactory.Sandboxes.LifecycleTracked(api.GetSandboxId(), lifecycleID) {
		return nil, errLifecycleTracked
	}
	ctx = featureflags.AddToContext(ctx, startFlagContexts(api)...)
	if err := s.admitEgressProxy(ctx, api); err != nil {
		return nil, err
	}
	if err := s.waitForAcquire(ctx, startGateResume); err != nil {
		return nil, err
	}
	defer s.startingSandboxes.Release(1)
	releaseHugePages, err := s.admitHugePages(ctx, api)
	if err != nil {
		return nil, err
	}
	defer releaseHugePages()

	template, meta, profile, releaseTemplate, err := s.pinTemplateWithProfile(ctx, buildID, true, api.GetMaxSandboxLength())
	if err != nil {
		return nil, err
	}
	if err := firecrackerVersionRefusal(ctx, profile, meta.Template.FirecrackerVersion); err != nil {
		releaseTemplate()

		return nil, err
	}
	config, err := sandboxConfigOf(api, firecrackerConfigFor(meta, profile, meta.Template.KernelVersion, meta.Template.FirecrackerVersion))
	if err != nil {
		releaseTemplate()

		return nil, err
	}
	sbx, err := s.sandboxFactory.ResumeSandbox(ctx, template, config,
		sandboxtypes.RuntimeMetadata{
			TemplateID:  api.GetTemplateId(),
			SandboxID:   api.GetSandboxId(),
			ExecutionID: api.GetExecutionId(),
			TeamID:      api.GetTeamId(),
			BuildID:     buildID,
			SandboxType: sandboxtypes.SandboxTypeSandbox,
		},
		startedAt, endAt, api,
		sandbox.WithVMPaused(), sandbox.WithLifecycleID(lifecycleID), sandbox.WithExecutionStartedAt(startedAt),
	)
	if err != nil {
		releaseTemplate()

		return nil, err
	}
	s.setupSandboxLifecycle(ctx, sbx, releaseTemplate)

	return sbx, nil
}

// PromoteRestored registers a sandbox RestorePaused returned, once
// Factory.ResumeVM ran it, live under reservation and starts its health
// checks. It refuses once ctx ended, decided with the registration, and
// leaves the sandbox as it is for another try. What the registration
// notifies, its routing record included, is written as for any start. A
// repeat after a success changes nothing: the registration accepts the
// lifecycle already live, and its health checks start once.
func (s *Server) PromoteRestored(ctx context.Context, sbx *sandbox.Sandbox, reservation *sandbox.Reservation) error {
	if err := reservation.MarkRunningUnlessEnded(ctx, sbx); err != nil {
		return err
	}
	go sbx.Checks().Start(context.WithoutCancel(ctx))

	return nil
}
