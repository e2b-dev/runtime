package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// minHarvestTimeoutMs is the floor applied to the harvest-timeout flag, so a
// misconfigured zero/negative value can't yield an already-expired context that
// silently skips every harvest.
const minHarvestTimeoutMs = 1000

// harvestReapTimeout bounds the throwaway teardown so a stuck Stop/Close can't
// hold the start slot (released only after the reap) indefinitely.
const harvestReapTimeout = 60 * time.Second

// clampHarvestTimeoutMs floors the configured harvest timeout to minHarvestTimeoutMs.
func clampHarvestTimeoutMs(ms int) int {
	return max(ms, minHarvestTimeoutMs)
}

// harvestOutcome classifies a harvest attempt for the attempts/duration metrics.
type harvestOutcome string

// The vocabulary, in the order a harvest can fail:
//
//	skipped          the snapshot's deferred seal failed or did not settle inside
//	                 the harvest deadline; no start slot was ever taken
//	slot_timeout     the seal settled but no start slot freed up inside the
//	                 deadline; the node's starts were the bottleneck
//	resume_failed    slot held, the throwaway could not be brought up
//	collect_failed   throwaway up, the trace could not be read
//	success          trace harvested (the persist, if any, is best-effort)
//	persist_deadline trace harvested but never reached the artifact
//
// skipped and slot_timeout are kept apart because their fixes differ: one is
// an export that did not land, the other is start-slot pressure.
const (
	harvestSuccess       harvestOutcome = "success"
	harvestResumeFailed  harvestOutcome = "resume_failed"
	harvestCollectFailed harvestOutcome = "collect_failed"
	harvestSkipped       harvestOutcome = "skipped"
	harvestSlotTimeout   harvestOutcome = "slot_timeout"
	// Kept apart from success deliberately: the customer's next resume then
	// demand-faults everything, and counting that as a success is what made the
	// loss invisible.
	harvestPersistDeadline harvestOutcome = "persist_deadline"
)

var (
	harvestMeter              = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	harvestAttemptsCounter    = utils.Must(telemetry.GetCounter(harvestMeter, telemetry.PauseResumePrefetchHarvestAttempts))
	harvestDurationHistogram  = utils.Must(telemetry.GetHistogram(harvestMeter, telemetry.PauseResumePrefetchHarvestDurationName))
	harvestPagesHistogram     = utils.Must(telemetry.GetHistogram(harvestMeter, telemetry.PauseResumePrefetchHarvestPagesName))
	sealWaitDurationHistogram = utils.Must(telemetry.GetHistogram(harvestMeter, telemetry.PauseResumePrefetchSealWaitDurationName))
	persistWaitHistogram      = utils.Must(telemetry.GetHistogram(harvestMeter, telemetry.PauseResumePrefetchPersistWaitDurationName))
)

// harvestSource names the operation that produced the snapshot a harvest
// resumes from; it is the "path" attribute on the harvest metrics.
type harvestSource string

const (
	harvestSourcePause      harvestSource = "pause"
	harvestSourceCheckpoint harvestSource = "checkpoint"
)

// harvestRun is what one harvest attempt produced. A struct rather than a tuple
// because the attempt has several independently interesting quantities, and
// positional returns of the same two types are easy to transpose at a call site.
type harvestRun struct {
	// pages is the harvested trace size in blocks. Meaningful only once a trace
	// was actually collected.
	pages int
	// outcome classifies the attempt for the attempts/duration metrics.
	outcome harvestOutcome
	// slotHold is how long the throwaway occupied a start slot: its resume, the
	// trace collection and the reap. This, and only this, is what the
	// harvest-timeout flag is a cap on.
	slotHold time.Duration
	// persistWait is how long the persist waited on the in-flight snapshot upload
	// before it could rewrite metadata. It holds no node resources, so it is
	// deliberately not part of slotHold. Only meaningful when persistAttempted.
	persistWait time.Duration
	// persistAttempted records whether the persist ran at all. A harvest that
	// failed to resume, collected an empty trace, or ran with consume off never
	// waits on the upload, and recording a 0 ms wait for it would bury the real
	// distribution under a spike at zero.
	persistAttempted bool
}

// harvestResumer resumes the throwaway instance the harvest records its trace
// from. It is an interface so the orchestration can be unit-tested with a fake.
type harvestResumer interface {
	// ResumeForHarvest resumes a throwaway, network-isolated, unregistered copy
	// of the snapshot; the caller reaps the returned instance.
	ResumeForHarvest(ctx context.Context, t sbxtemplate.Template, config *sandbox.Config, runtime sandboxtypes.RuntimeMetadata, startedAt, endAt time.Time) (harvestInstance, error)
}

// harvestInstance is the subset of a resumed sandbox the harvest uses.
type harvestInstance interface {
	MemoryPrefetchData(ctx context.Context) (block.PrefetchData, error)
	Stop(ctx context.Context) error
	Close(ctx context.Context) error
}

// harvestTemplates is the subset of the template cache the harvest uses.
type harvestTemplates interface {
	GetTemplatePinned(ctx context.Context, buildID string, isSnapshot, isBuilding bool, opts ...sbxtemplate.GetTemplateOpts) (sbxtemplate.Template, func(), error)
	UpdateMetadata(ctx context.Context, buildID string, meta metadata.Template) error
}

// harvestUpload is the subset of the in-flight snapshot upload the harvest waits on.
type harvestUpload interface {
	Wait(ctx context.Context) error
}

// factoryResumer adapts *sandbox.Factory to harvestResumer, applying the
// throwaway-specific resume options (deny egress, skip live registration) so the
// orchestration itself stays agnostic of them.
type factoryResumer struct {
	factory *sandbox.Factory
}

func (r factoryResumer) ResumeForHarvest(ctx context.Context, t sbxtemplate.Template, config *sandbox.Config, runtime sandboxtypes.RuntimeMetadata, startedAt, endAt time.Time) (harvestInstance, error) {
	sbx, err := r.factory.ResumeSandbox(
		ctx,
		t,
		config,
		runtime,
		startedAt,
		endAt,
		// Throwaway: no API config to store (it is never restarted/addressable).
		nil,
		// Network-isolated + unregistered; the set is asserted in package sandbox.
		sandbox.ThrowawayResumeOptions()...,
	)
	if err != nil {
		// Return a nil interface, not a typed-nil *Sandbox, on error.
		return nil, err
	}

	return sbx, nil
}

// prefetchHarvester runs the pause-side resume-prefetch harvest against injected
// collaborators, so its orchestration (resume, reap, consume gate, upload
// ordering) is unit-testable with fakes.
type prefetchHarvester struct {
	resumer   harvestResumer
	templates harvestTemplates
	// uploadMetadata re-uploads the prefetch-enriched metadata object remotely.
	uploadMetadata func(ctx context.Context, meta metadata.Template, objectMetadata storage.ObjectMetadata) error
	// acquire bounds concurrent harvests; release frees the slot acquire took.
	acquire func(context.Context) error
	release func()
	// persistBudget caps the persist's wait on the in-flight snapshot upload.
	// Zero means the upload's own retry budget, which is the only bound that
	// cannot cut a still-landing upload short. Tests set it to keep the wait short.
	persistBudget time.Duration
}

func (s *Server) newPrefetchHarvester() *prefetchHarvester {
	return &prefetchHarvester{
		resumer:   factoryResumer{factory: s.sandboxFactory},
		templates: s.templateCache,
		uploadMetadata: func(ctx context.Context, meta metadata.Template, objectMetadata storage.ObjectMetadata) error {
			return metadata.UploadMetadata(ctx, s.persistence, meta, objectMetadata)
		},
		acquire: s.waitForAcquire,
		release: func() { s.startingSandboxes.Release(1) },
	}
}

// harvestResumePrefetchAsync records a resume page-fault trace for a snapshot
// that was written without a real resume — a pause, or an in-place checkpoint
// — and (optionally) persists it as a prefetch mapping, so the next resume or
// launch from that snapshot can replay it.
//
// It is the analogue of the resume+harvest the resume-fresh checkpoint does,
// with these differences: the resumed instance is a throwaway (network
// isolated, kept out of the live registry, never promoted to a live sandbox),
// and the harvested mapping is carried through metadata that otherwise drops
// Prefetch (the same-version pause metadata, or the in-place checkpoint's,
// which deliberately records no live-tracker trace).
//
// It is best-effort and runs AFTER the RPC has returned, alongside the
// in-flight snapshot upload: it never affects the pause or checkpoint result,
// the local snapshot is already in the cache, and the consume path waits for
// the upload before touching metadata. Both gates default off, so this is a
// no-op until explicitly enabled. source labels the harvest metrics so the two
// producers can be read apart.
func (s *Server) harvestResumePrefetchAsync(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	res *snapshotResult,
	buildID string,
	objectMetadata storage.ObjectMetadata,
	source harvestSource,
) {
	// Flag checks run synchronously (ctx still carries the per-sandbox LD
	// context the Pause handler attached) before we detach into a goroutine.
	if !s.featureFlags.BoolFlag(ctx, featureflags.PauseResumePrefetchHarvestFlag) {
		return
	}

	timeoutMs := s.featureFlags.IntFlag(ctx, featureflags.PauseResumePrefetchHarvestTimeoutMsFlag)
	// Guard against a misconfigured (zero/negative) flag value, which would yield
	// an already-expired context and silently skip every harvest.
	timeoutMs = clampHarvestTimeoutMs(timeoutMs)
	// When off, the harvest still runs and logs its trace size but persists
	// nothing — so resumes are unaffected and we can validate harvest behaviour
	// with no customer-visible change before enabling prefetch on resume.
	consume := s.featureFlags.BoolFlag(ctx, featureflags.PauseResumePrefetchConsumeFlag)
	harvester := s.newPrefetchHarvester()

	releaseWork := s.info.TrackWork()
	go func() {
		defer releaseWork()

		// Detach from the request (Pause has returned) but keep the LD context
		// values; bound the whole harvest so a stuck resume can't pin the slot.
		hCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()

		hCtx, span := tracer.Start(hCtx, "harvest-resume-prefetch", trace.WithNewRoot())
		defer span.End()
		span.SetAttributes(
			attribute.String("build_id", buildID),
			attribute.Bool("consume", consume),
			attribute.String("path", string(source)),
		)

		// Both deferred exports seal in the background after the RPC returned, and
		// the throwaway warm resume below reads both artifacts: the rootfs diff
		// (reflinked off the critical path when deferred rootfs export is on) and,
		// for an in-place checkpoint through the CoW window, the memfile diff
		// (swept and flushed while the source keeps running). Wait for the seals
		// here instead of letting the resume block on — and burn its budget
		// against — them. Both return immediately for the synchronous paths. If a
		// seal fails, or the harvest deadline fires before it completes, skip the
		// harvest (it is best-effort and must never touch a half-sealed snapshot).
		// Record the wait as its own metric and start the harvest timer after it,
		// so the seal wait doesn't inflate the harvest-duration (slot-hold)
		// histogram.
		sealWaitStart := time.Now()
		sealWaitErr := waitSnapshotSealed(hCtx, res)
		sealWait := time.Since(sealWaitStart)

		var (
			result harvestRun
			err    error
		)
		if sealWaitErr != nil {
			result.outcome, err = harvestSkipped, sealWaitErr
		} else {
			result, err = harvester.run(hCtx, sbx, res.meta, res.upload, buildID, objectMetadata, consume)
		}

		// Every histogram below carries result and path: the two producers have
		// different seal waits (rootfs only for a pause, memfile sweep then rootfs
		// for an in-place checkpoint) and different slot holds, and a reader has
		// to be able to tell a skipped seal from a lost slot per path.
		resultAttr := metric.WithAttributes(
			attribute.String("result", string(result.outcome)),
			attribute.String("path", string(source)),
		)
		sealWaitDurationHistogram.Record(hCtx, sealWait.Milliseconds(), resultAttr)
		harvestAttemptsCounter.Add(hCtx, 1, resultAttr)
		// Slot hold and persist wait are recorded apart because only the first is
		// a node-capacity cost. Summing them into one "harvest duration" is what
		// made the timeout look like it bounded the persist as well.
		harvestDurationHistogram.Record(hCtx, result.slotHold.Milliseconds(), resultAttr)
		if result.persistAttempted {
			persistWaitHistogram.Record(hCtx, result.persistWait.Milliseconds(), resultAttr)
		}
		if result.outcome == harvestSuccess {
			// pages is meaningful only when a trace was harvested; its bottom
			// bucket then surfaces the empty-trace (idle-at-pause) rate.
			harvestPagesHistogram.Record(hCtx, int64(result.pages), metric.WithAttributes(attribute.String("path", string(source))))
		}

		span.SetAttributes(
			attribute.Int64("harvest.duration_ms", result.slotHold.Milliseconds()),
			attribute.Bool("harvest.persist_attempted", result.persistAttempted),
			attribute.Int64("harvest.persist_wait_ms", result.persistWait.Milliseconds()),
			attribute.Int("harvest.pages", result.pages),
			attribute.String("harvest.result", string(result.outcome)),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			logger.L().Warn(hCtx, "resume prefetch harvest failed",
				logger.WithSandboxID(sbx.Runtime.SandboxID),
				logger.WithBuildID(buildID),
				zap.String("path", string(source)),
				zap.Error(err),
			)
		}
	}()
}

// waitSnapshotSealed blocks until every deferred artifact of the snapshot a
// harvest resumes from has settled: the memfile seal of an in-place CoW-window
// export first (a memfile that is still being swept would feed the throwaway
// pre-window bytes), then the rootfs seal. Nil when nothing was deferred.
func waitSnapshotSealed(ctx context.Context, res *snapshotResult) error {
	if res.memoryExportDeferred && res.waitMemorySealed != nil {
		if err := res.waitMemorySealed(ctx); err != nil {
			return fmt.Errorf("waiting for memory seal: %w", err)
		}
	}
	if _, err := res.rootfsDiff.CachePath(ctx); err != nil {
		return fmt.Errorf("waiting for rootfs seal: %w", err)
	}

	return nil
}

// run performs the throwaway warm resume, collects the fault trace, and (when
// consume is set) persists the mapping into the pause artifact metadata locally
// and remotely. Returns the harvested page count and the attempt outcome. Once
// the trace is harvested the outcome is success even if a (best-effort) persist
// step then fails — the error is still returned for logging.
func (h *prefetchHarvester) run(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	meta metadata.Template,
	upload harvestUpload,
	buildID string,
	objectMetadata storage.ObjectMetadata,
	consume bool,
) (harvestRun, error) {
	// The throwaway resume and its start slot are confined to resumeMapping, so
	// they are released before we wait on the upload below — which is exactly why
	// its duration is the slot-hold cost and the rest of this function is not.
	slotHoldStart := time.Now()
	mapping, outcome, err := h.resumeMapping(ctx, sbx, buildID)
	slotHold := time.Since(slotHoldStart)
	if err != nil {
		return harvestRun{outcome: outcome, slotHold: slotHold}, err
	}

	// Count is nil-safe.
	result := harvestRun{pages: mapping.Count(), outcome: harvestSuccess, slotHold: slotHold}

	if !consume || mapping == nil {
		// Harvest-only: trace measured (page count returned/logged), persist
		// nothing, so the customer's resume is unaffected.
		return result, nil
	}

	// Everything from here on is resource-free: resumeMapping has already released
	// the throwaway and the start slot it held. The harvest timeout in ctx caps
	// that slot hold, so applying it here would throw away a finished mapping to
	// protect nothing. Detach from it, and bound the persist by the upload's own
	// retry budget instead — the upload is the only thing this waits on, and it
	// cannot outlive its own budget.
	persistBudget := h.persistBudget
	if persistBudget <= 0 {
		persistBudget = uploadTotalBudget
	}
	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(ctx), persistBudget)
	defer cancelPersist()

	// The async snapshot upload (uploadSnapshotAsync) is still in flight: it reads
	// this build's local metafile and writes the remote metadata object, with
	// retries, from another goroutine. Rewriting the metafile in place (the local
	// update) or the remote object while it runs would risk a torn read or a
	// clobbered mapping, so wait for it to finish first. Once Wait returns on its
	// own the upload is done with both files; only if it outlived even its own
	// retry budget do we leave them untouched — and then say so.
	persistWaitStart := time.Now()
	uploadErr := upload.Wait(persistCtx)
	result.persistWait, result.persistAttempted = time.Since(persistWaitStart), true
	if persistCtx.Err() != nil {
		result.outcome = harvestPersistDeadline

		return result, fmt.Errorf("waiting for snapshot upload: %w", persistCtx.Err())
	}

	// Carry the mapping through the same-version pause metadata. The local cache
	// update is enough for a same-node resume, so do it regardless of whether the
	// remote upload succeeded.
	meta = meta.WithPrefetch(&metadata.Prefetch{Memory: mapping, Origin: metadata.PrefetchOriginHarvest})
	var localUpdateErr error
	if err := h.templates.UpdateMetadata(persistCtx, buildID, meta); err != nil {
		localUpdateErr = fmt.Errorf("update local metadata: %w", err)
		if !errors.Is(err, metadata.ErrReplaceCommitted) {
			return result, localUpdateErr
		}
	}

	// Only enrich the remote metadata if the snapshot actually landed; on upload
	// failure the remote build is incomplete, so there is nothing to enrich (the
	// local update above still lets a same-node resume prefetch).
	if uploadErr != nil {
		return result, localUpdateErr
	}
	if err := h.uploadMetadata(persistCtx, meta, objectMetadata); err != nil {
		return result, errors.Join(localUpdateErr, fmt.Errorf("re-upload metadata: %w", err))
	}

	return result, localUpdateErr
}

// resumeMapping resumes a throwaway warm copy of the just-paused snapshot,
// records its resume page-fault trace, and returns it as a prefetch mapping (nil
// if the trace was empty). The throwaway and the start slot it holds are both
// released before this returns, so the caller can persist the mapping without
// pinning node resources.
func (h *prefetchHarvester) resumeMapping(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	buildID string,
) (*metadata.MemoryPrefetchMapping, harvestOutcome, error) {
	// Bound concurrent harvests the same way real starts are bounded, so a burst
	// of pauses or checkpoints can't overcommit the node. If no slot frees up
	// within the harvest deadline the run is dropped (best-effort) and booked as
	// slot_timeout, so start-slot pressure reads apart from a seal that failed.
	if err := h.acquire(ctx); err != nil {
		return nil, harvestSlotTimeout, fmt.Errorf("acquire start slot: %w", err)
	}
	defer h.release()

	// Load the just-written snapshot from the LOCAL cache (warm): the harvest
	// pays no cold GCS/NFS fetch, only a local re-fault. isSnapshot=true mirrors
	// Checkpoint's resume. The pause artifact carries no Prefetch (SameVersion
	// dropped it), so no prefetcher runs and the trace is clean demand faults.
	// Pinned: the throwaway below is a real Firecracker VM running off this
	// template's snapfile, so for its lifetime the template must not be
	// evictable — eviction Closes it, and Close removes the snapfile. Released
	// last, after the reap deferred below has torn the throwaway down.
	tmpl, releaseTemplate, err := h.templates.GetTemplatePinned(ctx, buildID, true, false,
		sbxtemplate.GetTemplateOpts{MaxSandboxLengthHours: sbx.Config.MaxSandboxLengthHours})
	if err != nil {
		return nil, harvestResumeFailed, fmt.Errorf("get template: %w", err)
	}

	defer releaseTemplate()

	// Throwaway identity: distinct SandboxID/ExecutionID from the (being-stopped)
	// original so it never collides in the sandbox map. ResumeSandbox registers
	// it in the factory's sandbox table (for network assignment and health), but
	// it is never added to the server lifecycle or proxy pool and is reaped here,
	// so it is not externally addressable.
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  sbx.Runtime.TemplateID,
		SandboxID:   "prefetch-harvest-" + sbx.Runtime.SandboxID,
		ExecutionID: uuid.NewString(),
		TeamID:      sbx.Runtime.TeamID,
		// The throwaway resumes the just-written pause snapshot (buildID), not the
		// original sandbox's build, so tag it with buildID for correct attribution.
		BuildID:     buildID,
		SandboxType: sbx.Runtime.SandboxType,
	}

	// Suppress volume mounts on the throwaway. The throwaway is network-isolated
	// (DenyEgress drops all guest egress, including to the orchestrator IP that
	// fronts the NFS proxy), so the synchronous foreground NFS mount envd runs at
	// /init for a volume-mounted sandbox would block and fail the resume. The
	// prefetch mapping is memfile-only: page-cache pages resident at pause fault
	// back from the memfile, not over NFS, and NFS data not already cached was
	// never a memfile page — so dropping the mount loses no prefetchable coverage
	// while letting volume-mounted sandboxes harvest cleanly. Clone the config so
	// the live original's is untouched; the clone snapshots the network config
	// under the source's lock, because after an in-place checkpoint the source
	// keeps running and a concurrent Update may rewrite its egress while the
	// throwaway's slot is being configured from it.
	harvestConfig := sbx.Config.Clone()
	harvestConfig.VolumeMounts = nil

	// The resumer isolates the throwaway from the network and keeps it out of the
	// live registry: the user workload stays frozen until envd /init completes and
	// the instance is reaped right after, but envd /init itself (and any briefly
	// unfrozen workload) must not reach the network.
	resumedSbx, err := h.resumer.ResumeForHarvest(ctx, tmpl, harvestConfig, runtime, sbx.GetStartedAt(), sbx.GetEndAt())
	if err != nil {
		return nil, harvestResumeFailed, fmt.Errorf("resume throwaway: %w", err)
	}

	// Reap the throwaway on every path — it is never promoted to a live sandbox.
	// Detach from the harvest deadline so teardown still runs after a timeout, but
	// bound it so a stuck Stop/Close can't pin the start slot (held until this
	// returns) indefinitely.
	defer func() {
		reapCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), harvestReapTimeout)
		defer cancel()
		if stopErr := resumedSbx.Stop(reapCtx); stopErr != nil {
			logger.L().Warn(reapCtx, "harvest: failed to stop throwaway", logger.WithSandboxID(runtime.SandboxID), zap.Error(stopErr))
		}
		if closeErr := resumedSbx.Close(reapCtx); closeErr != nil {
			logger.L().Warn(reapCtx, "harvest: failed to close throwaway", logger.WithSandboxID(runtime.SandboxID), zap.Error(closeErr))
		}
	}()

	// ResumeSandbox blocks on WaitForEnvd -> initEnvd, so by here the trace
	// covers the full resume-through-envd-init working set.
	prefetchData, err := resumedSbx.MemoryPrefetchData(ctx)
	if err != nil {
		return nil, harvestCollectFailed, fmt.Errorf("collect prefetch data: %w", err)
	}

	return metadata.PrefetchEntriesToMapping(
		slices.Collect(maps.Values(prefetchData.BlockEntries)),
		prefetchData.BlockSize,
	), harvestSuccess, nil
}
