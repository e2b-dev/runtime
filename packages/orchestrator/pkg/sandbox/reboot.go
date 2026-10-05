//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	buildenvd "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/constants"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	// minEnvdVersionForKVMClock is the minimum envd version that supports kvm-clock.
	minEnvdVersionForKVMClock = "0.2.11"

	// rebootEnvdTimeout bounds the systemd boot + envd start; a cold boot needs a
	// longer window than a memory resume (matches the template build's wait).
	rebootEnvdTimeout = 60 * time.Second
)

var (
	// Offline-upgrade rollout metrics (meter shared with sandbox.go).
	envdOfflineUpgradeAttempts     = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdOfflineUpgradeAttempts))
	envdOfflineUpgradeDurationHist = utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorEnvdOfflineUpgradeDurationName))

	// Pre-boot filesystem-recovery rollout metrics.
	fsRecoveryRuns               = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFsRecoveryRuns))
	fsRecoveryDurationHist       = utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorFsRecoveryDurationName))
	fsRecoveryToolingUnsupported = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorFsRecoveryToolingUnsupported))
)

// journalOnlyProbe fires the host e2fsck support probe once per process.
var journalOnlyProbe sync.Once

// checkRebootCPUTemplate reports whether the Firecracker version this reboot resolved to can
// apply the stored template. It can differ from the build's, since firecracker-versions remaps it.
func checkRebootCPUTemplate(tmpl *cputemplate.Template, fcVersion string) error {
	if tmpl == nil || tmpl.IsEmpty() {
		return nil
	}

	info, err := fcversion.New(fcVersion)
	if err != nil {
		return fmt.Errorf("parse firecracker version %q: %w", fcVersion, err)
	}

	if err := tmpl.Validate(info); err != nil {
		return fmt.Errorf("firecracker %s: %w", fcVersion, err)
	}

	return nil
}

// rebootCPUTemplate picks the template a cold boot applies: the build's, not whatever the last
// boot ran, unless override (the reboot-cpu-template-override flag) replaces it. An invalid
// override is returned as an error alongside the build's template.
func rebootCPUTemplate(meta metadata.Template, override ldvalue.Value) (*cputemplate.Template, bool, error) {
	if override.IsNull() {
		return meta.BuildCPUTemplate, false, nil
	}

	tmpl, err := cputemplate.Parse([]byte(override.JSONString()))
	if err != nil {
		return meta.BuildCPUTemplate, false, err
	}

	return tmpl, true, nil
}

// rebootAllowed reports whether a snapshot may be cold-booted: it is marked
// filesystem-only, or the request explicitly demanded a filesystem boot of its
// memory-inclusive snapshot, accepting crash-recovery semantics for the rootfs.
func rebootAllowed(meta metadata.Template, requestFilesystemBoot bool) bool {
	return meta.IsFilesystemOnly() || requestFilesystemBoot
}

// RebootSandbox cold-boots a fresh Firecracker VM from the template's rootfs,
// without restoring guest memory. Used to resume filesystem-only snapshots and
// explicitly requested filesystem boots of memory snapshots: guest RAM,
// processes, and sockets are lost; only the filesystem survives.
// The sandbox is marked running only after envd is ready, matching
// ResumeSandbox's routing guarantees; endAt is the caller's absolute end time.
// procOpts, if any, adjust the fc.ProcessOptions of the cold boot after the
// defaults are set (e.g. debugging tools forwarding the guest kernel console).
// IMPORTANT: You must Close() the sandbox after you are done with it.
func (f *Factory) RebootSandbox(
	ctx context.Context,
	t template.Template,
	config *Config,
	runtime sandboxtypes.RuntimeMetadata,
	endAt time.Time,
	apiConfigToStore *orchestrator.SandboxConfig,
	deferMarkRunning bool,
	requestFilesystemBoot bool,
	// recordRecovery reports the pre-boot recovery outcome (nil for callers that
	// don't pair it with a create metric, e.g. the resume-build tool).
	recordRecovery func(rootfs.RecoverOutcome),
	procOpts ...func(*fc.ProcessOptions),
) (*Sandbox, error) {
	ctx, span := tracer.Start(ctx, "reboot sandbox")
	defer span.End()

	if recordRecovery == nil {
		recordRecovery = func(rootfs.RecoverOutcome) {}
	}

	buildID, err := uuid.Parse(t.Files().BuildID)
	if err != nil {
		return nil, fmt.Errorf("parse build ID: %w", err)
	}

	// Safety gate: a memory snapshot's rootfs may be missing writes that lived
	// only in the guest page cache (restored on a memory resume), so cold-booting
	// it serves a crash-consistent disk at best. Refuse unless the snapshot is
	// marked fs-only or the request explicitly demanded the filesystem boot.
	meta, err := telemetry.Observe1(ctx, tracer, "get template metadata", func(context.Context) (metadata.Template, error) {
		return t.Metadata()
	})
	if err != nil {
		return nil, fmt.Errorf("get template metadata: %w", err)
	}
	if !rebootAllowed(meta, requestFilesystemBoot) {
		return nil, fmt.Errorf("refusing to reboot build %s: not a filesystem-only snapshot and the request did not demand a filesystem boot", buildID)
	}

	// It becomes the running template, so the next pause stores what this boot applied.
	override := f.featureFlags.JSONFlag(ctx, featureflags.RebootCPUTemplateOverride,
		featureflags.SandboxContext(runtime.SandboxID),
		featureflags.TemplateContext(runtime.TemplateID),
		featureflags.TeamContext(runtime.TeamID))
	cpuTemplate, overridden, err := rebootCPUTemplate(meta, override)
	if err != nil {
		logger.L().Error(ctx, "ignoring invalid reboot CPU template override", zap.Error(err))
	}
	if overridden {
		logger.L().Info(ctx, "reboot CPU template overridden",
			zap.String("build_cpu_template", cputemplate.AppliedDigest(meta.BuildCPUTemplate)),
			zap.String("cpu_template", cputemplate.AppliedDigest(cpuTemplate)))
	}
	meta.CPUTemplate = cpuTemplate
	span.SetAttributes(
		attribute.String("sandbox.build_cpu_template", cputemplate.AppliedDigest(meta.BuildCPUTemplate)),
		attribute.Bool("sandbox.cpu_template_overridden", overridden),
	)

	if err := checkRebootCPUTemplate(meta.CPUTemplate, config.FirecrackerConfig.FirecrackerVersion); err != nil {
		return nil, fmt.Errorf("reboot build %s: %w", buildID, err)
	}

	// A cold boot starts envd with no prior state, so unlike a memory resume it
	// can't inherit the template's default user/workdir from restored RAM — they
	// must be re-sent via /init, or envd falls back to root and /root.
	//
	// So this path must send something even when the metadata cannot establish what the
	// build sent, and it reconstructs rather than abstains. That is a GUESS, and it is
	// right only at or above templates.TemplateV2ReleaseVersion: below it finalize sent a
	// flat ("user", nil) regardless of what was recorded, so for such a build this sends
	// the recorded user where /init received "user" (see metadata.Template.EnvdDefaultUser
	// for why the two cannot be told apart). A memory resume refuses that guess because
	// the restored process still holds the truth; here there is no such process, and
	// declining would leave envd on its compiled-in fallback — a worse guess than this
	// one.
	if config.Envd.DefaultUser == nil {
		defaultUser := meta.Context.User
		if defaultUser == "" {
			defaultUser = consts.TemplateDefaultUser
		}
		config.Envd.DefaultUser = &defaultUser
	}
	if config.Envd.DefaultWorkdir == nil {
		config.Envd.DefaultWorkdir = meta.Context.WorkDir
	}

	// The masked empty memfile is used only for sizing NoopMemory — guest RAM
	// is FC's own fresh anonymous memory.
	pageSize := int64(header.PageSize)
	if config.HugePages {
		pageSize = int64(header.HugepageSize)
	}
	memfile, err := block.NewEmpty(units.MBToBytes(config.RamMB), pageSize, buildID)
	if err != nil {
		return nil, fmt.Errorf("create empty memfile: %w", err)
	}

	// With the resolved CPU template, so the next pause stores what this boot applied.
	maskedTemplate := template.NewMaskTemplate(t, template.WithMemfile(memfile), template.WithMetadata(meta))

	kvmClock, err := utils.IsGTEVersion(config.Envd.Version, minEnvdVersionForKVMClock)
	if err != nil {
		return nil, fmt.Errorf("compare envd version: %w", err)
	}

	// Sync IO engine so no async writes are in flight if the sandbox is paused again.
	ioEngine := models.DriveIoEngineSync

	// Always write MMDS metadata for a reboot so the cold-booted envd can
	// authenticate /init like a memory resume. An empty token hashes to the
	// "no token" value, matching ResumeSandbox's behavior for non-secure sandboxes.
	accessToken := ""
	if config.Envd.AccessToken != nil {
		accessToken = *config.Envd.AccessToken
	}

	timeout := time.Until(endAt)
	if timeout <= 0 {
		return nil, fmt.Errorf("sandbox end time %s is in the past", endAt)
	}

	processOptions := fc.ProcessOptions{
		InitScriptPath: constants.SystemdInitPath,
		KvmClock:       kvmClock,
		IoEngine:       &ioEngine,
		AccessToken:    &accessToken,
		// This is a cold boot, so unlike a memory resume it re-reads the command line —
		// and the snapshot's own metadata is the only record of what it was built with.
		// Replayed verbatim rather than re-resolved from the feature flag: what a
		// variant name means can change after the build, so re-resolving would boot a
		// different guest than the one this lineage was created as.
		CmdlineArgs: meta.CmdlineArgs,
		// Same as CmdlineArgs: the stored template, not the build flag (see the override above).
		CPUTemplate: meta.CPUTemplate,
	}

	// Recorded so a dropped variant is detectable after the fact: a cold boot carrying
	// the default under a lineage whose build recorded a variant means the field was
	// lost somewhere between build and boot. That is otherwise invisible, because the
	// guest simply comes back without whatever the variant provided.
	//
	// Reports what was APPLIED, not what was stored. buildKernelArgs drops an overlay
	// whose arguments this binary rejects — which is reachable if the reserved set grew
	// since the build that stamped them — so recording the stored name unconditionally
	// would claim a variant the guest never got, defeating the attribute's purpose.
	applied := ""
	if fc.ValidateCmdlineArgs(meta.CmdlineArgs) == nil {
		applied = fc.KernelArgs(meta.CmdlineArgs).String()
	}

	// A stored template is either applied or fails the boot, so it is what the guest got.
	appliedCPUTemplate := cputemplate.AppliedDigest(meta.CPUTemplate)

	span.SetAttributes(
		attribute.String("sandbox.cmdline_args", applied),
		attribute.String("sandbox.cpu_template", appliedCPUTemplate),
		attribute.Bool("sandbox.filesystem_boot_requested", requestFilesystemBoot),
	)
	for _, opt := range procOpts {
		opt(&processOptions)
	}

	preBoot := chainPreBoot(
		f.fsRecoverPreBoot(ctx, runtime, meta.IsFsQuiesced(), requestFilesystemBoot, recordRecovery),
		f.envdOfflineUpgradePreBoot(ctx, config, runtime, meta.IsFsQuiesced()),
	)

	sbx, err := f.CreateSandbox(
		ctx,
		config,
		runtime,
		maskedTemplate,
		timeout,
		// Empty rootfs cache path selects the NBD provider, same as a memory
		// resume, so guest TRIM keeps working and a later pause exports the
		// overlay diff exactly like a normal resume.
		"",
		processOptions,
		apiConfigToStore,
		preBoot,
		WithDeferredMarkRunning(),
		withNetworkAssignReason(NetworkAssignReasonReboot),
	)
	if err != nil {
		return nil, fmt.Errorf("create sandbox from rootfs: %w", err)
	}

	// CreateSandbox anchors the lifetime to now; honor the caller's absolute end
	// time so queue delay can't extend the TTL.
	sbx.SetEndAt(endAt)

	if err := sbx.WaitForEnvd(ctx, StartTypeReboot, rebootEnvdTimeout); err != nil {
		closeErr := sbx.Close(context.WithoutCancel(ctx))

		return nil, errors.Join(fmt.Errorf("wait for envd after reboot: %w", err), closeErr)
	}

	// deferMarkRunning: the caller promotes the sandbox to live itself after a
	// post-resume step (the resume-time envd live-upgrade's post-/init), so it is
	// not routable during the upgrade's pre-init auth window. Mirrors the resume
	// path's WithDeferredLiveRegistration.
	if !deferMarkRunning {
		if err := f.Sandboxes.MarkRunning(ctx, sbx); err != nil {
			closeErr := sbx.Close(context.WithoutCancel(ctx))

			return nil, errors.Join(err, closeErr)
		}

		go sbx.Checks.Start(context.WithoutCancel(ctx))
	}

	return sbx, nil
}

// offlineSwapDecision is the pure outcome of the offline-upgrade gate: whether to
// rewrite the rootfs envd, and — when not — how to report the no-op.
type offlineSwapDecision struct {
	swap         bool   // run the rootfs swap
	countResult  string // if set, record offline_upgrade.attempts{result=...} for this gated no-op
	logMisconfig bool   // log the resolver's no-op reason (a misconfigured / unstaged target)
}

// offlineNoopNotQuiesced is the countResult for the one no-op that is neither a
// resolver outcome nor an operator error: the resolver wanted an upgrade, but the
// snapshot's rootfs was not frozen at pause.
const offlineNoopNotQuiesced = "not_quiesced"

// decideOfflineSwap gates the cold-boot envd swap on the resolver outcome and the
// snapshot's crash-consistency. resolverPath is "" when the resolver returns no
// upgrade (reason says why); fsQuiesced is whether the rootfs was frozen at pause.
//
//   - flag off (off / no reason)                            -> no swap, silent, uncounted
//   - no upgrade, already on target (same_version)          -> no swap, count it
//   - no upgrade, binary not cached yet (binary_not_cached) -> no swap, count it
//   - no upgrade, misconfig (not_staged / downgrade / ...)  -> no swap, count AND log
//   - upgrade wanted but rootfs not frozen                  -> no swap, count not_quiesced
//   - upgrade wanted and rootfs frozen                      -> swap
//
// A resolver reason is handled on its own terms, frozen or not: no target was
// resolved, so no upgrade is known to be wanted, and only a resolved target is
// gated on the rootfs being frozen.
//
// Every no-op except `off` is counted, so the eligible population adds up: a cold boot
// that did not upgrade is either counted here or counted as an attempt below, and a ramp
// can read what fraction each reason holds. `off` is the deliberate exemption — it is the
// whole fs-only population minus the rest, already available as
// sandbox.create.duration{fs_only="true"}, and counting it would add a series per
// version pair for every cold boot in the fleet to say nothing.
func decideOfflineSwap(resolverPath, reason string, fsQuiesced bool) offlineSwapDecision {
	if resolverPath == "" {
		switch reason {
		case "", "off":
			return offlineSwapDecision{}
		case "same_version":
			// Expected, and the goal state of a ramp — counted so it is visible, not
			// logged because it recurs on every cold boot of an upgraded snapshot.
			return offlineSwapDecision{countResult: reason}
		case envdbin.ReasonNotCached:
			// The binary is not on local disk yet, so the swap is deferred rather than
			// paying for the mount during a boot. Counted, not logged: the background
			// warm makes this a one-resume state, and it is not an operator error.
			//
			// Reported even when the rootfs was not frozen, because on this arm no
			// version was ever obtained: a miss is refused upstream of the same_version
			// and downgrade comparisons, so an upgrade is not known to be wanted and
			// not_quiesced would be asserting one. The frozen state is a durable
			// property of the snapshot and is still there to report on the next cold
			// boot, once a warm has landed and a target has actually been resolved.
			return offlineSwapDecision{countResult: reason}
		case featureflags.ReasonSourceStalled:
			// The mount did not answer in time. Counted so a stall is visible apart
			// from warm-up, not logged per boot: the cache already warns once per
			// path per window, and a stall recurs on every boot until it clears.
			return offlineSwapDecision{countResult: reason}
		default:
			// not_staged / downgrade / invalid_target / getversion_failed, and anything
			// the resolver's vocabulary grows: an operator error, so log each one.
			return offlineSwapDecision{countResult: reason, logMisconfig: true}
		}
	}
	if !fsQuiesced {
		return offlineSwapDecision{countResult: offlineNoopNotQuiesced}
	}

	return offlineSwapDecision{swap: true}
}

// chainPreBoot composes pre-boot steps in order, skipping nils; the first
// failure stops the chain. Nil when every step is nil, preserving the
// no-callback fast path.
func chainPreBoot(fns ...PreBootFn) PreBootFn {
	steps := make([]PreBootFn, 0, len(fns))
	for _, fn := range fns {
		if fn != nil {
			steps = append(steps, fn)
		}
	}
	if len(steps) == 0 {
		return nil
	}

	return func(ctx context.Context, rootfsPath string) error {
		for _, fn := range steps {
			if err := fn(ctx, rootfsPath); err != nil {
				return err
			}
		}

		return nil
	}
}

// fsRecoverPreBoot returns a PreBootFn that repairs the rootfs filesystem
// before the cold boot, or nil when there is nothing to do: a quiesced
// snapshot was frozen at pause and is consistent by construction, and with
// the flag off cold boots keep today's behavior (the guest kernel replays
// the journal at mount time). It is one step of the reboot pre-boot chain,
// mutually exclusive in practice with the offline envd swap (that swaps only
// quiesced snapshots, this runs only on non-quiesced ones).
//
// A clean journal replay proceeds to boot; anything else fails the start with
// rootfs.ErrRecoveryFailed (snapshot untouched) instead of booting a possibly
// torn filesystem. Journal replay never condemns a snapshot: its exit codes
// cannot tell an unmountable filesystem apart from a transient device fault, so
// every non-replayed outcome is retryable, not a permanent verdict.
// record reports the recovery outcome to the caller (see Server.Create's
// fs_recovery metric); left uncalled on the flag-off path so that population
// stays "none".
func (f *Factory) fsRecoverPreBoot(
	ctx context.Context,
	runtime sandboxtypes.RuntimeMetadata,
	fsQuiesced bool,
	requestFilesystemBoot bool,
	record func(rootfs.RecoverOutcome),
) PreBootFn {
	ctx, span := tracer.Start(ctx, "resolve fs recovery", trace.WithAttributes(
		attribute.Bool("sandbox.fs_quiesced", fsQuiesced),
	))
	defer span.End()

	// Flag gate first: with the flag off, behavior is exactly today's (no metric,
	// no recovery) so nothing dilutes the ramp's result ratios. Every emission
	// below is therefore within the flag-on population.
	if !f.featureFlags.BoolFlag(ctx, featureflags.PrebootFsRecoveryFlag,
		featureflags.SandboxContext(runtime.SandboxID),
		featureflags.TemplateContext(runtime.TemplateID),
	) {
		span.SetAttributes(attribute.Bool("fs_recovery.enabled", false))

		return nil
	}
	span.SetAttributes(attribute.Bool("fs_recovery.enabled", true))

	// Once per process: flag a host whose e2fsck rejects -E journal_only, so an
	// unsupported image is visible on a dashboard. Boots are unaffected either way
	// (a rejected replay is a no-op the guest kernel redoes at mount); this only
	// surfaces the silent-inert case a per-run exit code cannot tell from a real
	// replay.
	journalOnlyProbe.Do(func() {
		if !rootfs.JournalOnlySupported(ctx) {
			fsRecoveryToolingUnsupported.Add(ctx, 1)
			logger.L().Warn(ctx, "host e2fsck rejects -E journal_only; pre-boot recovery is inert, boots fall back to the guest kernel's mount-time replay")
		}
	})

	// trigger separates the two admitted populations on every emission: an
	// explicit no-memory rescue vs a legacy sync-fallback fs-only snapshot.
	trigger := "legacy_fs_only"
	if requestFilesystemBoot {
		trigger = "rescue"
	}

	if fsQuiesced {
		record(rootfs.RecoverOutcomeSkippedQuiesced)
		fsRecoveryRuns.Add(ctx, 1, metric.WithAttributes(
			attribute.String("result", string(rootfs.RecoverOutcomeSkippedQuiesced)),
			attribute.String("reason", string(rootfs.RecoverReasonQuiesced)),
			attribute.String("trigger", trigger),
		))

		return nil
	}

	recoverFS := func(ctx context.Context, rootfsPath string) error {
		start := time.Now()
		outcome, reason, err := rootfs.RecoverFilesystem(ctx, rootfsPath)
		telemetry.SetAttributes(ctx,
			attribute.String("fs_recovery.outcome", string(outcome)),
			attribute.String("fs_recovery.reason", string(reason)),
		)
		record(outcome)
		attrs := metric.WithAttributes(
			attribute.String("result", string(outcome)),
			attribute.String("reason", string(reason)),
			attribute.String("trigger", trigger),
		)
		fsRecoveryRuns.Add(ctx, 1, attrs)
		fsRecoveryDurationHist.Record(ctx, time.Since(start).Milliseconds(), attrs)
		if err != nil {
			// Fail closed. Return, don't log: the create path (Server.Create /
			// CreateSandbox) reports this error once, and outcome+duration are already
			// on the metric above.
			return err
		}

		if outcome == rootfs.RecoverOutcomeFailedOpen {
			// e2fsck never opened the device (the jail could not launch it, or the host
			// cannot exec e2fsck), so the disk is what a flag-off cold boot would mount
			// and the guest kernel replays the journal itself. Boot anyway — but at Warn:
			// this is a host-image signal, not an expected per-sandbox outcome.
			runtime.Logger().Warn(ctx, "pre-boot filesystem recovery could not run; booting on the guest kernel's mount-time replay",
				zap.String("reason", string(reason)),
			)

			return nil
		}

		runtime.Logger().Info(ctx, "preboot filesystem recovery finished",
			zap.String("outcome", string(outcome)),
			zap.Duration("duration", time.Since(start)),
		)

		return nil
	}

	return func(ctx context.Context, rootfsPath string) error {
		return telemetry.Observe0(ctx, tracer, "fs-recover", func(ctx context.Context) error {
			return recoverFS(ctx, rootfsPath)
		}, trace.WithAttributes(attribute.String("fs_recovery.trigger", trigger)))
	}
}

// resolveOfflineTarget resolves the offline upgrade target and returns the
// resolver that did it, so the caller can honour the same entry at delivery.
//
// Extracted so the reason is observable: the relabel below is the difference
// between "this node has not warmed yet" and "this target cannot be read", and a
// test that can only see whether a swap happened cannot tell the two apart —
// dropping the relabel leaves such a test green while the operator-facing signal
// is silently wrong.
func (f *Factory) resolveOfflineTarget(
	ctx context.Context,
	from string,
	sbCtx, tmplCtx ldcontext.Context,
) (path, toVersion, reason string, binCache *envdbin.Resolver) {
	ctx, span := tracer.Start(ctx, "resolve offline envd upgrade", trace.WithAttributes(
		attribute.String("envd.from_version", from),
	))
	defer span.End()

	getVersion := buildenvd.GetEnvdVersion
	// The candidate checks stat the mount before any version probe, so they are
	// bounded whether or not the cache is engaged.
	stat := f.envdBinCache.BoundedStat
	// The nil check is not redundant with the flag: a Factory built as a struct
	// literal (as tests do) has no cache.
	if f.envdBinCache != nil && f.featureFlags.BoolFlag(ctx, featureflags.EnvdBinaryCacheFlag, sbCtx, tmplCtx) {
		binCache = envdbin.NewResolver(f.envdBinCache, envdbin.OpOffline)
		getVersion = binCache.Version
		stat = binCache.Stat
	}

	path, toVersion, reason = featureflags.ResolveEnvdOfflineUpgrade(
		ctx, f.featureFlags, from, f.config.HostEnvdPath, getVersion, stat, sbCtx, tmplCtx,
	)
	reason = envdbin.GatedReason(reason, binCache.DeferralOutcome())
	span.SetAttributes(
		attribute.String("envd.to_version", toVersion),
		attribute.String("envd.upgrade_reason", reason),
		attribute.Bool("envd.binary_cache", binCache != nil),
	)

	return path, toVersion, reason, binCache
}

// envdOfflineUpgradePreBoot returns a PreBootFn that rewrites the rootfs envd
// binary before the cold boot, or nil when no upgrade applies. It resolves the
// target through the shared envd-upgrade decision (ResolveEnvdOfflineUpgrade,
// sibling flag envd-offline-upgrade-target) keyed on the snapshot's built-with
// version — there is no running envd at cold-boot swap time, so unlike the live
// path there is no LiveEnvdVersion to key on, and the built-with never advances
// across an upgrade, so the swap re-fires idempotently on every resume until a
// re-pause re-bakes the version. The swap runs only when the snapshot's rootfs
// was frozen at pause (fs_quiesced): a legacy/sync-fallback snapshot is left on
// its current envd and becomes eligible after its next freezing pause.
//
// Best-effort with one exception: a failure that left the rootfs without a usable
// envd (rootfs.ErrOfflineSwapUnrecoverable) fails the boot, so CreateSandbox tears
// down and the dirty overlay is discarded. Every other failure boots the ORIGINAL
// envd.
func (f *Factory) envdOfflineUpgradePreBoot(
	ctx context.Context,
	config *Config,
	runtime sandboxtypes.RuntimeMetadata,
	fsQuiesced bool,
) PreBootFn {
	from := config.Envd.Version

	sbCtx := featureflags.SandboxContext(runtime.SandboxID)
	tmplCtx := featureflags.TemplateContext(runtime.TemplateID)
	// Same binary cache as the live path, and the same rule: a hit answers from a
	// local copy, a miss defers the swap to a later resume rather than reading the
	// mount. Off, it is the direct fork+exec of the source, unchanged.
	path, toVersion, reason, binCache := f.resolveOfflineTarget(ctx, from, sbCtx, tmplCtx)
	dec := decideOfflineSwap(path, reason, fsQuiesced)
	if !dec.swap {
		// A misconfigured / unstaged target is worth a logged signal on a ramp;
		// off / same_version are the expected per-resume no-ops and stay silent.
		if dec.logMisconfig {
			runtime.Logger().Info(ctx, "offline envd upgrade: target not resolved",
				zap.String("reason", reason),
				zap.String("built_with", from),
			)
		}
		// The resolver wanted an upgrade but the rootfs isn't known crash-consistent
		// (not frozen at pause): don't rewrite it — this population is waiting for a
		// future freezing pause, so it is worth a line each as well as the count.
		if dec.countResult == offlineNoopNotQuiesced {
			runtime.Logger().Info(ctx, "skipping offline envd upgrade: snapshot rootfs was not frozen at pause (not crash-consistent); awaiting a future freezing pause",
				zap.String("built_with", from),
				zap.String("target", path),
				zap.String("to_version", toVersion),
			)
		}
		// One emission for every counted no-op, whatever its kind — the logging above
		// varies by reason, the accounting must not.
		if dec.countResult != "" {
			envdOfflineUpgradeAttempts.Add(ctx, 1, metric.WithAttributes(
				attribute.String("result", dec.countResult),
				attribute.String("from_version", from),
				attribute.String("to_version", toVersion),
			))
		}

		return nil
	}

	return func(ctx context.Context, rootfsPath string) error {
		start := time.Now()
		// The swap stages its source into a directory bound into the jail, a copy
		// that is read off the gcsfuse mount unless the cache holds it. Resolved
		// here rather than where the version was, because this runs at boot,
		// arbitrarily later, and SourcePath re-checks that the copy is still on
		// disk. An entry evicted in between skips the swap: the source is not an
		// alternative, since reading it would be the mount cost this cache exists to
		// avoid and bytes whose version was never verified against them.
		swapSrc, swappable := binCache.SourcePath(ctx, path)
		if !swappable {
			// The copy that carried the resolved version is gone, so nothing on local
			// disk holds the bytes that version describes. Boot the original, which is
			// this path's best-effort default anyway, and let a later resume retry
			// once a warm has republished the copy.
			runtime.Logger().Warn(ctx, "skipping offline envd upgrade: cached binary went away after its version was resolved",
				zap.String("target", path),
				zap.String("built_with", from),
			)
			// Counted, not just logged. This returns before the attempt counter
			// below, so without this the refusal is invisible in the metric an
			// operator reads to account for the eligible population. This series is
			// the offline path's single vocabulary for what a boot did, so a no-op
			// belongs on it beside not_quiesced and not_staged; the live path, which has a
			// separate gated series, reports the same event there instead.
			envdOfflineUpgradeAttempts.Add(ctx, 1, metric.WithAttributes(
				attribute.String("result", envdbin.ReasonCopyVanished),
				attribute.String("from_version", from),
				attribute.String("to_version", toVersion),
			))

			return nil
		}

		// Cancel-free but time-bounded: a request cancellation must not kill
		// debugfs mid-write (a half-written inode would break the boot/export that
		// follows), yet a hung tool must not stall the boot forever. The budget is
		// EnvdSwapBudget, not the per-invocation EnvdSwapTimeout: the call is several
		// debugfs runs, and bounding the whole of it at one run's timeout lets a slow
		// backup starve the phases after it (see EnvdSwapBudget).
		swapCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rootfs.EnvdSwapBudget)
		swapped, err := f.offlineSwap()(swapCtx, rootfsPath, swapSrc)
		cancel()

		// An unrecoverable swap (failed AND the original was not restored) may leave
		// the rootfs without a usable envd. Booting it would hand back a running but
		// envd-less sandbox; instead fail the boot so CreateSandbox tears down and
		// the dirty overlay is discarded. Every other failure left the original in
		// place, so it stays best-effort and boots the original.
		unrecoverable := errors.Is(err, rootfs.ErrOfflineSwapUnrecoverable)
		result := "success"
		switch {
		case err == nil:
			runtime.Logger().Info(ctx, "swapped envd binary before reboot",
				zap.String("target", path),
				zap.String("to_version", toVersion),
				zap.String("built_with", from),
				// built_with is what the snapshot RECORDS; refire is what the rootfs
				// actually held. They disagree on every re-fire — see SwapResult.Refire.
				zap.Bool("refire", swapped.Refire),
			)
		case errors.Is(err, rootfs.ErrEnvdTooLarge):
			// Declined before anything was touched, so the guest boots its own envd.
			// Its own result value: this is a property of the rootfs, not a malfunction,
			// and on a ramp it should not read as swap breakage.
			result = "envd_too_large"
			runtime.Logger().Warn(ctx, "skipping offline envd upgrade: rootfs envd is too large to swap",
				zap.String("target", path),
				zap.String("built_with", from),
				zap.Error(err),
			)
		case errors.Is(err, rootfs.ErrEnvdMissing):
			// Declined at the pre-swap check: the rootfs has no envd to upgrade.
			// Nothing touched, so the guest boots its own envd. A rootfs property,
			// not a malfunction — its own result value, like envd_too_large.
			result = "envd_missing"
			runtime.Logger().Warn(ctx, "skipping offline envd upgrade: rootfs has no envd to swap",
				zap.String("target", path),
				zap.String("built_with", from),
				zap.Error(err),
			)
		case errors.Is(err, rootfs.ErrStatUnparseable):
			// The pre-swap stat could not be read (e.g. a filesystem debugfs could
			// not open, which exits 0). Nothing touched; boot the original. A rootfs
			// property, not swap breakage.
			result = "stat_unparseable"
			runtime.Logger().Warn(ctx, "skipping offline envd upgrade: rootfs envd stat was unreadable",
				zap.String("target", path),
				zap.String("built_with", from),
				zap.Error(err),
			)
		case unrecoverable:
			result = "unrecoverable"
			runtime.Logger().Error(ctx, "offline envd swap left the rootfs without a usable envd; failing boot to discard the overlay",
				zap.String("target", path),
				zap.String("built_with", from),
				zap.Error(err),
			)
		case binCache != nil && errors.Is(err, rootfs.ErrSwapSourceMissing):
			// A promotion can retire the staged copy between SourcePath's stat and the
			// swap's read of it. Nothing was written to the rootfs, so this is the same
			// deferral the stat above reports and not swap breakage: the guest boots
			// its own envd and a later resume retries once a warm has republished.
			// Placed after the unrecoverable case so a half-finished swap still wins.
			//
			// Keyed on the swap's own sentinel, not on fs.ErrNotExist: the swap raises
			// ENOENT from a vanished TMPDIR, an unwritable backup target and a missing
			// stage directory too, and those are host faults that must stay Error and
			// swap_failed. And gated on the cache being engaged, because with the flag
			// off the source IS what the swap reads, so a genuinely missing promoted
			// binary would be relabelled as a cache event on a node with no cache.
			result = envdbin.ReasonCopyVanished
			runtime.Logger().Warn(ctx, "skipping offline envd upgrade: cached binary went away before the swap read it",
				zap.String("target", path),
				zap.String("built_with", from),
				zap.Error(err),
			)
		default:
			result = "swap_failed"
			runtime.Logger().Error(ctx, "offline envd swap before reboot failed; booting original envd",
				zap.String("target", path),
				zap.String("built_with", from),
				zap.Error(err),
			)
		}

		attrs := []attribute.KeyValue{
			attribute.String("result", result),
			attribute.String("from_version", from),
			attribute.String("to_version", toVersion),
		}
		// from_version is a CLAIM read off the snapshot record, not an observation of the
		// rootfs, so a success count alone overstates how many sandboxes actually moved:
		// an already-upgraded snapshot re-resolves the same upgrade on every cold boot
		// (the record is never advanced) and rewrites the same bytes. refire splits the
		// two. Attached on success only — that is where the comparison is known to have
		// happened, and an absent label beats a false one that means "no idea".
		if err == nil {
			attrs = append(attrs, attribute.Bool("refire", swapped.Refire))
		}
		envdOfflineUpgradeAttempts.Add(ctx, 1, metric.WithAttributes(attrs...))
		envdOfflineUpgradeDurationHist.Record(ctx, time.Since(start).Milliseconds(),
			metric.WithAttributes(attribute.String("result", result)))

		if unrecoverable {
			return fmt.Errorf("offline envd upgrade left rootfs unbootable: %w", err)
		}

		// Best-effort: swallow a recoverable failure so the cold boot proceeds on
		// the original envd rather than failing the whole resume.
		return nil
	}
}
