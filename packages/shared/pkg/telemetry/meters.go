package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type (
	CounterType                 string
	ObservableCounterType       string
	GaugeFloatType              string
	GaugeIntType                string
	ObservableUpDownCounterType string
	HistogramType               string
)

const (
	ApiRateLimitRequests                 CounterType = "api.rate_limit.requests"
	ApiOrchestratorCreatedSandboxes      CounterType = "api.orchestrator.created_sandboxes"
	ApiOrchestratorResumeOriginNodeRemap CounterType = "api.orchestrator.resume_origin_node_remapped"
	// ApiOrchestratorPauseRefusalRestore counts what became of a pause the
	// node refused retryably: outcome restored | restore_failed |
	// route_restore_failed | superseded, caller request | eviction.
	// superseded: the ID was reclaimed by a new incarnation meanwhile; nothing
	// was restored, removed, or killed.
	ApiOrchestratorPauseRefusalRestore CounterType = "api.orchestrator.pause_refusal_restore"
	// ApiEvictorFsOnlyAutoPause counts timeout auto-pauses whose policy asked
	// for a filesystem-only snapshot. Unlabeled since the fs-only version
	// gate was dropped: every counted eviction takes the fs-only path.
	ApiEvictorFsOnlyAutoPause CounterType = "api.evictor.fs_only_auto_pause"
	// ApiEvictorAutoPauseDegraded counts timeout auto-pauses the evictor
	// downgraded to a filesystem-only snapshot after the node refused the
	// memory one, by cause: admission_refused (no retry budget, degraded at
	// the first refusal) or overstay_budget (the retry budget since the first
	// refusal ran out); a subset of ApiEvictorFsOnlyAutoPause.
	ApiEvictorAutoPauseDegraded CounterType = "api.evictor.auto_pause_degraded"
	SandboxCreateMeterName      CounterType = "api.env.instance.started"

	TeamSandboxCreated CounterType = "e2b.team.sandbox.created"

	EnvdInitCalls CounterType = "orchestrator.sandbox.envd.init.calls"

	// 2 MiB chunks the pre-pause envd heap collapse attempted, split by the
	// result attribute (collapsed|skipped): attempts = total, successful =
	// collapsed.
	EnvdCollapseChunks CounterType = "orchestrator.sandbox.envd.collapse.chunks"
	// Incremented by the balance_dirty_pages thread count at every 200 ms poll
	// for the lifetime of the process. rate() shows dirty-page throttle
	// intensity in real-time; 0 when no stalls are occurring.
	OrchestratorHostBalanceDirtyPagesThreads CounterType = "orchestrator.host.balance_dirty_pages.threads"

	OrchestratorSandboxKilledCounterName CounterType = "orchestrator.sandbox.killed"
	// OrchestratorSandboxPauseAdmissionCounterName counts every snapshot-
	// admission decision. refused is the told-the-caller-to-retry rate;
	// ready_after_wait counts the grace paying off — their ratio tunes the
	// grace flag.
	OrchestratorSandboxPauseAdmissionCounterName CounterType = "orchestrator.sandbox.pause_admission"
	// OrchestratorSandboxCheckpointCounterName counts Checkpoint RPCs by flow —
	// the denominator that makes the in_place-labeled duration histograms
	// cuttable as a ramp (what fraction went in-place, at what success rate).
	OrchestratorSandboxCheckpointCounterName CounterType = "orchestrator.sandbox.checkpoint"
	// OrchestratorFPRResumeCounterName counts free-page-reporting resume
	// outcomes after a CoW window paused reporting. Guest memory is hugetlb
	// with no swap and no memcg reclaim, so FPR discards are the ONLY
	// mechanism returning freed guest pages to the host: outcome="abandoned"
	// is lasting per-sandbox host-memory retention and the alertable signal
	// for a leaked pause.
	OrchestratorFPRResumeCounterName CounterType = "orchestrator.sandbox.fpr_resume"

	// OrchestratorSnapshotUploadFailedCounterName counts pause-snapshot uploads
	// that never landed durably (budget exhausted or a non-retryable error).
	// A non-zero rate means lost snapshots.
	OrchestratorSnapshotUploadFailedCounterName CounterType = "orchestrator.snapshot.upload.failed"

	// SandboxPauseFsQuiescedCounterName counts filesystem-only pauses by whether
	// the captured rootfs was frozen (quiesced=true, crash-consistent) vs a plain
	// sync fallback (quiesced=false). quiesced/total is the fraction of newly
	// minted fs-only snapshots that are safe to cold-boot / rewrite — the eligible
	// population for the offline envd upgrade built on top of this flag.
	SandboxPauseFsQuiescedCounterName CounterType = "orchestrator.sandbox.pause.fs_quiesced"

	// SandboxLifecycleUnstoppedCounterName counts sandbox lifecycles whose live-map
	// entry was reclaimed by the teardown cleanup chain rather than by an operation:
	// nobody marked the lifecycle stopping before the chain ran. Three teardowns
	// reach it — a guest exit or Firecracker death with no API teardown, a build
	// layer's bare deferred close, and an in-place checkpoint whose resume failed
	// and tore the sandbox down itself. Split by sandbox_type rather than filtered
	// to sandbox, because every successful build layer takes the same branch and
	// would otherwise dominate the bucket; dropping builds instead would make "the
	// build tree stopped reaching this" indistinguishable from "the build tree is
	// fine". It is a lower bound, never a total: the live map is in-process, so a
	// lifecycle spanning an orchestrator restart is never counted.
	SandboxLifecycleUnstoppedCounterName CounterType = "orchestrator.sandbox.lifecycle.unstopped"

	// SandboxResumeWPModeCounterName counts sandbox resumes by the write-protect
	// tracking mode the resume chose (mode=sync|async, the use-sync-wp flag
	// decision). This is the denominator for every sync-WP burn-in signal:
	// wp_resolve rates and dirty-divergence readings only come from mode=sync
	// sandboxes, so without this counter the fleet's sync coverage — and
	// whether an error rate is "all sync sandboxes" or "one loud one" — is
	// invisible.
	SandboxResumeWPModeCounterName CounterType = "orchestrator.sandbox.resume.wp_mode"

	// OrchestratorEnvdUpgradeAttempts counts resume-time envd live-upgrade
	// attempts, by result (success|delivery_failed|not_ready|panic) and
	// from_version/to_version. success/total is the rollout success rate;
	// attempts/resumes is the fire rate.
	OrchestratorEnvdUpgradeAttempts CounterType = "orchestrator.envd.upgrade.attempts"
	// OrchestratorEnvdUpgradeGated counts resumes the envd-upgrade-target flag
	// targeted but a gate declined, by reason — a silent no-op worth watching during
	// a ramp. The reasons are the resolver's and the gates' own vocabulary; read them
	// off `sum by (reason)` rather than from a list here, which would go stale.
	OrchestratorEnvdUpgradeGated CounterType = "orchestrator.envd.upgrade.gated"
	// OrchestratorEnvdBinaryCacheReads counts LOOKUPS of the host envd binary on
	// the resume path, by what the lookup found (hit|miss) and which upgrade path
	// asked (live|offline). Exactly one per resolution, so hit/total is the cache's
	// effectiveness. A standing "miss" means warms are not landing -- pair it with
	// binary_cache.warms{result="failed"} to see why.
	OrchestratorEnvdBinaryCacheReads CounterType = "orchestrator.envd.binary_cache.reads"
	// OrchestratorEnvdBinaryCacheDeliveries counts what a caller actually read, by
	// outcome (copy|stale). Separate from reads because its denominator is
	// different: only resolutions that go on to read the bytes reach it, and
	// counting both on one series would make hit/total exceed the number of
	// resolutions. "copy" is the cached file, which is the only thing an upgrade
	// ever delivers -- an upgrade whose binary is not cached is deferred rather than
	// served from the mount. "stale" is a refusal: the copy that carried the
	// resolved version was gone by delivery, so the read was skipped rather than
	// satisfied from a source whose bytes were never verified against it.
	OrchestratorEnvdBinaryCacheDeliveries CounterType = "orchestrator.envd.binary_cache.deliveries"
	// OrchestratorEnvdBinaryCacheWarms counts attempts to populate the host envd
	// binary cache, by result. Almost all of them are the background warmer's; the
	// exception is a lookup whose own source stat failed, which is reported here
	// rather than by the warm it makes no sense to spawn. "ok" means the binary is cached afterwards;
	// "superseded" that the warm completed but a promotion during it left nothing to
	// publish, which needs no retry because the next miss warms the new generation;
	// "suppressed" that the post-failure backoff declined to copy, and clears within
	// the minute; "pinned" that these exact bytes have already been judged unusable,
	// which lasts until a promotion changes them; "already_warming" that a warm of
	// the same bytes was already in flight; "bad_target" that the bytes copied but
	// would not run, which is a property of the promoted binary rather than of this
	// node; "failed" that it genuinely could not copy.
	//
	// "already_warming" rising while "ok" stays flat is a source that has stopped
	// answering mid-copy: the slot is held by a read that is not finishing, which no
	// other series can tell apart from a node that has simply not warmed yet.
	//
	// "bad_target" is the one to alert on. With the cache enabled the resolver never
	// probes the source, so a truncated or corrupt promotion is detectable here and
	// nowhere else -- at the upgrade call sites it arrives as the same
	// gated{reason="binary_not_cached"} a node that merely has not warmed yet emits.
	OrchestratorEnvdBinaryCacheWarms CounterType = "orchestrator.envd.binary_cache.warms"
	// OrchestratorEnvdUpgradeHandover counts live-upgrade handover items by item
	// (proc|retained|watcher) and result (ok|failed), reported back by the new
	// envd on /init. failed/(ok+failed) is the handover error rate — the
	// fleet-visible signal (otherwise only logged in-guest) that the new envd
	// dropped or degraded something it re-adopted across the swap.
	OrchestratorEnvdUpgradeHandover CounterType = "orchestrator.envd.upgrade.handover"

	// EnvdDefaultsApplied counts MEMORY RESUMES by where the default user the orchestrator
	// sent came from, so the derivation has a positive engagement signal rather than an
	// inferred one: a derivation that produced nothing and one that never ran read
	// identically on a dashboard.
	//
	// Resumes, not starts: the cold-boot path does its own reconstruction and is not
	// counted here. Split by sandbox_type, because a build's own internal resumes go
	// through the same path and are not customer sandbox starts.
	EnvdDefaultsApplied CounterType = "orchestrator.envd.defaults.applied"
	// EnvdDefaultsMismatch counts /init responses where the defaults envd reports as
	// EFFECTIVE differ from what the orchestrator sent, by field. Zero is the only
	// acceptable steady state: a non-zero value means a sandbox is resolving requests
	// against an identity nobody asked for. Note the header is written inside a guest,
	// so a single hostile or broken sandbox can hold this above zero; investigate via
	// the per-sandbox error log rather than treating the aggregate as authoritative.
	EnvdDefaultsMismatch CounterType = "orchestrator.envd.defaults.mismatch"
	// EnvdDefaultsWorkdirWithheld counts MEMORY RESUMES where the template recorded a
	// default workdir the orchestrator could not prove the build sent, so it was not
	// re-sent — the same event EnvdDefaultsApplied counts, so the two divide. An
	// UPPER BOUND on the population that keeps the wrong working directory after a live
	// upgrade: a workdir equal to the resolved user's home is withheld to no effect, and
	// whether it coincides cannot be decided outside the guest.
	EnvdDefaultsWorkdirWithheld CounterType = "orchestrator.envd.defaults.workdir_withheld"
	// EnvdDefaultsBuiltinFallback counts /init responses where envd reports it was never
	// told which user to run as, so it is serving the value it was compiled with. This is
	// the REALIZED loss, and the only signal that can observe one: a mismatch cannot,
	// because envd stores exactly what /init sent it and then reports that field back.
	//
	// Split by whether a user was sent, and by sandbox type. sent=true is a defect at any
	// type, because a delivered user must never read back as never-told. sent=false is only
	// about the population at risk at sandbox_type="sandbox": the build tree starts its own
	// sandboxes with no default user configured, on the host envd, so it lands in
	// sent=false from the first day of the rollout and would otherwise dominate the bucket.
	EnvdDefaultsBuiltinFallback CounterType = "orchestrator.envd.defaults.builtin_fallback"

	// OrchestratorEnvdOfflineUpgradeAttempts counts every OFFLINE envd upgrade
	// OUTCOME on the cold boot of a filesystem-only snapshot — not only the ones that
	// reach the rootfs binary swap — by result and from_version/to_version:
	//
	//	success | swap_failed | unrecoverable | envd_too_large | envd_missing | stat_unparseable
	//	    the swap ran, or the rootfs itself declined it (see rootfs.SwapEnvdBinary)
	//	not_quiesced
	//	    an upgrade was wanted, but the snapshot's rootfs was not frozen at pause,
	//	    so it must not be rewritten
	//	same_version
	//	    already on the target
	//	not_staged | downgrade | invalid_target | getversion_failed | binary_not_cached
	//	    the resolver refused or deferred the configured target
	//	copy_vanished
	//	    a version was resolved and the copy carrying it was then retired, before
	//	    anything was written to the rootfs
	//
	// Every no-op except flag-off is counted, so the eligible population adds up.
	// Flag-off is deliberately absent: it is the whole filesystem-only population
	// minus the rest, already available as sandbox.create.duration{fs_only="true"}.
	// to_version is empty on the resolver's refusals — it has no target to name.
	//
	// On result=success a refire label says whether the rootfs ALREADY held the target
	// bytes. from_version cannot answer that on its own: it is a claim read off the
	// snapshot record, and the swap keys on that record without ever advancing it, so
	// an already-upgraded snapshot resolves the same upgrade on every cold boot and
	// rewrites identical bytes while reporting the same from_version as a genuine
	// upgrade. refire="false" is the count that actually moved sandboxes. The label is
	// absent on every other result, where nothing was compared.
	OrchestratorEnvdOfflineUpgradeAttempts CounterType = "orchestrator.envd.offline_upgrade.attempts"

	// OrchestratorFsRecoveryRuns counts every pre-boot filesystem-recovery
	// decision on a cold boot, by result and trigger. Recovery is journal replay
	// only (`e2fsck -p -E journal_only`), so the results are:
	//
	//	skipped_quiesced
	//	    the rootfs was frozen at pause; nothing to replay
	//	replayed
	//	    the journal was replayed (or regenerated, or there was nothing to do);
	//	    the fs is mountable and the boot proceeded — the expected outcome for
	//	    the admitted population, NOT a corruption signal. The reason label
	//	    splits it: journal_replayed (the superblock's needs_recovery bit was
	//	    set before the run, or e2fsck applied a repair), nothing_to_do (the
	//	    bit was clear), replay_unknown (the superblock could not be read).
	//	failed_operational
	//	    the run did not complete a clean replay AND e2fsck may have opened the
	//	    device (an e2fsck non-replay exit, or a timeout that could have killed it
	//	    mid-write). Fail closed — the start failed — but always retryable on
	//	    another node, never a permanent snapshot verdict (its exit codes cannot
	//	    tell an unmountable filesystem apart from a transient fault).
	//	failed_open
	//	    recovery could not run AND e2fsck provably never opened the device (the
	//	    jail could not launch it, or the host cannot exec e2fsck), so the disk is
	//	    what a flag-off cold boot would mount and the guest kernel replays the
	//	    journal itself. The boot proceeded. A host-image signal — a non-trivial
	//	    rate means the recovery tooling is broken fleet-wide (roll back), even
	//	    though sandboxes still boot.
	//
	// A bounded "reason" attribute sub-labels each result so a ramp can act on it
	// from a dashboard instead of grepping create-failure logs:
	//
	//	replayed:         nothing_to_do (no replay needed) | journal_replayed
	//	                  (journal applied) — the efficacy split
	//	failed_operational: timeout (Go deadline fired mid-run) | killed (the unit was
	//	                  signalled mid-run — exit -1 or 128+N; OOM, RuntimeMaxSec,
	//	                  external stop; NOT a tooling failure) | e2fsck_4 | e2fsck_8 |
	//	                  e2fsck_other (e2fsck's
	//	                  own non-replay exits — an unreplayable snapshot, expected and
	//	                  small) | no_sentinel (ran but lost its result, or the pre-launch
	//	                  device guard)
	//	failed_open:      launcher_failure (the unit failed to START — e2fsck never ran) |
	//	                  exec_failure (126/127: the host could not exec e2fsck) —
	//	                  both host-image regressions, roll back
	//	skipped_quiesced: quiesced
	//
	// It never carries a raw exit code or any tenant-influenced bytes.
	//
	// trigger separates the two admitted populations: "rescue" (the request
	// demanded a filesystem boot of a memory snapshot) vs "legacy_fs_only"
	// (a filesystem-only snapshot whose pause fell back to sync).
	OrchestratorFsRecoveryRuns CounterType = "orchestrator.sandbox.fs_recovery.runs"

	// OrchestratorFsRecoveryToolingUnsupported fires once per orchestrator process
	// when the host e2fsck does not accept `-E journal_only` (probed against a
	// nonexistent device, so it reads no filesystem). Expected to be flat zero: node
	// images ship an e2fsprogs that has supported the option for years. A non-zero
	// fleet sum means some node's tooling silently no-ops recovery (the guest kernel
	// still replays at mount, so boots are unaffected) — a signal to fix that image,
	// not a per-sandbox failure.
	OrchestratorFsRecoveryToolingUnsupported CounterType = "orchestrator.sandbox.fs_recovery.tooling_unsupported"

	// TemplateBuildCmdlineArgs counts template builds by the guest kernel command line
	// parameters they actually booted with, after parsing and validation. This is the
	// engagement signal for the per-team cmdline-variant flag: a non-zero rate on a
	// non-empty label is proof builds ran the path, which is what the flag
	// reading "on" in the feature-flag service does not tell you. Zero here while the
	// flag is targeted is the alarm that the opt-in is silently inert. Because it
	// records what was APPLIED, a rejected fragment shows up as the empty label rather
	// than as the parameters that were asked for.
	TemplateBuildCmdlineArgs CounterType = "orchestrator.template.build.cmdline_args"

	// PauseResumePrefetchHarvestAttempts counts pause-resume prefetch harvest
	// attempts, by result (success|resume_failed|collect_failed|skipped). The
	// throwaway is absent from Prometheus otherwise (registration-skip), so this
	// is the harvest-activity / failure-rate signal.
	PauseResumePrefetchHarvestAttempts CounterType = "orchestrator.sandbox.pause_resume_prefetch.harvest.attempts"

	ApiRedisStoragePublisherPublished CounterType = "api.redis_storage.publisher.published"
	ApiRedisStoragePublisherDropped   CounterType = "api.redis_storage.publisher.dropped"

	// ApiRedisStorageExpirationIndexHealed counts sandboxes the healer re-added
	// to the global expiration index. Healthy steady state is zero; a sustained
	// non-zero rate means expiration index writes are being lost and sandboxes
	// would otherwise become invisible to the evictor (immortal).
	ApiRedisStorageExpirationIndexHealed CounterType = "api.redis_storage.expiration_index.healed"
	// ApiRedisStorageExpirationIndexSwept counts members removed from the
	// global expiration index by the evictor scan
	// (reason=orphan|dead_execution|invalid).
	ApiRedisStorageExpirationIndexSwept CounterType = "api.redis_storage.expiration_index.swept"
	// ApiRedisStorageExpirationIndexRescored counts live members whose index
	// score drifted from the stored EndTime and were re-scored by the evictor
	// scan. Sustained non-zero rate means score updates are being lost.
	ApiRedisStorageExpirationIndexRescored CounterType = "api.redis_storage.expiration_index.rescored"
)

const (
	ApiOrchestratorSbxCreateSuccess ObservableCounterType = "api.orchestrator.sandbox.create.success"
	ApiOrchestratorSbxCreateFailure ObservableCounterType = "api.orchestrator.sandbox.create.failure"
)

const (
	OrchestratorSandboxCountMeterName ObservableUpDownCounterType = "orchestrator.env.sandbox.running"

	ClientProxyServerConnectionsMeterCounterName ObservableUpDownCounterType = "client_proxy.proxy.server.connections.open"
	ClientProxyPoolConnectionsMeterCounterName   ObservableUpDownCounterType = "client_proxy.proxy.pool.connections.open"
	ClientProxyPoolSizeMeterCounterName          ObservableUpDownCounterType = "client_proxy.proxy.pool.size"

	OrchestratorProxyServerConnectionsMeterCounterName ObservableUpDownCounterType = "orchestrator.proxy.server.connections.open"
	OrchestratorProxyPoolConnectionsMeterCounterName   ObservableUpDownCounterType = "orchestrator.proxy.pool.connections.open"
	OrchestratorProxyPoolSizeMeterCounterName          ObservableUpDownCounterType = "orchestrator.proxy.pool.size"

	BuildCounterMeterName       ObservableUpDownCounterType = "api.env.build.running"
	EvictionsRunningCounterName ObservableUpDownCounterType = "api.evictor.evictions.running"

	TCPFirewallActiveConnections ObservableUpDownCounterType = "orchestrator.tcpfirewall.connections.active"

	ApiRedisStoragePublisherQueueDepth ObservableUpDownCounterType = "api.redis_storage.publisher.queue.depth"
)

const (
	SandboxCpuUsedGaugeName GaugeFloatType = "e2b.sandbox.cpu.used"
)

const (
	// Build timing histograms
	BuildDurationHistogramName      HistogramType = "template.build.duration"
	BuildPhaseDurationHistogramName HistogramType = "template.build.phase.duration"
	BuildStepDurationHistogramName  HistogramType = "template.build.step.duration"

	// Sandbox timing histograms
	OrchestratorSandboxCreateDurationName HistogramType = "orchestrator.sandbox.create.duration"
	WaitForEnvdDurationHistogramName      HistogramType = "orchestrator.sandbox.envd.init.duration"
	GuestSyncDurationHistogramName        HistogramType = "orchestrator.sandbox.guest_sync.duration"
	PauseDurationHistogramName            HistogramType = "orchestrator.sandbox.pause.duration"
	SnapshotProcessMemoryDurationName     HistogramType = "orchestrator.sandbox.snapshot.process_memory.duration"
	SnapshotProcessRootfsDurationName     HistogramType = "orchestrator.sandbox.snapshot.process_rootfs.duration"
	SnapshotRootfsSealDurationName        HistogramType = "orchestrator.sandbox.snapshot.rootfs_seal.duration"
	SnapshotGuestFreezeDurationName       HistogramType = "orchestrator.sandbox.snapshot.guest_freeze.duration"
	SnapshotMemorySealDurationName        HistogramType = "orchestrator.sandbox.snapshot.memory_seal.duration"

	// OrchestratorSandboxExecutionDurationName is one sample per Firecracker
	// run, so a sandbox that is paused and resumed records one per run.
	OrchestratorSandboxExecutionDurationName HistogramType = "orchestrator.sandbox.execution.duration"

	// OrchestratorEnvdUpgradeDurationName is the wall-time of a resume-time envd
	// live-upgrade (delivery + trigger + WaitForEnvd) = overhead added to the
	// resume. Labeled by result.
	OrchestratorEnvdUpgradeDurationName HistogramType = "orchestrator.envd.upgrade.duration"
	// OrchestratorEnvdUpgradePhaseDurationName splits that wall-time by phase
	// (resolve|deliver|ready). deliver and ready have INDEPENDENT budgets (30 s
	// and 15 s) and the combined histogram above cannot say which one a slow
	// upgrade is approaching, so a delivery running past its own deadline has to
	// be reconstructed from log timestamps. resolve is not in the combined
	// histogram at all -- it runs before it starts timing -- yet it is the largest
	// cost on a cold node, because probing the target's version execs the binary
	// off a network filesystem, which demand-pages it in small reads. Labeled by
	// phase and result.
	OrchestratorEnvdUpgradePhaseDurationName HistogramType = "orchestrator.envd.upgrade.phase.duration"

	// OrchestratorEnvdOfflineUpgradeDurationName is the wall-time of the offline
	// rootfs envd swap (jailed debugfs), recorded once per swap attempt. Catches
	// pathological rewrites; the swap runs in the cold-boot PreBootFn, so it adds
	// directly to resume latency.
	OrchestratorEnvdOfflineUpgradeDurationName HistogramType = "orchestrator.envd.offline_upgrade.duration"

	// OrchestratorFsRecoveryDurationName is the wall-time of the jailed pre-boot
	// journal-replay run on a cold boot, recorded for every outcome the run reaches
	// (replayed/failed_operational) — only skipped_quiesced,
	// which never runs e2fsck, has no duration sample. Replay is journal-bounded, so
	// this should stay sub-second even on large filesystems. Labeled like
	// OrchestratorFsRecoveryRuns.
	OrchestratorFsRecoveryDurationName HistogramType = "orchestrator.sandbox.fs_recovery.duration"

	// Pre-pause envd heap collapse round-trip duration (the pause-path cost of
	// POST /collapse: network plus envd's madvise work), recorded once per pause
	// when the collapse-envd-heap flag is on.
	EnvdCollapseDurationHistogramName HistogramType = "orchestrator.sandbox.envd.collapse.duration"

	// Background memfile dedup latency: provisional header creation at pause to
	// the durable-header swap. Recorded once per swap; no attributes.
	OrchestratorSandboxMemfileDedupDurationName HistogramType = "orchestrator.sandbox.memfile_dedup.duration"

	// How long snapshot admission actually waited whenever it waited, labeled
	// by outcome (ready_after_wait/refused only).
	OrchestratorSandboxPauseAdmissionWaitDurationName HistogramType = "orchestrator.sandbox.pause_admission.wait.duration"

	// Pause-resume prefetch harvest cost, recorded once per harvest attempt.
	// duration is the SLOT-HOLD cost alone — the throwaway resume, its trace
	// collection and its reap — which is what the harvest-timeout flag caps.
	// persist_wait is the separate, resource-free wait on the in-flight snapshot
	// upload that the mapping has to be written after; keeping it out of duration
	// is what lets "harvests at the timeout" stay a meaningful signal.
	// pages is the harvested trace size (distinct 2 MiB blocks), recorded only on
	// success, so its bottom bucket surfaces the empty-trace (idle-at-pause) rate.
	PauseResumePrefetchHarvestDurationName     HistogramType = "orchestrator.sandbox.pause_resume_prefetch.harvest.duration"
	PauseResumePrefetchHarvestPagesName        HistogramType = "orchestrator.sandbox.pause_resume_prefetch.harvest.pages"
	PauseResumePrefetchSealWaitDurationName    HistogramType = "orchestrator.sandbox.pause_resume_prefetch.seal_wait.duration"
	PauseResumePrefetchPersistWaitDurationName HistogramType = "orchestrator.sandbox.pause_resume_prefetch.persist_wait.duration"

	// Sandbox startup working-set histograms: demand-fault pages/bytes a guest
	// needed to reach a successful envd init, recorded once per start. Sampled
	// per start (not per fault), so histogram_quantile yields per-sandbox
	// percentiles.
	EnvdFreezeDurationHistogramName   HistogramType = "orchestrator.sandbox.envd.freeze.duration"
	EnvdFreezeSweepHistogramName      HistogramType = "orchestrator.sandbox.envd.freeze.sweep"
	EnvdFreezeWaitHistogramName       HistogramType = "orchestrator.sandbox.envd.freeze.wait"
	EnvdFreezeVisitedHistogramName    HistogramType = "orchestrator.sandbox.envd.freeze.visited"
	EnvdFreezeAuditHistogramName      HistogramType = "orchestrator.sandbox.envd.freeze.audit"
	EnvdFreezeCgroupsHistogramName    HistogramType = "orchestrator.sandbox.envd.freeze.cgroups"
	EnvdUnfreezeDurationHistogramName HistogramType = "orchestrator.sandbox.envd.unfreeze.duration"
	// Memory protection configured on the guest envd's cgroup chain, as envd reports it
	// on /init, recorded at most once per start (when the first /init's header decodes):
	// kind=request is envd's own memory.min, kind=floor the minimum over the chain below
	// the root. In MiB, the unit the template renders the protection in, so the two compare
	// without conversion; the truncation that costs is a setting below 1 MiB, which records
	// as 0. Capped at the sandbox's RAM, so that one unbounded request cannot cost the whole
	// series its bucket resolution: the exporter aggregates every histogram as base-2
	// exponential at a scale it lowers only to fit the observed range, and discards
	// per-instrument boundaries, so the range the series spans is the resolution every
	// sample in it gets.
	EnvdMemoryProtectionHistogramName   HistogramType = "orchestrator.sandbox.envd.memory.protection"
	UffdStartupPagesHistogramName       HistogramType = "orchestrator.sandbox.uffd.startup.pages"
	UffdStartupSourcePagesHistogramName HistogramType = "orchestrator.sandbox.uffd.startup.source_pages"
	UffdStartupBytesHistogramName       HistogramType = "orchestrator.sandbox.uffd.startup.bytes"

	// TCP Firewall histograms
	TCPFirewallConnectionDurationHistogramName    HistogramType = "orchestrator.tcpfirewall.connection.duration"
	TCPFirewallConnectionsPerSandboxHistogramName HistogramType = "orchestrator.tcpfirewall.connections.per_sandbox"

	// Ingress proxy histograms
	IngressProxyConnectionDurationHistogramName    HistogramType = "orchestrator.proxy.connection.duration"
	IngressProxyConnectionsPerSandboxHistogramName HistogramType = "orchestrator.proxy.connections.per_sandbox"
)

const (
	// Build result counters
	BuildResultCounterName      CounterType = "template.build.result"
	BuildCacheResultCounterName CounterType = "template.build.cache.result"

	// TCP Firewall counters
	TCPFirewallConnectionsTotal CounterType = "orchestrator.tcpfirewall.connections.total"
	TCPFirewallErrorsTotal      CounterType = "orchestrator.tcpfirewall.errors.total"
	TCPFirewallDecisionsTotal   CounterType = "orchestrator.tcpfirewall.decisions.total"

	// Ingress proxy counters
	IngressProxyConnectionsBlockedTotal CounterType = "orchestrator.proxy.connections.blocked.total"

	// cmux counters
	CmuxErrorsTotal CounterType = "orchestrator.cmux.errors.total"

	// Sandbox routing record counters (result=ok/error)
	RoutingPublishTotal CounterType = "orchestrator.routing.publish.total"
	RoutingDeleteTotal  CounterType = "orchestrator.routing.delete.total"

	// Firecracker net counters — global totals, no sandbox_id (low cardinality).
	// All carry a direction=tx/rx attribute. Per-sandbox distributions are histograms below.
	SandboxFCNetFails         CounterType = "orchestrator.sandbox.fc.net.fails"
	SandboxFCNetNoAvailBuffer CounterType = "orchestrator.sandbox.fc.net.no_avail_buffer"
	SandboxFCNetTapIOFails    CounterType = "orchestrator.sandbox.fc.net.tap_io_fails"

	// Firecracker block counters — global totals, no sandbox_id (low cardinality).
	// Carry a direction=read/write attribute where applicable.
	SandboxFCBlockFails         CounterType = "orchestrator.sandbox.fc.block.fails"
	SandboxFCBlockNoAvailBuffer CounterType = "orchestrator.sandbox.fc.block.no_avail_buffer"

	// SandboxFCSnapshotLoadFailures counts snapshot loads Firecracker refused,
	// by reason. A refused load makes its template unusable rather than slow:
	// the sandbox cannot start on any host that reads the same snapshot, so
	// every attempt fails and the caller sees an error. Until this counter,
	// such a failure was visible only as success=false on
	// orchestrator.sandbox.create.duration, indistinguishable from an envd
	// timeout, and otherwise only in the Firecracker log line.
	//
	// reason is bounded and never carries the fault text itself, which is
	// influenced by snapshot contents:
	//   vcpu_msr        the snapshot carries an MSR the reading host's kernel
	//                   will not accept — a kernel difference, not a CPU one.
	//   vcpu_other      vCPU restore failed for any other reason.
	//   bad_request     every other refusal, and where an unclassified fault
	//                   lands: memory backend, snapshot version, unreadable
	//                   snapshot file, or a fault a later Firecracker words
	//                   differently.
	//   unavailable     the API answered a non-400 status, so the load never
	//                   reached the snapshot.
	//   transport       the API socket could not be reached or replied
	//                   unintelligibly; says nothing about the snapshot.
	//   timeout         no answer before the request deadline; the load
	//                   overran and the sandbox never resumed, so it counts.
	//   canceled        the caller went away mid-load; not a broken snapshot.
	//
	// Alert on the total excluding canceled rather than on one reason: a
	// failure mode we have not seen still counts, under bad_request, and the
	// breakdown is for reading afterwards.
	SandboxFCSnapshotLoadFailures CounterType = "orchestrator.sandbox.fc.snapshot_load.failures"
)

const (
	// ApiRedisStorageExpirationIndexSize records the cardinality of the global
	// expiration ZSET (sandbox:storage:global:expiration) once per heal pass.
	// Use the max/p99 to detect unbounded growth; a rising trend without a
	// corresponding rise in live sandbox count indicates stale entries are
	// accumulating faster than they are swept.
	ApiRedisStorageExpirationIndexSize HistogramType = "api.redis_storage.expiration_index.size"

	// ApiRedisStorageExpirationIndexSweepDuration records the wall-clock time
	// of each ExpiredItems call (ZRANGEBYSCORE + MGET pipeline). Rising p99
	// alongside a rising expiration_index.size confirms O(log N + K) growth.
	ApiRedisStorageExpirationIndexSweepDuration HistogramType = "api.redis_storage.expiration_index.sweep_duration"

	ApiRedisStoragePublisherPublishDuration HistogramType = "api.redis_storage.publisher.publish.duration"

	// Firecracker net histograms — per-sandbox distribution per metrics flush, no sandbox_id.
	// Firecracker serializes SharedIncMetric as per-flush deltas (default flush interval: 60 s).
	// Symmetric TX/RX metrics carry a direction=tx/rx attribute; TX-only metrics always use direction=tx.
	SandboxFCNetBytes                HistogramType = "orchestrator.sandbox.fc.net.bytes"
	SandboxFCNetPackets              HistogramType = "orchestrator.sandbox.fc.net.packets"
	SandboxFCNetCount                HistogramType = "orchestrator.sandbox.fc.net.count"
	SandboxFCNetRateLimiterThrottled HistogramType = "orchestrator.sandbox.fc.net.rate_limiter_throttled"
	// TX-only: no RX equivalent in Firecracker metrics.
	SandboxFCNetRateLimiterEventCount HistogramType = "orchestrator.sandbox.fc.net.rate_limiter_event_count"
	SandboxFCNetRemainingReqs         HistogramType = "orchestrator.sandbox.fc.net.remaining_reqs"

	// Firecracker block histograms — per-sandbox distribution per metrics flush, no sandbox_id.
	// Symmetric read/write metrics carry a direction=read/write attribute.
	SandboxFCBlockBytes                 HistogramType = "orchestrator.sandbox.fc.block.bytes"
	SandboxFCBlockCount                 HistogramType = "orchestrator.sandbox.fc.block.count"
	SandboxFCBlockQueueEventCount       HistogramType = "orchestrator.sandbox.fc.block.queue_event_count"
	SandboxFCBlockRateLimiterThrottled  HistogramType = "orchestrator.sandbox.fc.block.rate_limiter_throttled"
	SandboxFCBlockRateLimiterEventCount HistogramType = "orchestrator.sandbox.fc.block.rate_limiter_event_count"
	SandboxFCBlockIOEngineThrottled     HistogramType = "orchestrator.sandbox.fc.block.io_engine_throttled"
	SandboxFCBlockRemainingReqs         HistogramType = "orchestrator.sandbox.fc.block.remaining_reqs"

	SnapshotDiffBytes  HistogramType = "orchestrator.sandbox.snapshot.diff.bytes"
	SnapshotDiffRatio  HistogramType = "orchestrator.sandbox.snapshot.diff.ratio"
	SnapshotTotalBytes HistogramType = "orchestrator.sandbox.snapshot.total.bytes"

	UploadUncompressedBytes HistogramType = "orchestrator.sandbox.upload.uncompressed.bytes"
	UploadCompressedBytes   HistogramType = "orchestrator.sandbox.upload.compressed.bytes"
	UploadCompressionRatio  HistogramType = "orchestrator.sandbox.upload.compression.ratio"
)

const (
	ApiOrchestratorCountMeterName        GaugeIntType = "api.orchestrator.status"
	OrchestratorStatusGaugeName          GaugeIntType = "orchestrator.status"
	OrchestratorSandboxLimitGaugeName    GaugeIntType = "orchestrator.sandbox.limit"
	FirecrackerProcesses                 GaugeIntType = "orchestrator.firecracker.processes"
	FirecrackerProcessesUntracked        GaugeIntType = "orchestrator.firecracker.processes.untracked"
	FirecrackerProcessesOverMaxLength    GaugeIntType = "orchestrator.firecracker.processes.over_max_length"
	FirecrackerProcessesUnknownMaxLength GaugeIntType = "orchestrator.firecracker.processes.unknown_max_length"
	FirecrackerTrackerSuccess            GaugeIntType = "orchestrator.firecracker.tracker.success"
	FirecrackerTrackerLastSuccessAge     GaugeIntType = "orchestrator.firecracker.tracker.last_success_age"

	// Orchestrator node resources allocated to running sandboxes (sum across running sandboxes)
	OrchestratorCpuAllocatedGaugeName    GaugeIntType = "orchestrator.sandbox.cpu.allocated"
	OrchestratorMemoryAllocatedGaugeName GaugeIntType = "orchestrator.sandbox.memory.allocated"
	OrchestratorDiskAllocatedGaugeName   GaugeIntType = "orchestrator.sandbox.disk.allocated"

	// Sandbox metrics
	SandboxRamUsedGaugeName   GaugeIntType = "e2b.sandbox.ram.used"
	SandboxRamTotalGaugeName  GaugeIntType = "e2b.sandbox.ram.total"
	SandboxRamCacheGaugeName  GaugeIntType = "e2b.sandbox.ram.cache"
	SandboxCpuTotalGaugeName  GaugeIntType = "e2b.sandbox.cpu.total"
	SandboxDiskUsedGaugeName  GaugeIntType = "e2b.sandbox.disk.used"
	SandboxDiskTotalGaugeName GaugeIntType = "e2b.sandbox.disk.total"

	// Template cache residency. mapping_bytes is the load-bearing one: a
	// Header's compact Mapping is the largest long-lived allocation the
	// orchestrator holds, and entry count alone hides its growth because
	// headers differ in size by orders of magnitude.
	OrchestratorTemplateCacheEntriesGaugeName        GaugeIntType = "orchestrator.templates.cache.entries"
	OrchestratorTemplateCachePinnedGaugeName         GaugeIntType = "orchestrator.templates.cache.pinned"
	OrchestratorTemplateCacheMappingEntriesGaugeName GaugeIntType = "orchestrator.templates.cache.mapping_entries"
	OrchestratorTemplateCacheMappingBytesGaugeName   GaugeIntType = "orchestrator.templates.cache.mapping_bytes"
	OrchestratorTemplateCachePinnedRefsGaugeName     GaugeIntType = "orchestrator.templates.cache.pinned_refs"
	OrchestratorTemplateCacheOldestPinAgeGaugeName   GaugeIntType = "orchestrator.templates.cache.pinned_oldest_age"

	// Team metrics
	TeamSandboxRunningGaugeName GaugeIntType = "e2b.team.sandbox.running"

	SandboxCountGaugeName GaugeIntType = "api.env.instance.running"

	// Build resource metrics
	BuildRootfsSizeHistogramName HistogramType = "template.build.rootfs.size"
)

var counterDesc = map[CounterType]string{
	ApiRateLimitRequests:                         "Number of API group rate-limit checks, by team, group, mode, and decision",
	SandboxCreateMeterName:                       "Number of currently waiting requests to create a new sandbox",
	ApiOrchestratorCreatedSandboxes:              "Number of successfully created sandboxes",
	ApiEvictorFsOnlyAutoPause:                    "Timeout auto-pauses with a filesystem-only policy.",
	ApiEvictorAutoPauseDegraded:                  "Timeout auto-pauses degraded to filesystem-only after a node refusal, by cause (admission_refused | overstay_budget).",
	ApiOrchestratorResumeOriginNodeRemap:         "Number of resume snapshots repointed to the fallback node a previous resume timed out on",
	ApiOrchestratorPauseRefusalRestore:           "Outcomes of restoring a sandbox whose pause the node refused retryably, by outcome and caller.",
	BuildResultCounterName:                       "Number of template build results",
	BuildCacheResultCounterName:                  "Number of build cache results",
	TeamSandboxCreated:                           "Counter of started sandboxes for the team in the interval",
	OrchestratorHostBalanceDirtyPagesThreads:     "Cumulative stalled thread-polls during sandbox resume; rate() gives throttle intensity",
	EnvdInitCalls:                                "Number of envd initialization calls",
	EnvdCollapseChunks:                           "2 MiB chunks the pre-pause envd heap collapse attempted, by result",
	OrchestratorSandboxKilledCounterName:         "Number of sandboxes killed, labeled by kill reason",
	OrchestratorSandboxPauseAdmissionCounterName: "Snapshot-admission decisions, labeled by outcome (ready/ready_after_wait/refused/latched_error) and rpc (pause/checkpoint)",
	OrchestratorSandboxCheckpointCounterName:     "Number of sandbox checkpoints taken, labeled by in_place and success",
	OrchestratorFPRResumeCounterName:             "Free-page-reporting resumes after a CoW window, labeled by outcome (inline, retry, fenced, fc_exited, abandoned)",
	OrchestratorSnapshotUploadFailedCounterName:  "Number of pause-snapshot uploads that never landed durably",
	SandboxPauseFsQuiescedCounterName:            "Filesystem-only pauses by whether the rootfs was frozen (quiesced) vs sync fallback",
	SandboxLifecycleUnstoppedCounterName:         "Sandbox lifecycles whose live-map entry the teardown chain reclaimed because no operation marked them stopping, by sandbox type",
	SandboxResumeWPModeCounterName:               "Sandbox resumes by write-protect tracking mode (sync|async)",
	EnvdDefaultsApplied:                          "Memory resumes by where the envd default user the orchestrator sent was derived from, and sandbox type",
	EnvdDefaultsMismatch:                         "/init responses where envd's effective defaults differ from what was sent, by field",
	EnvdDefaultsWorkdirWithheld:                  "Memory resumes where a recorded default workdir could not be proven and was not re-sent (upper bound on the unrepaired population)",
	EnvdDefaultsBuiltinFallback:                  "/init responses where envd reports it was never told which user to run as, by whether one was sent and sandbox type",
	OrchestratorEnvdUpgradeAttempts:              "Resume-time envd live-upgrade attempts, by result and from/to version",
	OrchestratorEnvdOfflineUpgradeAttempts:       "Cold-boot offline envd rootfs-swap attempts, by result and from/to version",
	OrchestratorFsRecoveryRuns:                   "Pre-boot filesystem-recovery decisions on cold boots, by result and trigger",
	OrchestratorFsRecoveryToolingUnsupported:     "Fires once per process when the host e2fsck rejects -E journal_only",
	TemplateBuildCmdlineArgs:                     "Template builds by the guest kernel cmdline parameters applied",
	OrchestratorEnvdUpgradeGated:                 "Resumes where the envd-upgrade-target flag named a target but a gate declined the upgrade, by reason",
	OrchestratorEnvdBinaryCacheReads:             "Host envd binary cache lookups on the resume path, by what the lookup found (hit|miss) and upgrade path",
	OrchestratorEnvdBinaryCacheDeliveries:        "Host envd binary reads at delivery time, by outcome (copy|stale) and upgrade path",
	OrchestratorEnvdBinaryCacheWarms:             "Host envd binary cache warms, by result (ok|superseded|suppressed|pinned|already_warming|bad_target|failed)",
	OrchestratorEnvdUpgradeHandover:              "Live-upgrade handover items by item (proc|retained|watcher) and result (ok|failed)",
	PauseResumePrefetchHarvestAttempts:           "Pause-resume prefetch harvest attempts, by result",
	TCPFirewallConnectionsTotal:                  "Total number of TCP firewall connections processed",
	TCPFirewallErrorsTotal:                       "Total number of TCP firewall errors",
	TCPFirewallDecisionsTotal:                    "Total number of TCP firewall allow/block decisions",

	IngressProxyConnectionsBlockedTotal: "Total number of ingress proxy connections blocked by connection limit",
	CmuxErrorsTotal:                     "Total number of cmux connection multiplexer errors",

	RoutingPublishTotal: "Total number of sandbox routing record writes on MarkRunning (result=ok/error)",
	RoutingDeleteTotal:  "Total number of sandbox routing record deletes on MarkStopping (result=ok/error)",

	SandboxFCNetFails:         "Total Firecracker VMM errors transmitting or receiving data (direction=tx/rx)",
	SandboxFCNetNoAvailBuffer: "Total Firecracker VMM events where no virtqueue buffer was available (direction=tx/rx)",
	SandboxFCNetTapIOFails:    "Total Firecracker VMM TAP I/O failures (direction=tx/rx)",

	SandboxFCBlockFails:         "Total Firecracker VMM block device execution/event failures",
	SandboxFCBlockNoAvailBuffer: "Total Firecracker VMM block events where no virtqueue buffer was available",

	SandboxFCSnapshotLoadFailures: "Total snapshot loads refused by Firecracker (reason=vcpu_msr|vcpu_other|bad_request|unavailable|transport|timeout|canceled)",

	ApiRedisStoragePublisherPublished: "Total Redis PUBLISH calls completed by the storage publisher (result=success|failure)",
	ApiRedisStoragePublisherDropped:   "Total storage notifications dropped before reaching Redis (reason=queue_full|closed)",

	ApiRedisStorageExpirationIndexHealed:   "Sandboxes re-added to the global expiration index by the healer; sustained non-zero rate means index writes are being lost",
	ApiRedisStorageExpirationIndexSwept:    "Members removed from the global expiration index by the evictor scan (reason=orphan|dead_execution|invalid)",
	ApiRedisStorageExpirationIndexRescored: "Live expiration index members re-scored after drifting from the stored EndTime",
}

var counterUnits = map[CounterType]string{
	ApiRateLimitRequests:                         "{request}",
	SandboxCreateMeterName:                       "{sandbox}",
	ApiOrchestratorCreatedSandboxes:              "{sandbox}",
	ApiEvictorFsOnlyAutoPause:                    "{pause}",
	ApiEvictorAutoPauseDegraded:                  "{pause}",
	ApiOrchestratorResumeOriginNodeRemap:         "{snapshot}",
	ApiOrchestratorPauseRefusalRestore:           "{pause}",
	BuildResultCounterName:                       "{build}",
	BuildCacheResultCounterName:                  "{layer}",
	TeamSandboxCreated:                           "{sandbox}",
	OrchestratorHostBalanceDirtyPagesThreads:     "{thread}",
	EnvdInitCalls:                                "1",
	EnvdCollapseChunks:                           "{chunk}",
	OrchestratorSandboxKilledCounterName:         "{sandbox}",
	OrchestratorSandboxPauseAdmissionCounterName: "{decision}",
	OrchestratorSandboxCheckpointCounterName:     "{checkpoint}",
	OrchestratorFPRResumeCounterName:             "{resume}",
	OrchestratorSnapshotUploadFailedCounterName:  "{snapshot}",
	SandboxPauseFsQuiescedCounterName:            "{snapshot}",
	SandboxLifecycleUnstoppedCounterName:         "{sandbox}",
	SandboxResumeWPModeCounterName:               "{resume}",
	EnvdDefaultsApplied:                          "{resume}",
	EnvdDefaultsMismatch:                         "{mismatch}",
	EnvdDefaultsWorkdirWithheld:                  "{resume}",
	EnvdDefaultsBuiltinFallback:                  "{response}",
	OrchestratorEnvdUpgradeAttempts:              "{attempt}",
	OrchestratorEnvdOfflineUpgradeAttempts:       "{attempt}",
	OrchestratorFsRecoveryRuns:                   "{run}",
	OrchestratorFsRecoveryToolingUnsupported:     "{probe}",
	TemplateBuildCmdlineArgs:                     "{build}",
	OrchestratorEnvdUpgradeGated:                 "{sandbox}",
	OrchestratorEnvdBinaryCacheReads:             "{read}",
	OrchestratorEnvdBinaryCacheWarms:             "{warm}",
	OrchestratorEnvdBinaryCacheDeliveries:        "{read}",
	OrchestratorEnvdUpgradeHandover:              "{item}",
	PauseResumePrefetchHarvestAttempts:           "{attempt}",
	TCPFirewallConnectionsTotal:                  "{connection}",
	TCPFirewallErrorsTotal:                       "{error}",
	TCPFirewallDecisionsTotal:                    "{decision}",

	IngressProxyConnectionsBlockedTotal: "{connection}",
	CmuxErrorsTotal:                     "{error}",

	RoutingPublishTotal: "{record}",
	RoutingDeleteTotal:  "{record}",

	SandboxFCNetFails:         "{error}",
	SandboxFCNetNoAvailBuffer: "{event}",
	SandboxFCNetTapIOFails:    "{error}",

	SandboxFCBlockFails:         "{error}",
	SandboxFCBlockNoAvailBuffer: "{event}",

	SandboxFCSnapshotLoadFailures: "{failure}",

	ApiRedisStoragePublisherPublished: "{notification}",
	ApiRedisStoragePublisherDropped:   "{notification}",

	ApiRedisStorageExpirationIndexHealed:   "{sandbox}",
	ApiRedisStorageExpirationIndexSwept:    "{member}",
	ApiRedisStorageExpirationIndexRescored: "{member}",
}

var observableCounterDesc = map[ObservableCounterType]string{
	ApiOrchestratorSbxCreateSuccess: "Counter of successful sandbox creation requests.",
	ApiOrchestratorSbxCreateFailure: "Counter of failed sandbox creation requests.",
}

var observableCounterUnits = map[ObservableCounterType]string{
	ApiOrchestratorSbxCreateSuccess: "{sandbox}",
	ApiOrchestratorSbxCreateFailure: "{sandbox}",
}

var observableUpDownCounterDesc = map[ObservableUpDownCounterType]string{
	OrchestratorSandboxCountMeterName:                  "Counter of running sandboxes on the orchestrator.",
	ClientProxyServerConnectionsMeterCounterName:       "Open connections to the client proxy from load balancer.",
	ClientProxyPoolConnectionsMeterCounterName:         "Open connections from the client proxy to the orchestrator proxy.",
	ClientProxyPoolSizeMeterCounterName:                "Size of the client proxy pool.",
	OrchestratorProxyServerConnectionsMeterCounterName: "Open connections to the orchestrator proxy from client proxies.",
	OrchestratorProxyPoolConnectionsMeterCounterName:   "Open connections from the orchestrator proxy to sandboxes.",
	OrchestratorProxyPoolSizeMeterCounterName:          "Size of the orchestrator proxy pool.",
	BuildCounterMeterName:                              "Counter of running builds.",
	EvictionsRunningCounterName:                        "Counter of currently running evictions.",

	TCPFirewallActiveConnections: "Number of currently active TCP firewall connections.",

	ApiRedisStoragePublisherQueueDepth: "Current depth of the Redis storage publisher queue (items awaiting PUBLISH).",
}

var observableUpDownCounterUnits = map[ObservableUpDownCounterType]string{
	OrchestratorSandboxCountMeterName:                  "{sandbox}",
	ClientProxyServerConnectionsMeterCounterName:       "{connection}",
	ClientProxyPoolConnectionsMeterCounterName:         "{connection}",
	ClientProxyPoolSizeMeterCounterName:                "{transport}",
	OrchestratorProxyServerConnectionsMeterCounterName: "{connection}",
	OrchestratorProxyPoolConnectionsMeterCounterName:   "{connection}",
	OrchestratorProxyPoolSizeMeterCounterName:          "{transport}",
	BuildCounterMeterName:                              "{build}",
	EvictionsRunningCounterName:                        "{eviction}",

	TCPFirewallActiveConnections: "{connection}",

	ApiRedisStoragePublisherQueueDepth: "{notification}",
}

var gaugeFloatDesc = map[GaugeFloatType]string{
	SandboxCpuUsedGaugeName: "Amount of CPU used by the sandbox.",
}

var gaugeFloatUnits = map[GaugeFloatType]string{
	SandboxCpuUsedGaugeName: "{percent}",
}

var gaugeIntDesc = map[GaugeIntType]string{
	FirecrackerProcesses:                 "Non-exited Firecracker OS processes in the tracker's last complete scan.",
	FirecrackerProcessesUntracked:        "Firecracker processes older than five minutes and absent from lifecycle tracking.",
	FirecrackerProcessesOverMaxLength:    "Customer Firecracker processes beyond their known sandbox start plus maximum length; excludes builds and temporary VMs.",
	FirecrackerProcessesUnknownMaxLength: "Firecracker processes whose customer maximum length cannot be determined; excludes known builds and temporary VMs.",
	FirecrackerTrackerSuccess:            "Whether the latest Firecracker process tracker scan completed successfully (1 or 0).",
	FirecrackerTrackerLastSuccessAge:     "Seconds since the last complete Firecracker tracker scan, computed at collection time; omitted before the first success.",
	ApiOrchestratorCountMeterName:        "Counter of running orchestrators.",
	OrchestratorStatusGaugeName:          "Self-reported orchestrator status (always 1, labelled with status and version).",
	OrchestratorSandboxLimitGaugeName:    "Configured maximum number of running sandboxes on the orchestrator node.",
	OrchestratorCpuAllocatedGaugeName:    "Total vCPUs allocated to running sandboxes on the orchestrator node.",
	OrchestratorMemoryAllocatedGaugeName: "Total memory allocated to running sandboxes on the orchestrator node.",
	OrchestratorDiskAllocatedGaugeName:   "Total disk space allocated to running sandboxes on the orchestrator node.",
	SandboxRamUsedGaugeName:              "Amount of RAM used by the sandbox.",
	SandboxRamTotalGaugeName:             "Amount of RAM available to the sandbox.",
	SandboxRamCacheGaugeName:             "Amount of RAM used by the page cache in the sandbox.",
	SandboxCpuTotalGaugeName:             "Amount of CPU available to the sandbox.",
	SandboxDiskUsedGaugeName:             "Amount of disk space used by the sandbox.",
	SandboxDiskTotalGaugeName:            "Amount of disk space available to the sandbox.",
	TeamSandboxRunningGaugeName:          "The number of sandboxes running for the team in the interval.",
	SandboxCountGaugeName:                "Number of running sandbox instances per team.",

	OrchestratorTemplateCacheEntriesGaugeName:        "Templates currently resident in the orchestrator's template cache.",
	OrchestratorTemplateCachePinnedGaugeName:         "Resident templates pinned by at least one live sandbox, and so exempt from eviction.",
	OrchestratorTemplateCacheMappingEntriesGaugeName: "Total header mapping entries held by resident templates.",
	OrchestratorTemplateCacheMappingBytesGaugeName:   "Approximate heap bytes held by resident templates' header mappings.",
	OrchestratorTemplateCachePinnedRefsGaugeName:     "Outstanding template pins, one per holder, so shared builds and leak magnitude are visible.",
	OrchestratorTemplateCacheOldestPinAgeGaugeName:   "Age of the oldest outstanding template pin. A pin never ages out on its own.",
}

var gaugeIntUnits = map[GaugeIntType]string{
	FirecrackerProcesses:                 "{process}",
	FirecrackerProcessesUntracked:        "{process}",
	FirecrackerProcessesOverMaxLength:    "{process}",
	FirecrackerProcessesUnknownMaxLength: "{process}",
	FirecrackerTrackerSuccess:            "1",
	FirecrackerTrackerLastSuccessAge:     "s",
	ApiOrchestratorCountMeterName:        "{orchestrator}",
	OrchestratorStatusGaugeName:          "{orchestrator}",
	OrchestratorSandboxLimitGaugeName:    "{sandbox}",
	OrchestratorCpuAllocatedGaugeName:    "{count}",
	OrchestratorMemoryAllocatedGaugeName: "{By}",
	OrchestratorDiskAllocatedGaugeName:   "{By}",
	SandboxRamUsedGaugeName:              "{By}",
	SandboxRamTotalGaugeName:             "{By}",
	SandboxRamCacheGaugeName:             "{By}",
	SandboxCpuTotalGaugeName:             "{count}",
	SandboxDiskUsedGaugeName:             "{By}",
	SandboxDiskTotalGaugeName:            "{By}",
	TeamSandboxRunningGaugeName:          "{sandbox}",
	SandboxCountGaugeName:                "{sandbox}",

	OrchestratorTemplateCacheEntriesGaugeName:        "{template}",
	OrchestratorTemplateCachePinnedGaugeName:         "{template}",
	OrchestratorTemplateCacheMappingEntriesGaugeName: "{entry}",
	OrchestratorTemplateCacheMappingBytesGaugeName:   "{By}",
	OrchestratorTemplateCachePinnedRefsGaugeName:     "{pin}",
	OrchestratorTemplateCacheOldestPinAgeGaugeName:   "s",
}

func GetCounter(meter metric.Meter, name CounterType) (metric.Int64Counter, error) {
	desc := counterDesc[name]
	unit := counterUnits[name]

	return meter.Int64Counter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

func GetObservableCounter(meter metric.Meter, name ObservableCounterType, callback metric.Int64Callback) (metric.Int64ObservableCounter, error) {
	desc := observableCounterDesc[name]
	unit := observableCounterUnits[name]

	return meter.Int64ObservableCounter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
		metric.WithInt64Callback(callback),
	)
}

func GetObservableUpDownCounter(meter metric.Meter, name ObservableUpDownCounterType, callback metric.Int64Callback) (metric.Int64ObservableUpDownCounter, error) {
	desc := observableUpDownCounterDesc[name]
	unit := observableUpDownCounterUnits[name]

	return meter.Int64ObservableUpDownCounter(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
		metric.WithInt64Callback(callback),
	)
}

func GetGaugeFloat(meter metric.Meter, name GaugeFloatType) (metric.Float64ObservableGauge, error) {
	desc := gaugeFloatDesc[name]
	unit := gaugeFloatUnits[name]

	return meter.Float64ObservableGauge(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

func GetGaugeInt(meter metric.Meter, name GaugeIntType) (metric.Int64ObservableGauge, error) {
	desc := gaugeIntDesc[name]
	unit := gaugeIntUnits[name]

	return meter.Int64ObservableGauge(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

var histogramDesc = map[HistogramType]string{
	ApiRedisStorageExpirationIndexSize:          "Cardinality of sandbox:storage:global:expiration ZSET at the time of each heal pass; use max/p99 to detect unbounded growth",
	ApiRedisStorageExpirationIndexSweepDuration: "Wall-clock duration of one ExpiredItems call (ZRANGEBYSCORE + MGET pipeline); rising p99 alongside rising index size confirms O(log N) growth",
	ApiRedisStoragePublisherPublishDuration:     "Duration of a single Redis PUBLISH round-trip from the storage publisher",

	BuildDurationHistogramName:                        "Time taken to build a template",
	BuildPhaseDurationHistogramName:                   "Time taken to build each phase of a template",
	BuildStepDurationHistogramName:                    "Time taken to build each step of a template",
	BuildRootfsSizeHistogramName:                      "Size of the built template rootfs in bytes",
	OrchestratorSandboxCreateDurationName:             "Time taken to create a sandbox",
	OrchestratorSandboxExecutionDurationName:          "Time a single sandbox execution ran, from the guest being ready until it stopped executing, labeled by stop reason",
	OrchestratorEnvdUpgradeDurationName:               "Wall-time of a resume-time envd upgrade (delivery + trigger + WaitForEnvd)",
	OrchestratorEnvdUpgradePhaseDurationName:          "Wall-time of one resume-time envd upgrade phase (resolve|deliver|ready); resolve is absent from the combined histogram",
	WaitForEnvdDurationHistogramName:                  "Time taken for Envd to initialize successfully",
	EnvdCollapseDurationHistogramName:                 "Time taken for the pre-pause envd heap collapse round-trip",
	GuestSyncDurationHistogramName:                    "Time taken for the mandatory pre-pause guest sync (filesystem-only pause)",
	PauseDurationHistogramName:                        "Time taken to pause a sandbox, labeled by fs_only (filesystem-only vs memory) and success",
	SnapshotProcessMemoryDurationName:                 "Time to export+diff the memory file during a pause snapshot (memory pauses only), labeled by success",
	SnapshotProcessRootfsDurationName:                 "Time to export+diff the rootfs during a pause snapshot, labeled by fs_only and success",
	SnapshotRootfsSealDurationName:                    "Time for the background deferred rootfs reflink seal (off the pause critical path), labeled by in_place and success",
	SnapshotGuestFreezeDurationName:                   "Wall time the guest is frozen during an in-place checkpoint, from the FC pause call to the in-place resume; success=false means the resume ran on the pause-failure cleanup path",
	SnapshotMemorySealDurationName:                    "Time for the background CoW-window memory capture (off the in-place resume critical path), labeled by success",
	OrchestratorSandboxMemfileDedupDurationName:       "Background memfile dedup latency, from the provisional header's creation at pause to the durable-header swap",
	OrchestratorSandboxPauseAdmissionWaitDurationName: "Time snapshot admission waited on the durable parent header whenever it waited, labeled by outcome (ready_after_wait/refused)",
	OrchestratorEnvdOfflineUpgradeDurationName:        "Wall-time of the offline cold-boot envd rootfs swap (jailed debugfs)",
	OrchestratorFsRecoveryDurationName:                "Wall-time of the jailed pre-boot e2fsck run on a cold boot",

	PauseResumePrefetchHarvestDurationName:     "Time the pause-resume prefetch harvest held a start slot (throwaway resume, trace collection, reap)",
	PauseResumePrefetchHarvestPagesName:        "Harvested resume-prefetch trace size in 2 MiB blocks, per successful harvest",
	PauseResumePrefetchSealWaitDurationName:    "Time the prefetch harvest waited for the deferred rootfs seal before its warm resume",
	PauseResumePrefetchPersistWaitDurationName: "Time the prefetch harvest waited for the in-flight snapshot upload before persisting the mapping",

	EnvdFreezeDurationHistogramName:     "Round-trip duration of the pre-pause workload freeze call, per pause",
	EnvdFreezeSweepHistogramName:        "Time envd spent issuing cgroup.freeze writes, per pause (scales with cgroup count)",
	EnvdFreezeWaitHistogramName:         "Time envd spent waiting for cgroups to quiesce, per pause (scales with guest I/O depth)",
	EnvdFreezeVisitedHistogramName:      "Cgroups the pre-pause freeze walk examined, per pause. Sizes the walk's bound: the bound should sit an order of magnitude above this",
	EnvdFreezeAuditHistogramName:        "Resume-time audit of the frozen cgroup set, by kind: escaped (ran through the snapshot, whether created after the sweep or missed by a truncated or failed one -- read alongside freeze.truncated and the failed outcome) and violations (a cgroup the resume depends on was frozen -- a bug, expected to be zero)",
	EnvdFreezeCgroupsHistogramName:      "Cgroups affected by a pre-pause freeze, per pause, split by outcome",
	EnvdUnfreezeDurationHistogramName:   "Round-trip duration of the pause-rollback workload thaw call, per rollback",
	EnvdMemoryProtectionHistogramName:   "Memory protection configured on envd's cgroup chain as the guest reports it on /init, at most once per start (when the first /init's header decodes), by kind: request (envd's own memory.min) and floor (the minimum of memory.min over the chain below the root, envd's own cgroup included; 0 means some level carries none, there is no level at all because envd is in the root cgroup, or a value could not be read -- the init instruments' protection attribute is what tells the unreadable case apart, as unknown rather than unprotected). Values are truncated to MiB, so a setting below 1 MiB records as 0, and capped at the sandbox's own RAM, since nothing can protect more memory than the guest has: a sample equal to the sandbox's RAM means at least that, and an unbounded request (memory.min = max) is the extreme case that reaches it, though any request above the guest's RAM does; a sandbox whose RAM the recording does not know records the value uncapped. The exact byte count is on the envd-init span",
	UffdStartupPagesHistogramName:       "Demand-fault pages a guest needed to reach a successful envd init, per start",
	UffdStartupSourcePagesHistogramName: "Subset of startup demand-fault pages pulled from the source (e.g. GCS), per start",
	UffdStartupBytesHistogramName:       "Bytes faulted into a guest to reach a successful envd init, per start",

	TCPFirewallConnectionDurationHistogramName:    "Duration of TCP firewall proxied connections",
	TCPFirewallConnectionsPerSandboxHistogramName: "Number of active TCP firewall connections per sandbox",

	IngressProxyConnectionDurationHistogramName:    "Duration of ingress proxy connections",
	IngressProxyConnectionsPerSandboxHistogramName: "Number of active ingress proxy connections per sandbox",

	// Firecracker net histograms (direction=tx/rx attribute; TX-only carry direction=tx)
	SandboxFCNetBytes:                 "Distribution of Firecracker VMM bytes per metrics flush",
	SandboxFCNetPackets:               "Distribution of Firecracker VMM packets per metrics flush",
	SandboxFCNetCount:                 "Distribution of Firecracker VMM I/O operations per metrics flush",
	SandboxFCNetRateLimiterThrottled:  "Distribution of Firecracker VMM ops throttled by rate limiter per metrics flush",
	SandboxFCNetRateLimiterEventCount: "Distribution of Firecracker VMM TX rate limiter events per metrics flush",
	SandboxFCNetRemainingReqs:         "Distribution of Firecracker VMM TX queue remaining-request events per metrics flush",

	// Firecracker block histograms (direction=read/write attribute)
	SandboxFCBlockBytes:                 "Distribution of Firecracker VMM block bytes per metrics flush",
	SandboxFCBlockCount:                 "Distribution of Firecracker VMM block I/O operations per metrics flush",
	SandboxFCBlockQueueEventCount:       "Distribution of Firecracker VMM block queue notifications per metrics flush",
	SandboxFCBlockRateLimiterThrottled:  "Distribution of Firecracker VMM block ops throttled by rate limiter per metrics flush",
	SandboxFCBlockRateLimiterEventCount: "Distribution of Firecracker VMM block rate limiter events per metrics flush",
	SandboxFCBlockIOEngineThrottled:     "Distribution of Firecracker VMM block ops throttled by io_uring engine per metrics flush",
	SandboxFCBlockRemainingReqs:         "Distribution of Firecracker VMM block queue remaining-request events per metrics flush",

	SnapshotDiffBytes:  "Per-snapshot dirty/empty bytes per file",
	SnapshotDiffRatio:  "Per-snapshot dirty/empty as fraction of total mapped size (1.0 = 100%)",
	SnapshotTotalBytes: "Per-snapshot total mapped size of the file",

	UploadUncompressedBytes: "Per-upload uncompressed artifact size",
	UploadCompressedBytes:   "Per-upload compressed artifact size",
	UploadCompressionRatio:  "Per-upload compressed/uncompressed ratio (1.0 = no compression)",
}

var histogramUnits = map[HistogramType]string{
	ApiRedisStorageExpirationIndexSize:          "{entry}",
	ApiRedisStorageExpirationIndexSweepDuration: "ms",
	ApiRedisStoragePublisherPublishDuration:     "ms",

	BuildDurationHistogramName:                        "ms",
	BuildPhaseDurationHistogramName:                   "ms",
	BuildStepDurationHistogramName:                    "ms",
	BuildRootfsSizeHistogramName:                      "{By}",
	OrchestratorSandboxCreateDurationName:             "ms",
	OrchestratorSandboxExecutionDurationName:          "ms",
	OrchestratorEnvdUpgradeDurationName:               "ms",
	OrchestratorEnvdUpgradePhaseDurationName:          "ms",
	OrchestratorEnvdOfflineUpgradeDurationName:        "ms",
	OrchestratorFsRecoveryDurationName:                "ms",
	WaitForEnvdDurationHistogramName:                  "ms",
	EnvdCollapseDurationHistogramName:                 "ms",
	GuestSyncDurationHistogramName:                    "ms",
	PauseDurationHistogramName:                        "ms",
	SnapshotProcessMemoryDurationName:                 "ms",
	SnapshotProcessRootfsDurationName:                 "ms",
	SnapshotRootfsSealDurationName:                    "ms",
	SnapshotGuestFreezeDurationName:                   "ms",
	SnapshotMemorySealDurationName:                    "ms",
	OrchestratorSandboxMemfileDedupDurationName:       "ms",
	OrchestratorSandboxPauseAdmissionWaitDurationName: "ms",
	PauseResumePrefetchHarvestDurationName:            "ms",
	PauseResumePrefetchHarvestPagesName:               "{page}",
	PauseResumePrefetchSealWaitDurationName:           "ms",
	PauseResumePrefetchPersistWaitDurationName:        "ms",
	EnvdFreezeDurationHistogramName:                   "ms",
	EnvdFreezeSweepHistogramName:                      "ms",
	EnvdFreezeWaitHistogramName:                       "ms",
	EnvdFreezeVisitedHistogramName:                    "{cgroup}",
	EnvdFreezeAuditHistogramName:                      "{cgroup}",
	EnvdFreezeCgroupsHistogramName:                    "{cgroup}",
	EnvdUnfreezeDurationHistogramName:                 "ms",
	EnvdMemoryProtectionHistogramName:                 "MiBy",
	UffdStartupPagesHistogramName:                     "{page}",
	UffdStartupSourcePagesHistogramName:               "{page}",
	UffdStartupBytesHistogramName:                     "{By}",
	TCPFirewallConnectionDurationHistogramName:        "ms",
	TCPFirewallConnectionsPerSandboxHistogramName:     "{connection}",

	IngressProxyConnectionDurationHistogramName:    "ms",
	IngressProxyConnectionsPerSandboxHistogramName: "{connection}",

	// Firecracker net histograms
	SandboxFCNetBytes:                 "{By}",
	SandboxFCNetPackets:               "{packet}",
	SandboxFCNetCount:                 "{op}",
	SandboxFCNetRateLimiterThrottled:  "{op}",
	SandboxFCNetRateLimiterEventCount: "{event}",
	SandboxFCNetRemainingReqs:         "{event}",

	// Firecracker block histograms
	SandboxFCBlockBytes:                 "{By}",
	SandboxFCBlockCount:                 "{op}",
	SandboxFCBlockQueueEventCount:       "{event}",
	SandboxFCBlockRateLimiterThrottled:  "{op}",
	SandboxFCBlockRateLimiterEventCount: "{event}",
	SandboxFCBlockIOEngineThrottled:     "{op}",
	SandboxFCBlockRemainingReqs:         "{event}",

	SnapshotDiffBytes:  "{By}",
	SnapshotDiffRatio:  "{1}",
	SnapshotTotalBytes: "{By}",

	UploadUncompressedBytes: "{By}",
	UploadCompressedBytes:   "{By}",
	UploadCompressionRatio:  "{1}",
}

// GetHistogram returns an Int64 histogram with the registered description and
// unit. It sets no bucket boundaries: histogramAggregation discards them.
func GetHistogram(meter metric.Meter, name HistogramType) (metric.Int64Histogram, error) {
	desc := histogramDesc[name]
	unit := histogramUnits[name]

	return meter.Int64Histogram(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

func GetFloatHistogram(meter metric.Meter, name HistogramType) (metric.Float64Histogram, error) {
	desc := histogramDesc[name]
	unit := histogramUnits[name]

	return meter.Float64Histogram(string(name),
		metric.WithDescription(desc),
		metric.WithUnit(unit),
	)
}

type TimerFactory struct {
	duration metric.Int64Histogram
	bytes    metric.Int64Counter
	count    metric.Int64Counter
}

func NewTimerFactory(
	blocksMeter metric.Meter,
	metricName, durationDescription, bytesDescription, counterDescription string,
) (TimerFactory, error) {
	duration, err := blocksMeter.Int64Histogram(metricName,
		metric.WithDescription(durationDescription),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return TimerFactory{}, fmt.Errorf("failed to get slices metric: %w", err)
	}

	bytes, err := blocksMeter.Int64Counter(metricName,
		metric.WithDescription(bytesDescription),
		metric.WithUnit("By"),
	)
	if err != nil {
		return TimerFactory{}, fmt.Errorf("failed to create total bytes requested metric: %w", err)
	}

	count, err := blocksMeter.Int64Counter(metricName,
		metric.WithDescription(counterDescription),
	)
	if err != nil {
		return TimerFactory{}, fmt.Errorf("failed to create total page faults metric: %w", err)
	}

	return TimerFactory{duration, bytes, count}, nil
}

// FloatTimerFactory records duration as fractional milliseconds so sub-ms
// operations aren't truncated to 0. The duration histogram and event counter
// share <metricName> (rate()-friendly); only the bytes counter splits out to
// <metricName>.size so Grafana's unit detection doesn't conflate ms with By.
type FloatTimerFactory struct {
	duration metric.Float64Histogram
	bytes    metric.Int64Counter
	count    metric.Int64Counter
}

// SubMillisecondMsBuckets resolve sub-ms operations (mmap / cache hits) that the
// default OTEL buckets (first boundary 5ms) collapse into one, while still
// covering remote reads to ~10s.
var SubMillisecondMsBuckets = []float64{
	0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
}

func NewFloatTimerFactory(
	meter metric.Meter,
	metricName, durationDescription, bytesDescription string,
) (FloatTimerFactory, error) {
	duration, err := meter.Float64Histogram(metricName,
		metric.WithDescription(durationDescription),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(SubMillisecondMsBuckets...),
	)
	if err != nil {
		return FloatTimerFactory{}, fmt.Errorf("failed to create duration histogram: %w", err)
	}

	bytes, err := meter.Int64Counter(metricName+".size",
		metric.WithDescription(bytesDescription),
		metric.WithUnit("By"),
	)
	if err != nil {
		return FloatTimerFactory{}, fmt.Errorf("failed to create bytes counter: %w", err)
	}

	count, err := meter.Int64Counter(metricName,
		metric.WithDescription("Total "+metricName+" events recorded"),
	)
	if err != nil {
		return FloatTimerFactory{}, fmt.Errorf("failed to create count counter: %w", err)
	}

	return FloatTimerFactory{duration, bytes, count}, nil
}

func (f *FloatTimerFactory) Record(ctx context.Context, dur time.Duration, total int64, attrs metric.MeasurementOption) {
	f.duration.Record(ctx, float64(dur)/float64(time.Millisecond), attrs)
	f.bytes.Add(ctx, total, attrs)
	f.count.Add(ctx, 1, attrs)
}

func (f *TimerFactory) Begin(kv ...attribute.KeyValue) *Stopwatch {
	return &Stopwatch{
		histogram: f.duration,
		sum:       f.bytes,
		count:     f.count,
		start:     time.Now(),
		kv:        kv,
	}
}

type Stopwatch struct {
	histogram  metric.Int64Histogram
	sum, count metric.Int64Counter
	start      time.Time
	kv         []attribute.KeyValue
}

const (
	resultAttr        = "result"
	resultTypeSuccess = "success"
	resultTypeFailure = "failure"
)

var (
	// Pre-allocated result attributes for use with PrecomputeAttrs.
	Success = attribute.String(resultAttr, resultTypeSuccess)
	Failure = attribute.String(resultAttr, resultTypeFailure)
)

func (t Stopwatch) Success(ctx context.Context, total int64, kv ...attribute.KeyValue) {
	t.end(ctx, resultTypeSuccess, total, kv...)
}

func (t Stopwatch) Failure(ctx context.Context, total int64, kv ...attribute.KeyValue) {
	t.end(ctx, resultTypeFailure, total, kv...)
}

func (t Stopwatch) end(ctx context.Context, result string, total int64, kv ...attribute.KeyValue) {
	kv = append(kv, attribute.KeyValue{Key: resultAttr, Value: attribute.StringValue(result)})
	kv = append(t.kv, kv...)
	opt := metric.WithAttributeSet(attribute.NewSet(kv...))
	t.RecordRaw(ctx, total, opt)
}

// PrecomputeAttrs builds a reusable MeasurementOption from the given attribute
// key-values. The option must include all attributes (including "result").
// Use with Stopwatch.Record to avoid per-call attribute allocation.
func PrecomputeAttrs(kv ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(kv...))
}

// RecordRaw records an operation using a precomputed attribute option, it does
// not include any previous attributes passed at Begin(). Zero-allocation
// alternative to Success/Failure for hot paths.
func (t Stopwatch) RecordRaw(ctx context.Context, total int64, allAttrs metric.MeasurementOption) {
	amount := time.Since(t.start).Milliseconds()
	t.histogram.Record(ctx, amount, allAttrs)
	t.sum.Add(ctx, total, allAttrs)
	t.count.Add(ctx, 1, allAttrs)
}
