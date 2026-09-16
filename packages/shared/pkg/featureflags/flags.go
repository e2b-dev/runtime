package featureflags

import (
	"context"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// kinds
const (
	SandboxKind                        ldcontext.Kind = "sandbox"
	SandboxTemplateAttribute           string         = "template-id"
	SandboxKernelVersionAttribute      string         = "kernel-version"
	SandboxFirecrackerVersionAttribute string         = "firecracker-version"
	SandboxEnvdVersionAttribute        string         = "envd-version"
	// SandboxTypeAttribute distinguishes "sandbox" from "build" runs.
	SandboxTypeAttribute string = "sandbox-type"

	TeamKind             ldcontext.Kind = "team"
	UserKind             ldcontext.Kind = "user"
	ClusterKind          ldcontext.Kind = "cluster"
	BatcherKind          ldcontext.Kind = "batcher.name"
	InstanceGroupKind    ldcontext.Kind = "instance-group"
	deploymentKind       ldcontext.Kind = "deployment"
	TierKind             ldcontext.Kind = "tier"
	ServiceKind          ldcontext.Kind = "service"
	TemplateKind         ldcontext.Kind = "template"
	VolumeKind           ldcontext.Kind = "volume"
	CompressFileTypeKind ldcontext.Kind = "compress-file-type"
	CompressUseCaseKind  ldcontext.Kind = "compress-use-case"
)

// All flags must be defined here: https://app.launchdarkly.com/projects/default/flags/

type JSONFlag struct {
	name     string
	fallback ldvalue.Value
}

func (f JSONFlag) Key() string {
	return f.name
}

func (f JSONFlag) String() string {
	return f.name
}

func (f JSONFlag) Fallback() ldvalue.Value {
	return f.fallback
}

func NewJSONFlag(name string, fallback ldvalue.Value) JSONFlag {
	flag := JSONFlag{name: name, fallback: fallback}
	builder := launchDarklyOfflineStore.Flag(flag.name).ValueForAll(fallback)
	launchDarklyOfflineStore.Update(builder)

	return flag
}

var CleanNFSCache = NewJSONFlag("clean-nfs-cache", ldvalue.Null())

// RateLimitConfigFlag provides per-team rate limit overrides.
// JSON format:
//
//	{
//	  "/sandboxes/": {"rate": 50, "burst": 100},
//	  "/sandboxes/:sandboxID/pause": {"rate": 10, "burst": 20}
//	}
//
// Entries set per-team route limits; routes absent from the flag remain unlimited.
var RateLimitConfigFlag = NewJSONFlag("rate-limit-config", ldvalue.Null())

const (
	APIGroupRateLimitDisabled = "disabled"
	APIGroupRateLimitShadow   = "shadow"
	APIGroupRateLimitEnabled  = "enabled"
)

var RateLimitV2Mode = NewStringFlag("rate-limit-v2-mode", APIGroupRateLimitDisabled)

type BoolFlag struct {
	name     string
	fallback bool
}

func (f BoolFlag) Key() string {
	return f.name
}

func (f BoolFlag) String() string {
	return f.name
}

func (f BoolFlag) Fallback() bool {
	return f.fallback
}

// envBoolOr reads key as a bool, falling back when it is unset or unparseable.
// It exists so a bool flag's fallback can be overridden on a cluster with no
// LaunchDarkly (dev), where a flag otherwise resolves to a value only a rebuild
// can change — the same reason EnvdUpgradeTargetFlag reads its fallback from the
// environment.
func envBoolOr(key string, fallback bool) bool {
	raw := env.GetEnv(key, "")
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}

	return parsed
}

func NewBoolFlag(name string, fallback bool) BoolFlag {
	flag := BoolFlag{name: name, fallback: fallback}
	builder := launchDarklyOfflineStore.Flag(flag.name).VariationForAll(fallback)
	launchDarklyOfflineStore.Update(builder)

	return flag
}

// OverrideBoolFlag forces a bool flag to a specific value in the offline store.
// Only takes effect when LAUNCH_DARKLY_API_KEY is not set (i.e. dev/CLI tools).
func OverrideBoolFlag(flag BoolFlag, value bool) {
	builder := launchDarklyOfflineStore.Flag(flag.name).VariationForAll(value)
	launchDarklyOfflineStore.Update(builder)
}

// OverrideJSONFlag forces a JSON flag to a specific value in the offline store.
// Only takes effect when LAUNCH_DARKLY_API_KEY is not set (i.e. dev/CLI tools).
func OverrideJSONFlag(flag JSONFlag, value ldvalue.Value) {
	builder := launchDarklyOfflineStore.Flag(flag.name).ValueForAll(value)
	launchDarklyOfflineStore.Update(builder)
}

var (
	SnapshotFeatureFlag                 = NewBoolFlag("use-nfs-for-snapshots", env.IsDevelopment())
	TemplateFeatureFlag                 = NewBoolFlag("use-nfs-for-templates", env.IsDevelopment())
	EnableWriteThroughCacheFlag         = NewBoolFlag("write-to-cache-on-writes", false)
	UseNFSCacheForBuildingTemplatesFlag = NewBoolFlag("use-nfs-for-building-templates", env.IsDevelopment())
	CreateStorageCacheSpansFlag         = NewBoolFlag("create-storage-cache-spans", env.IsDevelopment())
	OrchAcceptsCombinedHostFlag         = NewBoolFlag("orch-accepts-combined-host", false)
	WorkspacesEnabledFlag               = NewBoolFlag("workspacesEnabled", false)

	// FsFreezeViaExecFlag freezes the guest rootfs with `fsfreeze -f /` run through
	// the envd exec API before a filesystem-only pause, for guests whose envd
	// predates the native /fsfreeze endpoint. Off = those guests fall back to a
	// plain guest sync (today's behavior). Falls back to sync per-pause if the
	// guest lacks fsfreeze or the freeze fails.
	FsFreezeViaExecFlag = NewBoolFlag("fsfreeze-via-exec", false)

	// FsOnlyResumeAPIFlag accepts memory:false on resume/connect of a
	// memory-inclusive snapshot (an explicit cold-boot rescue). Off = the
	// request is rejected, never silently downgraded to a memory restore.
	FsOnlyResumeAPIFlag = NewBoolFlag("fs-only-resume-api", false)

	// PrebootFsRecoveryFlag runs a jailed filesystem recovery before every cold
	// boot of a rootfs that was not frozen at pause (fs_quiesced absent/false).
	// Separate from FsOnlyResumeAPIFlag because it also changes the behavior of
	// existing filesystem-only cold boots whose pause fell back to sync.
	PrebootFsRecoveryFlag = NewBoolFlag("preboot-fs-recovery", false)

	// StorageSoftDeleteCheckFlag enables reading the storage-index soft-delete
	// tombstone on header load (one extra GCS Attrs on cold load). Off = no overhead.
	StorageSoftDeleteCheckFlag = NewBoolFlag("storage-soft-delete-check", false)
	// StorageSoftDeleteEnforceFlag makes a soft-deleted object fail the read
	// (fail closed) instead of only emitting a metric + log. Requires the check flag.
	StorageSoftDeleteEnforceFlag = NewBoolFlag("storage-soft-delete-enforce", false)

	// UseMemFdFlag asks Firecracker to back guest memory with a memfd and
	// pass the fd over the UFFD socket; the orchestrator then mmaps it
	// directly instead of using process_vm_readv on pause.
	UseMemFdFlag = NewBoolFlag("use-memfd", true)

	// UseSyncWPFlag asks Firecracker (via use_sync_wp on snapshot load) to
	// register guest memory for SYNCHRONOUS userfault write-protect events,
	// which the orchestrator's serve loop resolves, instead of the kernel's
	// in-place WP_ASYNC clears. Foundation for the copy-on-write background
	// memory snapshot. Default off = WP_ASYNC, today's behavior. Enable only
	// where the deployed FC accepts the use_sync_wp field: FC rejects unknown
	// fields on snapshot load, so a mismatch fails the resume loudly.
	UseSyncWPFlag = NewBoolFlag("use-sync-wp", false)

	// InPlaceCheckpointFlag makes Checkpoint pause, snapshot and resume the
	// SAME Firecracker process (in-place) instead of resuming a fresh sandbox
	// from the new build. Only honored for sandboxes resumed with
	// UseSyncWPFlag on: in-place skips the snapshot re-load that re-arms
	// write-protection, so dirty tracking across repeated checkpoints relies
	// on the sync-WP serve loop; resume-fresh stays the fallback for async
	// sandboxes and when this flag is off.
	InPlaceCheckpointFlag = NewBoolFlag("in-place-checkpoint", false)

	// DeferMemoryExportFlag makes the in-place checkpoint export guest
	// memory through the CoW window instead of the synchronous dirty-RAM
	// copy: the dirty set is write-protect-armed while the VM is paused, the
	// guest resumes immediately, and pages are captured in the background
	// (first guest write to an uncaptured page copies the pre-image before
	// the write proceeds). Only takes effect on the in-place path with a
	// sync-WP UFFD backend. When the VM's balloon runs continuous free-page
	// REPORTING, reporting is PAUSED for the window's lifetime (a REMOVE
	// zapping an uncaptured page would export zeros where pause-time content
	// is owed); requires an FC build with /balloon/reporting — pause failures
	// fall back to the synchronous copy. (The synchronous pre-pause
	// free-page-hinting drain settles before the dirty readout and needs no
	// pause.) Default off = today's synchronous copy.
	DeferMemoryExportFlag = NewBoolFlag("defer-memory-export", false)

	// SyncWPTrackerDirtyFlag derives the pause-time dirty set from the
	// orchestrator's page tracker (installs + synchronous WP-fault
	// promotions) instead of Firecracker's GetDirtyMemory pagemap scan,
	// skipping that RPC entirely. Only consulted for sandboxes resumed with
	// UseSyncWPFlag on — under WP_ASYNC the kernel clears protections
	// in-place and the tracker never sees guest writes. Evaluated fresh at
	// each pause, so flipping it off immediately reverts running sandboxes to
	// the pagemap source (kill switch). Burn-in gate before enabling: the
	// dirty-source divergence log (emitted while this flag is off) must
	// show pagemap_only == 0 for sync-WP sandboxes — a nonzero count means
	// the tracker missed a write and would corrupt the snapshot.
	SyncWPTrackerDirtyFlag = NewBoolFlag("sync-wp-tracker-dirty", false)

	// MemfdBackgroundCopyFlag streams the memfd into the snapshot cache on
	// a goroutine so Pause returns as soon as the diff metadata is written.
	// Only takes effect when UseMemFdFlag is also on.
	MemfdBackgroundCopyFlag = NewBoolFlag("memfd-background-copy", true)

	// MemfileDiffDedupFlag enables 4 KiB-page dedup of the memfile diff
	// against the base memfile. bestEffort skips uncached blocks; directIO
	// opens the dedup output with O_DIRECT. The remaining keys budget fetch
	// defragmentation of the deduped diff — fetchRunWindowPages is the
	// uncompressed frame/window size served per backing fetch — see
	// orchestrator block.DedupBudget for semantics (0 = disabled/default).
	MemfileDiffDedupFlag = NewJSONFlag("memfile-diff-dedup", ldvalue.FromJSONMarshal(map[string]any{
		"enabled":                        false,
		"bestEffort":                     false,
		"directIO":                       false,
		"maxFetchWindowsPerBlock":        0,
		"maxPromotedParentPagesPerBlock": 0,
		"maxPagesPerPromotedFrame":       0,
		"blockFaultPct":                  0,
		"fetchRunWindowPages":            0,
	}))

	// MemfdDedupInflightServeFlag lets a resume that overlaps an in-flight
	// memfile dedup serve dirty pages straight from the still-mapped memfd
	// instead of blocking until dedup finishes. It gates both windows: serving
	// via a provisional local header while dedup is still computing the deduped
	// header, and serving during the dedup drain before the compacted diff is
	// ready. Only affects the memfd-dedup path; off restores the prior
	// wait-for-dedup behavior.
	MemfdDedupInflightServeFlag = NewBoolFlag("memfd-dedup-inflight-serve", false)

	// PeerToPeerChunkTransferFlag enables peer-to-peer chunk routing.
	PeerToPeerChunkTransferFlag = NewBoolFlag("peer-to-peer-chunk-transfer", false)
	// PeerToPeerAsyncCheckpointFlag makes Checkpoint upload fire-and-forget instead
	// of synchronous. Only safe to enable after PeerToPeerChunkTransferFlag is ON.
	PeerToPeerAsyncCheckpointFlag = NewBoolFlag("peer-to-peer-async-checkpoint", false)

	// DeferRootfsExportFlag moves the rootfs diff seal (the reflink, which forces a
	// synchronous host->NVMe writeback) off the pause critical path. On the
	// suspend (pause) path, pause() ejects the cache and stops the sandbox, then
	// reflinks the diff in the background — nothing reads the diff until a later
	// resume. On the in-place checkpoint path, pause() swaps a fresh writable
	// cache in, resumes the VM, seals the frozen old cache in the background and
	// folds it back into the writable cache when done. Off by default; falls
	// back to the synchronous export when off or on a non-NBD provider.
	DeferRootfsExportFlag = NewBoolFlag("defer-rootfs-export", false)

	PersistentVolumesFlag           = NewBoolFlag("can-use-persistent-volumes", env.IsDevelopment())
	SandboxLabelBasedSchedulingFlag = NewBoolFlag("sandbox-label-based-scheduling", false)
	FreePageReportingFlag           = NewBoolFlag("free-page-reporting", false)
	FreezeUserCgroupFlag            = NewBoolFlag("freeze-user-cgroup", env.IsDevelopment())
	// FreezeGuestHierarchyFlag selects the hierarchy walk over the static user/pty list,
	// i.e. whether cgroups the customer created anywhere in the tree are frozen before a
	// pause. Default off: it changes which cgroups are stopped, which is a behaviour
	// change on the pause path.
	//
	// Evaluated here rather than in envd because envd has no LaunchDarkly, so the mode
	// travels on the /freeze call. That also means it can be ON at pause and OFF at the
	// following resume, which is why the thaw discovers what is frozen instead of
	// recomputing what should have been.
	FreezeGuestHierarchyFlag = NewBoolFlag("freeze-guest-hierarchy", false)

	// FreezeGuestHierarchyMaxCgroupsFlag bounds one hierarchy sweep. A safety guard, not
	// a performance one -- the walk is breadth-bounded along a 2-deep chain, so a normal
	// guest visits tens. It exists for a pathological or hostile hierarchy, the guest
	// being the threat model, and truncation is reported rather than swallowed.
	//
	// Safe to lower; the THAW's bound (in envd) is separate and must never be lowered,
	// because a thaw that truncates below what a freeze covered strands a guest frozen.
	FreezeGuestHierarchyMaxCgroupsFlag = NewIntFlag("freeze-guest-hierarchy-max-cgroups", 512)

	// CollapseEnvdHeapFlag makes the orchestrator ask envd to collapse its own
	// anonymous heap into 2 MiB hugepages just before pause, reducing the number
	// of distinct frames envd faults on resume. Off by default; rolled out via LD.
	CollapseEnvdHeapFlag = NewBoolFlag("collapse-envd-heap", false)

	// CollapseEnvdHeapTimeoutMsFlag bounds the pre-pause POST /collapse call, in
	// milliseconds. Collapsing migrates envd's scattered heap pages into
	// hugepages, which is heavier than the freeze sysfs write, so it gets a
	// larger, independent budget. Collapse is best-effort: a cut-short run still
	// helps, so this can be tuned per rollout without redeploying. The fallback
	// (returned when LD is unavailable or the flag is unset) is the default.
	CollapseEnvdHeapTimeoutMsFlag = NewIntFlag("collapse-envd-heap-timeout-ms", 10000) // 10s in milliseconds

	// FreezeUserCgroupTimeoutMsFlag bounds the pre-pause freeze call that
	// FreezeUserCgroupFlag enables, in milliseconds. The call waits for the workload's
	// cgroups to actually stop, and quiesce latency is the guest's cost, not ours: a
	// cgroup whose tasks are idle confirms in single-digit milliseconds, one in
	// continuous I/O has been measured taking seconds. The default keeps the historical
	// budget; raise it once the freeze metrics show how often it is the binding
	// constraint. envd is told to confirm within a margin of this, so one knob moves
	// both halves.
	//
	// The value bounds pause latency directly: a sandbox that will not quiesce holds the
	// pause for this long before we give up on it. That is the cost being traded against
	// snapshotting a running workload, and it is why raising it wants evidence.
	//
	// Effective ceiling of 10s. The shared sandbox HTTP client caps every request at that,
	// so a larger value here is silently truncated to it while the failure is still
	// recorded against the value set here. Tracked separately; until it is lifted, a value
	// above 10s buys nothing and makes the timeout metric misleading.
	FreezeUserCgroupTimeoutMsFlag = NewIntFlag("freeze-user-cgroup-timeout-ms", 2000) // 2s in milliseconds

	// VolumeFallbackToUnmatchedNodesFlag allows volume operations to fall back to
	// orchestrator nodes that don't advertise the volume's type label when every
	// labeled node fails with a retryable error. This is a transitional flag for
	// the volume-label migration: once every node is labeled, unlabeled nodes will
	// fail 100% of the time, so this should be turned off and removed afterwards.
	VolumeFallbackToUnmatchedNodesFlag = NewBoolFlag("volume-fallback-to-unmatched-nodes", true)

	// SandboxVolumeLabelBasedSchedulingFlag enables filtering orchestrator nodes
	// based on the volume types required by the sandbox. When enabled, labels
	// like "persistent-volume-type=nfs" are added to the required node labels
	// for sandbox placement.
	SandboxVolumeLabelBasedSchedulingFlag = NewBoolFlag("sandbox-volume-label-based-scheduling", false)

	NetworkTransformRulesFlag = NewBoolFlag("network-transform-rules", env.IsDevelopment())
	MaxNetworkRuleDomains     = NewIntFlag("max-network-rule-domains", 10)

	BYOPProxyEnabledFlag = NewBoolFlag("byop-proxy-enabled", env.IsDevelopment())

	// EgressProxyInterceptTracingFlag turns on connection, request and upstream
	// spans for a sandbox's TLS-intercepted egress. Target the sandbox or team
	// context; the fallback keeps interception at its existing transform span.
	EgressProxyInterceptTracingFlag = NewBoolFlag("egress-proxy-intercept-tracing", false)

	// SandboxIamTokensFlag gates the sandbox IAM workload token configuration
	// (iam.tokens) per team during beta.
	SandboxIamTokensFlag = NewBoolFlag("enable-sandbox-iam-tokens", env.IsDevelopment())

	// CustomerSecretsFlag gates the customer-facing secret management routes
	// per project during rollout. It falls back to off, so a deployment that
	// cannot reach LaunchDarkly keeps the routes dark.
	CustomerSecretsFlag = NewBoolFlag("customer-secrets", false)

	DisableLegacyTeamMutationsFlag = NewBoolFlag("disable-legacy-team-mutations", false)

	// V4HeaderForUncompressedFlag forces the V4 header layout on uncompressed
	// uploads. Independent of compress-config: it changes the header format,
	// not whether data is compressed.
	V4HeaderForUncompressedFlag = NewBoolFlag("v4-header-for-uncompressed", false)

	// HeaderV5WriteFlag makes Pause emit V5 headers. When enabled it also
	// supersedes V4HeaderForUncompressedFlag for uncompressed uploads.
	HeaderV5WriteFlag = NewBoolFlag("header-v5-write", false)

	// ResumeOriginNodeRemapFlag enables repointing a snapshot's origin_node_id to
	// the fallback node a resume timed out on. The node's local cache is warming
	// from the in-progress snapshot pull, so pinning the retry to it avoids
	// re-pulling the snapshot onto yet another node.
	ResumeOriginNodeRemapFlag = NewBoolFlag("resume-origin-node-remap", false)

	// ExpirationIndexHealerFlag enables the API's Redis expiration index healer
	// loop, which re-adds sandboxes missing from the global expiration ZSET
	// (a missing member is never seen by the evictor and would live forever).
	// Checked on every heal tick, so it can be toggled without a redeploy.
	// On by default; acts as a kill switch if a heal pass misbehaves.
	ExpirationIndexHealerFlag = NewBoolFlag("expiration-index-healer", true)

	// BuildEnsureFreeDiskSpace grows the rootfs after build steps and before finalize.
	BuildEnsureFreeDiskSpace = NewBoolFlag("build-ensure-free-disk-space", false)
)

// envdTimeoutFallbackMs reads ENVD_TIMEOUT (Go duration string, e.g. "10s")
// and returns milliseconds. Falls back to 10 000 ms when unset or unparseable.
func envdTimeoutFallbackMs() int {
	raw := os.Getenv("ENVD_TIMEOUT")
	if raw == "" {
		return 10_000
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 10_000
	}

	return int(d.Milliseconds())
}

type IntFlag struct {
	name     string
	fallback int
}

func (f IntFlag) Key() string {
	return f.name
}

func (f IntFlag) String() string {
	return f.name
}

func (f IntFlag) Fallback() int {
	return f.fallback
}

func NewIntFlag(name string, fallback int) IntFlag {
	flag := IntFlag{name: name, fallback: fallback}
	builder := launchDarklyOfflineStore.Flag(flag.name).ValueForAll(ldvalue.Int(fallback))
	launchDarklyOfflineStore.Update(builder)

	return flag
}

var (
	MaxSandboxesPerNode = NewIntFlag("max-sandboxes-per-node", 200)
	// The LD keys keep the legacy "gcloud-" prefix, but the limits apply to uploads on all storage providers.
	StorageConcurrentUploadLimit  = NewIntFlag("gcloud-concurrent-upload-limit", 8)
	StorageMaxUploadTasks         = NewIntFlag("gcloud-max-tasks", 16)
	ClickhouseBatcherMaxBatchSize = NewIntFlag("clickhouse-batcher-max-batch-size", 1000)
	ClickhouseBatcherMaxDelay     = NewIntFlag("clickhouse-batcher-max-delay", 1000) // 1s in milliseconds
	ClickhouseBatcherQueueSize    = NewIntFlag("clickhouse-batcher-queue-size", 1000)
	BestOfKSampleSize             = NewIntFlag("best-of-k-sample-size", 3)      // Default K=3
	BestOfKMaxOvercommit          = NewIntFlag("best-of-k-max-overcommit", 400) // Default R=4 (stored as percentage, max over-commit ratio)
	BestOfKAlpha                  = NewIntFlag("best-of-k-alpha", 50)           // Default Alpha=0.5 (stored as percentage for int flag, current usage weight)
	// BestOfKHugepageMemory includes hugepage-pool load in best-of-K scoring
	// (max of CPU and memory). Off = CPU only. A node that reports no pool
	// scores 0.5. Turn on after nodes report a pool.
	BestOfKHugepageMemory       = NewBoolFlag("best-of-k-hugepage-memory", false)
	EnvdInitTimeoutMilliseconds = NewIntFlag("envd-init-request-timeout-milliseconds", 50)         // Timeout for envd init request in milliseconds
	EnvdTimeoutMilliseconds     = NewIntFlag("envd-timeout-milliseconds", envdTimeoutFallbackMs()) // Timeout for waiting for envd on resume; falls back to ENVD_TIMEOUT env var (default 10s)
	// GuestSyncTimeoutMs overrides the mandatory pre-pause guest-sync deadline
	// for filesystem-only snapshots, in milliseconds. 0 (default) derives the
	// timeout from guest RAM; a positive value pins it.
	GuestSyncTimeoutMs = NewIntFlag("guest-sync-timeout-milliseconds", 0)
	// PauseAdmissionGraceMs gates the pause/checkpoint snapshot-admission
	// pre-flight, in milliseconds. Negative (default) disables the pre-flight;
	// 0 probes the parent header's readiness without waiting; a positive value
	// waits up to that long before refusing retryably.
	PauseAdmissionGraceMs = NewIntFlag("pause-admission-grace-milliseconds", -1)
	// PauseRefusalRestoreFlag gates the API-side restore of a retryably
	// refused pause: record kept, routing re-registered, state back to
	// Running. Off (default), a refused pause still ends today's way — the
	// record is removed, the live sandbox is reaped as an orphan shortly
	// after, and the pause endpoint answers today's generic error rather than
	// a 503 whose retry could not succeed.
	PauseRefusalRestoreFlag = NewBoolFlag("pause-refusal-restore", false)
	// OrchestratorRoutingPublishFlag makes the orchestrator write the sandbox
	// routing record (sandbox:routing:{id}) on MarkRunning and delete it on
	// MarkStopping. Runs next to the API-owned sandbox:catalog:{id} record.
	// On by default: the write is best-effort and nothing reads the record
	// until OrchestratorRoutingPrioritizedFlag is on. Turn off in LaunchDarkly
	// to stop the extra Redis write.
	OrchestratorRoutingPublishFlag = NewBoolFlag("orchestrator-routing-publish", true)
	// OrchestratorRoutingPrioritizedFlag makes client-proxy resolve the node
	// from the orchestrator-owned sandbox:routing:{id} record instead of the
	// API-owned sandbox:catalog:{id} record. Turn on only after
	// OrchestratorRoutingPublishFlag has been on for one max sandbox length.
	OrchestratorRoutingPrioritizedFlag = NewBoolFlag("orchestrator-routing-prioritized", false)
	MaxCacheWriterConcurrencyFlag      = NewIntFlag("max-cache-writer-concurrency", 10)

	// BuildCacheMaxUsagePercentage the maximum percentage of the cache disk storage
	// that can be used before the cache starts evicting items.
	BuildCacheMaxUsagePercentage = NewIntFlag("build-cache-max-usage-percentage", 85)
	BuildProvisionVersion        = NewIntFlag("build-provision-version", 0)

	// BuildEnvdMemoryProtection, when enabled at build time, renders a fixed
	// memory.min/memory.low request for envd, the same on every template, and
	// installs a system.slice drop-in requesting the same, so the kernel grants
	// envd's protection instead of prorating it to nothing. Evaluated once per
	// build with the template and team contexts. When on, the request it
	// renders enters the base-layer cache key of every build that renders the
	// rootfs files, so such a build never reuses a layer built off, and off
	// keeps the key those layers are already stored under wherever
	// BuildProvisionVersion is set explicitly (a build from another template
	// inherits its parent's layer and files, whatever its own flag). Where that
	// flag is at its fallback the provision version is a hash of the baked
	// files, which the drop-in template changes once, so those deployments
	// rebuild their base layers on landing whatever this flag says.
	// Disabled by default: the unit keeps the request it carries today.
	BuildEnvdMemoryProtection = NewBoolFlag("build-envd-memory-protection", false)

	// NBDConnectionsPerDevice the number of NBD socket connections per device
	NBDConnectionsPerDevice = NewIntFlag("nbd-connections-per-device", 1)

	// NBDAsyncWriteZeroesFlag, when enabled, handles NBD WRITE_ZEROES/TRIM
	// commands in a goroutine instead of inline on the dispatch read loop.
	// Inline handling can stall the read loop via head-of-line blocking on the
	// shared write lock (when a reply writer is blocked on a full socket send
	// buffer), which makes the kernel time out the NBD connection and surfaces
	// as guest I/O errors. Disabled by default.
	NBDAsyncWriteZeroesFlag = NewBoolFlag("nbd-async-write-zeroes", false)

	// MemoryPrefetchMaxFetchWorkers is the maximum number of parallel fetch workers per sandbox for memory prefetching.
	// Fetching is I/O bound so we can have more parallelism.
	MemoryPrefetchMaxFetchWorkers = NewIntFlag("memory-prefetch-max-fetch-workers", 16)

	// MemoryPrefetchMaxCopyWorkers is the maximum number of parallel copy workers per sandbox for memory prefetching.
	// Copy uses uffd syscalls, so we limit parallelism to avoid overwhelming the system.
	MemoryPrefetchMaxCopyWorkers = NewIntFlag("memory-prefetch-max-copy-workers", 8)

	// MemoryPrefetchCoalesceMaxMB caps how many contiguous prefetch blocks are
	// merged into a single source.Slice fetch (in MiB of extent size). 0
	// disables coalescing: every block is fetched individually, matching
	// today's behavior. The copy phase is unaffected either way — it always
	// installs one page at a time, because Userfaultfd.Prefault installs a
	// single page per call.
	MemoryPrefetchCoalesceMaxMB = NewIntFlag("memory-prefetch-coalesce-max-mb", 0)

	// ResumePrefetchSourceFlag selects which trace the resume prefetcher
	// replays:
	//   "init"     — only the build-time / harvested read-hot init trace
	//                (meta.Prefetch.Memory), prefaulted. Preserves today's
	//                behavior, so this is the default and a no-op-equivalent.
	//   "last-cycle" — only the sandbox's own pause diff (the pages the last
	//                resume→pause cycle wrote), derived from the memfile header
	//                and replayed fetch-only.
	//   "both"     — init first (prefaulted), then last-cycle (fetch-only) behind
	//                a barrier, so the large last-cycle fetch stays off the
	//                resume-critical path.
	//   "off"      — kill switch, no resume prefetch.
	// Unknown values fall back to "init".
	ResumePrefetchSourceFlag = NewStringFlag("resume-prefetch-source", "init")

	// ResumeLastCyclePrefetchMaxMiBFlag caps how much of the last-cycle diff a single
	// resume prefetches, in MiB. -1 (the default, negative = no limit per the
	// codebase convention) is uncapped; the recorded diff is small by
	// construction, so this exists to throttle the heavy-churn tail against the
	// shared object-store pool without a redeploy. A non-negative N keeps the
	// first N MiB of blocks in offset order and leaves the rest to demand-fault.
	ResumeLastCyclePrefetchMaxMiBFlag = NewIntFlag("resume-last-cycle-prefetch-max-mib", -1)

	// PauseResumePrefetchHarvestFlag makes the orchestrator, after a pause
	// snapshot is durable, run a throwaway warm resume of the just-written
	// artifact (driven by envd /init, workload frozen, egress denied) to record
	// the resume page-fault trace and turn it into a prefetch mapping. Off by
	// default; the harvest is best-effort and never affects the pause result.
	PauseResumePrefetchHarvestFlag = NewBoolFlag("pause-resume-prefetch-harvest", false)

	// PauseResumePrefetchConsumeFlag controls whether a harvested mapping is
	// persisted into the pause artifact metadata (and therefore replayed on the
	// customer's next resume). When off, the harvest still runs and emits its
	// trace-size metrics but does NOT write the mapping, so resumes are
	// unaffected — letting us validate harvest behaviour with no customer-visible
	// change before enabling prefetch on resume. Off by default.
	PauseResumePrefetchConsumeFlag = NewBoolFlag("pause-resume-prefetch-consume", false)

	// PauseResumePrefetchHarvestTimeoutMsFlag bounds the throwaway harvest resume
	// (slot-hold cap), in milliseconds. The harvest is best-effort: a cut-short
	// run is discarded (the build is simply re-harvested on its next pause), so
	// erring short is cheap. A normal warm harvest completes in a few seconds; the
	// default leaves headroom for a large warm resume to fully drain while keeping
	// the worst-case slot hold modest. Tunable per rollout via LD; the fallback
	// (returned when LD is unavailable or the flag is unset) is the default.
	PauseResumePrefetchHarvestTimeoutMsFlag = NewIntFlag("pause-resume-prefetch-harvest-timeout-ms", 15000) // 15s

	// TCPFirewallMaxConnectionsPerSandbox is the maximum number of concurrent TCP firewall
	// connections allowed per sandbox. Negative means no limit.
	TCPFirewallMaxConnectionsPerSandbox = NewIntFlag("tcpfirewall-max-connections-per-sandbox", -1)

	// SandboxMaxIncomingConnections is the maximum number of concurrent HTTP proxy
	// connections allowed per sandbox. Negative means no limit.
	SandboxMaxIncomingConnections = NewIntFlag("sandbox-max-incoming-connections", -1)

	// BuildBaseRootfsSizeLimitMB is the maximum size of the base rootfs filesystem created from the OCI image, in MB.
	BuildBaseRootfsSizeLimitMB = NewIntFlag("build-base-rootfs-size-limit-mb", 25000)

	// MinAutoResumeTimeoutSeconds is the minimum auto-resume timeout in seconds.
	// This prevents thrashing from very short timeouts.
	MinAutoResumeTimeoutSeconds = NewIntFlag("minimum-autoresume-timeout", 300)

	// BuildReservedDiskSpaceMB is the amount of disk space in MB reserved for root on the guest filesystem.
	// Reserved blocks are only usable by root (uid 0), protecting the guest OS from disk-full conditions.
	BuildReservedDiskSpaceMB = NewIntFlag("build-reserved-disk-space-mb", 256)

	// MaxStartingInstancesPerNode limits concurrent sandbox start/resume operations on a single orchestrator node.
	// Must be > 0.
	MaxStartingInstancesPerNode = NewIntFlag("max-starting-instances-per-node", 3)

	// MaxConcurrentTemplateBuilds limits concurrent template builds on a single
	// template-manager node. Non-positive values disable the cap.
	MaxConcurrentTemplateBuilds = NewIntFlag("max-concurrent-template-builds", -1)

	// MaxConcurrentEvictions caps the number of sandbox evictions that can run
	// in parallel per API instance. Excess items remain expired in the store
	// and are picked up by the next eviction tick. Must be > 0; non-positive
	// values are ignored at refresh time.
	MaxConcurrentEvictions = NewIntFlag("max-concurrent-evictions", 256)

	// AutoPauseOverstayBudgetMs is how long the evictor keeps retrying a memory
	// snapshot for an expired auto-pause sandbox the node refuses, counted from
	// the first refusal of the current episode, before it requests a
	// filesystem-only snapshot instead (no memory parent, so it cannot be
	// refused for one). Same sign convention as PauseAdmissionGraceMs:
	// negative = never degrade (retry for as long as the node refuses), 0 = no
	// retries (degrade at the first refusal), positive = the budget. Only ever
	// consulted on a refusal, and refusals only reach the evictor with
	// PauseRefusalRestoreFlag on, so it is inert until that flag is.
	AutoPauseOverstayBudgetMs = NewIntFlag("auto-pause-overstay-budget-milliseconds", 120000)

	// MaxConcurrentSnapshotUpserts limits concurrent UpsertSnapshot calls (pause + snapshot template paths).
	// 0 or negative disables throttling (unlimited concurrency).
	MaxConcurrentSnapshotUpserts = NewIntFlag("max-concurrent-snapshot-upserts", 0)
	// MaxConcurrentSandboxListQueries limits concurrent GetSnapshotsWithCursor calls in the sandbox list path.
	// 0 or negative disables throttling (unlimited concurrency).
	MaxConcurrentSandboxListQueries = NewIntFlag("max-concurrent-sandbox-list-queries", 0)
	// MaxConcurrentSnapshotBuildQueries limits concurrent GetSnapshotBuilds calls (e.g. sandbox delete).
	// 0 or negative disables throttling (unlimited concurrency).
	MaxConcurrentSnapshotBuildQueries = NewIntFlag("max-concurrent-snapshot-build-queries", 0)

	MinChunkerReadSizeKB = NewIntFlag("min-chunker-read-size-kb", 16)

	// MaxParallelBuildReadSegments limits concurrent backing reads within one fragmented build read.
	// 1 or lower keeps the existing serial path.
	MaxParallelBuildReadSegments = NewIntFlag("max-parallel-build-read-segments", 1)
)

// ReclaimConfigFlag holds per-step caps in milliseconds for the pre-pause
// reclaim chain. Missing/zero/negative values disable the step.
// Example: {"sync":500,"drop_caches":200,"compact_memory":1000,"fstrim":500}
var ReclaimConfigFlag = NewJSONFlag("guest-pause-reclaim", ldvalue.Null())

// FreePageHintingConfig controls virtio-balloon free-page-hinting.
// "enabled" configures FreePageHinting=true on the balloon at install time
// (kernel-side eligibility is targeted separately via the LD context — the
// race fixed in https://lore.kernel.org/lkml/20240429125100.7393-1-david@redhat.com/
// is on the hinting flow, gated by the per-use-case timeouts below).
// "pause"/"build" are pre-pause drain timeouts in ms keyed by SnapshotUseCase;
// missing/zero/negative disables the drain for that use case.
// Example: {"enabled": true, "pause": 500, "build": 0}
var FreePageHintingConfig = NewJSONFlag("free-page-hinting-config", ldvalue.Null())

// IsFreePageHintingEnabled reports whether FPH should be configured on the
// balloon at install time.
func IsFreePageHintingEnabled(ctx context.Context, ff *Client, contexts ...ldcontext.Context) bool {
	return ff.JSONFlag(ctx, FreePageHintingConfig, contexts...).GetByKey("enabled").BoolValue()
}

// GetFreePageHintingTimeout returns the pre-pause FPH drain timeout for the
// given SnapshotUseCase. Zero means disabled.
func GetFreePageHintingTimeout(ctx context.Context, ff *Client, useCase string, contexts ...ldcontext.Context) time.Duration {
	ms := ff.JSONFlag(ctx, FreePageHintingConfig, contexts...).GetByKey(useCase).IntValue()
	if ms <= 0 {
		return 0
	}

	return time.Duration(ms) * time.Millisecond
}

type ReclaimConfig struct {
	Sync          time.Duration
	DropCaches    time.Duration
	CompactMemory time.Duration
	Fstrim        time.Duration
}

func GetReclaimConfig(ctx context.Context, ff *Client, contexts ...ldcontext.Context) ReclaimConfig {
	v := ff.JSONFlag(ctx, ReclaimConfigFlag, contexts...)
	ms := func(key string) time.Duration {
		return time.Duration(v.GetByKey(key).IntValue()) * time.Millisecond
	}

	return ReclaimConfig{
		Sync:          ms("sync"),
		DropCaches:    ms("drop_caches"),
		CompactMemory: ms("compact_memory"),
		Fstrim:        ms("fstrim"),
	}
}

type StringFlag struct {
	name     string
	fallback string
}

func (f StringFlag) Key() string {
	return f.name
}

func (f StringFlag) String() string {
	return f.name
}

func (f StringFlag) Fallback() string {
	return f.fallback
}

func NewStringFlag(name string, fallback string) StringFlag {
	flag := StringFlag{name: name, fallback: fallback}
	builder := launchDarklyOfflineStore.Flag(flag.name).ValueForAll(ldvalue.String(fallback))
	launchDarklyOfflineStore.Update(builder)

	return flag
}

const (
	DefaultKernelVersion = "vmlinux-6.1.158"

	// DefaultEnvdVersion is the envd new template builds bake when neither the
	// build-envd-version flag nor DEFAULT_ENVD_VERSION says otherwise:
	// "promoted" selects the node-local promoted binary (HOST_ENVD_PATH), the
	// behavior every build has always had, so deployments without
	// LaunchDarkly (dev, self-host) are unaffected.
	DefaultEnvdVersion = "promoted"
)

// The Firecracker version per release line: legacy lines pin
// last-tag_short-SHA dev builds; e2b lines (vX.Y-<e2b-major>) pin releases
// published by the Publish fc-versions workflow.
// TODO: The short tag here has only 7 characters — the one from our build pipeline will likely have exactly 8 so this will break.
const (
	DefaultFirecrackerV1_10Version = "v1.10.1_30cbb07"
	DefaultFirecrackerV1_12Version = "v1.12.1_210cbac"
	DefaultFirecrackerV1_14Version = "v1.14.1_431f1fc"
	// The v1.14-0 release line. 0.2.0 introduces the in-place checkpoint's
	// balloon reporting-pause API; filesystem-only snapshots ship with every
	// e2b release from 0.1.0 — the per-feature floors live in fcversion.
	DefaultFirecrackerV1_14_0Version = "v1.14-0.2.0"
	// New template builds get the current release; existing builds keep
	// resolving within their own line below (cross-line upgrades are an
	// operator decision via the firecracker-versions flag, never a baked
	// default — the map invariant key == LDKey(value) enforces it).
	DefaultFirecrackerVersion = DefaultFirecrackerV1_14_0Version
)

var FirecrackerVersionMap = map[string]string{
	"v1.10":   DefaultFirecrackerV1_10Version,
	"v1.12":   DefaultFirecrackerV1_12Version,
	"v1.14":   DefaultFirecrackerV1_14Version,
	"v1.14-0": DefaultFirecrackerV1_14_0Version,
}

// BuildIoEngine Sync is used by default as there seems to be a bad interaction between Async and a lot of io operations.
var (
	BuildFirecrackerVersion = NewStringFlag("build-firecracker-version", env.GetEnv("DEFAULT_FIRECRACKER_VERSION", DefaultFirecrackerVersion))
	BuildKernelVersion      = NewStringFlag("build-kernel-version", env.GetEnv("DEFAULT_KERNEL_VERSION", DefaultKernelVersion))
	// BuildEnvdVersion selects which staged envd binary a template build bakes
	// into the rootfs — the envd counterpart of BuildKernelVersion /
	// BuildFirecrackerVersion, same default mechanism. "promoted" (the
	// fallback) is the node-local promoted binary; a concrete version id
	// (e.g. v0.7.0, or a git SHA while those age out) selects a staged binary
	// (the flat envd.<id> sibling or the release bucket's <id>/envd layout,
	// see build/core/envd.ResolveBuildBinary). A pinned target that is not
	// staged FAILS the build rather than silently baking a different envd —
	// feature gates key on the baked version, so a silent substitute
	// misgates. The build-site LD context carries template/team, so cohort
	// canaries come for free.
	BuildEnvdVersion = NewStringFlag("build-envd-version", env.GetEnv("DEFAULT_ENVD_VERSION", DefaultEnvdVersion))
	BuildIoEngine    = NewStringFlag("build-io-engine", "Sync")

	// BuildKernelCmdlineArgs supplies extra guest kernel command line parameters at
	// template build time, keyed on team, as a command line fragment:
	//
	//	psi=1
	//	psi=1 nokaslr
	//
	// Empty (the default) is the default command line, so a team that is not targeted is
	// unaffected. Adding a parameter is a flag edit — no
	// orchestrator change and no deploy.
	//
	// Parsed the way the kernel parses a command line: whitespace separates parameters,
	// the first '=' separates a name from its value, and a parameter with no '=' has an
	// empty value. The orchestrator rejects the whole fragment if it sets a parameter it
	// reserves (init, clocksource, root, ip, console, rootflags, panic, reboot, loglevel,
	// quiet, selinux — see packages/orchestrator/pkg/sandbox/fc), falling back to the default
	// command line rather than failing the build. The parsed parameters are recorded in
	// the template's metadata and replayed when a filesystem-only snapshot cold-boots, so
	// a snapshot keeps booting the way it was built even if this flag later changes.
	BuildKernelCmdlineArgs = NewStringFlag("build-kernel-cmdline-args", "")

	// EnvdUpgradeTargetFlag drives the resume-time envd live-upgrade.
	// Multivariate string:
	//   "off"        (fallback) — no upgrade; dev has no LD so this is inert & safe.
	//   "promoted"   — track the node-local promoted envd (HOST_ENVD_PATH); upgrade
	//                  whenever it differs from the sandbox's built-with version
	//                  (no per-publish flag edits needed).
	//   "<version>"  — pin a specific staged binary, in either layout beside the
	//                  promoted one: the flat /fc-envd/envd.<version> sibling or
	//                  the release bucket's /fc-envd/<version>/envd directory.
	//                  Release names may carry dots and hyphens (v0.7.0,
	//                  v0.8.0-rc1); legacy git-SHA suffixes keep resolving while
	//                  the old envd.<sha> objects age out.
	// The resume-site LD context carries envd-version/team/template, so %-ramp
	// and cohort canaries come for free. The fallback is env-overridable
	// (ENVD_UPGRADE_TARGET) so it can be exercised where there is no LD (dev),
	// mirroring build-firecracker-version's DEFAULT_FIRECRACKER_VERSION.
	EnvdUpgradeTargetFlag = NewStringFlag("envd-upgrade-target", env.GetEnv("ENVD_UPGRADE_TARGET", "off"))
	// EnvdOfflineUpgradeTargetFlag drives the OFFLINE envd upgrade of a
	// filesystem-only snapshot: at cold-boot resume the rootfs binary is rewritten
	// (jailed debugfs) before the guest boots, reaching envd too old to self-upgrade
	// (< MinEnvdVersionForUpgrade). Same value grammar and resolver as
	// EnvdUpgradeTargetFlag ("off" / "promoted" / "<version>"); a SEPARATE flag so
	// the newer/riskier offline mechanism ramps independently of the live path. The
	// fallback is env-overridable (ENVD_OFFLINE_UPGRADE_TARGET) for dev, where there
	// is no LD. Default off.
	EnvdOfflineUpgradeTargetFlag = NewStringFlag("envd-offline-upgrade-target", env.GetEnv("ENVD_OFFLINE_UPGRADE_TARGET", "off"))

	// EnvdBinaryCacheFlag serves the envd upgrade paths from a node-local copy of
	// the host envd binary instead of re-reading it from the gcsfuse mount: the
	// version probe, the live delivery to a running envd, and the offline rootfs
	// swap's staging copy. Off keeps every one of those reading the source
	// directly, which is the behaviour before this flag existed.
	//
	// It gates a caching optimisation, not a new mechanism, so the risk it
	// isolates is the cache serving a stale binary — an upgrade delivering
	// something other than the version it recorded. That is recoverable (the
	// version arbiter refuses to record a success it cannot confirm) but worth a
	// kill switch that needs no deploy. Falls back on in development, where there
	// is no LD and the path would otherwise never be exercised.
	// The fallback is env-overridable (ENVD_BINARY_CACHE) so the cached and
	// uncached paths can be compared on a cluster with no LaunchDarkly, where the
	// alternative is two binary deploys.
	//
	// Ramp it on a NODE-SCOPED key only: deployment (the domain, so effectively the
	// cluster), instance-group, or orchestrator (the node, carrying its build as an
	// attribute). Those are what the orchestrator attaches to every evaluation, so
	// all three read sites agree on them. A sandbox- or team-keyed rule is absent
	// at the startup warm, which runs before any sandbox exists -- such a rule
	// still gates both upgrade paths but silently disables the pre-warm, so every
	// node's first eligible resume defers. A template-keyed rule matches the
	// offline path only, since the live path carries the template as an attribute
	// of the sandbox context rather than as a context of its own. None of that is a
	// limitation worth lifting: the subject of this flag is a cache shared by every
	// sandbox on the node, so the node, its group and its cluster are the cohorts
	// that mean anything.
	EnvdBinaryCacheFlag = NewBoolFlag("envd-binary-cache", envBoolOr("ENVD_BINARY_CACHE", env.IsDevelopment()))
	// FsOnlyResumeCPUModelFlag restricts where a filesystem-only snapshot may be
	// resumed: placement keeps only nodes reporting this CPU model, on top of the
	// build-compatibility rule every sandbox is already subject to. The value is
	// a bare CPU model as /proc/cpuinfo reports it — machineinfo.IceLakeModel is
	// "106" (n2), machineinfo.EmeraldRapidsModel is "207" (n4).
	//
	// Empty (the default) turns the restriction off, leaving filesystem-only
	// snapshots on the cross-generation rule a memory restore uses. A deployment
	// with no LaunchDarkly therefore keeps placing them exactly as before, rather
	// than needing an LD rule to unpin itself. Memory snapshots never read it.
	FsOnlyResumeCPUModelFlag = NewStringFlag("fs-only-resume-cpu-model", "")

	DefaultPersistentVolumeType = NewStringFlag("default-persistent-volume-type", "")
	BuildNodeInfo               = NewJSONFlag("preferred-build-node", ldvalue.Null())
	FirecrackerVersions         = NewJSONFlag("firecracker-versions", ldvalue.FromJSONMarshal(FirecrackerVersionMap))

	// ClickhouseReadEndpointFlag selects which ClickHouse DSN to use for reads.
	// "" (empty) → singular CLICKHOUSE_CONNECTION_STRING (self-managed default).
	// "0", "1", ... → index into CLICKHOUSE_CONNECTION_STRINGS
	ClickhouseReadEndpointFlag = NewStringFlag("clickhouse-read-endpoint", "")

	// ClickhouseWriteFanoutFlag: when false, drop writes to alternate
	// ClickHouse endpoints (CLICKHOUSE_CONNECTION_STRINGS). Default DSN
	// is unaffected.
	ClickhouseWriteFanoutFlag = NewBoolFlag("clickhouse-write-fanout", false)

	// ClickhouseHostStatsAsyncInsertFlag sets async_insert=1 on every
	// sandbox_host_stats flush. Each orchestrator flushes its own small batch,
	// so without it the server writes one tiny part per node per flush; with
	// it the server buffers those inserts and writes one part per buffer flush.
	// Used as the fallback when the shared async flag cannot be evaluated.
	ClickhouseHostStatsAsyncInsertFlag = NewBoolFlag("clickhouse-host-stats-async-insert", false)

	// ClickhouseAsyncInsertFlag shares the existing async key across named writers.
	// Do not seed the offline store: each writer supplies its own legacy fallback.
	ClickhouseAsyncInsertFlag        = BoolFlag{name: "clickhouse-async-insert", fallback: true}
	ClickhouseWaitForAsyncInsertFlag = NewBoolFlag("clickhouse-wait-for-async-insert", true)
)

// LogsWriteConfigFlag controls where sandbox/external logs are written, so
// operators can retarget log destinations from LaunchDarkly without a redeploy.
//
// Shape:
//
//	{
//	  "mode": "primary_only" | "primary_and_shadow",
//	  "primary_url": "http://localhost:30006",
//	  "shadow_urls": ["http://localhost:4321/logs"],
//	  "timeout_ms": 2000,
//	  "max_inflight_shadow_writes": 1024
//	}
//
// Semantics:
//   - null/missing/invalid  -> fall back to the legacy collector address only.
//   - "primary_only"        -> write to primary_url only.
//   - "primary_and_shadow"  -> write to primary_url; fire-and-forget shadow_urls
//     (shadow failures never affect the primary result).
//   - Empty primary_url in a non-disabled mode is invalid -> legacy fallback.
//   - shadow_urls must be an array of <= maxLogWriteShadowURLs safe string URLs.
//   - timeout_ms <= 0 or too large is clamped to a safe range.
//   - max_inflight_shadow_writes <= 0 defaults to defaultMaxInflightShadowWrites.
//   - Only http URLs pointing at local/private hosts or allowed internal DNS
//     suffixes are allowed; anything else is rejected and the whole config falls
//     back to legacy.
//
// The fallback collector address is a runtime env value the flag cannot know,
// so the default is Null() and the code substitutes the legacy address.
var LogsWriteConfigFlag = NewJSONFlag("logs-write-config", ldvalue.Null())

// LogsReadConfigFlag selects the backend used to read sandbox/build logs.
// false reads from Loki (unchanged behavior); true reads from the ClickHouse
// sandbox_logs table. The fallback comes from LOGS_READ_CONFIG and defaults to
// false, so a deployment without LaunchDarkly, or one whose LaunchDarkly has
// no value for the flag, reads where the variable says; a LaunchDarkly value
// wins when there is one.
var LogsReadConfigFlag = NewBoolFlag("logs-read-config", logsReadConfigFallback())

// logsReadConfigFallback reads LOGS_READ_CONFIG. Unset or unparseable means
// false, the way envdTimeoutFallbackMs treats ENVD_TIMEOUT.
func logsReadConfigFallback() bool {
	return parseLogsReadConfig(os.Getenv("LOGS_READ_CONFIG"))
}

func parseLogsReadConfig(raw string) bool {
	value, err := strconv.ParseBool(strings.TrimSpace(raw))

	return err == nil && value
}

// Log write routing modes for LogsWriteConfigFlag.
const (
	LogsWriteModePrimaryOnly      = "primary_only"
	LogsWriteModePrimaryAndShadow = "primary_and_shadow"
)

const (
	// defaultLogWriteTimeout is used when timeout_ms is missing/invalid.
	defaultLogWriteTimeout = 2000 * time.Millisecond
	// maxLogWriteTimeout caps operator-provided timeouts.
	maxLogWriteTimeout = 10000 * time.Millisecond
	// maxLogWriteShadowURLs caps fanout configured from LaunchDarkly.
	maxLogWriteShadowURLs = 4
	// defaultMaxInflightShadowWrites is used when max_inflight_shadow_writes is missing/invalid.
	defaultMaxInflightShadowWrites = 1024
)

var (
	logRoutingMeter                = otel.Meter("github.com/e2b-dev/infra/packages/shared/pkg/featureflags")
	logWriteConfigResolutionMetric = mustLogRoutingCounter(
		"log_write_config_resolution_count",
		"Number of logs-write-config resolutions by outcome and fallback reason",
	)
)

func mustLogRoutingCounter(name, description string) metric.Int64Counter {
	counter, err := logRoutingMeter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

// LogWriteConfig is the resolved, validated log write routing configuration.
// The zero value (PrimaryURL set by the resolver) preserves legacy behavior.
type LogWriteConfig struct {
	// PrimaryURL is the synchronous, success-controlling destination.
	PrimaryURL string
	// ShadowURLs are best-effort, fire-and-forget destinations.
	ShadowURLs []string
	// Timeout bounds each individual log write request.
	Timeout time.Duration
	// MaxInflightShadowWrites bounds concurrent best-effort shadow writes.
	MaxInflightShadowWrites int64
}

// ResolveLogWriteConfig reads LogsWriteConfigFlag and returns a validated
// LogWriteConfig. On any missing/malformed/unsafe input it falls back to
// writing only to fallbackURL (today's behavior).
func ResolveLogWriteConfig(ctx context.Context, ff *Client, fallbackURL string, contexts ...ldcontext.Context) LogWriteConfig {
	// The legacy fallback (flag null/invalid) preserves pre-flag behavior: it
	// leaves Timeout at 0 so callers skip the per-request WithTimeout and rely
	// solely on the HTTP client's own timeout, exactly as before this flag
	// existed. defaultLogWriteTimeout only applies to explicitly-configured
	// flags with a missing/invalid timeout_ms.
	legacy := LogWriteConfig{
		PrimaryURL:              strings.TrimSpace(fallbackURL),
		Timeout:                 0,
		MaxInflightShadowWrites: defaultMaxInflightShadowWrites,
	}

	if ff == nil {
		recordLogWriteConfigResolution(ctx, "legacy", "nil_client", "")

		return legacy
	}

	value := ff.JSONFlag(ctx, LogsWriteConfigFlag, contexts...)
	if value.IsNull() {
		recordLogWriteConfigResolution(ctx, "legacy", "null", "")

		return legacy
	}
	if value.Type() != ldvalue.ObjectType {
		recordLogWriteConfigResolution(ctx, "legacy", "non_object", "")

		return legacy
	}

	modeValue := value.GetByKey("mode")
	if modeValue.Type() != ldvalue.StringType {
		recordLogWriteConfigResolution(ctx, "legacy", "mode_not_string", "")

		return legacy
	}
	mode := strings.TrimSpace(modeValue.StringValue())
	switch mode {
	case LogsWriteModePrimaryOnly, LogsWriteModePrimaryAndShadow:
		// handled below
	default:
		// unknown/missing mode -> legacy fallback
		recordLogWriteConfigResolution(ctx, "legacy", "unknown_mode", mode)

		return legacy
	}

	primaryValue := value.GetByKey("primary_url")
	if primaryValue.Type() != ldvalue.StringType {
		recordLogWriteConfigResolution(ctx, "legacy", "primary_not_string", mode)

		return legacy
	}
	primary := strings.TrimSpace(primaryValue.StringValue())
	if !isSafeLogURL(primary) {
		recordLogWriteConfigResolution(ctx, "legacy", "unsafe_primary", mode)

		return legacy
	}

	var shadows []string
	if mode == LogsWriteModePrimaryAndShadow {
		raw := value.GetByKey("shadow_urls")
		if !raw.IsNull() {
			if raw.Type() != ldvalue.ArrayType {
				recordLogWriteConfigResolution(ctx, "legacy", "shadow_not_array", mode)

				return legacy
			}
			if raw.Count() > maxLogWriteShadowURLs {
				recordLogWriteConfigResolution(ctx, "legacy", "too_many_shadows", mode)

				return legacy
			}

			seen := map[string]struct{}{primary: {}}
			for i := range raw.Count() {
				item := raw.GetByIndex(i)
				if item.Type() != ldvalue.StringType {
					recordLogWriteConfigResolution(ctx, "legacy", "shadow_not_string", mode)

					return legacy
				}
				u := strings.TrimSpace(item.StringValue())
				// An unsafe shadow URL invalidates the whole config: fail safe to
				// legacy rather than silently exfiltrating to an external host.
				if !isSafeLogURL(u) {
					recordLogWriteConfigResolution(ctx, "legacy", "unsafe_shadow", mode)

					return legacy
				}
				if _, ok := seen[u]; ok {
					continue
				}
				seen[u] = struct{}{}
				shadows = append(shadows, u)
			}
		}
	}

	recordLogWriteConfigResolution(ctx, "configured", "", mode)

	return LogWriteConfig{
		PrimaryURL:              primary,
		ShadowURLs:              shadows,
		Timeout:                 clampLogWriteTimeout(value),
		MaxInflightShadowWrites: clampMaxInflightShadowWrites(value),
	}
}

func recordLogWriteConfigResolution(ctx context.Context, outcome, reason, mode string) {
	if logWriteConfigResolutionMetric == nil {
		return
	}

	logWriteConfigResolutionMetric.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
		attribute.String("mode", mode),
	))
}

// clampLogWriteTimeout reads timeout_ms and clamps it to a safe range.
func clampLogWriteTimeout(value ldvalue.Value) time.Duration {
	ms := value.GetByKey("timeout_ms").IntValue()
	if ms <= 0 {
		return defaultLogWriteTimeout
	}

	d := time.Duration(ms) * time.Millisecond
	if d > maxLogWriteTimeout {
		return maxLogWriteTimeout
	}

	return d
}

func clampMaxInflightShadowWrites(value ldvalue.Value) int64 {
	maxInflight := value.GetByKey("max_inflight_shadow_writes").IntValue()
	if maxInflight <= 0 {
		return defaultMaxInflightShadowWrites
	}

	return int64(maxInflight)
}

var allowedLogHostSuffixes = []string{
	".service.consul",
	".consul",
	".svc.cluster.local",
	".svc",
	".local",
	".internal",
}

// isSafeLogURL allows only http URLs pointing at loopback, link-local, private
// IPs, or internal service-discovery DNS suffixes. This keeps log routing on
// local/private infrastructure and prevents exfiltration to arbitrary external
// endpoints via the flag.
func isSafeLogURL(raw string) bool {
	if raw == "" {
		return false
	}

	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return false
	}

	host := u.Hostname()
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}

	hostLower := strings.ToLower(host)
	for _, suffix := range allowedLogHostSuffixes {
		if strings.HasSuffix(hostLower, suffix) {
			return true
		}
	}

	ip := net.ParseIP(host)
	if ip == nil {
		// Non-IP host without an explicitly allowed internal suffix -> reject.
		return false
	}

	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

// Named under orchestrator.* (the resolver's sole caller) so it passes the
// otel collector's include-only metric allow-list.
const firecrackerVersionResolutionMetricName = "orchestrator.firecracker.version_resolution"

var firecrackerVersionResolutionMetric = mustLogRoutingCounter(
	firecrackerVersionResolutionMetricName,
	"Number of firecracker version resolutions by outcome, fallback reason, map key and served version",
)

// The version attribute is set only on hits, where it is a flag-map value.
// The raw stored string never becomes a label: it carries per-build entropy
// that LDKey strips to the release line, so fallbacks record only the key.
func recordFirecrackerResolved(ctx context.Context, key, version string) {
	addFirecrackerResolution(ctx, "resolved", "", key, version)
}

func recordFirecrackerFallback(ctx context.Context, reason, key string) {
	addFirecrackerResolution(ctx, "fallback", reason, key, "")
}

func addFirecrackerResolution(ctx context.Context, outcome, reason, key, version string) {
	if firecrackerVersionResolutionMetric == nil {
		return
	}

	firecrackerVersionResolutionMetric.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
		attribute.String("key", key),
		attribute.String("version", version),
	))
}

// ResolveFirecrackerVersion resolves the firecracker version using the FirecrackerVersions feature flag.
// The stored version's LD key (e.g. "v1.12" for "v1.12.1_210cbac", "v1.14-0"
// for "v1.14-0.1.0") is looked up in the flag map; on parse failure or a
// missing key the stored version is returned unchanged.
func ResolveFirecrackerVersion(ctx context.Context, ff *Client, buildVersion string) string {
	info, err := fcversion.New(buildVersion)
	if err != nil {
		recordFirecrackerFallback(ctx, "parse_error", "")

		return buildVersion
	}

	key, ok := info.LDKey()
	if !ok {
		recordFirecrackerFallback(ctx, "no_ld_key", "")

		return buildVersion
	}

	versions := ff.JSONFlag(ctx, FirecrackerVersions).AsValueMap()

	if resolved, ok := versions.Get(key).AsOptionalString().Get(); ok {
		// An empty map value would blank the binary path fleet-wide; serve
		// the stored version instead and make the misconfiguration loud.
		if resolved == "" {
			recordFirecrackerFallback(ctx, "empty_value", key)

			return buildVersion
		}
		recordFirecrackerResolved(ctx, key, resolved)

		return resolved
	}

	recordFirecrackerFallback(ctx, "key_absent", key)

	return buildVersion
}

// ResolveEnvdUpgrade decides whether a resuming sandbox's envd should be swapped
// for a newer node-local build, per EnvdUpgradeTargetFlag, and returns the local
// path of the target binary ("" = no upgrade). It is the resume-time analog of
// ResolveFirecrackerVersion.
//
// hostEnvdPath is the promoted binary (cfg HostEnvdPath, e.g. /fc-envd/envd);
// versioned binaries live beside it as envd.<version> (legacy uploads:
// envd.<sha>). getVersion resolves a
// binary's baked version (orchestrator's build/core/envd.GetEnvdVersion) — it is
// injected so this shared package does not depend on the orchestrator.
//
// The "should we upgrade?" test compares baked version *strings* (built-with vs
// the target's version). This is sufficient because CLAUDE.md mandates bumping
// packages/envd/pkg/version.go on every behavioral change; if that ever stops
// holding, a same-version binary swap would be skipped and this must switch to
// comparing by git SHA.
// It returns the target binary's path and baked version ("" path = no upgrade),
// plus a reason for the no-upgrade case — off | not_staged | invalid_target |
// getversion_failed | same_version | downgrade, and "" when an upgrade IS returned
// — so the caller can tell a benign no-op (off / same_version) from a
// misconfigured target (not_staged from a bad SHA, a target that is not a bare
// identifier, getversion_failed, a refused downgrade).
func ResolveEnvdUpgrade(
	ctx context.Context,
	target string,
	builtWithVersion string,
	hostEnvdPath string,
	getVersion func(context.Context, string) (string, error),
) (path, version, reason string) {
	return resolveEnvdUpgradePath(ctx, target, builtWithVersion, hostEnvdPath, getVersion)
}

// EnvdUpgradeTarget reads the live-upgrade target flag. Split from ResolveEnvdUpgrade so a
// caller can test the cheap, in-memory gates before the resolver stats up to three paths and
// execs the candidate binary for its version — work a resume that is going to be gated
// anyway should not pay for.
func EnvdUpgradeTarget(ctx context.Context, ff *Client) string {
	return ff.StringFlag(ctx, EnvdUpgradeTargetFlag)
}

// EnvdUpgradeTargetDisabled reports whether a target value names no target at all. One
// definition, shared with the resolver, so an early-out at the call site and the resolver's
// own "off" answer cannot disagree about which values mean disabled.
func EnvdUpgradeTargetDisabled(target string) bool {
	return target == "" || target == envdUpgradeTargetOff
}

// ResolveEnvdOfflineUpgrade is the offline-swap analog of ResolveEnvdUpgrade
// same pure decision, keyed on EnvdOfflineUpgradeTargetFlag so the
// offline path ramps independently of the live one. builtWithVersion is the
// snapshot's recorded envd version (there is no running envd at cold-boot swap
// time), so — unlike the live path, which keys on the reported LiveEnvdVersion —
// the built-with never advances across an upgrade and this resolver keeps
// returning the same target on every resume until a re-pause re-bakes the
// version (an accepted, idempotent per-resume re-fire).
func ResolveEnvdOfflineUpgrade(
	ctx context.Context,
	ff *Client,
	builtWithVersion string,
	hostEnvdPath string,
	getVersion func(context.Context, string) (string, error),
	evalContexts ...ldcontext.Context,
) (path, version, reason string) {
	return resolveEnvdUpgradePath(ctx, ff.StringFlag(ctx, EnvdOfflineUpgradeTargetFlag, evalContexts...), builtWithVersion, hostEnvdPath, getVersion)
}

// resolveEnvdUpgradePath is the pure decision, split out so it can be unit-tested
// without a LaunchDarkly client (the flag value is passed directly). It returns
// the target path and its baked version, or ("", "", <reason>) for no upgrade.
// envdUpgradeTargetRe constrains a concrete EnvdUpgradeTargetFlag value to a
// bare version-ish identifier — a git SHA or a release name like v0.7.0. Dots
// and hyphens are admitted, but no path separators and no leading dot, so the
// value can't traverse out of the envd staging directory when joined into the
// candidate path.
var envdUpgradeTargetRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.-]*$`)

// envdUpgradeTargetOff is the flag value that names no target.
const envdUpgradeTargetOff = "off"

func resolveEnvdUpgradePath(
	ctx context.Context,
	target string,
	builtWithVersion string,
	hostEnvdPath string,
	getVersion func(context.Context, string) (string, error),
) (path, version, reason string) {
	candidate, reason := EnvdUpgradeCandidate(target, hostEnvdPath)
	if reason != "" {
		return "", "", reason
	}

	targetVersion, err := getVersion(ctx, candidate)
	if err != nil || targetVersion == "" {
		return "", "", "getversion_failed"
	}
	if targetVersion == builtWithVersion {
		return "", "", "same_version" // already on the target (idempotent re-resume)
	}
	// Upgrade only: refuse to swap in an older envd. A staged binary that is not
	// strictly newer than the sandbox's built-with version would otherwise be a
	// live downgrade on resume. (Rollback, if ever needed, must be an explicit
	// separate mechanism.)
	if newer, verr := utils.IsGTEVersion(targetVersion, builtWithVersion); verr != nil || !newer {
		return "", "", "downgrade"
	}

	return candidate, targetVersion, ""
}

// EnvdUpgradeCandidate maps an upgrade-target flag value to the binary it names,
// without probing it. Split out so the node-local binary cache can pre-warm the
// path a resume will actually ask for: a target naming a concrete version resolves
// to a staged sibling, not to hostEnvdPath, so warming hostEnvdPath alone leaves a
// version-pinned ramp with no pre-warm at all while still paying for one.
//
// Returns ("", reason) when no upgrade applies -- the same reason strings
// resolveEnvdUpgradePath reports -- and (candidate, "") when there is a binary to
// probe. Everything past this point costs an exec of that binary, which is what
// the caller may want to keep off its critical path.
func EnvdUpgradeCandidate(target, hostEnvdPath string) (candidate, reason string) {
	if EnvdUpgradeTargetDisabled(target) {
		return "", "off"
	}

	switch target {
	case "promoted":
		candidate = hostEnvdPath
	default:
		// A concrete version id -> the staged binary next to the promoted one,
		// in either layout: the flat "envd.<id>" sibling or the release
		// bucket's "<id>/envd" directory. The flag value becomes both a
		// filesystem path and an exec target (version probing runs
		// `<candidate> -version`), so reject anything that isn't a bare
		// identifier: a value with path separators (e.g. "../../bin/sh") would
		// otherwise escape the staging directory and run an arbitrary host
		// binary.
		if !envdUpgradeTargetRe.MatchString(target) {
			return "", "invalid_target"
		}
		dir := filepath.Dir(hostEnvdPath)
		for _, c := range []string{filepath.Join(dir, "envd."+target), filepath.Join(dir, target, "envd")} {
			if _, err := os.Stat(c); err == nil {
				candidate = c

				break
			}
		}
	}

	if candidate == "" {
		// Not staged on this node in either layout — e.g. a bad target /
		// rubbish flag value, or a node that has not fetched the target yet.
		return "", "not_staged"
	}

	if _, err := os.Stat(candidate); err != nil {
		// The promoted binary is absent (e.g. a version-free central mount
		// with no unversioned object).
		return "", "not_staged"
	}

	return candidate, ""
}

// defaultTrackedTemplates is the default map of template aliases tracked for metrics.
// This is used to reduce metric cardinality.
// JSON format: {"base": true, "code-interpreter-v1": true, ...}
var defaultTrackedTemplates = map[string]bool{
	"base":                  true,
	"code-interpreter-v1":   true,
	"code-interpreter-beta": true,
	"desktop":               true,
}

// TrackedTemplatesForMetrics is a JSON flag that defines which template aliases
// should be tracked in sandbox start time metrics. Templates not in this list
// will be grouped under "other" to reduce metric cardinality.
// JSON format: {"base": true, "code-interpreter-v1": true, ...}
var TrackedTemplatesForMetrics = NewJSONFlag("tracked-templates-for-metrics", ldvalue.FromJSONMarshal(defaultTrackedTemplates))

// GetTrackedTemplatesSet fetches the TrackedTemplatesForMetrics flag and returns it as a set for efficient lookup.
// Only keys with a truthy value are included; keys set to false are ignored.
func GetTrackedTemplatesSet(ctx context.Context, ff *Client) map[string]struct{} {
	value := ff.JSONFlag(ctx, TrackedTemplatesForMetrics)
	valueMap := value.AsValueMap()
	keys := valueMap.Keys(nil)
	result := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if valueMap.Get(key).BoolValue() {
			result[key] = struct{}{}
		}
	}

	return result
}

// CompressConfigFlag controls compression during template builds.
// When compressBuilds is true, builds upload exclusively compressed data
// (no uncompressed fallback). When false, exclusively uncompressed with V3
// headers (unless V4HeaderForUncompressedFlag is set).
var CompressConfigFlag = NewJSONFlag("compress-config", ldvalue.FromJSONMarshal(map[string]any{
	"compressBuilds":     false,
	"compressionType":    "",
	"compressionLevel":   0,
	"frameSizeKB":        0,
	"minPartSizeMB":      0,
	"frameEncodeWorkers": 0,
	"encoderConcurrency": 0,
}))

// TCPFirewallEgressThrottleConfig controls per-sandbox egress throttling via Firecracker's
// VMM-level token bucket rate limiters on the network interface.
// Structure mirrors the Firecracker RateLimiter API: two independent token buckets.
// Set bucketSize to -1 to disable a bucket.
//
// Ops bucket (packets):    effective rate = ops.bucketSize * 1000 / ops.refillTimeMs ops/s.
// Bandwidth bucket (bytes): effective rate = bandwidth.bucketSize * 1000 / bandwidth.refillTimeMs bytes/s.
var TCPFirewallEgressThrottleConfig = NewJSONFlag("tcpfirewall-egress-throttle-config", ldvalue.FromJSONMarshal(map[string]any{
	"ops":       map[string]any{"bucketSize": -1, "oneTimeBurst": 0, "refillTimeMs": 1000},
	"bandwidth": map[string]any{"bucketSize": -1, "oneTimeBurst": 0, "refillTimeMs": 1000},
}))

// TokenBucketConfig holds parameters for a single Firecracker token bucket.
// BucketSize < 0 disables the bucket.
type TokenBucketConfig struct {
	BucketSize   int64
	OneTimeBurst int64
	RefillTimeMs int64
}

// TCPFirewallEgressThrottleConfigValue holds the parsed values of TCPFirewallEgressThrottleConfig.
type TCPFirewallEgressThrottleConfigValue struct {
	Ops       TokenBucketConfig
	Bandwidth TokenBucketConfig
}

// parseThrottleBuckets parses "ops" and "bandwidth" token bucket configs from a JSON flag value.
func parseThrottleBuckets(value ldvalue.Value) (ops, bandwidth TokenBucketConfig) {
	parseBucket := func(key string) TokenBucketConfig {
		b := value.GetByKey(key)
		if b.IsNull() {
			return TokenBucketConfig{BucketSize: -1} // disabled
		}

		// Validate refill time
		refillTimeMs := int64(b.GetByKey("refillTimeMs").IntValue())
		if refillTimeMs <= 0 {
			return TokenBucketConfig{BucketSize: -1} // disabled — invalid refill time
		}

		return TokenBucketConfig{
			BucketSize:   int64(b.GetByKey("bucketSize").IntValue()),
			OneTimeBurst: int64(b.GetByKey("oneTimeBurst").IntValue()),
			RefillTimeMs: refillTimeMs,
		}
	}

	return parseBucket("ops"), parseBucket("bandwidth")
}

// GetTCPFirewallEgressThrottleConfig fetches and parses the TCPFirewallEgressThrottleConfig flag.
func GetTCPFirewallEgressThrottleConfig(ctx context.Context, ff *Client) TCPFirewallEgressThrottleConfigValue {
	value := ff.JSONFlag(ctx, TCPFirewallEgressThrottleConfig)
	ops, bw := parseThrottleBuckets(value)

	return TCPFirewallEgressThrottleConfigValue{
		Ops:       ops,
		Bandwidth: bw,
	}
}

// BlockDriveThrottleConfig controls per-sandbox block device (disk) throttling via Firecracker's
// VMM-level token bucket rate limiters on the rootfs drive.
// Structure mirrors the Firecracker RateLimiter API: two independent token buckets.
// Set bucketSize to -1 to disable a bucket.
//
// Ops bucket (IOPS):       effective rate = ops.bucketSize * 1000 / ops.refillTimeMs ops/s.
// Bandwidth bucket (bytes): effective rate = bandwidth.bucketSize * 1000 / bandwidth.refillTimeMs bytes/s.
var BlockDriveThrottleConfig = NewJSONFlag("block-drive-throttle-config", ldvalue.FromJSONMarshal(map[string]any{
	"ops":       map[string]any{"bucketSize": -1, "oneTimeBurst": 0, "refillTimeMs": 1000},
	"bandwidth": map[string]any{"bucketSize": -1, "oneTimeBurst": 0, "refillTimeMs": 1000},
}))

// BlockDriveThrottleConfigValue holds the parsed values of BlockDriveThrottleConfig.
type BlockDriveThrottleConfigValue struct {
	Ops       TokenBucketConfig
	Bandwidth TokenBucketConfig
}

// GetBlockDriveThrottleConfig fetches and parses the BlockDriveThrottleConfig flag.
func GetBlockDriveThrottleConfig(ctx context.Context, ff *Client) BlockDriveThrottleConfigValue {
	value := ff.JSONFlag(ctx, BlockDriveThrottleConfig)
	ops, bw := parseThrottleBuckets(value)

	return BlockDriveThrottleConfigValue{
		Ops:       ops,
		Bandwidth: bw,
	}
}

// IsolatedSchedulingHostsFlag names the cluster hosts on the isolated side of
// the sandbox scheduling partition, as a JSON array of cluster host (node) IDs:
//
//	["node-a", "node-b"]
//
// Null or empty (the default) leaves scheduling unpartitioned. The list is a
// policy, not a view of the fleet: it says nothing about which of those hosts
// are online, and hosts missing from the cluster simply never come up as
// candidates. Target it per cluster.
var IsolatedSchedulingHostsFlag = NewJSONFlag("isolated-scheduling-hosts", ldvalue.Null())

// GetIsolatedSchedulingHosts reads IsolatedSchedulingHostsFlag as a set. A
// value that is not an array of non-empty strings reads as empty, leaving
// scheduling unpartitioned rather than stranding every sandbox on a typo.
func GetIsolatedSchedulingHosts(ctx context.Context, ff *Client, contexts ...ldcontext.Context) map[string]struct{} {
	value := ff.JSONFlag(ctx, IsolatedSchedulingHostsFlag, contexts...)

	hosts := make(map[string]struct{}, value.Count())
	for i := range value.Count() {
		host := value.GetByIndex(i).StringValue()
		if host == "" {
			continue
		}

		hosts[host] = struct{}{}
	}

	return hosts
}
