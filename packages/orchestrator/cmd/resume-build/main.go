//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/coreos/go-iptables/iptables"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/tcpfirewall"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func main() {
	fromBuild := flag.String("from-build", "", "build ID (UUID) to resume from (required)")
	toBuild := flag.String("to-build", "", "output build ID (UUID) for pause snapshot (auto-generated if not specified)")
	storagePath := flag.String("storage", ".local-build", "storage: local path or gs://bucket")
	sandboxDir := flag.String("sandbox-dir", "", "override SANDBOX_DIR (the rootfs path baked into the snapshot)")
	iterations := flag.Int("iterations", 0, "run N iterations (0 = interactive)")
	coldStart := flag.Bool("cold", false, "clear cache between iterations (cold start each time)")
	noPrefetch := flag.Bool("no-prefetch", false, "disable memory prefetching")
	noEgress := flag.Bool("no-egress", false, "block all guest internet egress")
	disableMemfd := flag.Bool("disable-memfd", false, "disable memfd-backed guest memory")
	syncWP := flag.Bool("sync-wp", false, "resume with synchronous userfault write-protect delivery (use_sync_wp; requires an FC build that accepts the field)")
	trackerDirty := flag.Bool("tracker-dirty", false, "serve the pause dirty set from the page tracker instead of the pagemap RPC (implies -sync-wp)")
	memfileDiffDedup := flag.Bool("memfile-diff-dedup", false, "enable 4KiB-page deduplication of memfile diff against the base template")
	inPlace := flag.Bool("in-place", false, "checkpoint in place (pause, snapshot, resume the same FC process; implies -sync-wp)")
	deferMemoryExport := flag.Bool("defer-memory-export", false, "capture the in-place memory export through the async CoW window (implies -in-place; requires an FC build with /balloon/reporting)")
	verbose := flag.Bool("v", false, "verbose logging")
	console := flag.Bool("console", false, "forward Firecracker's output and the guest kernel serial console (tty) to stdout (fresh boot / -reboot only)")
	firecracker := flag.String("firecracker", "", "override the build's Firecracker version (e.g. when the baked version isn't on this node); safe for a cold boot/-reboot, risky for a memory resume")
	envdVersionFlag := flag.String("envd-version", "", fmt.Sprintf("the guest's envd version, which the snapshot does not record (default: %s, a placeholder above every version gate)", placeholderEnvdVersion))

	// Command execution (no pause)
	cmd := flag.String("cmd", "", "execute command in sandbox and exit (no snapshot)")

	// Pause flags
	pause := flag.Bool("pause", false, "start and immediately pause (snapshot)")
	signalPause := flag.String("signal-pause", "", "wait for signal before pause (e.g., SIGTERM, SIGUSR1)")
	cmdPause := flag.String("cmd-pause", "", "execute command in sandbox, then pause on success")
	cmdSignalPause := flag.String("cmd-signal-pause", "", "execute command in sandbox, then wait for SIGUSR1 before pausing")
	optimize := flag.Bool("optimize", false, "collect fresh prefetch mapping after pause (resumes snapshot to record page faults)")
	fsOnly := flag.Bool("fs-only", false, "pause without a memory snapshot (filesystem-only; resume reboots the guest)")
	reboot := flag.Bool("reboot", false, "cold-boot from the build's rootfs instead of resuming from memory")
	forceReboot := flag.Bool("force-reboot", false, "cold-boot like -reboot, but bypass the filesystem-only safety gate for memory-snapshot builds (the disk is only crash-consistent)")
	shell := flag.Bool("shell", false, "attach an interactive PTY shell via envd (no sshd required in the sandbox)")

	fphTimeoutMs := flag.Int("fph-timeout-ms", 0, "override free-page-hinting-config pause timeout LD flag (0 = use LD default)")
	reclaim := flag.Bool("reclaim", false, "enable pre-pause reclaim chain (fstrim 500ms, sync 500ms, drop_caches 200ms, compact 1s)")
	collapseEnvdHeap := flag.Bool("collapse-envd-heap", false, "collapse envd's heap before pause (overrides the collapse-envd-heap flag)")
	envdBinCache := flag.Bool("envd-binary-cache", false, "cache the host envd binary on local disk and gate the resume-time upgrade on a cache hit (overrides the envd-binary-cache flag; -envd-binary-cache=false forces it off)")
	prebootFsRecovery := flag.Bool("preboot-fs-recovery", false, "run the jailed pre-boot filesystem recovery (journal replay) before a cold boot (overrides the preboot-fs-recovery flag; -reboot/-force-reboot on a non-quiesced snapshot)")

	fphBench := flag.Bool("fph-bench", false, "compare pause memfile size with vs without FPH; requires -cmd-pause workload, uses -iterations (default 3), forces FPR on")
	fphBenchDelay := flag.Duration("fph-bench-delay", 0, "wait this long between workload completion and pause (lets FPR settle)")

	gdbDebug := flag.Bool("gdb", false, "resume under gdb: hold the guest at the kernel entry breakpoint with a gdb-enabled FC and hand over a ready gdb session for source-level guest-kernel debugging")
	gdbFC := flag.String("gdb-fc", "", "path to a firecracker built --features gdb (default: firecracker-debug resolved next to the snapshot's firecracker)")
	gdbSymbols := flag.String("gdb-symbols", "", "path to the guest kernel's DWARF symbols, vmlinux.debug (default: resolved next to the snapshot's vmlinux.bin)")
	gdbSocket := flag.String("gdb-socket", "", "gdb unix socket path (default: a temp path)")
	gdbExec := flag.String("gdb-exec", "", "scripted mode: run these gdb commands in batch (newline/';'-separated) instead of an interactive prompt")
	gdbScript := flag.String("gdb-script", "", "scripted mode: run this gdb command file in batch")

	flag.Parse()

	if *fphTimeoutMs > 0 {
		featureflags.NewJSONFlag("free-page-hinting-config", ldvalue.FromJSONMarshal(map[string]any{
			"enabled": true,
			"pause":   *fphTimeoutMs,
		}))
	}

	if *reclaim {
		featureflags.NewJSONFlag("guest-pause-reclaim", ldvalue.FromJSONMarshal(map[string]int{
			"sync":           500,
			"drop_caches":    200,
			"compact_memory": 1000,
			"fstrim":         500,
		}))
	}

	if *disableMemfd {
		featureflags.OverrideBoolFlag(featureflags.UseMemFdFlag, false)
	}

	if *syncWP || *trackerDirty || *inPlace || *deferMemoryExport {
		featureflags.OverrideBoolFlag(featureflags.UseSyncWPFlag, true)
	}

	if *inPlace || *deferMemoryExport {
		featureflags.OverrideBoolFlag(featureflags.InPlaceCheckpointFlag, true)
	}

	if *deferMemoryExport {
		featureflags.OverrideBoolFlag(featureflags.DeferMemoryExportFlag, true)
	}

	if *trackerDirty {
		featureflags.OverrideBoolFlag(featureflags.SyncWPTrackerDirtyFlag, true)
	}

	// Overridden in BOTH directions, and only when it was actually passed. This
	// flag's code fallback is on in development, so a "true means override" form
	// would make the option a no-op here and leave the off arm of a local A/B
	// unreachable -- the opposite of every sibling above, whose fallback is off.
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "envd-binary-cache" {
			featureflags.OverrideBoolFlag(featureflags.EnvdBinaryCacheFlag, *envdBinCache)
		}
	})

	if *collapseEnvdHeap {
		featureflags.OverrideBoolFlag(featureflags.CollapseEnvdHeapFlag, true)
	}

	if *prebootFsRecovery {
		featureflags.OverrideBoolFlag(featureflags.PrebootFsRecoveryFlag, true)
	}

	if *memfileDiffDedup {
		featureflags.OverrideJSONFlag(featureflags.MemfileDiffDedupFlag, ldvalue.FromJSONMarshal(map[string]any{
			"enabled": true,
		}))
	}

	if *fromBuild == "" {
		log.Fatal("-from-build required")
	}

	envdVer, err := resolveEnvdVersion(*envdVersionFlag)
	if err != nil {
		log.Fatal(err)
	}

	if os.Geteuid() != 0 {
		log.Fatal("run as root")
	}

	// Count pause options - only one allowed
	pauseCount := 0
	if *pause {
		pauseCount++
	}
	if *signalPause != "" {
		pauseCount++
	}
	if *cmdPause != "" {
		pauseCount++
	}
	if *cmdSignalPause != "" {
		pauseCount++
	}
	if pauseCount > 1 {
		log.Fatal("only one of -pause, -signal-pause, -cmd-pause, or -cmd-signal-pause can be specified")
	}

	// -cmd is incompatible with pause flags
	isCmdMode := *cmd != ""
	if isCmdMode && pauseCount > 0 {
		log.Fatal("-cmd is incompatible with pause flags")
	}

	isPauseMode := pauseCount > 0

	// fph-bench reuses -cmd-pause and -iterations.
	if !*fphBench && *iterations > 0 && (*signalPause != "" || *cmdPause != "" || *cmdSignalPause != "") {
		log.Fatal("-signal-pause, -cmd-pause, and -cmd-signal-pause are incompatible with -iterations")
	}

	// -to-build only makes sense with pause
	if *toBuild != "" && !isPauseMode {
		log.Fatal("-to-build requires a pause flag (-pause, -signal-pause, -cmd-pause, or -cmd-signal-pause)")
	}

	if *optimize && !isPauseMode {
		log.Fatal("-optimize requires a pause flag (-pause, -signal-pause, -cmd-pause, or -cmd-signal-pause)")
	}
	if *optimize && *iterations > 0 {
		log.Fatal("-optimize is incompatible with -iterations (benchmarking doesn't upload)")
	}
	if *fsOnly && !isPauseMode {
		log.Fatal("-fs-only requires a pause flag (-pause, -signal-pause, -cmd-pause, or -cmd-signal-pause)")
	}
	if *fsOnly && (*optimize || *fphBench) {
		log.Fatal("-fs-only is incompatible with -optimize and -fph-bench (no memory snapshot)")
	}

	if *shell && (isCmdMode || isPauseMode || *iterations > 0) {
		log.Fatal("-shell can only be used in interactive mode (no -cmd, no pause flags, no -iterations)")
	}

	if *fphBench && (*cmdPause == "" || *fphTimeoutMs > 0) {
		log.Fatal("-fph-bench requires -cmd-pause and is incompatible with -fph-timeout-ms")
	}

	// Generate new build ID if not specified and pause mode is enabled
	outputBuildID := *toBuild
	if isPauseMode && outputBuildID == "" {
		outputBuildID = uuid.New().String()
	}

	storageExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "storage" {
			storageExplicit = true
		}
	})

	if err := setupEnv(*storagePath, *sandboxDir, storageExplicit); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() { <-sig; fmt.Println("\n🛑 Stopping..."); cancel() }()

	isRemoteStorage := strings.HasPrefix(*storagePath, "gs://")
	pauseOpts := pauseOptions{
		immediate:       *pause,
		signalName:      *signalPause,
		command:         *cmdPause,
		commandSignal:   *cmdSignalPause,
		storagePath:     *storagePath,
		isRemoteStorage: isRemoteStorage,
		newBuildID:      outputBuildID,
		iterations:      *iterations,
		optimize:        *optimize,
		fsOnly:          *fsOnly,
		inPlace:         *inPlace || *deferMemoryExport,
	}

	runOpts := runOptions{
		cmd:                *cmd,
		iterations:         *iterations,
		console:            *console,
		firecrackerVersion: *firecracker,
		envdVersion:        envdVer,
	}

	benchIters := *iterations
	if *fphBench && benchIters <= 0 {
		benchIters = 3
	}
	fphBenchOpts := fphBenchOptions{enabled: *fphBench, workload: *cmdPause, iterations: benchIters, delay: *fphBenchDelay}

	gdbOpts := gdbOptions{
		enabled:  *gdbDebug,
		fcBinary: *gdbFC,
		symbols:  *gdbSymbols,
		socket:   *gdbSocket,
		execCmds: *gdbExec,
		script:   *gdbScript,
	}

	err = run(ctx, *fromBuild, *iterations, *coldStart, *noPrefetch, *noEgress, *verbose, *shell, *reboot, *forceReboot, pauseOpts, runOpts, fphBenchOpts, gdbOpts)
	cancel()

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if ctx.Err() != nil {
			fmt.Fprintf(os.Stderr, "(context: %v)\n", ctx.Err())
		}
		os.Exit(1)
	}
}

type pauseOptions struct {
	immediate       bool
	signalName      string
	command         string
	commandSignal   string
	storagePath     string
	isRemoteStorage bool
	newBuildID      string
	iterations      int // for benchmarking pause (only with immediate)
	optimize        bool
	fsOnly          bool
	// inPlace checkpoints in place (WithMaintainSandbox): the same FC
	// process pauses, snapshots and resumes, exercising the in-place seal —
	// and, with -defer-memory-export, the CoW window. The single-run path's
	// upload blocks on the deferred diffs, so the window completes before
	// the tool tears the sandbox down; the benchmark path measures timings
	// only and lets teardown cancel the window.
	inPlace bool
}

func (p pauseOptions) enabled() bool {
	return p.immediate || p.signalName != "" || p.command != "" || p.commandSignal != ""
}

// pauseTimings holds timing breakdown for a pause operation
type pauseTimings struct {
	resume time.Duration
	pause  time.Duration
	total  time.Duration
	err    error
}

type runOptions struct {
	cmd                string      // command to run and exit (no pause)
	iterations         int         // number of iterations (0 = single run)
	console            bool        // forward the guest kernel console + FC output to stdout/stderr (fresh boot)
	firecrackerVersion string      // override the build's Firecracker version (empty = use the build's own)
	envdVersion        envdVersion // the guest envd version to report and gate on
}

func (r runOptions) enabled() bool {
	return r.cmd != ""
}

// cmdTimings holds timing breakdown for a command run
type cmdTimings struct {
	resume  time.Duration
	command time.Duration
	total   time.Duration
	err     error
}

func setupEnv(from string, sandboxDir string, storageExplicit bool) error {
	abs := func(s string) string { return utils.Must(filepath.Abs(s)) }

	if sandboxDir != "" {
		os.Setenv("SANDBOX_DIR", sandboxDir)
	}

	// Derive dataDir from 'from' when it's a local path
	var dataDir string
	if strings.HasPrefix(from, "gs://") {
		dataDir = ".local-build"
	} else {
		dataDir = from
	}

	for _, d := range []string{"kernels", "templates", "sandbox", "orchestrator", "fc-versions", "envd"} {
		if err := os.MkdirAll(filepath.Join(dataDir, d), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	for _, d := range []string{"build", "build-templates", "sandbox", "template"} {
		if err := os.MkdirAll(filepath.Join(dataDir, "orchestrator", d), 0o755); err != nil {
			return fmt.Errorf("mkdir orchestrator/%s: %w", d, err)
		}
	}

	env := map[string]string{
		"ARTIFACTS_REGISTRY_PROVIDER": "Local",
		"FIRECRACKER_VERSIONS_DIR":    abs(filepath.Join(dataDir, "fc-versions")),
		"HOST_ENVD_PATH":              abs(filepath.Join(dataDir, "envd", "envd")),
		"HOST_KERNELS_DIR":            abs(filepath.Join(dataDir, "kernels")),
		"ORCHESTRATOR_BASE_PATH":      abs(filepath.Join(dataDir, "orchestrator")),
	}

	for k, v := range env {
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}

	templateURL := from
	if !strings.HasPrefix(from, "gs://") {
		templateURL = "file://" + abs(filepath.Join(dataDir, "templates"))
	}

	// An explicit -storage flag overrides ambient storage config; the flag's
	// default value only applies when nothing is configured.
	if storageExplicit || (os.Getenv("TEMPLATE_STORAGE_URL") == "" && os.Getenv("STORAGE_PROVIDER") == "") {
		os.Setenv("TEMPLATE_STORAGE_URL", templateURL)
	}

	return nil
}

type runner struct {
	factory     *sandbox.Factory
	tmpl        template.Template
	sbxConfig   *sandbox.Config
	buildID     string
	cache       *template.Cache
	coldStart   bool
	noPrefetch  bool
	shell       bool
	reboot      bool
	forceReboot bool
	console     bool
	config      cfg.BuilderConfig
	storage     storage.StorageProvider
	// gdbOrigVersionsDir preserves the original FirecrackerVersionsDir in gdb mode, where
	// config.FirecrackerVersionsDir is redirected to a writable staging dir; the published
	// firecracker-debug is resolved from this original (read-only) dir.
	gdbOrigVersionsDir string
}

// wrapTemplate applies the CLI's template masks (-no-prefetch drops the
// prefetch mapping). -force-reboot needs no mask: it is passed to RebootSandbox
// as the request-side demand, the same predicate a memory:false resume takes.
func wrapTemplate(tmpl template.Template, noPrefetch bool) template.Template {
	if noPrefetch {
		tmpl = &noPrefetchTemplate{tmpl}
	}

	return tmpl
}

// startSandbox starts a sandbox from the build, either resuming from its memory
// snapshot or cold-booting (rebooting) from its rootfs when -reboot or
// -force-reboot is set.
func (r *runner) startSandbox(ctx context.Context, runtime sandboxtypes.RuntimeMetadata, start, end time.Time) (*sandbox.Sandbox, error) {
	if r.reboot || r.forceReboot {
		var procOpts []func(*fc.ProcessOptions)
		if r.console {
			// Forward the guest kernel console (tty) + FC output to this process.
			procOpts = append(procOpts, func(o *fc.ProcessOptions) {
				o.KernelLogs = true
				o.Stdout = os.Stdout
				o.Stderr = os.Stderr
			})
		}

		return r.factory.RebootSandbox(ctx, r.tmpl, r.sbxConfig, runtime, end, nil, false, r.forceReboot, nil, procOpts...)
	}

	return r.factory.ResumeSandbox(ctx, r.tmpl, r.sbxConfig, runtime, start, end, nil)
}

func (r *runner) resumeOnce(ctx context.Context, iter int) (time.Duration, error) {
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("sbx-%d-%d", time.Now().UnixNano(), iter),
		ExecutionID: fmt.Sprintf("exec-%d-%d", time.Now().UnixNano(), iter),
	}

	t0 := time.Now()
	sbx, err := r.startSandbox(ctx, runtime, t0, t0.Add(24*time.Hour))
	dur := time.Since(t0)

	if sbx != nil {
		sbx.Close(context.WithoutCancel(ctx))
	}

	return dur, err
}

func (r *runner) interactive(ctx context.Context) error {
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("sbx-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("exec-%d", time.Now().UnixNano()),
	}

	fmt.Println("🚀 Starting...")
	t0 := time.Now()
	sbx, err := r.startSandbox(ctx, runtime, t0, t0.Add(24*time.Hour))
	if err != nil {
		return err
	}

	fmt.Printf("✅ Running (resumed in %s)\n", time.Since(t0))
	fmt.Printf("   sudo nsenter --net=/var/run/netns/%s ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@169.254.0.21\n", sbx.Slot.NamespaceID())

	defer func() {
		fmt.Println("🧹 Cleanup...")
		sbx.Close(context.WithoutCancel(ctx))
	}()

	if r.shell {
		err := attachShell(ctx, sbx)
		if err != nil && !isShellExited(err) {
			return err
		}

		return nil
	}

	fmt.Println("Ctrl+C to stop")
	<-ctx.Done()

	return nil
}

func (r *runner) cmdMode(ctx context.Context, opts runOptions) error {
	if opts.iterations > 0 {
		return r.cmdBenchmark(ctx, opts)
	}

	_, err := r.cmdOnce(ctx, opts, true)

	return err
}

func (r *runner) cmdOnce(ctx context.Context, opts runOptions, verbose bool) (cmdTimings, error) {
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("sbx-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("exec-%d", time.Now().UnixNano()),
	}

	if verbose {
		fmt.Println("🚀 Starting sandbox...")
	}
	t0 := time.Now()
	sbx, err := r.startSandbox(ctx, runtime, t0, t0.Add(24*time.Hour))
	resumeDur := time.Since(t0)
	if err != nil {
		return cmdTimings{resume: resumeDur, err: err}, err
	}
	defer sbx.Close(context.WithoutCancel(ctx))

	if verbose {
		fmt.Printf("✅ Sandbox resumed in %s\n", resumeDur)
		fmt.Printf("🔧 Running: %s\n", opts.cmd)
	}

	cmdStart := time.Now()
	cmdErr := runCommandInSandbox(ctx, sbx, opts.cmd)
	cmdDur := time.Since(cmdStart)
	totalDur := time.Since(t0)

	timings := cmdTimings{
		resume:  resumeDur,
		command: cmdDur,
		total:   totalDur,
		err:     cmdErr,
	}

	if verbose {
		if cmdErr != nil {
			fmt.Printf("❌ Command failed: %v\n", cmdErr)
		} else {
			fmt.Println()
			fmt.Println("📊 Timing breakdown:")
			fmt.Printf("   Resume:  %s\n", fmtDur(resumeDur))
			fmt.Printf("   Command: %s\n", fmtDur(cmdDur))
			fmt.Printf("   Total:   %s\n", fmtDur(totalDur))
		}
	}

	return timings, cmdErr
}

func (r *runner) cmdBenchmark(ctx context.Context, opts runOptions) error {
	results := make([]cmdTimings, 0, opts.iterations)

	fmt.Printf("📦 Benchmarking command: %s\n", opts.cmd)
	fmt.Printf("   Iterations: %d, Cold start: %v\n\n", opts.iterations, r.coldStart)

	for i := range opts.iterations {
		if ctx.Err() != nil {
			break
		}

		// Clear caches for cold start
		if r.coldStart && i > 0 {
			r.cache.InvalidateAll()
			if err := dropPageCache(); err != nil {
				return fmt.Errorf("drop page cache: %w", err)
			}
			tmpl, err := r.cache.GetTemplate(ctx, r.buildID, false, false)
			if err != nil {
				return fmt.Errorf("reload template: %w", err)
			}
			r.tmpl = wrapTemplate(tmpl, r.noPrefetch)
		}

		fmt.Printf("\r[%d/%d] Running...    ", i+1, opts.iterations)
		timings, _ := r.cmdOnce(ctx, opts, false)
		results = append(results, timings)

		if timings.err != nil {
			fmt.Printf("\r[%d/%d] ❌ Failed: %v\n", i+1, opts.iterations, timings.err)

			break
		}
	}
	fmt.Print("\r                         \r")

	printCmdResults(results)

	// Return last error if any
	for _, t := range results {
		if t.err != nil {
			return t.err
		}
	}

	return nil
}

func printCmdResults(results []cmdTimings) {
	if len(results) == 0 {
		return
	}

	// Separate successful results
	var successful []cmdTimings
	for _, r := range results {
		if r.err == nil {
			successful = append(successful, r)
		}
	}

	if len(successful) == 0 {
		fmt.Println("\n❌ All runs failed")

		return
	}

	// Calculate averages
	var totalResume, totalCmd, totalTotal time.Duration
	for _, t := range successful {
		totalResume += t.resume
		totalCmd += t.command
		totalTotal += t.total
	}
	n := len(successful)
	avgResume := totalResume / time.Duration(n)
	avgCmd := totalCmd / time.Duration(n)
	avgTotal := totalTotal / time.Duration(n)

	// Print individual results
	fmt.Println("\n📋 Run times (resume / command / total):")
	for i, t := range results {
		if t.err != nil {
			fmt.Printf("   [%2d] ❌ Failed: %v\n", i+1, t.err)

			continue
		}

		resumeDiff := float64(t.resume-avgResume) / float64(avgResume) * 100
		cmdDiff := float64(t.command-avgCmd) / float64(avgCmd) * 100

		fmt.Printf("   [%2d] %s / %s / %s  (resume: %s%+.1f%%%s, cmd: %s%+.1f%%%s)\n",
			i+1,
			fmtDur(t.resume), fmtDur(t.command), fmtDur(t.total),
			colorForDiff(resumeDiff), resumeDiff, colorReset,
			colorForDiff(cmdDiff), cmdDiff, colorReset)
	}

	// Print summary
	fmt.Printf("\n📊 Summary (%d runs):\n", n)
	fmt.Printf("   Resume:  Avg %s\n", fmtDur(avgResume))
	fmt.Printf("   Command: Avg %s\n", fmtDur(avgCmd))
	fmt.Printf("   Total:   Avg %s\n", fmtDur(avgTotal))

	// Min/Max for each
	if n > 1 {
		minR, maxR := successful[0].resume, successful[0].resume
		minC, maxC := successful[0].command, successful[0].command
		for _, t := range successful[1:] {
			if t.resume < minR {
				minR = t.resume
			}
			if t.resume > maxR {
				maxR = t.resume
			}
			if t.command < minC {
				minC = t.command
			}
			if t.command > maxC {
				maxC = t.command
			}
		}
		fmt.Printf("   Resume:  Min %s | Max %s\n", fmtDur(minR), fmtDur(maxR))
		fmt.Printf("   Command: Min %s | Max %s\n", fmtDur(minC), fmtDur(maxC))
	}
}

func colorForDiff(diff float64) string {
	switch {
	case diff < -5:
		return colorGreen
	case diff > 5:
		return colorRed
	default:
		return colorYellow
	}
}

func (r *runner) pauseMode(ctx context.Context, opts pauseOptions) error {
	// Benchmark mode for immediate pause
	if opts.immediate && opts.iterations > 0 {
		return r.pauseBenchmark(ctx, opts)
	}

	_, err := r.pauseOnce(ctx, opts, true)

	return err
}

func (r *runner) pauseOnce(ctx context.Context, opts pauseOptions, verbose bool) (pauseTimings, error) {
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("sbx-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("exec-%d", time.Now().UnixNano()),
	}

	if verbose {
		fmt.Println("🚀 Starting sandbox...")
	}
	t0 := time.Now()
	sbx, err := r.startSandbox(ctx, runtime, t0, t0.Add(24*time.Hour))
	resumeDur := time.Since(t0)
	if err != nil {
		return pauseTimings{resume: resumeDur, err: err}, err
	}
	defer sbx.Close(context.WithoutCancel(ctx))

	if verbose {
		fmt.Printf("✅ Sandbox resumed in %s\n", resumeDur)
	}

	// Handle pause trigger based on options
	switch {
	case opts.command != "":
		if verbose {
			fmt.Printf("🔧 Running command: %s\n", opts.command)
		}
		if err := runCommandInSandbox(ctx, sbx, opts.command); err != nil {
			return pauseTimings{resume: resumeDur, err: err}, fmt.Errorf("command failed: %w", err)
		}
		if verbose {
			fmt.Println("✅ Command completed successfully")
		}
	case opts.commandSignal != "":
		if verbose {
			fmt.Printf("🔧 Starting command: %s\n", opts.commandSignal)
		}
		cmdErrCh := runCommandInSandboxAsync(ctx, sbx, opts.commandSignal)
		if err := waitForPauseSignal(ctx, sbx, "SIGUSR1", cmdErrCh); err != nil {
			return pauseTimings{resume: resumeDur, err: err}, err
		}
	case opts.signalName != "":
		if err := waitForPauseSignal(ctx, sbx, opts.signalName, nil); err != nil {
			return pauseTimings{resume: resumeDur, err: err}, err
		}
	}
	// For opts.immediate, we proceed directly to pause

	if verbose {
		fmt.Printf("⏸️  Pausing sandbox (new build: %s)...\n", opts.newBuildID)
	}

	// Sync and drop caches before pause
	if err := syncAndDropCaches(ctx, sbx); err != nil && verbose {
		fmt.Printf("⚠️  Warning: sync/drop_caches failed: %v\n", err)
	}

	// Get original metadata and update for new build
	origMeta, err := r.tmpl.Metadata()
	if err != nil {
		return pauseTimings{resume: resumeDur, err: err}, fmt.Errorf("failed to get metadata: %w", err)
	}

	newMeta := origMeta
	newMeta.Template.BuildID = opts.newBuildID

	// Strip stale prefetch data — it corresponds to the parent build, not this snapshot.
	// Fresh prefetch will be collected in the optimize step if -optimize is set.
	if opts.optimize {
		newMeta.Prefetch = nil
	}

	// Pause and create snapshot
	var pauseSnapshotOpts []sandbox.PauseOption
	if opts.fsOnly {
		pauseSnapshotOpts = append(pauseSnapshotOpts, sandbox.WithFilesystemSnapshot())
	}
	if opts.inPlace {
		pauseSnapshotOpts = append(pauseSnapshotOpts, sandbox.WithMaintainSandbox())
	}
	pauseStart := time.Now()
	snapshot, err := sbx.Pause(ctx, newMeta, sandbox.SnapshotUseCasePause, pauseSnapshotOpts...)
	pauseDur := time.Since(pauseStart)
	totalDur := time.Since(t0)

	timings := pauseTimings{
		resume: resumeDur,
		pause:  pauseDur,
		total:  totalDur,
		err:    err,
	}

	if err != nil {
		return timings, fmt.Errorf("failed to pause: %w", err)
	}
	defer snapshot.Close(context.WithoutCancel(ctx))

	if verbose {
		fmt.Println()
		fmt.Println("📊 Timing breakdown:")
		fmt.Printf("   Resume: %s\n", fmtDur(resumeDur))
		fmt.Printf("   Pause:  %s\n", fmtDur(pauseDur))
		fmt.Printf("   Total:  %s\n", fmtDur(totalDur))
	}

	// Only upload when not in benchmark mode (verbose = true means single run)
	if verbose {
		if opts.isRemoteStorage {
			fmt.Println("📤 Uploading snapshot...")
		} else {
			fmt.Println("💾 Saving snapshot to local storage...")
		}

		upload, err := sandbox.NewUpload(ctx, nil, snapshot, r.storage, storage.CompressConfig{}, nil, "", nil)
		if err != nil {
			return timings, fmt.Errorf("failed to prepare upload: %w", err)
		}

		if err := upload.Run(ctx); err != nil {
			return timings, fmt.Errorf("failed to upload snapshot: %w", err)
		}

		fmt.Println("✅ Snapshot uploaded successfully")

		fmt.Printf("\n✅ Build finished: %s\n", opts.newBuildID)
		printArtifactSizes(opts.storagePath, opts.newBuildID)

		if opts.optimize {
			if err := r.collectAndUploadPrefetch(ctx, opts); err != nil {
				return timings, fmt.Errorf("prefetch optimization: %w", err)
			}
		}
	}

	return timings, nil
}

func (r *runner) pauseBenchmark(ctx context.Context, opts pauseOptions) error {
	results := make([]pauseTimings, 0, opts.iterations)

	fmt.Println("📦 Benchmarking pause (resume -> snapshot)")
	fmt.Printf("   Iterations: %d, Cold start: %v\n\n", opts.iterations, r.coldStart)

	for i := range opts.iterations {
		if ctx.Err() != nil {
			break
		}

		// Clear caches for cold start
		if r.coldStart && i > 0 {
			r.cache.InvalidateAll()
			if err := dropPageCache(); err != nil {
				return fmt.Errorf("drop page cache: %w", err)
			}
			tmpl, err := r.cache.GetTemplate(ctx, r.buildID, false, false)
			if err != nil {
				return fmt.Errorf("reload template: %w", err)
			}
			r.tmpl = wrapTemplate(tmpl, r.noPrefetch)
		}

		// Generate unique build ID for each iteration (not saved)
		iterOpts := opts
		iterOpts.newBuildID = uuid.New().String()

		fmt.Printf("\r[%d/%d] Running...    ", i+1, opts.iterations)
		timings, _ := r.pauseOnce(ctx, iterOpts, false)
		results = append(results, timings)

		if timings.err != nil {
			fmt.Printf("\r[%d/%d] ❌ Failed: %v\n", i+1, opts.iterations, timings.err)

			break
		}
	}
	fmt.Print("\r                         \r")

	printPauseResults(results)

	// Return last error if any
	for _, t := range results {
		if t.err != nil {
			return t.err
		}
	}

	return nil
}

func printPauseResults(results []pauseTimings) {
	if len(results) == 0 {
		return
	}

	// Separate successful results
	var successful []pauseTimings
	for _, r := range results {
		if r.err == nil {
			successful = append(successful, r)
		}
	}

	if len(successful) == 0 {
		fmt.Println("\n❌ All runs failed")

		return
	}

	// Calculate averages
	var totalResume, totalPause, totalTotal time.Duration
	for _, t := range successful {
		totalResume += t.resume
		totalPause += t.pause
		totalTotal += t.total
	}
	n := len(successful)
	avgResume := totalResume / time.Duration(n)
	avgPause := totalPause / time.Duration(n)
	avgTotal := totalTotal / time.Duration(n)

	// Print individual results
	fmt.Println("\n📋 Run times (resume / pause / total):")
	for i, t := range results {
		if t.err != nil {
			fmt.Printf("   [%2d] ❌ Failed: %v\n", i+1, t.err)

			continue
		}

		resumeDiff := float64(t.resume-avgResume) / float64(avgResume) * 100
		pauseDiff := float64(t.pause-avgPause) / float64(avgPause) * 100

		fmt.Printf("   [%2d] %s / %s / %s  (resume: %s%+.1f%%%s, pause: %s%+.1f%%%s)\n",
			i+1,
			fmtDur(t.resume), fmtDur(t.pause), fmtDur(t.total),
			colorForDiff(resumeDiff), resumeDiff, colorReset,
			colorForDiff(pauseDiff), pauseDiff, colorReset)
	}

	// Print summary
	fmt.Printf("\n📊 Summary (%d runs):\n", n)
	fmt.Printf("   Resume: Avg %s\n", fmtDur(avgResume))
	fmt.Printf("   Pause:  Avg %s\n", fmtDur(avgPause))
	fmt.Printf("   Total:  Avg %s\n", fmtDur(avgTotal))

	// Min/Max for each
	if n > 1 {
		minR, maxR := successful[0].resume, successful[0].resume
		minP, maxP := successful[0].pause, successful[0].pause
		for _, t := range successful[1:] {
			if t.resume < minR {
				minR = t.resume
			}
			if t.resume > maxR {
				maxR = t.resume
			}
			if t.pause < minP {
				minP = t.pause
			}
			if t.pause > maxP {
				maxP = t.pause
			}
		}
		fmt.Printf("   Resume: Min %s | Max %s\n", fmtDur(minR), fmtDur(maxR))
		fmt.Printf("   Pause:  Min %s | Max %s\n", fmtDur(minP), fmtDur(maxP))
	}
}

const prefetchCollectionIterations = 2

// collectAndUploadPrefetch resumes the snapshot to collect fresh page fault data,
// then updates the metadata with the resulting prefetch mapping.
func (r *runner) collectAndUploadPrefetch(ctx context.Context, opts pauseOptions) error {
	fmt.Println("\n🔍 Collecting prefetch mapping...")

	r.cache.Invalidate(opts.newBuildID)
	tmpl, err := r.cache.GetTemplate(ctx, opts.newBuildID, false, false)
	if err != nil {
		return fmt.Errorf("load template: %w", err)
	}

	// Disable any leftover prefetch during collection runs
	tmpl = &noPrefetchTemplate{tmpl}

	var allPrefetchData []block.PrefetchData
	for i := range prefetchCollectionIterations {
		fmt.Printf("   Run %d/%d...", i+1, prefetchCollectionIterations)

		runtime := sandboxtypes.RuntimeMetadata{
			TemplateID:  opts.newBuildID,
			TeamID:      "local",
			SandboxID:   fmt.Sprintf("prefetch-%d-%d", time.Now().UnixNano(), i),
			ExecutionID: fmt.Sprintf("prefetch-exec-%d-%d", time.Now().UnixNano(), i),
		}

		t0 := time.Now()
		sbx, err := r.factory.ResumeSandbox(ctx, tmpl, r.sbxConfig, runtime, t0, t0.Add(5*time.Minute), nil)
		if err != nil {
			fmt.Println()

			return fmt.Errorf("resume sandbox (run %d): %w", i+1, err)
		}

		data, dataErr := sbx.MemoryPrefetchData(ctx)
		sbx.Close(context.WithoutCancel(ctx))
		if dataErr != nil {
			return fmt.Errorf("collect prefetch (run %d): %w", i+1, dataErr)
		}

		fmt.Printf(" %d blocks (%s)\n", len(data.BlockEntries), fmtDur(time.Since(t0)))
		allPrefetchData = append(allPrefetchData, data)
	}

	commonEntries := computeCommonPrefetchEntries(allPrefetchData)
	if len(commonEntries) == 0 {
		fmt.Println("⚠️  No common prefetch blocks found")

		return nil
	}

	mapping := metadata.PrefetchEntriesToMapping(commonEntries, allPrefetchData[0].BlockSize)
	fmt.Printf("   Common: %d blocks\n", mapping.Count())

	existingMeta, err := metadata.FromBuildID(ctx, r.storage, opts.newBuildID)
	if err != nil {
		return fmt.Errorf("load metadata: %w", err)
	}

	updatedMeta := existingMeta.WithPrefetch(&metadata.Prefetch{
		Memory: mapping,
	})

	if err := metadata.UploadMetadata(ctx, r.storage, updatedMeta, nil); err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}

	r.cache.Invalidate(opts.newBuildID)

	fmt.Printf("✅ Prefetch mapping saved: %d blocks\n", mapping.Count())

	return nil
}

// computeCommonPrefetchEntries computes the intersection of multiple prefetch data sets.
// Only pages present in ALL runs are included, with averaged ordering.
func computeCommonPrefetchEntries(allData []block.PrefetchData) []block.PrefetchBlockEntry {
	if len(allData) == 0 {
		return nil
	}

	var commonEntries []block.PrefetchBlockEntry

	for idx, entry1 := range allData[0].BlockEntries {
		totalOrder := entry1.Order
		accessType := entry1.AccessType
		allMatch := true

		for i := 1; i < len(allData); i++ {
			entry, exists := allData[i].BlockEntries[idx]
			if !exists {
				allMatch = false

				break
			}
			totalOrder += entry.Order
			if entry.AccessType != accessType {
				accessType = block.Read
			}
		}

		if !allMatch {
			continue
		}

		commonEntries = append(commonEntries, block.PrefetchBlockEntry{
			Index:      idx,
			Order:      totalOrder / uint64(len(allData)),
			AccessType: accessType,
		})
	}

	return commonEntries
}

func (r *runner) benchmark(ctx context.Context, n int) error {
	results := make([]benchResult, 0, n)
	var lastErr error

	for i := range n {
		if ctx.Err() != nil {
			break
		}

		// Clear all caches for cold start
		if r.coldStart && i > 0 {
			r.cache.InvalidateAll()
			if err := dropPageCache(); err != nil {
				return fmt.Errorf("drop page cache: %w", err)
			}
			tmpl, err := r.cache.GetTemplate(ctx, r.buildID, false, false)
			if err != nil {
				return fmt.Errorf("reload template: %w", err)
			}
			r.tmpl = wrapTemplate(tmpl, r.noPrefetch)
		}

		fmt.Printf("\r[%d/%d] Running...    ", i+1, n)
		dur, err := r.resumeOnce(ctx, i)
		results = append(results, benchResult{dur, err})

		if err != nil {
			fmt.Printf("\r[%d/%d] ❌ Failed\n", i+1, n)
			lastErr = err

			break
		}
	}
	fmt.Print("\r                    \r") // Clear progress line

	printResults(results)

	return lastErr
}

// placeholderEnvdVersion is not the guest's real version — see the envd version caveat in the README.
const placeholderEnvdVersion = "1.0.0"

// envdVersion is the guest envd version the tool reports and gates on. resolveEnvdVersion
// is its only constructor, and a value it did not produce reports placeholderEnvdVersion.
type envdVersion struct {
	value string
	given bool
}

// resolveEnvdVersion builds an envdVersion from the -envd-version flag.
//
// An unparseable version is refused here because the capability gates read a parse error as
// "capability absent" and only log it, so a typo would silently leave /freeze, /fsfreeze
// and /collapse off.
func resolveEnvdVersion(flagValue string) (envdVersion, error) {
	if flagValue == "" {
		return envdVersion{}, nil
	}

	// The floor never matters; this calls the gates' own parser for its error.
	if _, err := utils.IsGTEVersion(flagValue, "0.0.0"); err != nil {
		return envdVersion{}, fmt.Errorf("invalid -envd-version %q: %w", flagValue, err)
	}

	return envdVersion{value: flagValue, given: true}, nil
}

func (v envdVersion) String() string {
	if !v.given {
		return placeholderEnvdVersion
	}

	return v.value
}

// describe reports the version and where it came from.
func (v envdVersion) describe() string {
	if v.given {
		return fmt.Sprintf("%s (-envd-version)", v)
	}

	return fmt.Sprintf("%s (placeholder, not the guest's version — pass -envd-version)", v)
}

func run(ctx context.Context, buildID string, iterations int, coldStart, noPrefetch, noEgress, verbose, shell, reboot, forceReboot bool, pauseOpts pauseOptions, runOpts runOptions, fphBenchOpts fphBenchOptions, gdbOpts gdbOptions) error {
	// Silence other loggers unless verbose mode
	var l logger.Logger
	if !verbose {
		cmdutil.SuppressNoisyLogs()
		l = logger.NewNopLogger()
		sbxlogger.SetSandboxLoggerInternal(logger.NewNopLogger())
	} else {
		var err error
		l, err = logger.NewDevelopmentLogger()
		if err != nil {
			return fmt.Errorf("logger: %w", err)
		}
		logger.ReplaceGlobals(ctx, l)
		sbxlogger.SetSandboxLoggerExternal(l)
		sbxlogger.SetSandboxLoggerInternal(l)
	}

	tel, err := telemetry.NewAnonymous(ctx, "resume-build")
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer func() {
		if err := tel.Shutdown(context.WithoutCancel(ctx)); err != nil {
			log.Printf("error shutting down telemetry: %v", err)
		}
	}()

	if os.Getenv("NODE_IP") == "" {
		os.Setenv("NODE_IP", "127.0.0.1")
	}

	if verbose {
		fmt.Println("🔧 Parsing config...")
	}
	config, err := cfg.Parse()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	if verbose {
		fmt.Println("🔧 Creating feature flags client...")
	}
	logLevel := ldlog.Error
	if verbose {
		logLevel = ldlog.Info
	}
	flags, _ := featureflags.NewClientWithLogLevel(logLevel)

	sandboxes := sandbox.NewSandboxesMap()

	if verbose {
		fmt.Println("🔧 Starting TCP firewall...")
	}
	tcpFw := tcpfirewall.New(l, config.NetworkConfig, sandboxes, tel.MeterProvider, flags)
	go tcpFw.Start(ctx)
	defer tcpFw.Close(context.WithoutCancel(ctx))

	// gdb mode debugs real customer snapshots. The guest is frozen at the entry
	// breakpoint and never boots, so it cannot itself open a connection — but force
	// egress off regardless, so a debugging run can never leave an exfiltration path
	// open. This makes the runbook's "always -no-egress" mechanical rather than a
	// manual step the operator can forget.
	if gdbOpts.enabled {
		noEgress = true
	}

	var egressProxy network.EgressProxy = network.NoopEgressProxy{}
	if noEgress {
		egressProxy = noEgressProxy{}
	}

	if verbose {
		fmt.Println("🔧 Creating network storage...")
	}
	slotStorage, err := network.NewStorageLocal(ctx, config.NetworkConfig, egressProxy)
	if err != nil {
		return fmt.Errorf("network storage: %w", err)
	}

	if verbose {
		fmt.Println("🔧 Creating network pool...")
	}
	networkPool := network.NewPool(8, 8, slotStorage, config.NetworkConfig)
	go networkPool.Populate(ctx)
	defer networkPool.Close(context.WithoutCancel(ctx))

	if verbose {
		fmt.Println("🔧 Creating NBD device pool...")
	}
	devicePool, err := nbd.NewDevicePool(config.NBDPoolSize)
	if err != nil {
		return fmt.Errorf("nbd pool: %w", err)
	}
	go devicePool.Populate(ctx)
	defer devicePool.Close(context.WithoutCancel(ctx))

	if verbose {
		fmt.Println("🔧 Creating storage provider...")
	}
	templateSpec, err := cfg.TemplateStorage()
	if err != nil {
		return fmt.Errorf("resolve template storage: %w", err)
	}
	persistence, err := storage.NewProvider(ctx, templateSpec)
	if verbose {
		fmt.Println("🔧 Storage provider created, err:", err)
	}
	if err != nil {
		return fmt.Errorf("storage provider: %w", err)
	}
	if persistence == nil {
		return errors.New("storage provider is nil")
	}

	if verbose {
		fmt.Println("🔧 Creating block metrics...")
	}
	blockMetrics, err := blockmetrics.NewMetrics(tel.MeterProvider)
	if err != nil {
		return fmt.Errorf("block metrics: %w", err)
	}

	if verbose {
		fmt.Println("🔧 Creating template cache...")
	}
	cache, err := template.NewCache(config, flags, persistence, blockMetrics, peerclient.NopResolver())
	if err != nil {
		return fmt.Errorf("template cache: %w", err)
	}
	cache.Start(ctx)
	defer cache.Stop()

	// In gdb mode the launch must run the gdb-enabled Firecracker, but the factory
	// resolves the FC binary from FirecrackerVersionsDir, which on cluster nodes is a
	// read-only gcsfuse mount. Redirect it to a writable temp dir (populated by gdbMode)
	// before the factory captures the config; the original dir is preserved for resolving
	// the published firecracker-debug. Only the FC dir is affected — the kernel dir stays.
	//
	// INVARIANT: this must run before anything resolves the FC binary from
	// FirecrackerVersionsDir. Today only the factory (created just below) does; the
	// template cache above does not. gdbMode also verifies the binary it is about to
	// launch is gdb-enabled, as a backstop against this assumption drifting.
	gdbOrigVersionsDir := ""
	if gdbOpts.enabled {
		gdbOrigVersionsDir = config.BuilderConfig.FirecrackerVersionsDir
		stageDir, mkErr := os.MkdirTemp("", "fc-gdb-versions-")
		if mkErr != nil {
			return fmt.Errorf("gdb fc staging dir: %w", mkErr)
		}
		defer os.RemoveAll(stageDir)
		config.BuilderConfig.FirecrackerVersionsDir = stageDir
	}

	if verbose {
		fmt.Println("🔧 Creating sandbox factory...")
	}
	factory := sandbox.NewFactory(ctx, config.BuilderConfig, networkPool, devicePool, flags, hoststats.NewNoopDelivery(), cgroup.NewNoopManager(), egressProxy, sandbox.NoopNetworkAssignHook{}, sandboxes)

	fmt.Printf("📦 Loading %s...\n", buildID)
	tmpl, err := cache.GetTemplate(ctx, buildID, false, false)
	if err != nil {
		return err
	}

	meta, err := tmpl.Metadata()
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}

	printTemplateInfo(ctx, tmpl, meta)

	if noPrefetch {
		fmt.Println("   Prefetch: disabled")
	}
	if forceReboot && !meta.IsFilesystemOnly() {
		fmt.Println("⚠️  Forcing reboot of a memory-snapshot build: the disk is only crash-consistent — writes that lived in the guest page cache at pause time may be missing.")
	}
	tmpl = wrapTemplate(tmpl, noPrefetch)

	fcVersion := meta.Template.FirecrackerVersion
	if runOpts.firecrackerVersion != "" {
		fmt.Printf("   Overriding Firecracker version: %s -> %s\n", meta.Template.FirecrackerVersion, runOpts.firecrackerVersion)
		fcVersion = runOpts.firecrackerVersion
	}

	fmt.Printf("   envd: %s\n", runOpts.envdVersion.describe())

	token := "local"
	sbxCfg := sandbox.NewConfig(sandbox.Config{
		BaseTemplateID:    buildID,
		Vcpu:              1,
		RamMB:             512,
		FreePageReporting: fphBenchOpts.enabled,
		Envd:              sandbox.EnvdMetadata{Vars: map[string]string{}, AccessToken: &token, Version: runOpts.envdVersion.String()},
		FirecrackerConfig: fc.Config{
			KernelVersion:      meta.Template.KernelVersion,
			FirecrackerVersion: fcVersion,
		},
	})

	r := &runner{
		factory:     factory,
		tmpl:        tmpl,
		buildID:     buildID,
		cache:       cache,
		coldStart:   coldStart,
		noPrefetch:  noPrefetch,
		shell:       shell,
		reboot:      reboot,
		forceReboot: forceReboot,
		console:     runOpts.console,
		config:      config.BuilderConfig,
		storage:     persistence,
		sbxConfig:   sbxCfg,

		gdbOrigVersionsDir: gdbOrigVersionsDir,
	}

	if gdbOpts.enabled {
		// gdb mode holds the guest at the entry breakpoint and never runs a
		// workload, so it is mutually exclusive with the pause/cmd/bench modes —
		// reject the combination rather than silently ignoring the other flags.
		if fphBenchOpts.enabled || runOpts.enabled() || pauseOpts.enabled() || iterations > 0 || reboot || forceReboot || shell {
			return errors.New("-gdb cannot be combined with -pause/-cmd/-iterations/-reboot/-force-reboot/-shell/-fph-bench")
		}

		return r.gdbMode(ctx, gdbOpts)
	}

	if fphBenchOpts.enabled {
		return r.fphBench(ctx, fphBenchOpts)
	}

	if runOpts.enabled() {
		return r.cmdMode(ctx, runOpts)
	}

	if pauseOpts.enabled() {
		return r.pauseMode(ctx, pauseOpts)
	}

	if iterations > 0 {
		return r.benchmark(ctx, iterations)
	}

	return r.interactive(ctx)
}

func printTemplateInfo(ctx context.Context, tmpl template.Template, meta metadata.Template) {
	fmt.Printf("   Kernel: %s, Firecracker: %s\n", meta.Template.KernelVersion, meta.Template.FirecrackerVersion)

	if memfile, err := tmpl.Memfile(ctx); err == nil {
		if size, err := memfile.Size(ctx); err == nil {
			blockSize := memfile.BlockSize()
			fmt.Printf("   Memfile: %d MB (%d KB blocks)\n", size>>20, blockSize>>10)
		}
	}

	if rootfs, err := tmpl.Rootfs(); err == nil {
		if size, err := rootfs.Size(ctx); err == nil {
			fmt.Printf("   Rootfs: %d MB (%d KB blocks)\n", size>>20, rootfs.BlockSize()>>10)
		}
	}

	if meta.Prefetch != nil && meta.Prefetch.Memory != nil {
		fmt.Printf("   Prefetch: %d blocks\n", meta.Prefetch.Memory.Count())
	}
}

// runCommandInSandboxTimeout caps how long a single resume-build command may
// run before envd kills it. Restores the prior 10-minute upper bound that the
// shared http.Client used to enforce, so a stuck command can't block the CLI.
const runCommandInSandboxTimeout = 10 * time.Minute

// runCommandInSandbox runs a command inside the sandbox via envd as a
// login shell so /etc/profile is sourced.
func runCommandInSandbox(ctx context.Context, sbx *sandbox.Sandbox, command string) error {
	stream, err := sbx.StartEnvdShell(ctx, "/bin/bash", []string{"-l", "-c", command}, "root", runCommandInSandboxTimeout)
	if err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}
	defer stream.Close()

	// Read stream until completion
	for stream.Receive() {
		msg := stream.Msg()
		event := msg.GetEvent()
		if event == nil {
			continue
		}

		switch e := event.GetEvent().(type) {
		case *process.ProcessEvent_Data:
			// Handle data events (stdout/stderr)
			if data := e.Data; data != nil {
				if stdout := data.GetStdout(); stdout != nil {
					fmt.Print(string(stdout))
				}
				if stderrData := data.GetStderr(); stderrData != nil {
					fmt.Print(string(stderrData))
				}
			}
		case *process.ProcessEvent_End:
			// Handle exit event
			if end := e.End; end != nil {
				if !end.GetExited() || end.GetExitCode() != 0 {
					return fmt.Errorf("command exited with code %d", end.GetExitCode())
				}

				return nil
			}
		}
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	return nil
}

func runCommandInSandboxAsync(ctx context.Context, sbx *sandbox.Sandbox, command string) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- runCommandInSandbox(ctx, sbx, command)
		close(errCh)
	}()

	return errCh
}

func waitForPauseSignal(ctx context.Context, sbx *sandbox.Sandbox, signalName string, cmdErrCh <-chan error) error {
	sig := parseSignal(signalName)
	if sig == nil {
		return fmt.Errorf("unknown signal: %s", signalName)
	}

	fmt.Printf("⏳ Waiting for %s signal...\n", signalName)
	fmt.Printf("   sudo nsenter --net=/var/run/netns/%s ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@169.254.0.21\n", sbx.Slot.NamespaceID())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, sig)
	defer signal.Stop(sigCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sigCh:
			fmt.Printf("📨 Received %s signal\n", signalName)

			return nil
		case err, ok := <-cmdErrCh:
			if !ok {
				cmdErrCh = nil

				continue
			}
			if err != nil {
				fmt.Printf("⚠️  Command exited before pause signal: %v\n", err)
			} else {
				fmt.Println("ℹ️  Command completed before pause signal")
			}
			cmdErrCh = nil
		}
	}
}

// syncAndDropCaches syncs filesystem and drops caches before snapshot
func syncAndDropCaches(ctx context.Context, sbx *sandbox.Sandbox) error {
	// Run sync command
	if err := runCommandInSandbox(ctx, sbx, rootfs.SandboxBusyBoxPath+" sync"); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	// Drop caches
	if err := runCommandInSandbox(ctx, sbx, "echo 3 > /proc/sys/vm/drop_caches"); err != nil {
		return fmt.Errorf("drop_caches failed: %w", err)
	}

	return nil
}

// parseSignal converts a signal name to os.Signal
func parseSignal(name string) os.Signal {
	name = strings.ToUpper(strings.TrimPrefix(name, "SIG"))
	signals := map[string]os.Signal{
		"TERM":  syscall.SIGTERM,
		"USR1":  syscall.SIGUSR1,
		"USR2":  syscall.SIGUSR2,
		"HUP":   syscall.SIGHUP,
		"INT":   syscall.SIGINT,
		"QUIT":  syscall.SIGQUIT,
		"CONT":  syscall.SIGCONT,
		"WINCH": syscall.SIGWINCH,
	}

	return signals[name]
}

// printArtifactSizes prints artifact sizes
func printArtifactSizes(_, buildID string) {
	spec, err := cfg.TemplateStorage()
	if err != nil || spec.Provider != storage.LocalStorageProvider {
		return
	}

	dir := filepath.Join(spec.BasePath, buildID)

	fmt.Println("\n📦 Artifacts:")

	for _, a := range cmdutil.MainArtifacts() {
		path := filepath.Join(dir, a.File)
		_, actual, err := cmdutil.GetFileSizes(path)
		if err != nil {
			continue
		}

		headerPath := filepath.Join(dir, a.HeaderFile)
		totalSize, blockSize := cmdutil.GetHeaderInfo(headerPath)
		if totalSize == 0 {
			fmt.Printf("   %s: %d MB (this layer)\n", a.Name, actual>>20)

			continue
		}

		pct := float64(actual) / float64(totalSize) * 100
		fmt.Printf("   %s: %d MB diff / %d MB total (%.1f%%), block size: %d KB\n",
			a.Name, actual>>20, totalSize>>20, pct, blockSize>>10)
	}

	for _, a := range cmdutil.SmallArtifacts() {
		path := filepath.Join(dir, a.File)
		if actual, err := cmdutil.GetActualFileSize(path); err == nil {
			fmt.Printf("   %s: %d KB\n", a.Name, actual>>10)
		}
	}
}

// Benchmark output formatting

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
)

type benchResult struct {
	dur time.Duration
	err error
}

func fmtDur(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)

	return fmt.Sprintf("%.1fms", ms)
}

// dropPageCache drops the OS page cache to simulate cold starts.
// This ensures files aren't served from memory on subsequent runs.
func dropPageCache() error {
	// Sync first to flush dirty pages
	unix.Sync()

	// Drop page cache (requires root)
	// 3 = free pagecache, dentries, and inodes
	return os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0o644)
}

func printResults(results []benchResult) {
	if len(results) == 0 {
		return
	}

	// Calculate average
	var total time.Duration
	var successCount int
	for _, r := range results {
		if r.err == nil {
			total += r.dur
			successCount++
		}
	}

	if successCount == 0 {
		fmt.Println("\n❌ All runs failed")

		return
	}

	avg := total / time.Duration(successCount)

	// Print individual results
	fmt.Println("\n📋 Run times:")
	for i, r := range results {
		if r.err != nil {
			fmt.Printf("   [%2d] ❌ Failed: %v\n", i+1, r.err)

			continue
		}

		diff := r.dur - avg
		pct := float64(diff) / float64(avg) * 100

		var color string
		switch {
		case diff < 0:
			color = colorGreen
		case diff > 0:
			color = colorRed
		default:
			color = colorYellow
		}

		fmt.Printf("   [%2d] %s  %s%+.1f%%%s\n", i+1, fmtDur(r.dur), color, pct, colorReset)
	}

	// Print summary stats
	durations := make([]time.Duration, 0, successCount)
	for _, r := range results {
		if r.err == nil {
			durations = append(durations, r.dur)
		}
	}

	sorted := slices.Clone(durations)
	slices.Sort(sorted)

	// Calculate standard deviation
	var variance float64
	avgFloat := float64(avg)
	for _, d := range durations {
		diff := float64(d) - avgFloat
		variance += diff * diff
	}
	variance /= float64(len(durations))
	stdDev := time.Duration(math.Sqrt(variance))

	n := len(sorted)
	fmt.Printf("\n📊 Summary (%d runs):\n", n)
	fmt.Printf("   Min: %s | Max: %s | Avg: %s | StdDev: %s\n", fmtDur(sorted[0]), fmtDur(sorted[n-1]), fmtDur(avg), fmtDur(stdDev))
	if n > 1 {
		p95idx := int(float64(n-1) * 0.95)
		p99idx := int(float64(n-1) * 0.99)
		fmt.Printf("   P95: %s | P99: %s\n", fmtDur(sorted[p95idx]), fmtDur(sorted[p99idx]))
	}
}

// noPrefetchTemplate wraps a template to disable prefetching by returning nil Prefetch in metadata.
type noPrefetchTemplate struct {
	template.Template
}

func (t *noPrefetchTemplate) Metadata() (metadata.Template, error) {
	meta, err := t.Template.Metadata()
	if err != nil {
		return meta, err
	}
	meta.Prefetch = nil

	return meta, nil
}

// noEgressProxy is an EgressProxy that removes the default route from the
// sandbox's netns at slot-creation time.
type noEgressProxy struct {
	network.NoopEgressProxy
}

func (noEgressProxy) OnSlotCreate(s *network.Slot, _ *iptables.IPTables) error {
	nsPath := filepath.Join("/var/run/netns", s.NamespaceID())

	handle, err := ns.GetNS(nsPath)
	if err != nil {
		return fmt.Errorf("get netns %q: %w", nsPath, err)
	}
	defer handle.Close()

	// Match the route installed earlier in Slot.CreateNetwork:
	//   Scope = SCOPE_UNIVERSE, Gw = VethIP.
	return handle.Do(func(_ ns.NetNS) error {
		if err := netlink.RouteDel(&netlink.Route{
			Scope: netlink.SCOPE_UNIVERSE,
			Gw:    s.VethIP(),
		}); err != nil {
			return fmt.Errorf("delete default route in %s: %w", s.NamespaceID(), err)
		}

		return nil
	})
}
