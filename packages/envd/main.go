package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"connectrpc.com/authn"
	connectcors "connectrpc.com/cors"
	"github.com/go-chi/chi/v5"
	"github.com/rs/cors"

	"github.com/e2b-dev/infra/packages/envd/internal/api"
	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/host"
	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/logs/exporter"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	publicport "github.com/e2b-dev/infra/packages/envd/internal/port"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	filesystemRpc "github.com/e2b-dev/infra/packages/envd/internal/services/filesystem"
	processRpc "github.com/e2b-dev/infra/packages/envd/internal/services/process"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
	"github.com/e2b-dev/infra/packages/envd/pkg"
)

const (
	// Downstream timeout should be greater than upstream (in orchestrator proxy).
	idleTimeout = 640 * time.Second
	maxAge      = 2 * time.Hour

	defaultPort = 49983

	portScannerInterval = 1000 * time.Millisecond

	// handoverFallbackThawTimeout bounds how long a live-upgraded envd keeps its
	// re-adopted workload frozen waiting for the orchestrator's post-upgrade
	// /init to thaw it. Comfortably past the orchestrator's upgrade readiness
	// budget, so it only fires when that /init genuinely never arrives.
	handoverFallbackThawTimeout = 60 * time.Second

	kilobyte = 1024
	megabyte = 1024 * kilobyte
)

var (
	commitSHA string

	isNotFC bool
	port    int64

	versionFlag bool
	commitFlag  bool
	cgroupRoot  string
	noCgroups   bool
	verbose     bool

	// resumeHandover: set by the outgoing envd when it re-execs itself during a
	// live self-upgrade. On boot the new image re-adopts the
	// processes described in the tmpfs handover blob.
	resumeHandover bool
)

func parseFlags() {
	flag.BoolVar(
		&isNotFC,
		"isnotfc",
		false,
		"run outside of Firecracker (skips MMDS poll and HTTP log exporter)",
	)

	flag.BoolVar(
		&versionFlag,
		"version",
		false,
		"print envd version",
	)

	flag.BoolVar(
		&commitFlag,
		"commit",
		false,
		"print envd source commit",
	)

	flag.Int64Var(
		&port,
		"port",
		defaultPort,
		"a port on which the daemon should run",
	)

	flag.StringVar(
		&cgroupRoot,
		"cgroup-root",
		"/sys/fs/cgroup",
		"cgroup root directory",
	)

	flag.BoolVar(
		&noCgroups,
		"no-cgroups",
		false,
		"disable cgroup management; use a no-op cgroup manager instead",
	)

	flag.BoolVar(
		&verbose,
		"verbose",
		false,
		"write envd logs to stdout",
	)

	flag.BoolVar(
		&resumeHandover,
		"resume-handover",
		false,
		"internal: re-adopt processes from the live-upgrade handover blob (set by the outgoing envd)",
	)

	flag.Parse()
}

func withCORS(h http.Handler) http.Handler {
	middleware := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			http.MethodHead,
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		},
		AllowedHeaders: []string{"*"},
		ExposedHeaders: append(
			connectcors.ExposedHeaders(),
			"Location",
			"Cache-Control",
			"X-Content-Type-Options",
		),
		MaxAge: int(maxAge.Seconds()),
	})

	return middleware.Handler(h)
}

func main() {
	parseFlags()

	if versionFlag {
		fmt.Printf("%s\n", pkg.Version)

		return
	}

	if commitFlag {
		fmt.Printf("%s\n", commitSHA)

		return
	}

	if err := run(); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := os.MkdirAll(host.E2BRunDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "error creating E2B run directory: %v\n", err)
	}

	defaults := &execcontext.Defaults{
		User:    execcontext.BuiltinDefaultUser,
		EnvVars: utils.NewEnvVars(),
	}
	isFCBoolStr := strconv.FormatBool(!isNotFC)
	defaults.EnvVars.Store("E2B_SANDBOX", isFCBoolStr)
	if err := os.WriteFile(filepath.Join(host.E2BRunDir, ".E2B_SANDBOX"), []byte(isFCBoolStr), 0o444); err != nil {
		fmt.Fprintf(os.Stderr, "error writing sandbox file: %v\n", err)
	}

	// Not closed - producers may outlive the consumer.
	mmdsChan := make(chan *host.MMDSOpts, 1)
	if !isNotFC {
		go host.PollForMMDSOpts(ctx, mmdsChan, defaults.EnvVars)
	}

	logFlusher := api.NewNoopLogFlusher()
	var logWriters []io.Writer
	if !isNotFC {
		logExporter := exporter.NewHTTPLogsExporter(ctx, mmdsChan)
		logWriters = append(logWriters, logExporter)
		logFlusher = logExporter
	}
	l := logs.NewLogger(verbose, logWriters...)

	m := chi.NewRouter()

	envLogger := l.With().Str("logger", "envd").Logger()
	fsLogger := l.With().Str("logger", "filesystem").Logger()
	filesystemService := filesystemRpc.Handle(m, &fsLogger, defaults)

	cgroupManager := createCgroupManager()
	defer func() {
		err := cgroupManager.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close cgroup manager: %v\n", err)
		}
	}()

	// One freezer shared by the process service (live-upgrade handover) and the
	// HTTP API (/freeze, /unfreeze, /init thaw) so every freeze/unfreeze caller
	// serializes on a single lock.
	workloadFreezer := cgroups.NewWorkloadFreezer(cgroupManager)
	// Backstop for a freeze that never gets its thaw -- an orchestrator that resumes and
	// never calls /init, or a thaw that fails outright. Without it such a guest stays
	// frozen for the rest of its life.
	workloadFreezer.SetThawWatchdog(cgroups.DefaultThawWatchdogWindow, func(res cgroups.ThawResult, err error) {
		e := l.Warn().Int("thawed", res.Thawed).Int("visited", res.Visited).
			Int("failed", res.Failed).Bool("truncated", res.Truncated)
		if err != nil {
			e = e.Err(err)
		}
		e.Msg("thaw watchdog fired: a freeze was never followed by a thaw")
	})

	processLogger := l.With().Str("logger", "process").Logger()
	processService := processRpc.Handle(m, &processLogger, defaults, workloadFreezer)

	// Live-upgrade incoming side: if this envd was re-exec'd by an outgoing
	// envd, re-adopt the handed-over processes before serving.
	var handover processRpc.HandoverResult
	// handoverFailed is true if ResumeFromHandover errored or panicked: the
	// workload was not re-adopted, so /init must advertise the failure (below) —
	// otherwise the orchestrator, which confirms the upgrade by version alone,
	// would record a broken sandbox (orphaned, unreaped workload) as a success.
	var handoverFailed bool
	if resumeHandover {
		func() {
			// A panic here must not crash the new envd: systemd would restart a
			// fresh (old-binary) envd with a new PID that can neither re-adopt nor
			// reap the frozen workload. Recover and keep serving; the workload is
			// thawed by ResumeFromHandover's deferred unfreeze regardless.
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "resume-from-handover panic (recovered): %v\n", r)
					handoverFailed = true
					processService.UnfreezeWorkload()
				}
			}()

			// Pass ImportWatchers as a callback so watchers are re-armed while the
			// workload is still frozen inside ResumeFromHandover (on success it
			// stays frozen until the post-upgrade /init thaws it), leaving no gap
			// in which a filesystem event could be missed between the thaw and the
			// re-arm.
			res, err := processService.ResumeFromHandover(filesystemService.ImportWatchers)
			if err != nil {
				fmt.Fprintf(os.Stderr, "resume-from-handover failed: %v\n", err)
				handoverFailed = true
			}
			handover = res
		}()
	}

	service := api.New(&envLogger, defaults, mmdsChan, isNotFC, workloadFreezer, logFlusher)
	if resumeHandover {
		// Restore the NFS mount ledger carried across the upgrade before the
		// post-upgrade /init runs setupNFS, so it recognizes a still-live mount
		// (matching lifecycle) instead of force-unmounting and remounting it.
		service.ImportMounts(handover.Mounts)

		// Surface the handover outcome on the next /init so the orchestrator can
		// record it — the envd-side result (re-adopted procs, restored retained
		// exits, watcher re-arm success/failures) is otherwise only logged
		// in-guest and invisible fleet-wide.
		service.SetHandoverResult(handoverFailed, handover.Procs, handover.ProcsFailed, handover.Retained, handover.RetainedFailed, handover.Watchers, handover.WatchersFailed)

		// Safety net for the freeze-until-/init handover: ResumeFromHandover left
		// the workload frozen for the orchestrator's post-upgrade /init to thaw.
		// If that /init never lands (e.g. WaitForEnvd fails / deadlines), thaw
		// anyway after a grace window so the sandbox degrades rather than hangs
		// for the rest of its life. Gated on Initialized() so it never races a
		// legitimate later /freeze, and /upgrade stays gated until /init even on
		// this path, so the fallback can't reopen the unauthenticated-upgrade
		// window.
		time.AfterFunc(handoverFallbackThawTimeout, func() {
			if !service.Initialized() {
				fmt.Fprintf(os.Stderr, "envd: post-upgrade /init did not arrive within %s; thawing workload as fallback\n", handoverFallbackThawTimeout)
				processService.UnfreezeWorkload()
			}
		})
	}
	handler := api.HandlerFromMux(service, m)
	middleware := authn.NewMiddleware(permissions.AuthenticateUsername)

	s := &http.Server{
		Handler: withCORS(
			service.WithAuthorization(
				middleware.Wrap(handler),
			),
		),
		Addr: fmt.Sprintf("0.0.0.0:%d", port),
		// We remove the timeouts as the connection is terminated by closing of the sandbox and keepalive close.
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  idleTimeout,
	}

	portLogger := l.With().Str("logger", "port-forwarder").Logger()

	// Bind all open ports on 127.0.0.1 and localhost to the eth0 interface
	portScanner := publicport.NewScanner(&portLogger, portScannerInterval)
	defer portScanner.Destroy()

	portForwarder := publicport.NewForwarder(&portLogger, portScanner, cgroupManager)
	if resumeHandover {
		// Re-adopt the socats carried across the upgrade before the forwarder's
		// first scan, so it recognizes already-forwarded ports instead of spawning
		// duplicate socats (and leaking the originals as un-reaped zombies).
		if readopted := portForwarder.ImportForwards(handover.Forwards); readopted > 0 {
			fmt.Fprintf(os.Stderr, "envd: re-adopted %d forwarded port(s) after handover\n", readopted)
		}
	}
	go portForwarder.StartForwarding(ctx)

	go portScanner.ScanAndBroadcast()

	// Live-upgrade outgoing trigger. doUpgrade performs the outgoing
	// choreography: freeze the workload, then re-exec into newBin (empty
	// = re-exec self). It does not return on success. The port scanner is left
	// running: the fd relocation runs under syscall.ForkLock (see Upgrade), which
	// already serializes it against the scanner's fd/socat creation, so there is
	// no CLOEXEC race to quiesce — and leaving it up means a failed upgrade never
	// disturbs port forwarding.
	doUpgrade := func(newBin string) error {
		fmt.Fprintf(os.Stderr, "envd: self-upgrade (from v%s, newBin=%q)\n", pkg.Version, newBin)
		// Hold the freeze lock across the WHOLE handover (freeze -> serialize ->
		// execve), not just the freeze sweep, so a concurrent /init or /unfreeze
		// can't acquire the lock and thaw the workload while Upgrade is
		// snapshotting the process table.
		releaseFreeze, freezeErr := processService.FreezeWorkloadHold() //nolint:contextcheck // (un)freeze uses a non-cancellable context internally so the thaw always lands
		// Release the freeze hold and thaw on ANY non-exec exit — an error OR a
		// panic in ExportWatchers/Upgrade (net/http recovers the handler, so envd
		// keeps serving; a leaked hold would then deadlock every later Unfreeze,
		// including /init, and strand the workload frozen). Order matters: drop
		// the lock BEFORE thawing, since Unfreeze acquires it. A successful execve
		// replaces this process, so this defer never runs on success.
		defer func() {
			releaseFreeze()
			processService.UnfreezeWorkload() //nolint:contextcheck // (un)freeze uses a non-cancellable context internally so the thaw always lands
		}()
		if freezeErr != nil {
			// The workload isn't fully frozen, so snapshotting the process table
			// would be racy/stale and the pre-init security assumptions (nothing
			// runs until /init restores auth) wouldn't hold. Abort the swap; the
			// deferred release+thaw keeps the old envd serving.
			return fmt.Errorf("handover aborted: freeze workload: %w", freezeErr)
		}

		// Export the state owned by the other services so the new envd can restore
		// it after the swap: filesystem watches, the NFS mount ledger (so /init
		// doesn't remount a live mount), and the active port-forwards (so socats
		// are re-adopted, not duplicated).
		//
		// Watches and forwards are exported with the owning lock HELD across the
		// execve (the *Hold variants). The freeze quiesces the workload, but the
		// filesystem watcher-drain (GetWatcherEvents) and the port scanner are
		// envd-internal and keep running; holding their locks through the swap
		// stops a post-snapshot event-drain or socat-spawn that the snapshot could
		// not capture. On a successful execve this process is replaced and the
		// held locks vanish with it; the defers below only run on the failure
		// path, restoring both services under the old envd.
		watchers, releaseWatchers := filesystemService.ExportWatchersHold()
		defer releaseWatchers()
		mounts := service.ExportMounts()
		forwards, releaseForwards := portForwarder.ExportForwardsHold()
		defer releaseForwards()

		// Upgrade only returns on failure — a successful execve replaces this
		// process (and drops the held freeze/watcher/forward locks with it). On
		// failure the OLD envd is still running; the deferred releases + thaw keep
		// the old version serving rather than leaving the sandbox hung.
		return processService.Upgrade(newBin, pkg.Version, watchers, mounts, forwards)
	}

	// Orchestrator-driven trigger: authenticated POST /upgrade with the target
	// binary path in the X-Envd-Upgrade-Bin header (empty = re-exec self). This
	// is the production trigger the orchestrator calls at resume after delivering
	// the new binary into the guest. On success it never responds (envd execs);
	// the caller treats a dropped connection as success.
	m.Post("/upgrade", func(w http.ResponseWriter, r *http.Request) {
		// Refuse upgrades until the first authenticated /init. A re-exec'd envd
		// serves before /init with its access token cleared, so without this a
		// guest process could drive an unauthenticated upgrade in that window
		// (and after the fallback thaw). The orchestrator only triggers /upgrade
		// on an already-initialized envd, so this never blocks the real caller.
		if !service.Initialized() {
			http.Error(w, "envd not initialized", http.StatusConflict)

			return
		}
		// The delivered binary is always written to and exec'd from a fixed path;
		// reject a caller asking for anything else rather than writing/exec'ing an
		// arbitrary path (defense-in-depth — the endpoint is authenticated).
		if hdr := r.Header.Get("X-Envd-Upgrade-Bin"); hdr != "" && hdr != processRpc.DefaultUpgradeBinPath {
			http.Error(w, "unsupported upgrade target", http.StatusBadRequest)

			return
		}
		newBin := ""
		// If the new binary is streamed in the request body (the orchestrator's
		// authenticated host-side delivery), write it to the fixed path before
		// swapping. This avoids the unauthenticated /files path that fails on a
		// runtime sandbox.
		if r.ContentLength != 0 {
			newBin = processRpc.DefaultUpgradeBinPath
			// On a chained upgrade this envd is itself running from
			// DefaultUpgradeBinPath, so opening it O_TRUNC would fail with ETXTBSY.
			// Unlink first: the create then lands on a fresh inode while the
			// running process keeps executing its now-unlinked image.
			_ = os.Remove(processRpc.DefaultUpgradeBinPath)
			f, err := os.OpenFile(processRpc.DefaultUpgradeBinPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}
			if _, err := io.Copy(f, r.Body); err != nil {
				f.Close()
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}
			f.Close()
		}
		if err := doUpgrade(newBin); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	err := s.ListenAndServe()
	// Signal goroutines to stop before deferred cleanup closes their resources.
	// TODO: shutdown synchronization needs to be revisited.
	cancel()

	return err
}

func createCgroupManager() (m cgroups.Manager) {
	defer func() {
		if m == nil {
			fmt.Fprintf(os.Stderr, "falling back to no-op cgroup manager\n")
			m = cgroups.NewNoopManager()
		}
	}()

	// Explicit opt-out: callers can tell envd to skip cgroup setup entirely.
	// We return the no-op manager directly without touching /sys/fs/cgroup
	// so we don't log spurious errors.
	if noCgroups {
		fmt.Fprintf(os.Stderr, "cgroups disabled via --no-cgroups; using no-op cgroup manager\n")

		return cgroups.NewNoopManager()
	}

	metrics, err := host.GetMetrics()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to calculate host metrics: %v\n", err)

		return nil
	}

	// try to keep 1/8 of the memory free, but no more than 128 MB
	maxMemoryReserved := min(metrics.MemTotal/8, uint64(128)*megabyte)
	memoryMax := metrics.MemTotal - maxMemoryReserved
	memoryHigh := memoryMax // same as memory.max — OOM-kill immediately when throttling can't reclaim enough

	opts := []cgroups.Cgroup2ManagerOption{
		cgroups.WithCgroup2ProcessType(cgroups.ProcessTypePTY, "ptys", map[string]string{
			"cpu.weight":  "200",
			"io.weight":   "default 50",
			"memory.high": fmt.Sprintf("%d", memoryHigh),
			"memory.max":  fmt.Sprintf("%d", memoryMax),
		}),
		cgroups.WithCgroup2ProcessType(cgroups.ProcessTypeSocat, "socats", map[string]string{
			"cpu.weight": "150",
			"io.weight":  "default 50",
			"memory.min": fmt.Sprintf("%d", 5*megabyte),
			"memory.low": fmt.Sprintf("%d", 8*megabyte),
		}),
		cgroups.WithCgroup2ProcessType(cgroups.ProcessTypeUser, "user", map[string]string{
			"memory.high": fmt.Sprintf("%d", memoryHigh),
			"memory.max":  fmt.Sprintf("%d", memoryMax),
			"cpu.weight":  "50",
			"io.weight":   "default 10",
		}),
	}
	if cgroupRoot != "" {
		opts = append(opts, cgroups.WithCgroup2RootSysFSPath(cgroupRoot))
	}

	mgr, err := cgroups.NewCgroup2Manager(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create cgroup2 manager: %v\n", err)

		return nil
	}

	return mgr
}
