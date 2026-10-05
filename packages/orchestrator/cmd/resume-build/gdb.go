package main

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/artifact"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// gdbOptions controls the `resume-build -gdb` guest-kernel debugging flow.
type gdbOptions struct {
	enabled bool

	// Artifact overrides. By default resume-build fetches the debug artifacts by
	// version (firecracker-debug and vmlinux.debug) from the release buckets, the
	// same way create-build fetches the prod kernel/FC. These override the fetch with
	// a local path: a Firecracker built `--features gdb` and the kernel's split DWARF
	// symbols (vmlinux.debug).
	fcBinary string // -gdb-fc
	symbols  string // -gdb-symbols

	socket   string // -gdb-socket (optional; default: a temp path)
	execCmds string // -gdb-exec   (scripted mode: gdb commands, newline/';'-separated)
	script   string // -gdb-script (scripted mode: a gdb command file)
}

func (o gdbOptions) scripted() bool { return o.execCmds != "" || o.script != "" }

// gdbMode resumes the snapshot with Firecracker's gdb stub armed (held at the entry
// breakpoint), loads the guest kernel symbols, hands a ready gdb session to the
// caller, and tears everything down on exit.
func (r *runner) gdbMode(ctx context.Context, opts gdbOptions) error {
	// 1. Fail fast, before we touch the sandbox or fetch anything: gdb must be on
	//    PATH (checked first so we don't download a ~500 MB vmlinux.debug only to
	//    fall over here), then resolve the debug artifacts — the -gdb-* overrides if
	//    given, else a local staged copy, else fetch them by version.
	if _, err := exec.LookPath("gdb"); err != nil {
		return fmt.Errorf("gdb not found on PATH: %w", err)
	}
	fcBinary, symbols, err := r.gdbResolveArtifacts(opts)
	if err != nil {
		return err
	}

	// 2. The kernel image runs at its link-time base — there is no KASLR slide to
	//    recover. Firecracker boots the uncompressed vmlinux ELF directly (no bzImage
	//    decompressor), so the image-KASLR relocation never runs and both
	//    CONFIG_RANDOMIZE_BASE and CONFIG_RANDOMIZE_MEMORY stay inert (gated on a flag
	//    only the decompressor sets). So we load symbols at offset 0.

	// 3. Stage the gdb-enabled Firecracker into the writable temp FirecrackerVersionsDir
	//    that run() points the factory at for gdb mode. The factory (and thus the launch)
	//    resolves FC from this dir at resume, so we never overwrite the prod binary in the
	//    real versions dir — which on cluster nodes is a read-only gcsfuse mount where the
	//    old in-place swap failed. The kernel dir is untouched.
	stagedFC := filepath.Join(r.config.FirecrackerVersionsDir, r.sbxConfig.FirecrackerConfig.FirecrackerVersion, utils.TargetArch(), artifact.FirecrackerBinaryName)
	if err := os.MkdirAll(filepath.Dir(stagedFC), 0o755); err != nil {
		return fmt.Errorf("gdb fc staging dir: %w", err)
	}
	if err := copyFile(fcBinary, stagedFC, 0o755); err != nil {
		return fmt.Errorf("stage debug firecracker: %w", err)
	}
	fcPath := r.sbxConfig.FirecrackerConfig.FirecrackerPath(r.config)

	// Backstop: confirm the binary we are about to launch is actually gdb-enabled. This
	// guards both a stale/wrong firecracker-debug and any future regression that resolves
	// FC from somewhere other than the staging dir — otherwise FC starts but never opens
	// the stub, surfacing only as an opaque "gdb socket never bound" later.
	if ok, gdbErr := fileContainsGdbStub(fcPath); gdbErr != nil {
		return fmt.Errorf("check staged firecracker: %w", gdbErr)
	} else if !ok {
		return fmt.Errorf("firecracker to launch (%s) is not gdb-enabled (no FIRECRACKER_GDB_SOCKET); "+
			"it must be built with --features gdb (see fc-versions build.sh)", fcPath)
	}

	// 4. Arm the stub via the env var (FC inherits resume-build's env; no jailer
	//    here), and tell the resume path not to wait for envd — the guest never
	//    boots it while held at the entry breakpoint.
	socket := opts.socket
	if socket == "" {
		socket = filepath.Join(os.TempDir(), fmt.Sprintf("fc-gdb-%d.sock", time.Now().UnixNano()))
	}
	_ = os.Remove(socket)
	if err := os.Setenv("FIRECRACKER_GDB_SOCKET", socket); err != nil {
		return fmt.Errorf("set FIRECRACKER_GDB_SOCKET: %w", err)
	}
	r.sbxConfig.SkipEnvdWait = true

	// 5. Resume concurrently. Firecracker's gdb stub holds the snapshot load open
	//    until a debugger attaches, so ResumeSandbox does not return until we connect
	//    gdb. Run it in the background and connect gdb once FC binds the socket; doing
	//    it the other way around (resume, then connect) deadlocks.
	runtime := sandboxtypes.RuntimeMetadata{
		TemplateID:  r.buildID,
		TeamID:      "local",
		SandboxID:   fmt.Sprintf("sbx-gdb-%d", time.Now().UnixNano()),
		ExecutionID: fmt.Sprintf("exec-gdb-%d", time.Now().UnixNano()),
	}
	fmt.Println("🚀 Resuming under gdb (guest held at entry breakpoint)...")
	t0 := time.Now()

	type startResult struct {
		sbx *sandbox.Sandbox
		err error
	}
	resumeCtx, cancelResume := context.WithCancel(ctx)
	startCh := make(chan startResult, 1)
	go func() {
		sbx, err := r.startSandbox(resumeCtx, runtime, t0, t0.Add(24*time.Hour))
		startCh <- startResult{sbx: sbx, err: err}
	}()
	defer func() {
		// Unblock ResumeSandbox if it is still waiting (e.g. gdb never connected),
		// then reclaim the sandbox once it returns.
		cancelResume()
		res := <-startCh
		if res.sbx != nil {
			fmt.Println("🧹 Cleanup...")
			res.sbx.Close(context.WithoutCancel(ctx))
		}
		_ = os.Remove(socket)
	}()

	// FC binds the gdb socket while loading the snapshot — before the load blocks on
	// the debugger — so the socket appears even though startSandbox is still running.
	// Race the socket wait against the resume result: if the resume fails before the
	// socket appears, surface that real error instead of a misleading socket timeout.
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	socketErr := make(chan error, 1)
	go func() { socketErr <- waitForSocket(waitCtx, socket, 90*time.Second) }()

	select {
	case res := <-startCh:
		startCh <- res // hand back to the deferred cleanup
		if res.err != nil {
			return fmt.Errorf("resume: %w", res.err)
		}

		return fmt.Errorf("resume completed without binding gdb socket %s", socket)
	case err := <-socketErr:
		if err != nil {
			return fmt.Errorf("gdb socket %s never appeared: %w", socket, err)
		}
	}
	fmt.Printf("✅ FC bound gdb socket in %s\n", time.Since(t0))

	// 6. Generate the parameterized init script and print the debug-context block.
	initScript, err := writeInitScript(symbols, socket)
	if err != nil {
		return fmt.Errorf("write gdb init script: %w", err)
	}
	defer os.Remove(initScript)

	printGdbContext(fcPath, r.sbxConfig.FirecrackerConfig.FirecrackerVersion, symbols, socket, initScript)

	// 7. Drive gdb. FC's stub shuts the VM down on disconnect, so this is one session
	//    per invocation; teardown (defer above) reclaims FC/UFFD/NBD/temp.
	return runGdb(ctx, initScript, opts)
}

// gdbResolveArtifacts resolves the debug FC binary and the vmlinux.debug symbols. Each
// is taken from its -gdb-* override if set, else a local copy next to the snapshot's FC /
// kernel — where the fc-versions/fc-kernels buckets, and copy-build -gdb, place them. The
// artifacts are not fetched over the network.
func (r *runner) gdbResolveArtifacts(opts gdbOptions) (fcBinary, symbols string, err error) {
	fcVer := r.sbxConfig.FirecrackerConfig.FirecrackerVersion
	kernelVer := r.sbxConfig.FirecrackerConfig.KernelVersion
	// Resolve the debug artifacts from the ORIGINAL versions dir: in gdb mode run() points
	// the runner's FirecrackerVersionsDir at a writable temp staging dir, but the published
	// firecracker-debug lives in the original (read-only) dir. The kernel dir is not
	// overridden.
	fcVersionsDir := r.config.FirecrackerVersionsDir
	if r.gdbOrigVersionsDir != "" {
		fcVersionsDir = r.gdbOrigVersionsDir
	}
	// Prefer the arch-prefixed layout (where releases and copy-build -gdb publish), falling
	// back to the legacy flat layout — independently of FirecrackerPath/HostKernelPath,
	// which resolve the prod binary and may sit in a different layout on un-migrated nodes.
	fcBinary, fcErr := resolveLocal(opts.fcBinary, archOrLegacyArtifact(fcVersionsDir, fcVer, "firecracker-debug"))
	symbols, symErr := resolveLocal(opts.symbols, archOrLegacyArtifact(r.config.HostKernelsDir, kernelVer, "vmlinux.debug"))

	var missing []string
	if fcErr != nil {
		missing = append(missing, fmt.Sprintf("firecracker-debug (FC %s): %v", fcVer, fcErr))
	}
	if symErr != nil {
		missing = append(missing, fmt.Sprintf("vmlinux.debug (kernel %s): %v", kernelVer, symErr))
	}
	if len(missing) > 0 {
		return "", "", fmt.Errorf(
			"could not find gdb debug artifacts locally:\n  - %s\n"+
				"firecracker-debug must sit next to the snapshot's firecracker, and vmlinux.debug\n"+
				"next to its vmlinux.bin (the fc-versions/fc-kernels buckets; copy-build -gdb stages\n"+
				"them). Otherwise pass -gdb-fc / -gdb-symbols explicitly",
			strings.Join(missing, "\n  - "))
	}

	return fcBinary, symbols, nil
}

// archOrLegacyArtifact returns <base>/<ver>/<arch>/<file> when it exists, else the legacy
// flat <base>/<ver>/<file>, mirroring FirecrackerPath/HostKernelPath so a debug artifact
// resolves under either layout (releases and copy-build -gdb publish it arch-prefixed).
func archOrLegacyArtifact(base, ver, file string) string {
	if archPath := filepath.Join(base, ver, utils.TargetArch(), file); fileExists(archPath) {
		return archPath
	}

	return filepath.Join(base, ver, file)
}

// resolveLocal returns the -gdb-* override if set (erroring if it does not exist),
// otherwise the local copy if present, else an error. Artifacts are not fetched.
func resolveLocal(override, localPath string) (string, error) {
	if override != "" {
		if fileExists(override) {
			return override, nil
		}

		return "", fmt.Errorf("override path %s does not exist", override)
	}
	if fileExists(localPath) {
		return localPath, nil
	}

	return "", fmt.Errorf("not present at %s", localPath)
}

// writeInitScript generates the parameterized gdb init script: load the versioned
// macro library, the symbols (at their link-time addresses — see gdbMode: FC boots the
// vmlinux ELF directly, so there is no KASLR image slide), and connect to the stub.
func writeInitScript(symbols, socket string) (string, error) {
	macros, err := macroLibContent()
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp("", "fc-debug-init-*.gdb")
	if err != nil {
		return "", err
	}
	// Inline the macros rather than `source`-ing a separate file: the init script is the
	// only temp file, and it is removed on exit (see gdbMode), so nothing leaks.
	if _, err := fmt.Fprintf(f, `set pagination off
set confirm off
%s
# FC boots the uncompressed vmlinux ELF directly, so KASLR never relocates the image:
# symbols sit at their link-time addresses (offset 0).
add-symbol-file %s -o 0x0
# FC binds the gdb socket while still loading the snapshot, so its first packet
# ack can lag past gdb's 2s default. That makes gdb retransmit qSupported, the
# stub double-replies, and the reply stream desyncs (gdb aborts with
# "Remote replied unexpectedly to 'vMustReplyEmpty'"). Raise the timeout so gdb
# does not prematurely retransmit during connect.
set remotetimeout 120
target remote %s
`, macros, symbols, socket); err != nil {
		f.Close()
		_ = os.Remove(f.Name())

		return "", fmt.Errorf("write gdb init script: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())

		return "", fmt.Errorf("close gdb init script: %w", err)
	}

	return f.Name(), nil
}

// fcDebugMacros is the checked-in gdb macro library, embedded so a standalone binary
// is self-contained (resume-build is typically scp'd to a node away from its source).
//
//go:embed fc-debug.gdb
var fcDebugMacros string

// macroLibContent returns the fc-debug.gdb macro definitions: a copy colocated with the
// binary if present (lets you iterate on macros without rebuilding), otherwise the
// embedded copy. The caller inlines this into the init script, so no temp file is made.
func macroLibContent() (string, error) {
	if exe, err := os.Executable(); err == nil {
		if p := filepath.Join(filepath.Dir(exe), "fc-debug.gdb"); fileExists(p) {
			b, err := os.ReadFile(p)
			if err != nil {
				return "", err
			}

			return string(b), nil
		}
	}

	return fcDebugMacros, nil
}

// printGdbContext prints the debug-context block so the session is drivable any way
// (interactive, batch, or a long-lived agent-driven gdb subprocess).
func printGdbContext(fcPath, fcVer, symbols string, socket, initScript string) {
	fmt.Println("\n──────────────── gdb debug context ────────────────")
	fmt.Printf("  debug firecracker : %s (version %s)\n", fcPath, fcVer)
	fmt.Printf("  kernel symbols    : %s (link addresses; FC ELF boot, no KASLR slide)\n", symbols)
	fmt.Printf("  gdb socket        : %s\n", socket)
	fmt.Printf("  gdb init script   : %s\n", initScript)
	fmt.Printf("  attach manually   : gdb -q -x %s\n", initScript)
	fmt.Println("  macros            : fc-faults [N], fc-curr, fc-task <p>, fc-regions, fc-va <phys>")
	fmt.Println("────────────────────────────────────────────────────")
}

// runGdb runs gdb against the generated init script: interactive by default, or
// batch when -gdb-exec / -gdb-script is given (the agent/CI path).
func runGdb(ctx context.Context, initScript string, opts gdbOptions) error {
	var args []string
	if opts.scripted() {
		args = append(args, "-batch")
	} else {
		args = append(args, "-q")
	}
	args = append(args, "-x", initScript)
	if opts.script != "" {
		args = append(args, "-x", opts.script)
	}
	for _, line := range strings.FieldsFunc(opts.execCmds, func(r rune) bool { return r == '\n' || r == ';' }) {
		if cmd := strings.TrimSpace(line); cmd != "" {
			args = append(args, "-ex", cmd)
		}
	}

	cmd := exec.CommandContext(ctx, "gdb", args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if !opts.scripted() {
		fmt.Printf("🐞 launching gdb (VM shuts down on disconnect)...\n\n")
	}

	return cmd.Run()
}

// --- small helpers ---

func fileExists(p string) bool {
	info, err := os.Stat(p)

	return err == nil && !info.IsDir()
}

// fileContainsGdbStub reports whether the binary at path was built with the gdb feature,
// detected by the FIRECRACKER_GDB_SOCKET env-var literal — present iff the
// #[cfg(feature = "gdb")] code is compiled in, and it survives stripping.
func fileContainsGdbStub(path string) (bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	return bytes.Contains(b, []byte("FIRECRACKER_GDB_SOCKET")), nil
}

func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if info, err := os.Stat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()

		return err
	}

	return out.Close()
}
