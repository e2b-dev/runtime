package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/creack/pty"
	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

const (
	defaultNice      = 0
	defaultOomScore  = 100
	defaultIoClass   = 2 // ionice best-effort
	defaultIoPrio    = 4
	outputBufferSize = 64
	systemTag        = "_system"
	stdChunkSize     = 32 << 10 // 32 KiB
	ptyChunkSize     = 16 << 10 // 16 KiB
)

type ProcessExit struct {
	Error  *string
	Status string
	Exited bool
	Code   int32
}

type Handler struct {
	Config *rpc.ProcessConfig

	logger *zerolog.Logger

	Tag *string
	cmd *exec.Cmd
	tty *os.File

	// freezer admits the fork. Kept for Start, which forks a non-PTY child after New has
	// returned; see spawn for why the admission has to sit right around the clone.
	freezer *cgroups.WorkloadFreezer

	// childIO are this process's copies of the pipe ends the child inherits. Start closes
	// them once it is done with the fork -- refused, failed or started: after a fork the
	// child holds its own copies, and without one nothing ever will -- so closing them is
	// what lets the readers see EOF either way. See newChildPipes.
	childIO []*os.File

	cancel context.CancelFunc

	outCtx    context.Context //nolint:containedctx // todo: refactor so this can be removed
	outCancel context.CancelFunc

	stdinMu sync.Mutex
	stdin   io.WriteCloser

	stdoutBytes atomic.Int64
	stderrBytes atomic.Int64
	ptyBytes    atomic.Int64

	DataEvent *MultiplexedChannel[rpc.ProcessEvent_Data]
	EndEvent  *MultiplexedChannel[rpc.ProcessEvent_End]

	// --- live-upgrade handover ---
	// pid is stored at Start so it survives an envd self-upgrade where cmd is
	// gone (a re-adopted handler has cmd == nil). cgType records the cgroup the
	// child runs in. stdoutF/stderrF/stdinF are the raw pipe fds captured at
	// New() so they can be carried across execve; tty is the PTY master.
	pid       uint32
	cgType    cgroups.ProcessType
	readopted bool
	stdoutF   *os.File
	stderrF   *os.File
	stdinF    *os.File
	// deadline is the process's timeout deadline (zero = no timeout). Captured
	// so it can be carried across a live-upgrade and re-armed on the new envd.
	// deadlineMu guards it: the readopt reaper writes it asynchronously once the
	// workload thaws, while Deadline() may be read concurrently by a handover.
	deadlineMu sync.Mutex
	deadline   time.Time
	// readoptTimeout is the remaining timeout carried across a live-upgrade,
	// re-armed when BeginReaping is called.
	readoptTimeout time.Duration
	// thawed, if non-nil (re-adopted handlers only), is closed when the workload
	// is unfrozen after the upgrade; the carried kill-timer waits on it so the
	// timeout is not burned down while the process is still frozen.
	thawed <-chan struct{}
	// OnExit, if set, is invoked by the re-adopt reaper with the terminal event
	// immediately before EndEvent is closed, so the service can retain the exit
	// synchronously. A Connect that forks after the close then always finds the
	// retained exit in the cache rather than racing an asynchronous retain.
	OnExit func(*rpc.ProcessEvent_EndEvent)
}

// This method must be called only after the process has been started
func (p *Handler) Pid() uint32 {
	if p.cmd != nil && p.cmd.Process != nil {
		return uint32(p.cmd.Process.Pid)
	}

	return p.pid
}

// CgType returns the cgroup type the child was placed in (for handover).
func (p *Handler) CgType() cgroups.ProcessType { return p.cgType }

// Deadline returns the process's timeout deadline and whether one is set, so a
// live-upgrade handover can carry the remaining timeout.
func (p *Handler) Deadline() (time.Time, bool) {
	p.deadlineMu.Lock()
	d := p.deadline
	p.deadlineMu.Unlock()
	if d.IsZero() {
		return time.Time{}, false
	}

	return d, true
}

// setDeadline records the process's timeout deadline under deadlineMu.
func (p *Handler) setDeadline(t time.Time) {
	p.deadlineMu.Lock()
	p.deadline = t
	p.deadlineMu.Unlock()
}

// HandoverFds returns the raw fds to carry across an envd self-upgrade:
// stdout/stderr read ends, stdin write end, and the PTY master. Absent fds
// are -1. The fds remain owned by the Handler.
func (p *Handler) HandoverFds() (stdout, stderr, stdin, tty int) {
	fd := func(f *os.File) int {
		if f == nil {
			return -1
		}

		return int(f.Fd())
	}

	return fd(p.stdoutF), fd(p.stderrF), fd(p.stdinF), fd(p.tty)
}

// userCommand returns a human-readable representation of the user's original command,
// without the internal OOM/nice wrapper that is prepended to the actual exec.
func (p *Handler) userCommand() string {
	return strings.Join(append([]string{p.Config.GetCmd()}, p.Config.GetArgs()...), " ")
}

// currentNice returns the nice value of the current process.
func currentNice() int {
	prio, err := syscall.Getpriority(syscall.PRIO_PROCESS, 0)
	if err != nil {
		return 0
	}

	// Getpriority returns 20 - nice on Linux.
	return 20 - prio
}

// ioniceNicePrefix builds the ionice/nice part of the process wrapper from
// whatever the image actually ships. Both are util-linux/coreutils
// conveniences that minimal and busybox-based images (Alpine, UBI) may lack or
// keep elsewhere than /usr/bin — a missing helper must degrade to running the
// command without that priority adjustment, never to a failed spawn (exit 127
// killed every process on such images). lookPath is injected for testability;
// production passes exec.LookPath.
func ioniceNicePrefix(ioClass, ioPrio, niceDelta int, lookPath func(string) (string, error)) string {
	prefix := ""
	if p, err := lookPath("ionice"); err == nil {
		prefix += fmt.Sprintf("%s -c %d -n %d ", p, ioClass, ioPrio)
	}
	if p, err := lookPath("nice"); err == nil {
		prefix += fmt.Sprintf("%s -n %d ", p, niceDelta)
	}

	return prefix
}

func New(
	ctx context.Context,
	user *user.User,
	req *rpc.StartRequest,
	logger *zerolog.Logger,
	defaults *execcontext.Defaults,
	freezer *cgroups.WorkloadFreezer,
	cancel context.CancelFunc,
) (*Handler, error) {
	// User command string for logging (without the internal wrapper details).
	userCmd := strings.Join(append([]string{req.GetProcess().GetCmd()}, req.GetProcess().GetArgs()...), " ")

	// Wrap in a shell that resets oom_score_adj, ioprio (ionice best-effort/4),
	// and nice. The oom_score_adj write is pure /proc and always applied; the
	// priority helpers are used only where the image provides them.
	niceDelta := defaultNice - currentNice()
	oomWrapperScript := fmt.Sprintf(`echo %d > /proc/$$/oom_score_adj && exec %s"${@}"`, defaultOomScore, ioniceNicePrefix(defaultIoClass, defaultIoPrio, niceDelta, exec.LookPath))
	wrapperArgs := append([]string{"-c", oomWrapperScript, "--", req.GetProcess().GetCmd()}, req.GetProcess().GetArgs()...)
	cmd := exec.CommandContext(ctx, "/bin/sh", wrapperArgs...)

	uid, gid, err := permissions.GetUserIdUints(user)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	groups := []uint32{gid}
	if gids, err := user.GroupIds(); err != nil {
		logger.Warn().Err(err).Str("user", user.Username).Msg("failed to get supplementary groups")
	} else {
		for _, g := range gids {
			if parsed, err := strconv.ParseUint(g, 10, 32); err == nil {
				groups = append(groups, uint32(parsed))
			}
		}
	}

	cgroupFD, ok := freezer.Manager().GetFileDescriptor(getProcType(req))

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    uid,
			Gid:    gid,
			Groups: groups,
		},
	}
	configureProcessGroup(cmd.SysProcAttr, req.GetPty() != nil)
	applyCgroupFD(cmd.SysProcAttr, cgroupFD, ok)

	resolvedPath, err := permissions.ExpandAndResolve(req.GetProcess().GetCwd(), user, defaults.Workdir)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Check if the cwd resolved path exists
	if _, err := os.Stat(resolvedPath); errors.Is(err, os.ErrNotExist) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("cwd '%s' does not exist", resolvedPath))
	}

	cmd.Dir = resolvedPath

	var formattedVars []string

	// Take only 'PATH' variable from the current environment
	// The 'PATH' should ideally be set in the environment
	formattedVars = append(formattedVars, "PATH="+os.Getenv("PATH"))
	formattedVars = append(formattedVars, "HOME="+user.HomeDir)
	formattedVars = append(formattedVars, "USER="+user.Username)
	formattedVars = append(formattedVars, "LOGNAME="+user.Username)

	// Add the environment variables from the global environment
	if defaults.EnvVars != nil {
		for key, value := range defaults.EnvVars.All() {
			formattedVars = append(formattedVars, key+"="+value)
		}
	}

	// Only the last values of the env vars are used - this allows for overwriting defaults
	for key, value := range req.GetProcess().GetEnvs() {
		formattedVars = append(formattedVars, key+"="+value)
	}

	cmd.Env = formattedVars

	outMultiplex := NewMultiplexedChannel[rpc.ProcessEvent_Data](outputBufferSize)

	var outWg sync.WaitGroup

	// Create a context for waiting for and cancelling output pipes.
	// Cancellation of the process via timeout will propagate and cancel this context too.
	outCtx, outCancel := context.WithCancel(ctx)

	h := &Handler{
		Config:    req.GetProcess(),
		cmd:       cmd,
		Tag:       req.Tag,
		DataEvent: outMultiplex,
		cancel:    cancel,
		outCtx:    outCtx,
		outCancel: outCancel,
		EndEvent:  NewMultiplexedChannel[rpc.ProcessEvent_End](0),
		logger:    logger,
		freezer:   freezer,
	}
	h.cgType = getProcType(req)

	// Capture the process timeout deadline (if any) so it can be carried across
	// a live-upgrade and re-armed on the new envd.
	if d, ok := ctx.Deadline(); ok {
		h.setDeadline(d)
	}

	if req.GetPty() != nil {
		// The pty should ideally start only in the Start method, but the package does not support that and we would have to code it manually.
		// The output of the pty should correctly be passed though.
		var tty *os.File
		err := h.spawn(ctx, func() (err error) {
			tty, err = pty.StartWithSize(cmd, &pty.Winsize{
				Cols: uint16(req.GetPty().GetSize().GetCols()),
				Rows: uint16(req.GetPty().GetSize().GetRows()),
			})

			return err
		})
		if err != nil {
			// Neither fan-out loop has anything left to deliver, and nothing else will ever
			// close their sources: the readers and their closer below are never started, and
			// Wait is never reached.
			outCancel()
			close(outMultiplex.Source)
			close(h.EndEvent.Source)

			startErr := fmt.Errorf("error starting pty with command '%s' in dir '%s' with '%d' cols and '%d' rows: %w", userCmd, cmd.Dir, req.GetPty().GetSize().GetCols(), req.GetPty().GetSize().GetRows(), err)

			return nil, connect.NewError(StartErrorCode(startErr), startErr)
		}

		outWg.Go(func() {
			readBuf := make([]byte, ptyChunkSize)

			for {
				n, readErr := tty.Read(readBuf)

				if n > 0 {
					h.ptyBytes.Add(int64(n))

					if outMultiplex.HasSubscribers() {
						data := slices.Clone(readBuf[:n])

						outMultiplex.Source <- rpc.ProcessEvent_Data{
							Data: &rpc.ProcessEvent_DataEvent{
								Output: &rpc.ProcessEvent_DataEvent_Pty{
									Pty: data,
								},
							},
						}
					}
				}

				if errors.Is(readErr, io.EOF) || errors.Is(readErr, syscall.EIO) {
					break
				}

				if readErr != nil {
					logger.Error().Err(readErr).Msg("error reading from pty")

					break
				}
			}
		})

		h.tty = tty
	} else {
		// The pipes are created here rather than through exec.Cmd's StdoutPipe and friends,
		// because only cmd.Start and cmd.Wait close those, so a start that never reaches
		// cmd.Start would leak their fds and the readers below. Owning them lets Start release
		// them on every outcome -- including a start refused while the workload is frozen for
		// a pause, which a client polling through the pause meets on every attempt.
		pipes, err := newChildPipes(req.Stdin == nil || req.GetStdin())
		if err != nil {
			// As for a failed PTY start: nothing will ever feed or close the fan-out sources.
			outCancel()
			close(outMultiplex.Source)
			close(h.EndEvent.Source)

			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error creating stdio pipes for command '%s': %w", userCmd, err))
		}

		cmd.Stdout, cmd.Stderr = pipes.stdoutW, pipes.stderrW
		// Captured for live-upgrade handover.
		h.stdoutF, h.stderrF = pipes.stdoutR, pipes.stderrR
		h.childIO = []*os.File{pipes.stdoutW, pipes.stderrW}
		// For backwards compatibility we still set the stdin if not explicitly disabled.
		// If stdin is disabled, the process will use /dev/null as stdin.
		if pipes.stdinR != nil {
			cmd.Stdin = pipes.stdinR
			h.stdin, h.stdinF = pipes.stdinW, pipes.stdinW
			h.childIO = append(h.childIO, pipes.stdinR)
		}

		stdout, stderr := pipes.stdoutR, pipes.stderrR

		// Each reader closes its own read end once the stream ends: at EOF nothing more can
		// arrive, and nobody else closes it now that exec.Cmd does not own the pipe.
		outWg.Go(func() {
			defer stdout.Close()

			readBuf := make([]byte, stdChunkSize)

			for {
				n, readErr := stdout.Read(readBuf)

				if n > 0 {
					h.stdoutBytes.Add(int64(n))

					if outMultiplex.HasSubscribers() {
						data := slices.Clone(readBuf[:n])

						outMultiplex.Source <- rpc.ProcessEvent_Data{
							Data: &rpc.ProcessEvent_DataEvent{
								Output: &rpc.ProcessEvent_DataEvent_Stdout{
									Stdout: data,
								},
							},
						}
					}
				}

				if errors.Is(readErr, io.EOF) {
					break
				}

				if readErr != nil {
					logger.Error().Err(readErr).Msg("error reading from stdout")

					break
				}
			}
		})

		outWg.Go(func() {
			defer stderr.Close()

			readBuf := make([]byte, stdChunkSize)

			for {
				n, readErr := stderr.Read(readBuf)

				if n > 0 {
					h.stderrBytes.Add(int64(n))

					if outMultiplex.HasSubscribers() {
						data := slices.Clone(readBuf[:n])

						outMultiplex.Source <- rpc.ProcessEvent_Data{
							Data: &rpc.ProcessEvent_DataEvent{
								Output: &rpc.ProcessEvent_DataEvent_Stderr{
									Stderr: data,
								},
							},
						}
					}
				}

				if errors.Is(readErr, io.EOF) {
					break
				}

				if readErr != nil {
					logger.Error().Err(readErr).Msg("error reading from stderr")

					break
				}
			}
		})
	}

	go func() {
		outWg.Wait()

		close(outMultiplex.Source)

		outCancel()
	}()

	return h, nil
}

func getProcType(req *rpc.StartRequest) cgroups.ProcessType {
	if req != nil && req.GetTag() == systemTag {
		return cgroups.ProcessTypeSystem
	}

	if req != nil && req.GetPty() != nil {
		return cgroups.ProcessTypePTY
	}

	return cgroups.ProcessTypeUser
}

func configureProcessGroup(attr *syscall.SysProcAttr, hasPTY bool) {
	// PTY startup creates a new session (and therefore a new process group).
	// Non-PTY commands need an explicit process group so callers can opt into
	// signalling the command and its descendants without affecting envd.
	if !hasPTY {
		attr.Setpgid = true
	}
}

func (p *Handler) SendSignal(signal syscall.Signal, descendants bool) error {
	pid := int(p.Pid())
	if pid == 0 {
		return errors.New("process not started")
	}

	var err error
	switch {
	case descendants:
		pgid, groupErr := syscall.Getpgid(pid)
		if groupErr != nil {
			return fmt.Errorf("get process group for pid %d: %w", pid, groupErr)
		}
		if pgid != pid {
			return fmt.Errorf("process %d does not own a process group", pid)
		}

		err = syscall.Kill(-pgid, signal)
	case p.cmd == nil:
		// Re-adopted handler (post live-upgrade): no cmd, signal by stored pid.
		err = syscall.Kill(pid, signal)
	default:
		err = p.cmd.Process.Signal(signal)
	}
	if err != nil {
		return err
	}

	// Keep delivering output when signal validation or delivery fails. Once a terminal signal has
	// actually been sent, stop the pumps promptly instead of waiting for every inherited pipe to close.
	if signal == syscall.SIGKILL || signal == syscall.SIGTERM {
		p.outCancel()
	}

	return nil
}

func (p *Handler) ResizeTty(size *pty.Winsize) error {
	if p.tty == nil {
		return errors.New("tty not assigned to process")
	}

	return pty.Setsize(p.tty, size)
}

func (p *Handler) WriteStdin(data []byte) error {
	if p.tty != nil {
		return errors.New("tty assigned to process — input should be written to the pty, not the stdin")
	}

	p.stdinMu.Lock()
	defer p.stdinMu.Unlock()

	if p.stdin == nil {
		return errors.New("stdin not enabled or closed")
	}

	_, err := p.stdin.Write(data)
	if err != nil {
		return fmt.Errorf("error writing to stdin of process '%d': %w", p.Pid(), err)
	}

	return nil
}

// CloseStdin closes the stdin pipe to signal EOF to the process.
// Only works for non-PTY processes.
func (p *Handler) CloseStdin() error {
	if p.tty != nil {
		return errors.New("cannot close stdin for PTY process — send Ctrl+D (0x04) instead")
	}

	p.stdinMu.Lock()
	defer p.stdinMu.Unlock()

	if p.stdin == nil {
		return nil
	}

	err := p.stdin.Close()
	// We still set the stdin to nil even on error as there are no errors,
	// for which it is really safe to retry close across all distributions.
	p.stdin = nil

	return err
}

func (p *Handler) WriteTty(data []byte) error {
	if p.tty == nil {
		return errors.New("tty not assigned to process — input should be written to the stdin, not the tty")
	}

	_, err := p.tty.Write(data)
	if err != nil {
		return fmt.Errorf("error writing to tty of process '%d': %w", p.Pid(), err)
	}

	return nil
}

func (p *Handler) Start(ctx context.Context, requestTimeout time.Duration) (uint32, error) {
	// Pty is already started in the New method
	if p.tty == nil {
		err := p.spawn(ctx, p.cmd.Start)
		// Whatever happened, our copies of the child's ends must go: a child that was forked
		// has its own, and one that was refused or failed never will. Either way this is
		// what lets the readers see EOF and exit.
		p.closeChildIO()
		if err != nil {
			// No process will ever read stdin, and Wait, which closes it on the normal path,
			// is never reached for a start that did not happen. Wait is also what closes the
			// end event's source, whose fan-out loop would otherwise run for good. The data
			// event's closes on its own once the readers above see EOF.
			p.closeStdinPipe()
			close(p.EndEvent.Source)

			return 0, fmt.Errorf("error starting process '%s': %w", p.userCommand(), err)
		}
	}

	p.pid = uint32(p.cmd.Process.Pid)

	p.logger.
		Info().
		Str("event_type", "process_start").
		Int("pid", p.cmd.Process.Pid).
		Str("command", p.userCommand()).
		Dur("request_timeout_ms", requestTimeout).
		Msg(fmt.Sprintf("Process with pid %d started", p.cmd.Process.Pid))

	return uint32(p.cmd.Process.Pid), nil
}

// spawn runs fork, admitted by the freezer for the pre-spawn probe and the fork.
//
// A process placed into a frozen cgroup with clone3(CLONE_INTO_CGROUP) never reaches
// execve, and Go's fork/exec carries CLONE_VFORK, so the forking thread blocks in the
// kernel without giving up its P -- and the next garbage-collection stop-the-world stops
// every goroutine in envd behind it, the resume thaw included. The freezer refuses the
// spawn while such a freeze is in effect, and holds its sweep off until the fork has
// returned. See cgroups.WorkloadFreezer.BeginSpawn.
//
// Scoped to the fork itself, not to everything that prepares one: a freeze waits for every
// admitted spawn, so anything held inside -- a user-database lookup, a stat of a cwd on a
// network mount -- would be charged to the pause's drain budget.
func (p *Handler) spawn(ctx context.Context, fork func() error) error {
	release, err := p.freezer.BeginSpawn(ctx, p.cgType)
	if err != nil {
		return err
	}
	defer release()

	return fork()
}

// closeChildIO closes this process's copies of the child's pipe ends. Idempotent.
func (p *Handler) closeChildIO() {
	for _, f := range p.childIO {
		_ = f.Close()
	}
	p.childIO = nil
}

// closeStdinPipe closes the stdin write end if CloseStdin has not already. The handover
// copy in stdinF is the same *os.File, whose Close is idempotent.
func (p *Handler) closeStdinPipe() {
	p.stdinMu.Lock()
	defer p.stdinMu.Unlock()

	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
}

func (p *Handler) Wait() {
	// Wait for the output pipes to be closed or cancelled.
	<-p.outCtx.Done()

	err := p.cmd.Wait()

	p.tty.Close()
	// The handler owns the stdio pipes, so it closes them here as exec.Cmd.Wait closes the
	// ones it creates. The read ends matter on a kill or a timeout: a grandchild still
	// holding the write ends would otherwise keep the readers -- and the Start stream that
	// waits on them -- open until it exits. A reader that already hit EOF has closed its
	// end, and a second Close is harmless.
	p.closeStdinPipe()
	_ = p.stdoutF.Close()
	_ = p.stderrF.Close()

	var errMsg *string

	if err != nil {
		msg := err.Error()
		errMsg = &msg
	}

	endEvent := &rpc.ProcessEvent_EndEvent{
		Error:    errMsg,
		ExitCode: int32(p.cmd.ProcessState.ExitCode()),
		Exited:   p.cmd.ProcessState.Exited(),
		Status:   p.cmd.ProcessState.String(),
	}

	event := rpc.ProcessEvent_End{
		End: endEvent,
	}

	p.EndEvent.Source <- event
	// Retain the terminal event synchronously — BEFORE closing the source — so a
	// Connect that forks after the close and falls back to the retention cache is
	// guaranteed to find this exit rather than race an asynchronous retain. This
	// mirrors the re-adopt reaper's ordering (readopt.go).
	if p.OnExit != nil {
		p.OnExit(endEvent)
	}
	// Close the source after the terminal event, mirroring the re-adopt reaper.
	// A late Connect that subscribes after the event was fanned out (the process
	// exited during the no-subscriber window) sees the closed channel and falls
	// back to the (now-populated) retention cache instead of blocking forever.
	close(p.EndEvent.Source)

	p.logger.
		Info().
		Str("event_type", "process_end").
		Interface("process_result", endEvent).
		Int64("stdout_bytes", p.stdoutBytes.Load()).
		Int64("stderr_bytes", p.stderrBytes.Load()).
		Int64("pty_bytes", p.ptyBytes.Load()).
		Msg(fmt.Sprintf("Process with pid %d ended", p.cmd.Process.Pid))

	// Ensure the process cancel is called to cleanup resources.
	// As it is called after end event and Wait, it should not affect command execution or returned events.
	p.cancel()
}

// childPipes are the stdio pipes of a non-PTY child. The *W / stdinR ends go to the child;
// stdoutR, stderrR and stdinW stay with envd.
type childPipes struct {
	stdoutR, stdoutW *os.File
	stderrR, stderrW *os.File
	stdinR, stdinW   *os.File
}

// newChildPipes creates every pipe up front, so a failure part-way closes what was created
// before anything reads from it. withStdin leaves stdin nil when disabled, which exec.Cmd
// turns into /dev/null.
func newChildPipes(withStdin bool) (childPipes, error) {
	var (
		p       childPipes
		created []*os.File
	)
	pipe := func(r, w **os.File) error {
		var err error
		if *r, *w, err = os.Pipe(); err != nil {
			return err
		}
		created = append(created, *r, *w)

		return nil
	}

	err := pipe(&p.stdoutR, &p.stdoutW)
	if err == nil {
		err = pipe(&p.stderrR, &p.stderrW)
	}
	if err == nil && withStdin {
		err = pipe(&p.stdinR, &p.stdinW)
	}
	if err != nil {
		for _, f := range created {
			_ = f.Close()
		}

		return childPipes{}, err
	}

	return p, nil
}
