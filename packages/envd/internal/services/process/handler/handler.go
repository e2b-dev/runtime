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
	cgroupManager cgroups.Manager,
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

	cgroupFD, ok := cgroupManager.GetFileDescriptor(getProcType(req))

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    uid,
			Gid:    gid,
			Groups: groups,
		},
	}
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
		tty, err := pty.StartWithSize(cmd, &pty.Winsize{
			Cols: uint16(req.GetPty().GetSize().GetCols()),
			Rows: uint16(req.GetPty().GetSize().GetRows()),
		})
		if err != nil {
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
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error creating stdout pipe for command '%s': %w", userCmd, err))
		}
		if f, ok := stdout.(*os.File); ok {
			h.stdoutF = f // captured for live-upgrade handover
		}

		outWg.Go(func() {
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

		stderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error creating stderr pipe for command '%s': %w", userCmd, err))
		}
		if f, ok := stderr.(*os.File); ok {
			h.stderrF = f // captured for live-upgrade handover
		}

		outWg.Go(func() {
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

		// For backwards compatibility we still set the stdin if not explicitly disabled
		// If stdin is disabled, the process will use /dev/null as stdin
		if req.Stdin == nil || req.GetStdin() == true {
			stdin, err := cmd.StdinPipe()
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error creating stdin pipe for command '%s': %w", userCmd, err))
			}

			h.stdin = stdin
			if f, ok := stdin.(*os.File); ok {
				h.stdinF = f // captured for live-upgrade handover
			}
		}
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

func (p *Handler) SendSignal(signal syscall.Signal) error {
	if signal == syscall.SIGKILL || signal == syscall.SIGTERM {
		p.outCancel()
	}

	// Re-adopted handler (post live-upgrade): no cmd, signal by stored pid.
	if p.cmd == nil {
		if p.pid == 0 {
			return errors.New("process not started")
		}

		return syscall.Kill(int(p.pid), signal)
	}

	if p.cmd.Process == nil {
		return errors.New("process not started")
	}

	return p.cmd.Process.Signal(signal)
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

func (p *Handler) Start(requestTimeout time.Duration) (uint32, error) {
	// Pty is already started in the New method
	if p.tty == nil {
		err := p.cmd.Start()
		if err != nil {
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

func (p *Handler) Wait() {
	// Wait for the output pipes to be closed or cancelled.
	<-p.outCtx.Done()

	err := p.cmd.Wait()

	// Processes started without a PTY have no tty to close.
	if p.tty != nil {
		p.tty.Close()
	}

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
