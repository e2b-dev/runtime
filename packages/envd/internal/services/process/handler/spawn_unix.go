//go:build unix

package handler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

// newCommand builds the child for a start request: the user's command wrapped
// in a shell that resets oom_score_adj, ioprio (ionice best-effort/4) and
// nice, running with the user's credentials inside the request's cgroup. The
// oom_score_adj write is pure /proc and only applied when it raises the
// inherited score; the priority helpers are used only where the image provides
// them.
func newCommand(ctx context.Context, user *user.User, req *rpc.StartRequest, logger *zerolog.Logger, cgroupManager cgroups.Manager) (*exec.Cmd, *sysState, error) {
	niceDelta := defaultNice - currentNice()
	oomWrapperScript := oomScoreAdjPrefix(defaultOomScore, readOwnOomScoreAdj) + fmt.Sprintf(`exec %s"${@}"`, ioniceNicePrefix(defaultIoClass, defaultIoPrio, niceDelta, exec.LookPath))
	wrapperArgs := append([]string{"-c", oomWrapperScript, "--", req.GetProcess().GetCmd()}, req.GetProcess().GetArgs()...)
	cmd := exec.CommandContext(ctx, "/bin/sh", wrapperArgs...)

	uid, gid, err := permissions.GetUserIdUints(user)
	if err != nil {
		return nil, nil, err
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

	return cmd, &sysState{}, nil
}

// oomScoreAdjPrefix returns the shell prefix that raises the child's
// oom_score_adj to target before exec, or "" when the inherited score is
// already at least target. An unprivileged process may only raise its OOM
// score (proc(5)); lowering a score assigned by the container runtime fails,
// and the wrapper's "&&" would turn that into a failed spawn. Keeping the
// higher inherited score also preserves the runtime's OOM protection. If the
// current score cannot be read, the unconditional write is kept, matching the
// previous behavior; /proc/self/oom_score_adj is always readable on Linux.
// readOomScoreAdj is injected for testability; production passes
// readOwnOomScoreAdj.
func oomScoreAdjPrefix(target int, readOomScoreAdj func() (int, error)) string {
	if current, err := readOomScoreAdj(); err == nil && current >= target {
		return ""
	}

	return fmt.Sprintf("echo %d > /proc/$$/oom_score_adj && ", target)
}

// readOwnOomScoreAdj returns envd's oom_score_adj, which its children inherit.
func readOwnOomScoreAdj() (int, error) {
	value, err := os.ReadFile("/proc/self/oom_score_adj")
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(strings.TrimSpace(string(value)))
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

// signalPid delivers a signal to a process that is not this envd's child (a
// re-adopted one after a live upgrade).
func signalPid(pid int, signal syscall.Signal) error {
	return syscall.Kill(pid, signal)
}
