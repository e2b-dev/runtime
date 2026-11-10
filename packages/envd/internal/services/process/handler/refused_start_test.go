//go:build linux

package handler

import (
	"context"
	"os"
	"os/user"
	"runtime"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func openFDs(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)

	return len(entries)
}

// TestStart_RefusedStartLeaksNothing pins that a start refused while the workload is frozen
// gives back everything New set up for it. New creates the stdio pipes and starts their
// readers before the fork is attempted, and a refusal means the fork never is -- so unless
// the refusal path closes them itself, every plain attempt leaves six fds and several
// goroutines behind, and a PTY attempt leaves both of its fan-out loops running. The clients that meet this refusal are the ones polling through a pause, so the
// leak would grow with every retry.
//
// Deliberately not parallel: it counts fds and goroutines for the whole process, which only
// means something while no other test is running. A leak is several fds or goroutines per
// attempt, so over many attempts it stands far above the slack the assertions allow.
//
//nolint:paralleltest // counts process-wide fds and goroutines
func TestStart_RefusedStartLeaksNothing(t *testing.T) {
	u, err := user.Current()
	require.NoError(t, err)

	cwd := t.TempDir()
	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{EnvVars: utils.NewEnvVars(), User: u.Username, Workdir: &cwd}

	freezer := cgroups.NewWorkloadFreezer(cgroups.NewNoopManager())
	_, err = freezer.Freeze(t.Context(), cgroups.FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	proc := &rpc.ProcessConfig{Cmd: "echo", Args: []string{"hello"}}
	start := func() {
		// A plain start is refused in Start, after New has set up its pipes and readers.
		ctx, cancel := context.WithCancel(t.Context())
		h, err := New(ctx, u, &rpc.StartRequest{Process: proc}, &logger, defaults, freezer, cancel)
		require.NoError(t, err)

		_, err = h.Start(ctx, 0)
		require.ErrorIs(t, err, cgroups.ErrWorkloadFrozen)
		cancel()

		// A PTY start is refused inside New itself, before its readers exist.
		ctx, cancel = context.WithCancel(t.Context())
		_, err = New(ctx, u, &rpc.StartRequest{
			Process: proc,
			Pty:     &rpc.PTY{Size: &rpc.PTY_Size{Cols: 80, Rows: 24}},
		}, &logger, defaults, freezer, cancel)
		require.ErrorIs(t, err, cgroups.ErrWorkloadFrozen)
		cancel()
	}

	// One attempt first, so whatever the first call initialises once is in the baseline.
	start()
	fdsBefore, goroutinesBefore := openFDs(t), runtime.NumGoroutine()

	const attempts = 50
	for range attempts {
		start()
	}

	// Polled rather than read once: the readers exit on EOF, asynchronously to the refusal.
	// Nothing here waits on a finalizer -- every fd is closed explicitly.
	require.Eventually(t, func() bool {
		return openFDs(t) <= fdsBefore+2 && runtime.NumGoroutine() <= goroutinesBefore+2
	}, 5*time.Second, 10*time.Millisecond,
		"refused starts leaked: fds %d -> %d, goroutines %d -> %d",
		fdsBefore, openFDs(t), goroutinesBefore, runtime.NumGoroutine())
}
